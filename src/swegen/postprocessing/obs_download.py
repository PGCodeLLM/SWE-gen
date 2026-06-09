#!/usr/bin/env python3
"""Download a repository snapshot from Huawei OBS to a local path.

The command-line interface is intentionally explicit for generated task images:
pass ``repo_ref`` and ``dst_path`` directly. ``repo_ref`` can be either a GitHub
``owner/name`` or a ready-repo identifier such as ``123__owner__name``. It is
used to build the OBS key ``{OBS_PREFIX}/{OBS_READY_REPO_ROOT}/{repo_ref}``.
Repository archives are extracted into ``dst_path`` by default, so ``/app/src``
contains the repo checkout instead of the downloaded archive file.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path


def _get_env(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


_SECRET_OBFUSCATION_MASK = b"coder-harbor-obs-download"
_OBS_AK_OBFUSCATED = "__CODER_HARBOR_OBS_AK_OBFUSCATED__"
_OBS_SK_OBFUSCATED = "__CODER_HARBOR_OBS_SK_OBFUSCATED__"


def _decode_obfuscated_secret(payload: str) -> str | None:
    value = str(payload or "").strip()
    if not value or (value.startswith("__") and value.endswith("__")):
        return None
    try:
        obfuscated = base64.b64decode(value.encode("ascii"), validate=True)
        secret = bytes(
            byte ^ _SECRET_OBFUSCATION_MASK[index % len(_SECRET_OBFUSCATION_MASK)]
            for index, byte in enumerate(obfuscated)
        )
        return secret.decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return None


def _baked_or_env_secret(payload: str, *env_names: str) -> str:
    return _decode_obfuscated_secret(payload) or _get_env(*env_names) or ""


# ``dockerfile_rewrite_obskey.py`` renders an obfuscated copy of this helper
# into generated task images before build. Raw AK/SK must not appear in this
# source file, the rewritten Dockerfile, or image-build logs.
OBS_ENDPOINT = os.environ.get("OBS_ENDPOINT", "obs.cn-southwest-2.myhuaweicloud.com")
OBS_AK = _baked_or_env_secret(_OBS_AK_OBFUSCATED, "OBS_AK")
OBS_SK = _baked_or_env_secret(_OBS_SK_OBFUSCATED, "OBS_SK")
OBS_BUCKET = os.environ.get("OBS_BUCKET", "coder-litellm-prod")
OBS_PREFIX = os.environ.get("OBS_PREFIX", "cwm-prod").strip("/")
OBS_READY_REPO_ROOT = os.environ.get(
    "OBS_READY_REPO_ROOT", "repo_commits_events/repos"
).strip("/")
SYMLINK_MANIFEST_NAME = ".__cwm_symlinks__.json"
SUPPORTED_ARCHIVE_SUFFIXES = (
    ".tar.zst",
    ".tar.gz",
    ".tgz",
    ".tar.bz2",
    ".tbz2",
    ".tar.xz",
    ".txz",
    ".tar",
    ".zip",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download a repository snapshot from OBS to a local path.",
    )
    parser.add_argument(
        "repo_full_name",
        help=(
            "Repository reference used under OBS_READY_REPO_ROOT, e.g. owner/name "
            "or 123__owner__name."
        ),
    )
    parser.add_argument(
        "dst_path",
        help="Destination directory or file path for the downloaded repository.",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Download archive objects as files instead of extracting repo contents.",
    )
    args = parser.parse_args()
    args.obs_path = _repo_obs_key_from_repo_full_name(args.repo_full_name)
    args.output_path = args.dst_path
    return args


def _normalize_obs_key(raw_path: str, bucket: str) -> str:
    value = str(raw_path or "").strip()
    if not value:
        raise ValueError("obs_path is required")

    if value.startswith("obs://"):
        without_scheme = value[len("obs://") :]
        parts = without_scheme.split("/", 1)
        uri_bucket = parts[0].strip()
        if not uri_bucket:
            raise ValueError(f"Invalid OBS URI: {raw_path}")
        if uri_bucket != bucket:
            raise ValueError(
                f"Bucket mismatch: uri bucket={uri_bucket!r}, configured bucket={bucket!r}"
            )
        value = parts[1] if len(parts) > 1 else ""

    return value.lstrip("/")


def _repo_obs_key_from_repo_full_name(repo_full_name: str) -> str:
    value = str(repo_full_name or "").strip().strip("/")
    if not value:
        raise ValueError("repo_full_name is required")
    if value.startswith("obs://") or "://" in value:
        raise ValueError("repo_full_name must be a repository name, not a URI")
    if any(part in {"", ".", ".."} for part in value.split("/")):
        raise ValueError(f"invalid repo_full_name: {repo_full_name!r}")

    path = f"{OBS_READY_REPO_ROOT}/{value}" if OBS_READY_REPO_ROOT else value
    path = path.strip("/")
    if OBS_PREFIX and path != OBS_PREFIX and not path.startswith(OBS_PREFIX + "/"):
        path = f"{OBS_PREFIX}/{path}"
    return path


def _looks_like_directory_path(raw_output: str) -> bool:
    return raw_output.endswith(os.sep) or raw_output.endswith("/")


def _resolve_file_output_path(obs_key: str, raw_output: str) -> Path:
    output = Path(raw_output)
    if output.exists() and output.is_dir():
        return output / Path(obs_key).name
    if _looks_like_directory_path(raw_output):
        return output / Path(obs_key).name
    return output


def _archive_suffix(path: str | Path) -> str | None:
    name = str(path).lower()
    for suffix in SUPPORTED_ARCHIVE_SUFFIXES:
        if name.endswith(suffix):
            return suffix
    return None


def _archive_key_candidates(obs_key: str) -> list[str]:
    if _archive_suffix(obs_key):
        return []
    base_key = obs_key.rstrip("/")
    if not base_key:
        return []
    return [base_key + suffix for suffix in SUPPORTED_ARCHIVE_SUFFIXES]


def _alternate_repo_archive_key_candidates(repo_full_name: str) -> list[str]:
    value = str(repo_full_name or "").strip().strip("/")
    if not value:
        return []
    root = OBS_READY_REPO_ROOT
    if root.endswith("/repos"):
        root = root[: -len("/repos")]
    base_key = f"{root}/{value}" if root else value
    base_key = base_key.strip("/")
    if OBS_PREFIX and base_key != OBS_PREFIX and not base_key.startswith(OBS_PREFIX + "/"):
        base_key = f"{OBS_PREFIX}/{base_key}"
    return [base_key + suffix for suffix in SUPPORTED_ARCHIVE_SUFFIXES]


def _ensure_within_directory(root: Path, candidate: Path) -> None:
    root_resolved = root.resolve(strict=False)
    candidate_resolved = candidate.resolve(strict=False)
    try:
        candidate_resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise RuntimeError(
            f"Archive member would extract outside target: {candidate}"
        ) from exc


def _validate_tar_members(tar: tarfile.TarFile, extract_root: Path) -> None:
    for member in tar.getmembers():
        member_path = extract_root / member.name
        _ensure_within_directory(extract_root, member_path)

        if member.issym() or member.islnk():
            link_name = member.linkname
            if os.path.isabs(link_name):
                raise RuntimeError(
                    f"Archive member has absolute link target: {member.name}"
                )
            if member.issym():
                link_target = member_path.parent / link_name
            else:
                link_target = extract_root / link_name
            _ensure_within_directory(extract_root, link_target)


def _extract_archive(archive_path: Path, extract_root: Path) -> None:
    suffix = _archive_suffix(archive_path)
    if suffix is None:
        raise RuntimeError(f"Unsupported archive format: {archive_path}")

    extract_root.mkdir(parents=True, exist_ok=True)
    if suffix == ".tar.zst":
        temp_root = Path(tempfile.mkdtemp(prefix="obs-tar-zst-"))
        temp_tar = temp_root / "repo.tar"
        try:
            decompressed = False
            try:
                import zstandard as zstd

                with archive_path.open("rb") as source, temp_tar.open("wb") as target:
                    zstd.ZstdDecompressor().copy_stream(source, target)
                decompressed = True
            except ModuleNotFoundError:
                try:
                    subprocess.run(
                        [
                            sys.executable,
                            "-m",
                            "pip",
                            "install",
                            "--no-cache-dir",
                            "zstandard==0.23.0",
                        ],
                        env={
                            **os.environ,
                            "PIP_INDEX_URL": os.environ.get(
                                "PIP_INDEX_URL",
                                "https://pypi.tuna.tsinghua.edu.cn/simple",
                            ),
                            "PIP_TRUSTED_HOST": os.environ.get(
                                "PIP_TRUSTED_HOST",
                                "pypi.tuna.tsinghua.edu.cn",
                            ),
                            "PIP_DEFAULT_TIMEOUT": os.environ.get("PIP_DEFAULT_TIMEOUT", "120"),
                            "PIP_RETRIES": os.environ.get("PIP_RETRIES", "5"),
                            "PIP_BREAK_SYSTEM_PACKAGES": os.environ.get(
                                "PIP_BREAK_SYSTEM_PACKAGES",
                                "1",
                            ),
                        },
                        check=True,
                    )
                    import zstandard as zstd

                    with archive_path.open("rb") as source, temp_tar.open("wb") as target:
                        zstd.ZstdDecompressor().copy_stream(source, target)
                    decompressed = True
                except (ModuleNotFoundError, subprocess.CalledProcessError):
                    decompressed = False
            if not decompressed:
                try:
                    with temp_tar.open("wb") as target:
                        subprocess.run(
                            ["zstd", "-dc", str(archive_path)],
                            stdout=target,
                            check=True,
                        )
                    decompressed = True
                except (FileNotFoundError, subprocess.CalledProcessError) as exc:
                    raise RuntimeError(
                        "Extracting .tar.zst archives requires either the Python "
                        "'zstandard' package or the 'zstd' CLI"
                    ) from exc
            if not decompressed or not temp_tar.exists():
                raise RuntimeError(f"Failed to decompress archive: {archive_path}")
            with tarfile.open(temp_tar, "r:") as tar:
                _validate_tar_members(tar, extract_root)
                tar.extractall(extract_root)
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)
        return
    if suffix == ".zip":
        with zipfile.ZipFile(archive_path) as zf:
            for info in zf.infolist():
                _ensure_within_directory(extract_root, extract_root / info.filename)
            zf.extractall(extract_root)
        return

    with tarfile.open(archive_path, "r:*") as tar:
        _validate_tar_members(tar, extract_root)
        tar.extractall(extract_root)


def _replace_path(target: Path) -> None:
    if not target.name:
        raise RuntimeError(f"Refusing to replace unsafe output path: {target}")
    if target.is_symlink() or target.is_file():
        target.unlink()
    elif target.exists():
        shutil.rmtree(target)


def _is_bare_git_dir(path: Path) -> bool:
    return path.is_dir() and (path / "HEAD").is_file() and (path / "objects").is_dir()


def _convert_bare_to_checkout(bare_dir: Path) -> Path:
    name = bare_dir.name
    if name.endswith(".git") and len(name) > len(".git"):
        name = name[: -len(".git")]

    wrapper = bare_dir.parent / f".{name}_bare2checkout"
    _replace_path(wrapper)
    wrapper.mkdir(parents=True, exist_ok=True)
    shutil.move(str(bare_dir), str(wrapper / ".git"))

    checkout_dir = bare_dir.parent / name
    _replace_path(checkout_dir)
    wrapper.rename(checkout_dir)
    subprocess.run(
        ["git", "config", "core.bare", "false"],
        cwd=str(checkout_dir),
        check=True,
        capture_output=True,
    )
    return checkout_dir


def _locate_extracted_repo(extract_root: Path) -> Path:
    if (extract_root / ".git").exists():
        return extract_root

    git_dirs = sorted(extract_root.rglob(".git"), key=lambda path: len(path.parts))
    if git_dirs:
        return git_dirs[0].parent

    for child in sorted(extract_root.iterdir()):
        if _is_bare_git_dir(child):
            return _convert_bare_to_checkout(child)
    if _is_bare_git_dir(extract_root):
        return _convert_bare_to_checkout(extract_root)

    top_level = [
        child
        for child in extract_root.iterdir()
        if child.name not in {"__MACOSX", ".DS_Store"}
    ]
    if len(top_level) == 1 and top_level[0].is_dir():
        return top_level[0]
    return extract_root


def _materialize_archive_as_repo(archive_path: Path, target_dir: Path) -> Path:
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    temp_root = Path(
        tempfile.mkdtemp(prefix=f".{target_dir.name}-extract-", dir=str(target_dir.parent))
    )
    staged_target = Path(
        tempfile.mkdtemp(prefix=f".{target_dir.name}-repo-", dir=str(target_dir.parent))
    )
    shutil.rmtree(staged_target)

    try:
        _extract_archive(archive_path, temp_root)
        repo_root = _locate_extracted_repo(temp_root)
        shutil.move(str(repo_root), str(staged_target))
        _replace_path(target_dir)
        staged_target.rename(target_dir)
        return target_dir
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
        if staged_target.exists():
            shutil.rmtree(staged_target, ignore_errors=True)


def _single_downloaded_archive(download_dir: Path) -> Path | None:
    files = [path for path in download_dir.rglob("*") if path.is_file()]
    meaningful_files = [
        path for path in files if path.name not in {SYMLINK_MANIFEST_NAME, ".DS_Store"}
    ]
    archives = [path for path in meaningful_files if _archive_suffix(path)]
    if len(meaningful_files) == 1 and len(archives) == 1:
        return archives[0]
    return None


def _validate_obs_config() -> None:
    missing = [
        name
        for name, value in {
            "OBS_ENDPOINT": OBS_ENDPOINT,
            "OBS_AK": OBS_AK,
            "OBS_SK": OBS_SK,
            "OBS_BUCKET": OBS_BUCKET,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(
            "Missing OBS configuration: "
            + ", ".join(missing)
            + (
                ". Check baked-in defaults or set the non-secret values "
                "in environment variables."
            )
        )


class ObsDownloader:
    def __init__(self) -> None:
        try:
            from obs import ObsClient
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Missing Python package 'obs'. Install the Huawei OBS Python SDK "
                "in the image before running downloads."
            ) from exc

        self.client = ObsClient(
            access_key_id=OBS_AK,
            secret_access_key=OBS_SK,
            server=OBS_ENDPOINT,
        )
        self.bucket = OBS_BUCKET

    def head_object_safe(self, obs_key: str) -> dict | None:
        resp = self.client.getObjectMetadata(self.bucket, obs_key)
        if resp.status >= 300:
            return None
        body = getattr(resp, "body", None)
        return {
            "key": obs_key,
            "size": getattr(body, "contentLength", None),
            "etag": getattr(body, "etag", None),
            "last_modified": getattr(body, "lastModified", None),
        }

    def download_object(self, obs_key: str, local_path: str) -> dict:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        resp = self.client.getObject(self.bucket, obs_key, downloadPath=local_path)
        if resp.status >= 300:
            raise RuntimeError(
                f"OBS getObject failed for {obs_key}: {resp.status} {resp.reason}"
            )
        return {"key": obs_key, "local_path": local_path}

    def prefix_has_objects(self, obs_prefix: str) -> bool:
        marker = None
        while True:
            resp = self.client.listObjects(
                self.bucket,
                prefix=obs_prefix,
                marker=marker,
                max_keys=1000,
            )
            if resp.status >= 300:
                raise RuntimeError(
                    f"OBS listObjects failed for {obs_prefix}: "
                    f"{resp.status} {resp.reason}"
                )
            contents = getattr(resp.body, "contents", []) or []
            for obj in contents:
                if not obj.key.endswith("/"):
                    return True
            if not contents or not resp.body.is_truncated:
                return False
            marker = resp.body.next_marker or contents[-1].key

    def download_directory(self, obs_prefix: str, local_dir: str) -> int:
        downloaded = 0
        marker = None
        manifest_path: str | None = None

        while True:
            resp = self.client.listObjects(
                self.bucket,
                prefix=obs_prefix,
                marker=marker,
                max_keys=1000,
            )
            if resp.status >= 300:
                raise RuntimeError(
                    f"OBS listObjects failed for {obs_prefix}: "
                    f"{resp.status} {resp.reason}"
                )

            contents = getattr(resp.body, "contents", []) or []
            if not contents:
                break

            for obj in contents:
                key = obj.key
                if key.endswith("/"):
                    continue

                rel_path = key[len(obs_prefix):].lstrip("/")
                if not rel_path:
                    continue
                if rel_path == SYMLINK_MANIFEST_NAME:
                    manifest_path = os.path.join(local_dir, rel_path)

                local_file = os.path.join(local_dir, rel_path)
                os.makedirs(os.path.dirname(local_file), exist_ok=True)

                get_resp = self.client.getObject(
                    self.bucket, key, downloadPath=local_file
                )
                if get_resp.status >= 300:
                    raise RuntimeError(
                        f"OBS getObject failed for {key}: "
                        f"{get_resp.status} {get_resp.reason}"
                    )

                downloaded += 1

            if not resp.body.is_truncated:
                break
            marker = resp.body.next_marker or contents[-1].key

        if manifest_path and os.path.exists(manifest_path):
            self._restore_symlinks_from_manifest(local_dir, manifest_path)
            os.unlink(manifest_path)

        return downloaded

    def _restore_symlinks_from_manifest(self, local_dir: str, manifest_path: str) -> None:
        payload = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        symlinks = payload.get("symlinks") if isinstance(payload, dict) else None
        if not isinstance(symlinks, list):
            return

        root = Path(local_dir)
        for item in symlinks:
            if not isinstance(item, dict):
                continue
            rel_path = item.get("path")
            target = item.get("target")
            if not isinstance(rel_path, str) or not rel_path or not isinstance(target, str):
                continue
            link_path = root / rel_path
            link_path.parent.mkdir(parents=True, exist_ok=True)
            if link_path.exists() or link_path.is_symlink():
                if link_path.is_dir() and not link_path.is_symlink():
                    os.rmdir(link_path)
                else:
                    link_path.unlink()
            os.symlink(target, link_path)


def _download_file(downloader: ObsDownloader, obs_key: str, raw_output: str) -> Path:
    local_path = _resolve_file_output_path(obs_key, raw_output)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    downloader.download_object(obs_key, str(local_path))
    return local_path


def _download_archive_as_repo(
    downloader: ObsDownloader,
    obs_key: str,
    raw_output: str,
) -> Path:
    suffix = _archive_suffix(obs_key) or ".archive"
    fd, tmp_name = tempfile.mkstemp(prefix="obs-download-", suffix=suffix)
    os.close(fd)
    tmp_path = Path(tmp_name)

    try:
        downloader.download_object(obs_key, str(tmp_path))
        return _materialize_archive_as_repo(tmp_path, Path(raw_output))
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


def _find_archive_object(
    downloader: ObsDownloader,
    obs_key: str,
    *,
    repo_full_name: str | None = None,
) -> str | None:
    for candidate in _archive_probe_candidates(obs_key, repo_full_name):
        if downloader.head_object_safe(candidate) is not None:
            return candidate
    return None


def _archive_probe_candidates(obs_key: str, repo_full_name: str | None) -> list[str]:
    primary = _archive_key_candidates(obs_key)
    alternate = _alternate_repo_archive_key_candidates(repo_full_name or "")
    candidates: list[str] = []
    for index in range(max(len(primary), len(alternate))):
        if index < len(primary):
            candidates.append(primary[index])
        if index < len(alternate):
            candidates.append(alternate[index])
    return list(dict.fromkeys(candidates))


def main() -> int:
    args = _parse_args()
    _validate_obs_config()

    downloader = ObsDownloader()
    obs_key = _normalize_obs_key(args.obs_path, downloader.bucket)
    print(f"Resolved OBS repo: obs://{downloader.bucket}/{obs_key}")
    exact_file = downloader.head_object_safe(obs_key)

    if exact_file is not None:
        if not args.raw and _archive_suffix(obs_key):
            repo_dir = _download_archive_as_repo(downloader, obs_key, args.output_path)
            print(
                f"Downloaded archive: obs://{downloader.bucket}/{obs_key} -> "
                f"{repo_dir} (extracted repo)"
            )
        else:
            local_path = _download_file(downloader, obs_key, args.output_path)
            print(
                f"Downloaded file: obs://{downloader.bucket}/{obs_key} -> {local_path}"
            )
        return 0

    archive_key = _find_archive_object(
        downloader,
        obs_key,
        repo_full_name=args.repo_full_name,
    )
    if archive_key is not None:
        if args.raw:
            local_path = _download_file(downloader, archive_key, args.output_path)
            print(
                f"Downloaded file: obs://{downloader.bucket}/{archive_key} -> "
                f"{local_path}"
            )
        else:
            repo_dir = _download_archive_as_repo(
                downloader, archive_key, args.output_path
            )
            print(
                f"Downloaded archive: obs://{downloader.bucket}/{archive_key} -> "
                f"{repo_dir} (extracted repo)"
            )
        return 0

    obs_prefix = obs_key.rstrip("/") + "/"
    if downloader.prefix_has_objects(obs_prefix):
        output_dir = Path(args.output_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        count = downloader.download_directory(obs_prefix, str(output_dir))
        archive_path = None if args.raw else _single_downloaded_archive(output_dir)
        if archive_path is not None:
            archive_name = archive_path.name
            repo_dir = _materialize_archive_as_repo(archive_path, output_dir)
            print(
                f"Downloaded directory archive: obs://{downloader.bucket}/{obs_prefix} -> "
                f"{output_dir} ({count} files, extracted {archive_name})"
            )
        else:
            print(
                f"Downloaded directory: obs://{downloader.bucket}/{obs_prefix} -> "
                f"{output_dir} ({count} files)"
            )
        return 0

    tried = _archive_probe_candidates(obs_key, args.repo_full_name)
    tried_display = ", ".join(f"obs://{downloader.bucket}/{key}" for key in tried[:12])
    raise FileNotFoundError(
        f"OBS path not found as file or directory: obs://{downloader.bucket}/{obs_key}; "
        f"archive candidates tried: {tried_display}"
    )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"obs_download.py failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
