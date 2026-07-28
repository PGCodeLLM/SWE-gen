#!/usr/bin/env python3
"""Postprocess Harbor instances for Voyager compatibility.

The script copies successfully transformed instances from an input directory to a
separate sibling directory named ``<input>_voyager_compatible``.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


REPO_MAP_FILE = "19576_repo_pr_pairs_dated.jsonl"
FAILURE_LOG = "postprocess_failures.log"
FROM_RE = re.compile(r"^FROM\s+\S+(.*)$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True)
class InstanceMetadata:
    instance_name: str
    repo_full_name: str
    pr_id: str
    base_commit: str


@dataclass(frozen=True)
class ImageResolution:
    image_ref: str | None
    details: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create Voyager-compatible copies of Harbor instances."
    )
    parser.add_argument(
        "input_dir",
        type=Path,
        help="Folder containing Harbor instance directories.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional output folder. Defaults to <input_dir>_voyager_compatible.",
    )
    parser.add_argument(
        "--repo-map",
        type=Path,
        default=Path(__file__).resolve().parent / REPO_MAP_FILE,
        help=f"JSONL mapping file with base commits. Defaults to {REPO_MAP_FILE}.",
    )
    parser.add_argument(
        "--keep-existing-output",
        action="store_true",
        help="Do not delete an existing output directory before writing.",
    )
    return parser.parse_args()


def default_output_dir(input_dir: Path) -> Path:
    return input_dir.with_name(f"{input_dir.name}_voyager_compatible")


def load_base_commits(path: Path) -> dict[tuple[str, str], str]:
    commits: dict[tuple[str, str], str] = {}
    with path.open("r", encoding="utf-8") as fh:
        for line_number, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_number}: invalid JSON: {exc}") from exc
            repo = str(payload.get("repo", ""))
            pull_number = str(payload.get("pull_number", ""))
            base_commit = str(payload.get("base_commit", ""))
            if repo and pull_number and base_commit:
                commits[(repo, pull_number)] = base_commit
    return commits


def parse_task_metadata(task_file: Path, instance_name: str) -> tuple[str, str]:
    text = task_file.read_text(encoding="utf-8")
    repo_match = re.search(r'^\s*repo_full_name\s*=\s*"([^"]+)"\s*$', text, re.M)
    pr_match = re.search(r'^\s*pr_id\s*=\s*"([^"]+)"\s*$', text, re.M)
    if repo_match and pr_match:
        return repo_match.group(1), pr_match.group(1)

    fallback = re.match(r"^(.+)__(.+)-(\d+)$", instance_name)
    if not fallback:
        raise ValueError("missing cwm_task_metadata repo_full_name/pr_id")
    owner, repo, pr_id = fallback.groups()
    return f"{owner}/{repo}", pr_id


def load_instance_metadata(
    instance_dir: Path, base_commits: dict[tuple[str, str], str]
) -> InstanceMetadata:
    repo_full_name, pr_id = parse_task_metadata(
        instance_dir / "task.toml", instance_dir.name
    )
    base_commit = base_commits.get((repo_full_name, pr_id))
    if not base_commit:
        raise ValueError(f"missing base_commit for {repo_full_name} PR {pr_id}")
    if not COMMIT_RE.match(base_commit):
        raise ValueError(f"invalid base_commit for {repo_full_name} PR {pr_id}: {base_commit}")
    return InstanceMetadata(instance_dir.name, repo_full_name, pr_id, base_commit)


def make_cwm_resolver() -> Callable[[str], ImageResolution]:
    try:
        from cwm import CwmClient
    except ImportError as exc:
        raise RuntimeError("cannot import cwm.CwmClient") from exc

    client = CwmClient(
        platform_api_base_url="http://159.138.1.45:6066",
        platform_username="Alex Yang",
        platform_password="Alex Yang",
    )
    client.__enter__()

    def resolve(repo_full_name: str) -> ImageResolution:
        payload: dict[str, Any] = client.images.resolve(
            kind="repo",
            repo_full_name=repo_full_name,
        )
        if not payload.get("pullable"):
            return ImageResolution(
                None,
                str(payload.get("message") or payload.get("status") or payload),
            )
        image_ref = payload.get("image_ref")
        if not image_ref:
            return ImageResolution(None, f"missing image_ref in payload: {payload}")
        return ImageResolution(str(image_ref), str(payload.get("status") or "pullable"))

    def close() -> None:
        client.__exit__(None, None, None)

    setattr(resolve, "close", close)
    return resolve


def path_adjustment(repo_full_name: str) -> list[str]:
    source = f"/app/{repo_full_name}"
    parent = str(Path(source).parent)
    return [
        "",
        "# Move the preloaded platform repository to the path expected by the task.",
        f"RUN mkdir -p {parent} && \\",
        f"    mv {source} /app/src && \\",
        f"    ln -s /app/src {source}",
        "",
    ]


def replace_from(lines: list[str], image_ref: str) -> list[str]:
    if not lines or not lines[0].startswith("FROM "):
        raise ValueError("Dockerfile first line is not a FROM instruction")
    suffix = FROM_RE.match(lines[0])
    lines[0] = f"FROM {image_ref}{suffix.group(1) if suffix else ''}"
    return lines


def remove_obs_block_and_insert_checkout(lines: list[str], base_commit: str) -> list[str]:
    output: list[str] = []
    inserted_checkout = False
    i = 0
    while i < len(lines):
        line = lines[i]

        if line.startswith("RUN curl -LsSf https://astral.sh/uv/install.sh"):
            i += 1
            while i < len(lines):
                current = lines[i]
                if current.startswith("WORKDIR /app/src"):
                    output.append(f"RUN cd /app/src && git checkout --detach {base_commit}")
                    output.append("")
                    inserted_checkout = True
                    break
                i += 1
            continue

        if "obs_download" in line:
            i += 1
            continue

        if re.search(r"\bgit\s+submodule\s+update\b", line):
            i += 1
            continue

        output.append(line)
        i += 1

    if not inserted_checkout:
        for idx, line in enumerate(output):
            if line.startswith("WORKDIR /app/src"):
                output.insert(idx, "")
                output.insert(idx, f"RUN cd /app/src && git checkout --detach {base_commit}")
                inserted_checkout = True
                break

    if not inserted_checkout:
        raise ValueError("could not find insertion point for git checkout")
    return output


def remove_bug_patch_application(lines: list[str]) -> list[str]:
    output: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if re.search(r"^\s*COPY\s+bug\.patch\b", line):
            i += 1
            continue
        if "bug.patch" in line and re.search(r"\bpatch\b", line):
            i += 1
            while output and output[-1].strip().startswith("#"):
                output.pop()
            continue
        output.append(line)
        i += 1
    return output


def insert_path_adjustment(lines: list[str], repo_full_name: str) -> list[str]:
    marker = "# Move the preloaded platform repository to the path expected by the task."
    if any(marker in line for line in lines):
        return lines
    return lines[:1] + path_adjustment(repo_full_name) + lines[1:]


def transform_dockerfile(text: str, image_ref: str, metadata: InstanceMetadata) -> str:
    trailing_newline = text.endswith("\n")
    lines = text.splitlines()
    lines = replace_from(lines, image_ref)
    lines = insert_path_adjustment(lines, metadata.repo_full_name)
    lines = remove_obs_block_and_insert_checkout(lines, metadata.base_commit)
    lines = remove_bug_patch_application(lines)
    rendered = "\n".join(lines)
    if trailing_newline:
        rendered += "\n"
    return rendered


def validate_dockerfile(text: str, image_ref: str, metadata: InstanceMetadata) -> list[str]:
    errors: list[str] = []
    lines = text.splitlines()
    if not lines or lines[0] != f"FROM {image_ref}":
        errors.append("first Dockerfile line does not use resolved image_ref")
    if f"mv /app/{metadata.repo_full_name} /app/src" not in text:
        errors.append("missing Voyager path adjustment mv")
    if f"ln -s /app/src /app/{metadata.repo_full_name}" not in text:
        errors.append("missing Voyager path adjustment symlink")
    if "obs_download" in text:
        errors.append("still references obs_download")
    if re.search(r"^\s*COPY\s+bug\.patch\b", text, re.M):
        errors.append("still copies bug.patch")
    if "bug.patch" in text and re.search(r"\bpatch\b", text):
        errors.append("still applies bug.patch")
    checkout = f"git checkout --detach {metadata.base_commit}"
    if checkout not in text:
        errors.append("missing base_commit checkout")
    return errors


def iter_instances(input_dir: Path) -> list[Path]:
    return sorted(
        child
        for child in input_dir.iterdir()
        if child.is_dir() and (child / "environment" / "Dockerfile").is_file()
    )


def copy_successful_instance(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)
    obs_file = dst / "environment" / "obs_download.py"
    if obs_file.exists():
        obs_file.unlink()


def process_instances(input_dir: Path, output_dir: Path, repo_map: Path) -> int:
    if not input_dir.is_dir():
        raise ValueError(f"input directory does not exist: {input_dir}")
    base_commits = load_base_commits(repo_map)
    instances = iter_instances(input_dir)
    failures: list[str] = []
    image_cache: dict[str, ImageResolution] = {}
    processed = 0

    resolver = make_cwm_resolver()
    try:
        for instance_dir in instances:
            try:
                metadata = load_instance_metadata(instance_dir, base_commits)
                if metadata.repo_full_name not in image_cache:
                    image_cache[metadata.repo_full_name] = resolver(metadata.repo_full_name)
                image_resolution = image_cache[metadata.repo_full_name]
                if not image_resolution.image_ref:
                    failures.append(
                        f"{instance_dir.name}\timage resolution failed\t{image_resolution.details}"
                    )
                    continue

                dockerfile = instance_dir / "environment" / "Dockerfile"
                transformed = transform_dockerfile(
                    dockerfile.read_text(encoding="utf-8"),
                    image_resolution.image_ref,
                    metadata,
                )
                validation_errors = validate_dockerfile(
                    transformed, image_resolution.image_ref, metadata
                )
                if validation_errors:
                    failures.append(
                        f"{instance_dir.name}\tvalidation failed\t"
                        + "; ".join(validation_errors)
                    )
                    continue

                destination = output_dir / instance_dir.name
                copy_successful_instance(instance_dir, destination)
                (destination / "environment" / "Dockerfile").write_text(
                    transformed, encoding="utf-8"
                )
                processed += 1
            except Exception as exc:
                failures.append(f"{instance_dir.name}\tpostprocess failed\t{exc}")
    finally:
        close = getattr(resolver, "close", None)
        if close:
            close()

    log_text = "\n".join(failures)
    if log_text:
        log_text += "\n"
    (output_dir / FAILURE_LOG).write_text(log_text, encoding="utf-8")

    print(f"processed={processed}")
    print(f"skipped={len(failures)}")
    print(f"output_dir={output_dir}")
    print(f"failure_log={output_dir / FAILURE_LOG}")
    return 0 if processed else 1


def main() -> int:
    args = parse_args()
    input_dir = args.input_dir.resolve()
    output_dir = (
        args.output_dir.resolve() if args.output_dir else default_output_dir(input_dir)
    )
    if output_dir == input_dir:
        raise ValueError("output directory must be separate from input directory")

    if output_dir.exists() and not args.keep_existing_output:
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    return process_instances(input_dir, output_dir, args.repo_map.resolve())


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
