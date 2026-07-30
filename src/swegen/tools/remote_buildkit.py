"""Client and routing helpers for the shared remote BuildKit farm."""

from __future__ import annotations

import hashlib
import io
import json
import os
import stat
import tarfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote
from uuid import uuid4

import requests

_TERMINAL_FAILURE_STATUSES = {
    "cancelled",
    "failed",
    "rejected",
    "timeout",
}
_TERMINAL_STATUSES = {*_TERMINAL_FAILURE_STATUSES, "success"}
_ACTIVE_STATUSES = {"queued", "running"}
_ROUTER_MODES = {"hybrid", "local", "remote"}
_PROXY_BUILD_ARG_NAMES = (
    "http_proxy",
    "https_proxy",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "no_proxy",
    "NO_PROXY",
)


class RemoteBuildkitError(RuntimeError):
    """The remote BuildKit farm could not produce a usable image."""


class BuildTracker(Protocol):
    def record_submission(
        self,
        *,
        request_id: str,
        environment_name: str,
        worker_id: str | None,
        node_name: str | None,
        image_ref: str,
        context_digest: str,
    ) -> None: ...

    def update(
        self,
        request_id: str,
        *,
        status: str,
        owner_api_pod: str | None = None,
        error: str | None = None,
        terminal: bool = False,
    ) -> None: ...


class DatabaseBuildTracker:
    """Best-effort PostgreSQL tracking for dashboard-visible remote builds."""

    _INSERT_SQL = """
        INSERT INTO pipeline_remote_builds (
            request_id,
            environment_name,
            worker_id,
            node_name,
            route,
            status,
            image_ref,
            context_digest
        )
        VALUES (%s, %s, %s, %s, 'remote', 'submitting', %s, %s)
        ON CONFLICT (request_id) DO UPDATE SET
            environment_name = EXCLUDED.environment_name,
            worker_id = EXCLUDED.worker_id,
            node_name = EXCLUDED.node_name,
            route = EXCLUDED.route,
            status = EXCLUDED.status,
            image_ref = EXCLUDED.image_ref,
            context_digest = EXCLUDED.context_digest,
            updated_at = now(),
            finished_at = NULL,
            error = NULL
    """
    _UPDATE_SQL = """
        UPDATE pipeline_remote_builds
        SET status = %s,
            owner_api_pod = COALESCE(%s, owner_api_pod),
            updated_at = now(),
            finished_at = CASE WHEN %s THEN now() ELSE NULL END,
            error = %s
        WHERE request_id = %s
    """

    def _execute(self, sql: str, parameters: tuple[object, ...]) -> None:
        # Import lazily so using SWEgen outside the distributed workers does not
        # initialize PostgreSQL merely by importing the Harbor environment.
        from swegen.db import get_pool

        with get_pool().connection(timeout=5) as connection:
            connection.execute(sql, parameters)

    def record_submission(
        self,
        *,
        request_id: str,
        environment_name: str,
        worker_id: str | None,
        node_name: str | None,
        image_ref: str,
        context_digest: str,
    ) -> None:
        try:
            self._execute(
                self._INSERT_SQL,
                (
                    request_id,
                    environment_name,
                    worker_id,
                    node_name,
                    image_ref,
                    context_digest,
                ),
            )
        except Exception:
            # Build tracking must never make validation fail. The dashboard is
            # observational and also tolerates the table being absent during a
            # rolling upgrade.
            return

    def update(
        self,
        request_id: str,
        *,
        status: str,
        owner_api_pod: str | None = None,
        error: str | None = None,
        terminal: bool = False,
    ) -> None:
        try:
            self._execute(
                self._UPDATE_SQL,
                (status, owner_api_pod, terminal, error, request_id),
            )
        except Exception:
            return


@dataclass(frozen=True)
class RemoteBuildkitConfig:
    mode: str
    base_url: str
    registry_url: str
    repository: str
    callback_url: str
    pull_registry_url: str | None = None
    registry_username: str | None = field(default=None, repr=False)
    registry_password: str | None = field(default=None, repr=False)
    pull_username: str | None = field(default=None, repr=False)
    pull_password: str | None = field(default=None, repr=False)
    base_image_registry_source: str | None = None
    base_image_registry_mirror: str | None = None
    build_args: dict[str, str] = field(default_factory=dict, repr=False)
    remote_percent: int = 75
    build_timeout_seconds: float = 3600
    submit_timeout_seconds: float = 900
    status_timeout_seconds: float = 180
    poll_interval_seconds: float = 5
    pull_timeout_seconds: float = 900
    fallback_local: bool = True

    @classmethod
    def from_env(cls) -> RemoteBuildkitConfig | None:
        mode = os.environ.get("SWEGEN_BUILD_ROUTER_MODE", "local").strip().lower()
        if mode not in _ROUTER_MODES:
            raise ValueError("SWEGEN_BUILD_ROUTER_MODE must be one of hybrid, local, or remote")
        if mode == "local":
            return None

        raw_url = os.environ.get("SWEGEN_REMOTE_BUILDKIT_URL", "").strip()
        registry_url = os.environ.get("SWEGEN_REMOTE_BUILDKIT_REGISTRY", "").strip()
        repository = os.environ.get("SWEGEN_REMOTE_BUILDKIT_REPOSITORY", "").strip()
        callback_url = os.environ.get("SWEGEN_REMOTE_BUILDKIT_CALLBACK_URL", "").strip()
        pull_registry_url = os.environ.get(
            "SWEGEN_REMOTE_BUILDKIT_PULL_REGISTRY_URL", ""
        ).strip()
        registry_username = os.environ.get("SWEGEN_REMOTE_BUILDKIT_REGISTRY_USERNAME", "").strip()
        registry_password = os.environ.get("SWEGEN_REMOTE_BUILDKIT_REGISTRY_PASSWORD", "")
        pull_username = os.environ.get("SWEGEN_REMOTE_BUILDKIT_PULL_USERNAME", "").strip()
        pull_password = os.environ.get("SWEGEN_REMOTE_BUILDKIT_PULL_PASSWORD", "")
        base_image_registry_source = os.environ.get(
            "SWEGEN_REMOTE_BUILDKIT_BASE_IMAGE_SOURCE_REGISTRY", ""
        ).strip()
        base_image_registry_mirror = os.environ.get(
            "SWEGEN_REMOTE_BUILDKIT_BASE_IMAGE_MIRROR_REGISTRY", ""
        ).strip()
        build_args = {
            name: value
            for name in _PROXY_BUILD_ARG_NAMES
            if (value := os.environ.get(name, ""))
        }
        if not raw_url or not registry_url or not repository or not callback_url:
            raise ValueError(
                "remote BuildKit routing requires URL, registry, repository, and callback URL"
            )
        if repository.startswith("/") or ":" in repository:
            raise ValueError("SWEGEN_REMOTE_BUILDKIT_REPOSITORY must be an untagged relative path")
        _validate_credential_pair("remote registry", registry_username, registry_password)
        _validate_credential_pair("remote pull registry", pull_username, pull_password)
        if bool(base_image_registry_source) != bool(base_image_registry_mirror):
            raise ValueError(
                "remote base-image source and mirror registries must be configured together"
            )

        base_url = raw_url.rstrip("/")
        if base_url.endswith("/build"):
            base_url = base_url[: -len("/build")]
        remote_percent = int(os.environ.get("SWEGEN_REMOTE_BUILDKIT_PERCENT", "75"))
        if not 0 <= remote_percent <= 100:
            raise ValueError("SWEGEN_REMOTE_BUILDKIT_PERCENT must be between 0 and 100")

        return cls(
            mode=mode,
            base_url=base_url,
            registry_url=registry_url.rstrip("/"),
            repository=repository.strip("/"),
            callback_url=callback_url,
            pull_registry_url=pull_registry_url.rstrip("/") or None,
            registry_username=registry_username or None,
            registry_password=registry_password or None,
            pull_username=pull_username or None,
            pull_password=pull_password or None,
            base_image_registry_source=base_image_registry_source.rstrip("/") or None,
            base_image_registry_mirror=base_image_registry_mirror.rstrip("/") or None,
            build_args=build_args,
            remote_percent=remote_percent,
            build_timeout_seconds=float(
                os.environ.get("SWEGEN_REMOTE_BUILDKIT_TIMEOUT_SECONDS", "3600")
            ),
            submit_timeout_seconds=float(
                os.environ.get("SWEGEN_REMOTE_BUILDKIT_SUBMIT_TIMEOUT_SECONDS", "900")
            ),
            status_timeout_seconds=float(
                os.environ.get("SWEGEN_REMOTE_BUILDKIT_STATUS_TIMEOUT_SECONDS", "180")
            ),
            poll_interval_seconds=float(os.environ.get("SWEGEN_REMOTE_BUILDKIT_POLL_SECONDS", "5")),
            pull_timeout_seconds=float(
                os.environ.get("SWEGEN_REMOTE_BUILDKIT_PULL_TIMEOUT_SECONDS", "900")
            ),
            fallback_local=_env_bool("SWEGEN_REMOTE_BUILDKIT_FALLBACK_LOCAL", default=True),
        )

    @property
    def build_url(self) -> str:
        return f"{self.base_url}/build"

    def image_ref(self, context_digest: str) -> str:
        return f"{self.registry_url}/{self.repository}:sha256-{context_digest}"

    def image_tag(self, context_digest: str) -> str:
        return f"{self.repository}:sha256-{context_digest}"

    @property
    def dockerfile_registry_rewrites(self) -> tuple[tuple[str, str], ...]:
        if self.base_image_registry_source is None or self.base_image_registry_mirror is None:
            return ()
        return ((self.base_image_registry_source, self.base_image_registry_mirror),)


@dataclass(frozen=True)
class RemoteBuildResult:
    request_id: str
    owner_api_pod: str | None
    image_ref: str
    status_payload: dict[str, Any]


def find_successful_remote_image(
    *,
    environment_name: str,
    context_digest: str,
) -> str | None:
    """Return the latest farm-pushed image for one exact build context."""

    from swegen.db import get_pool

    try:
        with get_pool().connection(timeout=5) as connection:
            row = connection.execute(
                """
                SELECT image_ref
                FROM pipeline_remote_builds
                WHERE environment_name = %s
                  AND context_digest = %s
                  AND status = 'success'
                ORDER BY finished_at DESC NULLS LAST, updated_at DESC
                LIMIT 1
                """,
                (environment_name, context_digest),
            ).fetchone()
    except Exception:
        return None
    if row is None:
        return None
    value = row.get("image_ref") if isinstance(row, dict) else row[0]
    return _optional_string(value)


def _env_bool(name: str, *, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value")


def _validate_credential_pair(label: str, username: str, password: str) -> None:
    if bool(username) != bool(password):
        raise ValueError(f"{label} username and password must be configured together")


def context_digest(
    environment_dir: Path,
    *,
    dockerfile_registry_rewrites: tuple[tuple[str, str], ...] = (),
) -> str:
    """Hash build-relevant paths and contents without volatile timestamps."""

    root = environment_dir.resolve()
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
        relative = path.relative_to(root).as_posix()
        metadata = path.lstat()
        mode = stat.S_IMODE(metadata.st_mode)
        if path.is_symlink():
            kind = b"symlink"
        elif path.is_dir():
            kind = b"directory"
        elif path.is_file():
            kind = b"file"
        else:
            raise RemoteBuildkitError(f"unsupported build-context entry: {relative}")
        digest.update(kind)
        digest.update(b"\0")
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(f"{mode:o}".encode("ascii"))
        digest.update(b"\0")
        if path.is_symlink():
            digest.update(os.readlink(path).encode("utf-8"))
        elif path.is_file():
            transformed = _transformed_file_bytes(
                path,
                relative,
                dockerfile_registry_rewrites,
            )
            if transformed is not None:
                digest.update(transformed)
            else:
                with path.open("rb") as source:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        digest.update(chunk)
        digest.update(b"\0")
    return digest.hexdigest()


def create_context_archive(
    environment_dir: Path,
    archive_path: Path,
    *,
    dockerfile_registry_rewrites: tuple[tuple[str, str], ...] = (),
) -> None:
    """Write a deterministic tar whose build context is ``environment/``."""

    root = environment_dir.resolve()
    with tarfile.open(archive_path, mode="w", format=tarfile.PAX_FORMAT) as archive:
        root_info = tarfile.TarInfo("environment")
        root_info.type = tarfile.DIRTYPE
        root_info.mode = 0o755
        root_info.mtime = 0
        archive.addfile(root_info)

        for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
            relative = path.relative_to(root).as_posix()
            archive_name = f"environment/{relative}"
            info = archive.gettarinfo(str(path), arcname=archive_name)
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.mtime = 0
            if info.isfile():
                transformed = _transformed_file_bytes(
                    path,
                    relative,
                    dockerfile_registry_rewrites,
                )
                if transformed is not None:
                    info.size = len(transformed)
                    archive.addfile(info, io.BytesIO(transformed))
                else:
                    with path.open("rb") as source:
                        archive.addfile(info, source)
            elif info.isdir() or info.issym():
                archive.addfile(info)
            else:
                raise RemoteBuildkitError(f"unsupported build-context entry: {relative}")


def _transformed_file_bytes(
    path: Path,
    relative: str,
    registry_rewrites: tuple[tuple[str, str], ...],
) -> bytes | None:
    if relative != "Dockerfile" or not registry_rewrites:
        return None
    content = path.read_bytes()
    for source, mirror in registry_rewrites:
        content = content.replace(source.encode("utf-8"), mirror.encode("utf-8"))
    return content


def select_build_route(config: RemoteBuildkitConfig, digest: str) -> str:
    if config.mode in {"local", "remote"}:
        return config.mode
    bucket = int(digest[:8], 16) % 100
    return "remote" if bucket < config.remote_percent else "local"


class RemoteBuildkitClient:
    def __init__(
        self,
        config: RemoteBuildkitConfig,
        *,
        session: requests.Session | None = None,
        sleep: Any = time.sleep,
        monotonic: Any = time.monotonic,
    ) -> None:
        self.config = config
        self.session = session or requests.Session()
        # This explicit bypass is intentional. The farm's private NodePort
        # returns 504 when requests are sent through the corporate proxy.
        self.session.trust_env = False
        self._sleep = sleep
        self._monotonic = monotonic

    def ready(self) -> bool:
        try:
            response = self.session.get(
                f"{self.config.base_url}/ready",
                timeout=(5, min(self.config.status_timeout_seconds, 30)),
            )
            return response.status_code == 200
        except requests.RequestException:
            return False

    def build(
        self,
        *,
        archive_path: Path,
        context_digest: str,
        environment_name: str,
        tracker: BuildTracker,
        worker_id: str | None,
        node_name: str | None,
    ) -> RemoteBuildResult:
        if not self.ready():
            raise RemoteBuildkitError("remote BuildKit farm is not ready")

        request_id = f"swegen-{context_digest[:16]}-{uuid4().hex[:12]}"
        expected_image_ref = self.config.image_ref(context_digest)
        request_data = {
            "image_tag": self.config.image_tag(context_digest),
            "registry_url": self.config.registry_url,
            "request_id": request_id,
            "callback_url": self.config.callback_url,
            "callback_parameters": {
                "source": "swegen",
                "environment_name": environment_name,
                "context_digest": context_digest,
                "worker_id": worker_id or "",
                "node_name": node_name or "",
            },
            "dockerfile_path": "environment/Dockerfile",
            "platform": "linux/amd64",
        }
        optional_credentials = {
            "pull_registry_url": self.config.pull_registry_url,
            "registry_username": self.config.registry_username,
            "registry_password": self.config.registry_password,
            "pull_username": self.config.pull_username,
            "pull_password": self.config.pull_password,
            "build_args": dict(self.config.build_args) if self.config.build_args else None,
        }
        request_data.update(
            {key: value for key, value in optional_credentials.items() if value is not None}
        )

        try:
            with archive_path.open("rb") as context_file:
                response = self.session.post(
                    self.config.build_url,
                    data={"data": json.dumps(request_data, separators=(",", ":"))},
                    files={
                        "file": (
                            archive_path.name,
                            context_file,
                            "application/x-tar",
                        )
                    },
                    timeout=(10, self.config.submit_timeout_seconds),
                )
            response.raise_for_status()
            submission = response.json()
        except (requests.RequestException, ValueError) as error:
            raise RemoteBuildkitError(f"remote build submission failed: {error}") from error

        if not submission.get("success"):
            message = _error_message(submission) or "remote build submission was rejected"
            raise RemoteBuildkitError(message)

        # The deployed worker-local API namespaces caller IDs as
        # ``<owner-worker>:<caller-id>``. Persist and query the canonical ID
        # returned by the farm rather than assuming it echoes our input.
        request_id = str(submission.get("request_id") or request_id)
        owner_api_pod = _optional_string(submission.get("owner_api_pod"))
        tracker.record_submission(
            request_id=request_id,
            environment_name=environment_name,
            worker_id=worker_id,
            node_name=node_name,
            image_ref=expected_image_ref,
            context_digest=context_digest,
        )
        tracker.update(
            request_id,
            status="queued",
            owner_api_pod=owner_api_pod,
        )
        deadline = self._monotonic() + self.config.build_timeout_seconds
        last_status = "queued"
        last_status_error: str | None = None
        while self._monotonic() < deadline:
            try:
                payload = self._status(request_id, owner_api_pod)
                last_status_error = None
            except RemoteBuildkitError as error:
                # Status timeouts do not imply build failure. Keep the request
                # pending and retry until the overall build deadline.
                last_status_error = str(error)
                self._sleep(self.config.poll_interval_seconds)
                continue
            owner_api_pod = _optional_string(payload.get("owner_api_pod") or owner_api_pod)
            status_value = payload.get("status")
            if isinstance(status_value, dict):
                status_value = status_value.get("status")
            status = str(status_value or "unknown").lower()
            if status not in _ACTIVE_STATUSES and status not in _TERMINAL_STATUSES:
                self._sleep(self.config.poll_interval_seconds)
                continue
            if status != last_status:
                tracker.update(
                    request_id,
                    status=status,
                    owner_api_pod=owner_api_pod,
                    error=(
                        _error_message(payload) if status in _TERMINAL_FAILURE_STATUSES else None
                    ),
                    terminal=status in _TERMINAL_STATUSES,
                )
                last_status = status
            if status == "success":
                returned_image_ref = _optional_string(payload.get("image_tag"))
                image_ref = (
                    returned_image_ref
                    if returned_image_ref == expected_image_ref
                    else expected_image_ref
                )
                return RemoteBuildResult(
                    request_id=request_id,
                    owner_api_pod=owner_api_pod,
                    image_ref=image_ref,
                    status_payload=payload,
                )
            if status in _TERMINAL_FAILURE_STATUSES:
                message = _error_message(payload) or f"remote build ended with {status}"
                raise RemoteBuildkitError(message)
            self._sleep(self.config.poll_interval_seconds)

        message = (
            f"remote build did not finish within {self.config.build_timeout_seconds:g} seconds"
        )
        if last_status_error:
            message = f"{message}; last status error: {last_status_error}"
        tracker.update(
            request_id,
            status="client_timeout",
            owner_api_pod=owner_api_pod,
            error=message,
            terminal=True,
        )
        raise RemoteBuildkitError(message)

    def _status(self, request_id: str, owner_api_pod: str | None) -> dict[str, Any]:
        url = f"{self.config.build_url}/{quote(request_id, safe='')}/status"
        parameters = {"owner_api_pod": owner_api_pod} if owner_api_pod is not None else None
        try:
            response = self.session.get(
                url,
                params=parameters,
                timeout=(10, self.config.status_timeout_seconds),
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as error:
            raise RemoteBuildkitError(f"remote build status query failed: {error}") from error
        if not isinstance(payload, dict):
            raise RemoteBuildkitError("remote build status response is not an object")
        return payload


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _error_message(payload: dict[str, Any]) -> str | None:
    for key in ("error_message", "error", "detail", "message"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()[:4000]
    return None


__all__ = [
    "BuildTracker",
    "DatabaseBuildTracker",
    "RemoteBuildResult",
    "RemoteBuildkitClient",
    "RemoteBuildkitConfig",
    "RemoteBuildkitError",
    "context_digest",
    "create_context_archive",
    "find_successful_remote_image",
    "select_build_route",
]
