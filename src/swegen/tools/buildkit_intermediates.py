"""Safe PostgreSQL registry for reusable BuildKit dependency intermediates."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from collections.abc import Mapping
from typing import Any
from uuid import UUID, uuid4

_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_DEFAULT_REGISTRY = "swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com"
_DEFAULT_REPOSITORY = "swesandbox/public/swe-gen/feature-implementation/generated"
DEFAULT_CLAIM_LEASE_SECONDS = 7200.0
DEFAULT_MANIFEST_TIMEOUT_SECONDS = 120.0

_ROW_COLUMNS = """
    id, repo, dependency_key, build_key, commit_sha, lockfile_path,
    lockfile_sha256, status, claim_token, claim_owner, image_ref,
    image_digest, source_task_id, source_task_version, build_seconds,
    cold_build_seconds, dockerfile_sha256, metadata, error, created_at,
    updated_at, ready_at, last_used_at
"""

_CLAIM_SQL = f"""
    INSERT INTO buildkit_intermediates (
        repo, dependency_key, build_key, commit_sha, lockfile_path,
        lockfile_sha256, status, claim_token, claim_owner, source_task_id,
        source_task_version, cold_build_seconds, dockerfile_sha256, metadata
    )
    VALUES (%s, %s, %s, %s, %s, %s, 'building', %s, %s, %s, %s, %s, %s, %s::jsonb)
    ON CONFLICT (repo, dependency_key, build_key) DO UPDATE SET
        commit_sha = EXCLUDED.commit_sha,
        lockfile_path = EXCLUDED.lockfile_path,
        lockfile_sha256 = EXCLUDED.lockfile_sha256,
        status = 'building',
        claim_token = EXCLUDED.claim_token,
        claim_owner = EXCLUDED.claim_owner,
        source_task_id = EXCLUDED.source_task_id,
        source_task_version = EXCLUDED.source_task_version,
        cold_build_seconds = EXCLUDED.cold_build_seconds,
        dockerfile_sha256 = EXCLUDED.dockerfile_sha256,
        metadata = buildkit_intermediates.metadata || EXCLUDED.metadata,
        image_ref = NULL,
        image_digest = NULL,
        build_seconds = NULL,
        error = NULL,
        ready_at = NULL,
        updated_at = now()
    WHERE buildkit_intermediates.status = 'failed'
       OR (
            buildkit_intermediates.status = 'building'
            AND buildkit_intermediates.updated_at
                < now() - make_interval(secs => %s)
       )
    RETURNING {_ROW_COLUMNS}
"""

_SELECT_EXACT_SQL = f"""
    SELECT {_ROW_COLUMNS}
    FROM buildkit_intermediates
    WHERE repo = %s AND dependency_key = %s AND build_key = %s
"""

_LIST_READY_SQL = f"""
    SELECT {_ROW_COLUMNS}
    FROM buildkit_intermediates
    WHERE repo = %s
      AND status = 'ready'
      AND (%s::text IS NULL OR dependency_key = %s)
    ORDER BY
        CASE WHEN metadata @> '{{"authoritative_validation":true}}'::jsonb
             THEN 0 ELSE 1 END,
        last_used_at DESC NULLS LAST,
        ready_at DESC NULLS LAST,
        id DESC
"""

_LIST_ALL_SQL = f"""
    SELECT {_ROW_COLUMNS}
    FROM buildkit_intermediates
    WHERE repo = %s
      AND (%s::text IS NULL OR dependency_key = %s)
    ORDER BY updated_at DESC, id DESC
"""

_COMPLETE_SQL = f"""
    UPDATE buildkit_intermediates
    SET status = 'ready',
        image_ref = %s,
        image_digest = %s,
        build_seconds = %s,
        cold_build_seconds = %s,
        metadata = metadata || %s::jsonb,
        error = NULL,
        ready_at = now(),
        last_used_at = now(),
        updated_at = now()
    WHERE claim_token = %s AND status = 'building'
    RETURNING {_ROW_COLUMNS}
"""

_FAIL_SQL = f"""
    UPDATE buildkit_intermediates
    SET status = 'failed', error = %s, updated_at = now()
    WHERE claim_token = %s AND status = 'building'
    RETURNING {_ROW_COLUMNS}
"""

_TOUCH_SQL = f"""
    UPDATE buildkit_intermediates
    SET last_used_at = now(), updated_at = now()
    WHERE id = %s AND status = 'ready'
    RETURNING {_ROW_COLUMNS}
"""


class BuildkitIntermediateError(RuntimeError):
    """An intermediate claim or registration is unsafe or no longer valid."""


def _row_dict(row: object | None) -> dict[str, Any] | None:
    if row is None:
        return None
    if isinstance(row, Mapping):
        return dict(row)
    raise BuildkitIntermediateError("database connection must return mapping rows")


def _canonical_repo(repo: str) -> str:
    canonical = repo.strip().lower()
    parts = canonical.split("/")
    if len(parts) != 2 or any(not part or any(char.isspace() for char in part) for part in parts):
        raise ValueError("repo must use the OWNER/REPO form")
    return canonical


def _digest(value: str, label: str) -> str:
    normalized = value.strip().lower()
    if not _DIGEST_RE.fullmatch(normalized):
        raise ValueError(f"{label} must be a lowercase 64-character SHA-256 digest")
    return normalized


def _positive_seconds(value: float, label: str) -> float:
    seconds = float(value)
    if seconds <= 0:
        raise ValueError(f"{label} must be positive")
    return seconds


def _metadata_json(metadata: Mapping[str, Any] | None) -> str:
    return json.dumps(dict(metadata or {}), sort_keys=True, separators=(",", ":"))


def expected_image_prefix() -> str:
    registry = os.environ.get("SWEGEN_REMOTE_BUILDKIT_REGISTRY", _DEFAULT_REGISTRY).strip()
    repository = os.environ.get("SWEGEN_REMOTE_BUILDKIT_REPOSITORY", _DEFAULT_REPOSITORY).strip(
        " /"
    )
    if not registry or not repository:
        raise ValueError("remote BuildKit registry and repository must not be blank")
    return f"{registry.rstrip('/')}/{repository}:"


def suggested_image_ref(repo: str, dependency_key: str, build_key: str) -> str:
    canonical = _canonical_repo(repo)
    dependency_digest = _digest(dependency_key, "dependency_key")
    build_digest = _digest(build_key, "build_key")
    slug = re.sub(r"[^a-z0-9_.-]+", "-", canonical.replace("/", "-")).strip("-.")[:48]
    return f"{expected_image_prefix()}dep-{slug}-{dependency_digest[:12]}-{build_digest[:12]}"


def list_intermediates(
    connection: Any,
    *,
    repo: str,
    dependency_key: str | None = None,
    ready_only: bool = True,
) -> list[dict[str, Any]]:
    canonical = _canonical_repo(repo)
    normalized_key = _digest(dependency_key, "dependency_key") if dependency_key else None
    sql = _LIST_READY_SQL if ready_only else _LIST_ALL_SQL
    rows = connection.execute(sql, (canonical, normalized_key, normalized_key)).fetchall()
    return [dict(row) for row in rows]


def claim_intermediate(
    connection: Any,
    *,
    repo: str,
    dependency_key: str,
    build_key: str,
    claim_owner: str,
    cold_build_seconds: float,
    commit_sha: str | None = None,
    lockfile_path: str | None = None,
    lockfile_sha256: str | None = None,
    dockerfile_sha256: str | None = None,
    source_task_id: str | None = None,
    source_task_version: int | None = None,
    metadata: Mapping[str, Any] | None = None,
    lease_seconds: float = DEFAULT_CLAIM_LEASE_SECONDS,
    token_factory: Any = uuid4,
) -> dict[str, Any]:
    canonical = _canonical_repo(repo)
    dependency_digest = _digest(dependency_key, "dependency_key")
    build_digest = _digest(build_key, "build_key")
    owner = claim_owner.strip()
    if not owner:
        raise ValueError("claim_owner must not be blank")
    cold_seconds = _positive_seconds(cold_build_seconds, "cold_build_seconds")
    if cold_seconds <= 600:
        raise ValueError("cold_build_seconds must exceed the farm's 600-second boundary")
    lease = _positive_seconds(lease_seconds, "lease_seconds")
    lock_digest = _digest(lockfile_sha256, "lockfile_sha256") if lockfile_sha256 else None
    dockerfile_digest = (
        _digest(dockerfile_sha256, "dockerfile_sha256") if dockerfile_sha256 else None
    )
    if source_task_version is not None and source_task_version <= 0:
        raise ValueError("source_task_version must be positive")
    claim_token = token_factory()
    if not isinstance(claim_token, UUID):
        claim_token = UUID(str(claim_token))
    row = _row_dict(
        connection.execute(
            _CLAIM_SQL,
            (
                canonical,
                dependency_digest,
                build_digest,
                commit_sha.strip() if commit_sha else None,
                lockfile_path.strip() if lockfile_path else None,
                lock_digest,
                claim_token,
                owner,
                source_task_id.strip() if source_task_id else None,
                source_task_version,
                cold_seconds,
                dockerfile_digest,
                _metadata_json(metadata),
                lease,
            ),
        ).fetchone()
    )
    claimed = row is not None
    if row is None:
        row = _row_dict(
            connection.execute(
                _SELECT_EXACT_SQL,
                (canonical, dependency_digest, build_digest),
            ).fetchone()
        )
    if row is None:
        raise BuildkitIntermediateError("intermediate claim disappeared after conflict")
    row["claimed"] = claimed
    row["suggested_image_ref"] = suggested_image_ref(canonical, dependency_digest, build_digest)
    return row


def complete_intermediate(
    connection: Any,
    *,
    claim_token: UUID | str,
    image_ref: str,
    image_digest: str,
    build_seconds: float,
    cold_build_seconds: float,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    reference = image_ref.strip()
    if not reference.startswith(expected_image_prefix()) or "@" in reference:
        raise ValueError("image_ref must be a tagged image in the configured SWE-gen SWR namespace")
    digest = image_digest.strip().lower()
    if not _IMAGE_DIGEST_RE.fullmatch(digest):
        raise ValueError("image_digest must use sha256:<64 lowercase hex characters>")
    actual_seconds = _positive_seconds(build_seconds, "build_seconds")
    cold_seconds = _positive_seconds(cold_build_seconds, "cold_build_seconds")
    if cold_seconds <= 600:
        raise ValueError("cold_build_seconds must exceed the farm's 600-second boundary")
    token = UUID(str(claim_token))
    row = _row_dict(
        connection.execute(
            _COMPLETE_SQL,
            (
                reference,
                digest,
                actual_seconds,
                cold_seconds,
                _metadata_json(metadata),
                token,
            ),
        ).fetchone()
    )
    if row is None:
        raise BuildkitIntermediateError("claim is absent, expired, or already completed")
    return row


def fail_intermediate(
    connection: Any,
    *,
    claim_token: UUID | str,
    error: str,
) -> dict[str, Any]:
    reason = " ".join(error.split())[:4000]
    if not reason:
        raise ValueError("error must not be blank")
    row = _row_dict(connection.execute(_FAIL_SQL, (reason, UUID(str(claim_token)))).fetchone())
    if row is None:
        raise BuildkitIntermediateError("claim is absent, expired, or already completed")
    return row


def touch_intermediate(connection: Any, *, intermediate_id: int) -> dict[str, Any]:
    if intermediate_id <= 0:
        raise ValueError("intermediate_id must be positive")
    row = _row_dict(connection.execute(_TOUCH_SQL, (intermediate_id,)).fetchone())
    if row is None:
        raise BuildkitIntermediateError("ready intermediate was not found")
    return row


def inspect_manifest_digest(
    image_ref: str,
    *,
    timeout_seconds: float = DEFAULT_MANIFEST_TIMEOUT_SECONDS,
) -> str:
    """Resolve an immutable registry digest without exposing Docker credentials."""

    reference = image_ref.strip()
    if not reference.startswith(expected_image_prefix()) or "@" in reference:
        raise ValueError("image_ref must be a tagged image in the configured SWE-gen SWR namespace")
    completed = subprocess.run(
        ["docker", "buildx", "imagetools", "inspect", "--raw", reference],
        check=False,
        capture_output=True,
        timeout=_positive_seconds(timeout_seconds, "timeout_seconds"),
    )
    if completed.returncode != 0 or not completed.stdout:
        raise BuildkitIntermediateError(
            f"registry manifest verification failed with status {completed.returncode}"
        )
    return f"sha256:{hashlib.sha256(completed.stdout).hexdigest()}"


__all__ = [
    "BuildkitIntermediateError",
    "DEFAULT_CLAIM_LEASE_SECONDS",
    "claim_intermediate",
    "complete_intermediate",
    "expected_image_prefix",
    "fail_intermediate",
    "inspect_manifest_digest",
    "list_intermediates",
    "suggested_image_ref",
    "touch_intermediate",
]
