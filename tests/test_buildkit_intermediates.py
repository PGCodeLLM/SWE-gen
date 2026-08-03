from __future__ import annotations

import hashlib
from types import SimpleNamespace
from uuid import UUID

import pytest

from swegen.tools import buildkit_intermediates as intermediates


class FakeCursor:
    def __init__(self, *, one=None, many=None):
        self.one = one
        self.many = [] if many is None else many

    def fetchone(self):
        return self.one

    def fetchall(self):
        return self.many


class FakeConnection:
    def __init__(self, cursors):
        self.cursors = iter(cursors)
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return next(self.cursors)


def test_claim_intermediate_atomically_claims_dependency_and_build_key(monkeypatch) -> None:
    token = UUID("12345678-1234-5678-1234-567812345678")
    row = {
        "id": 7,
        "repo": "owner/repo",
        "dependency_key": "a" * 64,
        "build_key": "b" * 64,
        "status": "building",
        "claim_token": token,
    }
    connection = FakeConnection([FakeCursor(one=row)])
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_REGISTRY", "registry.example")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_REPOSITORY", "team/generated")

    claimed = intermediates.claim_intermediate(
        connection,
        repo="Owner/Repo",
        dependency_key="a" * 64,
        build_key="b" * 64,
        claim_owner="repair-1",
        cold_build_seconds=601,
        lockfile_path="Cargo.lock",
        lockfile_sha256="a" * 64,
        dockerfile_sha256="b" * 64,
        source_task_id="owner__repo-1",
        source_task_version=1,
        token_factory=lambda: token,
    )

    assert claimed["claimed"] is True
    assert claimed["suggested_image_ref"] == (
        "registry.example/team/generated:dep-owner-repo-aaaaaaaaaaaa-bbbbbbbbbbbb"
    )
    sql, params = connection.calls[0]
    assert "ON CONFLICT (repo, dependency_key, build_key) DO UPDATE" in sql
    assert "buildkit_intermediates.status = 'failed'" in sql
    assert "make_interval(secs => %s)" in sql
    assert params[0] == "owner/repo"
    assert params[6] == token


def test_claim_intermediate_reports_existing_ready_row_without_duplicate() -> None:
    existing = {
        "id": 8,
        "repo": "owner/repo",
        "dependency_key": "a" * 64,
        "build_key": "b" * 64,
        "status": "ready",
        "image_ref": "registry/image:tag",
    }
    connection = FakeConnection([FakeCursor(one=None), FakeCursor(one=existing)])

    result = intermediates.claim_intermediate(
        connection,
        repo="owner/repo",
        dependency_key="a" * 64,
        build_key="b" * 64,
        claim_owner="repair-2",
        cold_build_seconds=900,
    )

    assert result["claimed"] is False
    assert result["status"] == "ready"
    assert len(connection.calls) == 2
    assert "WHERE repo = %s AND dependency_key = %s AND build_key = %s" in connection.calls[1][0]


def test_claim_rejects_builds_that_fit_inside_farm_boundary() -> None:
    with pytest.raises(ValueError, match="must exceed"):
        intermediates.claim_intermediate(
            FakeConnection([]),
            repo="owner/repo",
            dependency_key="a" * 64,
            build_key="b" * 64,
            claim_owner="repair-3",
            cold_build_seconds=600,
        )


def test_complete_requires_configured_namespace_and_live_claim(monkeypatch) -> None:
    token = UUID("12345678-1234-5678-1234-567812345678")
    ready = {"id": 9, "status": "ready", "image_digest": f"sha256:{'c' * 64}"}
    connection = FakeConnection([FakeCursor(one=ready)])
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_REGISTRY", "registry.example")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_REPOSITORY", "team/generated")

    result = intermediates.complete_intermediate(
        connection,
        claim_token=token,
        image_ref="registry.example/team/generated:dep-owner-repo-a-b",
        image_digest=f"sha256:{'c' * 64}",
        build_seconds=720,
        cold_build_seconds=900,
    )

    assert result == ready
    assert "claim_token = %s AND status = 'building'" in connection.calls[0][0]
    with pytest.raises(ValueError, match="configured SWE-gen SWR namespace"):
        intermediates.complete_intermediate(
            FakeConnection([]),
            claim_token=token,
            image_ref="other.example/team/generated:dep",
            image_digest=f"sha256:{'c' * 64}",
            build_seconds=720,
            cold_build_seconds=900,
        )


def test_manifest_digest_hashes_raw_registry_manifest(monkeypatch) -> None:
    raw_manifest = b'{"schemaVersion":2}'
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_REGISTRY", "registry.example")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_REPOSITORY", "team/generated")
    monkeypatch.setattr(
        intermediates.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=raw_manifest),
    )

    digest = intermediates.inspect_manifest_digest(
        "registry.example/team/generated:dep-owner-repo-a-b"
    )

    assert digest == f"sha256:{hashlib.sha256(raw_manifest).hexdigest()}"


def test_list_ready_prefers_authoritatively_validated_intermediates() -> None:
    connection = FakeConnection([FakeCursor(many=[{"id": 1}, {"id": 2}])])

    rows = intermediates.list_intermediates(
        connection,
        repo="owner/repo",
        dependency_key="a" * 64,
    )

    assert rows == [{"id": 1}, {"id": 2}]
    sql, params = connection.calls[0]
    assert "metadata @>" in sql
    assert "status = 'ready'" in sql
    assert params == ("owner/repo", "a" * 64, "a" * 64)
