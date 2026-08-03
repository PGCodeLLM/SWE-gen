from __future__ import annotations

import asyncio
import json
import os
import tarfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from harbor.environments.docker.docker import DockerEnvironment

from swegen.dashboard.distributed_status import REMOTE_BUILD_PENDING_STATUSES
from swegen.tools import suffixed_docker
from swegen.tools.remote_buildkit import (
    _ACTIVE_STATUSES,
    _TERMINAL_STATUSES,
    ORPHANED_STATUS,
    RemoteBuildkitClient,
    RemoteBuildkitConfig,
    _error_message,
    context_digest,
    create_context_archive,
    select_build_route,
    sweep_orphaned_builds,
)
from swegen.tools.suffixed_docker import SwegenDockerEnvironment


class _Response:
    def __init__(self, payload: dict[str, object], status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> dict[str, object]:
        return self.payload


class _Session:
    def __init__(self) -> None:
        self.trust_env = True
        self.posts: list[dict[str, object]] = []
        self.statuses = [
            {"status": "running", "owner_api_pod": "api-1"},
            {
                "status": "success",
                "owner_api_pod": "api-1",
                "image_tag": "registry.example/team/builds/sha256-digest:amd64",
            },
        ]

    def get(self, url: str, **kwargs: object) -> _Response:
        if url.endswith("/ready"):
            return _Response({"ready": True})
        return _Response(self.statuses.pop(0))

    def post(self, url: str, **kwargs: object) -> _Response:
        files = kwargs["files"]
        uploaded_file = files["file"][1]
        self.posts.append(
            {
                "url": url,
                "data": json.loads(kwargs["data"]["data"]),
                "upload": uploaded_file.read(),
            }
        )
        caller_request_id = kwargs["data"]["data"].split('"request_id":"', 1)[1].split('"', 1)[0]
        return _Response(
            {
                "success": True,
                "request_id": f"buildkit-worker-1:{caller_request_id}",
                "owner_api_pod": "api-1",
            }
        )


class _Tracker:
    def __init__(self) -> None:
        self.submissions: list[dict[str, object]] = []
        self.updates: list[tuple[str, dict[str, object]]] = []

    def record_submission(self, **kwargs: object) -> None:
        self.submissions.append(kwargs)

    def update(self, request_id: str, **kwargs: object) -> None:
        self.updates.append((request_id, kwargs))


def _config(**overrides: object) -> RemoteBuildkitConfig:
    values: dict[str, object] = {
        "mode": "hybrid",
        "base_url": "http://buildkit.example:32083",
        "registry_url": "registry.example",
        "repository": "team/builds",
        "callback_url": "http://callback.example/callback",
        "poll_interval_seconds": 0,
    }
    values.update(overrides)
    return RemoteBuildkitConfig(**values)


def test_context_digest_and_tar_are_deterministic(tmp_path: Path) -> None:
    environment = tmp_path / "environment"
    environment.mkdir()
    dockerfile = environment / "Dockerfile"
    dockerfile.write_text("FROM scratch\nCOPY data /data\n")
    (environment / "data").write_text("payload")
    first_digest = context_digest(environment)

    os.utime(dockerfile, (1_000_000, 1_000_000))
    assert context_digest(environment) == first_digest

    first_tar = tmp_path / "first.tar"
    second_tar = tmp_path / "second.tar"
    create_context_archive(environment, first_tar)
    create_context_archive(environment, second_tar)
    assert first_tar.read_bytes() == second_tar.read_bytes()
    with tarfile.open(first_tar) as archive:
        names = archive.getnames()
        assert names == ["environment", "environment/Dockerfile", "environment/data"]
        assert all(member.mtime == 0 for member in archive.getmembers())

    (environment / "data").write_text("changed")
    assert context_digest(environment) != first_digest


def test_context_digest_and_tar_apply_remote_only_registry_rewrite(tmp_path: Path) -> None:
    environment = tmp_path / "environment"
    environment.mkdir()
    dockerfile = environment / "Dockerfile"
    dockerfile.write_text("FROM legacy.example/team/ubuntu:24.04\n")
    rewrites = (("legacy.example", "mirror.example"),)

    original_digest = context_digest(environment)
    rewritten_digest = context_digest(
        environment,
        dockerfile_registry_rewrites=rewrites,
    )
    archive_path = tmp_path / "rewritten.tar"
    create_context_archive(
        environment,
        archive_path,
        dockerfile_registry_rewrites=rewrites,
    )

    assert rewritten_digest != original_digest
    assert dockerfile.read_text() == "FROM legacy.example/team/ubuntu:24.04\n"
    with tarfile.open(archive_path) as archive:
        archived = archive.extractfile("environment/Dockerfile")
        assert archived is not None
        assert archived.read() == b"FROM mirror.example/team/ubuntu:24.04\n"


def test_hybrid_route_is_deterministic_and_keeps_local_share() -> None:
    config = _config(remote_percent=75)

    assert select_build_route(config, "00000000" + "0" * 56) == "remote"
    assert select_build_route(config, "00000063" + "0" * 56) == "local"
    assert select_build_route(_config(mode="remote"), "f" * 64) == "remote"
    assert select_build_route(_config(mode="local_overflow"), "f" * 64) == "local_overflow"


def test_config_normalizes_build_endpoint_and_reads_proxy_safe_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SWEGEN_BUILD_ROUTER_MODE", "hybrid")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_URL", "http://farm:32083/build")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_REGISTRY", "registry/team")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_PULL_REGISTRY_URL", "registry/base")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_REPOSITORY", "builds")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_CALLBACK_URL", "http://callback")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_BASE_IMAGE_SOURCE_REGISTRY", "legacy.example")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_BASE_IMAGE_MIRROR_REGISTRY", "mirror.example")

    config = RemoteBuildkitConfig.from_env()

    assert config is not None
    assert config.base_url == "http://farm:32083"
    assert config.build_url == "http://farm:32083/build"
    assert config.pull_registry_url == "registry/base"
    assert config.dockerfile_registry_rewrites == (("legacy.example", "mirror.example"),)


def test_config_reads_repo_backend_pools(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWEGEN_BUILD_ROUTER_MODE", "remote")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_URL", "http://farm:32083")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_REGISTRY", "registry.example")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_REPOSITORY", "team/builds")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_CALLBACK_URL", "http://callback")
    monkeypatch.setenv(
        "SWEGEN_REMOTE_BUILDKIT_REPO_BACKEND_MAP",
        '{"runbox/runbox7":["buildkit-worker-46","buildkit-worker-94"]}',
    )

    config = RemoteBuildkitConfig.from_env()

    assert config is not None
    assert config.repo_backend_pools == (
        ("runbox/runbox7", ("buildkit-worker-46", "buildkit-worker-94")),
    )
    assert (
        config.preferred_backend_name(
            environment_name="runbox__runbox7-249",
            context_digest="0" * 64,
        )
        == "buildkit-worker-46"
    )
    assert (
        config.preferred_backend_name(
            environment_name="other__repo-1",
            context_digest="0" * 64,
        )
        is None
    )


def test_remote_client_bypasses_proxy_submits_tar_and_tracks_status(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "context.tar"
    archive.write_bytes(b"tar payload")
    session = _Session()
    tracker = _Tracker()
    client = RemoteBuildkitClient(_config(), session=session, sleep=lambda _: None)

    result = client.build(
        archive_path=archive,
        context_digest="a" * 64,
        environment_name="owner__repo-1",
        tracker=tracker,
        worker_id="validate-1",
        node_name="node-1",
    )

    assert session.trust_env is False
    assert session.posts[0]["url"] == "http://buildkit.example:32083/build"
    assert session.posts[0]["upload"] == b"tar payload"
    submitted = session.posts[0]["data"]
    assert submitted["dockerfile_path"] == "environment/Dockerfile"
    assert submitted["platform"] == "linux/amd64"
    assert submitted["image_tag"] == f"team/builds:sha256-{'a' * 64}"
    assert "registry_username" not in submitted
    assert "registry_password" not in submitted
    assert result.owner_api_pod == "api-1"
    assert result.request_id.startswith("buildkit-worker-1:swegen-")
    assert tracker.submissions[0]["request_id"] == result.request_id
    assert result.image_ref.endswith(f"team/builds:sha256-{'a' * 64}")
    assert tracker.submissions[0]["context_digest"] == "a" * 64
    assert [update[1]["status"] for update in tracker.updates] == [
        "queued",
        "running",
        "success",
    ]
    assert tracker.updates[-1][1]["terminal"] is True


def test_remote_client_sends_explicit_registry_credentials(tmp_path: Path) -> None:
    archive = tmp_path / "context.tar"
    archive.write_bytes(b"tar payload")
    session = _Session()
    client = RemoteBuildkitClient(
        _config(
            registry_username="push-user",
            registry_password="push-password",
            pull_registry_url="registry.example/base",
            pull_username="pull-user",
            pull_password="pull-password",
            build_args={
                "HTTP_PROXY": "http://proxy.example:8080",
                "NO_PROXY": ".example",
            },
        ),
        session=session,
        sleep=lambda _: None,
    )

    client.build(
        archive_path=archive,
        context_digest="b" * 64,
        environment_name="owner__repo-2",
        tracker=_Tracker(),
        worker_id="validate-2",
        node_name="node-2",
    )

    submitted = session.posts[0]["data"]
    assert submitted["registry_username"] == "push-user"
    assert submitted["registry_password"] == "push-password"
    assert submitted["pull_registry_url"] == "registry.example/base"
    assert submitted["pull_username"] == "pull-user"
    assert submitted["pull_password"] == "pull-password"
    assert submitted["build_args"] == {
        "HTTP_PROXY": "http://proxy.example:8080",
        "NO_PROXY": ".example",
    }


def test_remote_client_sends_optional_repo_backend_affinity(tmp_path: Path) -> None:
    archive = tmp_path / "context.tar"
    archive.write_bytes(b"tar payload")
    session = _Session()
    client = RemoteBuildkitClient(
        _config(repo_backend_pools=(("runbox/runbox7", ("buildkit-worker-46",)),)),
        session=session,
        sleep=lambda _: None,
    )

    client.build(
        archive_path=archive,
        context_digest="c" * 64,
        environment_name="runbox__runbox7-249",
        tracker=_Tracker(),
        worker_id="validate-3",
        node_name="node-3",
    )

    submitted = session.posts[0]["data"]
    assert submitted["preferred_backend_name"] == "buildkit-worker-46"
    assert submitted["preferred_backend_required"] is False
    assert submitted["callback_parameters"]["preferred_backend_name"] == ("buildkit-worker-46")


def test_config_rejects_partial_registry_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SWEGEN_BUILD_ROUTER_MODE", "remote")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_URL", "http://farm:32083")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_REGISTRY", "registry.example")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_REPOSITORY", "team/builds")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_CALLBACK_URL", "http://callback")
    monkeypatch.setenv("SWEGEN_REMOTE_BUILDKIT_REGISTRY_USERNAME", "user-only")

    with pytest.raises(ValueError, match="configured together"):
        RemoteBuildkitConfig.from_env()


def test_swegen_environment_uses_cached_remote_image(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    environment_dir = tmp_path / "environment"
    environment_dir.mkdir()
    (environment_dir / "Dockerfile").write_text("FROM scratch\n")
    environment = object.__new__(SwegenDockerEnvironment)
    environment.environment_dir = environment_dir
    environment.environment_name = "owner__repo-1"
    environment.task_env_config = SimpleNamespace(docker_image=None)
    environment._env_vars = SimpleNamespace(prebuilt_image_name=None)
    environment._use_prebuilt = False
    environment._pull_image = AsyncMock(return_value=True)
    environment._run_docker_compose_command = AsyncMock()
    environment.logger = SimpleNamespace(warning=lambda *args: None)

    monkeypatch.setattr(
        suffixed_docker.RemoteBuildkitConfig,
        "from_env",
        classmethod(lambda cls: _config(mode="remote")),
    )
    monkeypatch.setattr(suffixed_docker, "context_digest", lambda _, **kwargs: "a" * 64)

    asyncio.run(environment.start(force_build=False))

    expected_image = _config(mode="remote").image_ref("a" * 64)
    assert environment._env_vars.prebuilt_image_name == expected_image
    assert environment._use_prebuilt is True
    environment._pull_image.assert_awaited_once_with(expected_image, 900)
    environment._run_docker_compose_command.assert_awaited_once_with(["up", "-d"])


def test_local_overflow_uses_local_build_when_a_slot_is_available(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    environment_dir = tmp_path / "environment"
    environment_dir.mkdir()
    (environment_dir / "Dockerfile").write_text("FROM scratch\n")
    environment = object.__new__(SwegenDockerEnvironment)
    environment.environment_dir = environment_dir
    environment.environment_name = "owner__repo-local"
    environment.task_env_config = SimpleNamespace(docker_image=None)
    environment._pull_image = AsyncMock()
    environment.logger = SimpleNamespace(info=lambda *args: None, warning=lambda *args: None)
    observed: list[str | None] = []

    async def local_start(_self: object, _force_build: bool) -> None:
        observed.append(os.environ.get("SWEGEN_BUILD_SLOT_NONBLOCKING"))

    monkeypatch.setattr(DockerEnvironment, "start", local_start)
    monkeypatch.setattr(
        suffixed_docker.RemoteBuildkitConfig,
        "from_env",
        classmethod(lambda cls: _config(mode="local_overflow", fallback_local=False)),
    )
    monkeypatch.setattr(suffixed_docker, "context_digest", lambda _, **kwargs: "b" * 64)

    asyncio.run(environment.start(force_build=False))

    assert observed == ["1"]
    assert "SWEGEN_BUILD_SLOT_NONBLOCKING" not in os.environ
    environment._pull_image.assert_not_awaited()


def test_local_overflow_routes_to_remote_when_local_slots_are_full(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    environment_dir = tmp_path / "environment"
    environment_dir.mkdir()
    (environment_dir / "Dockerfile").write_text("FROM scratch\n")
    environment = object.__new__(SwegenDockerEnvironment)
    environment.environment_dir = environment_dir
    environment.environment_name = "owner__repo-overflow"
    environment.task_env_config = SimpleNamespace(docker_image=None)
    environment._env_vars = SimpleNamespace(prebuilt_image_name=None)
    environment._use_prebuilt = False
    environment._pull_image = AsyncMock(return_value=True)
    environment._run_docker_compose_command = AsyncMock()
    environment.logger = SimpleNamespace(info=lambda *args: None, warning=lambda *args: None)

    async def local_start(_self: object, _force_build: bool) -> None:
        raise RuntimeError(f"compose failed: {suffixed_docker.LOCAL_SLOTS_FULL_MARKER}")

    monkeypatch.setattr(DockerEnvironment, "start", local_start)
    monkeypatch.setattr(
        suffixed_docker.RemoteBuildkitConfig,
        "from_env",
        classmethod(lambda cls: _config(mode="local_overflow", fallback_local=False)),
    )
    monkeypatch.setattr(suffixed_docker, "context_digest", lambda _, **kwargs: "c" * 64)

    asyncio.run(environment.start(force_build=False))

    expected_image = _config(mode="local_overflow").image_ref("c" * 64)
    environment._pull_image.assert_awaited_once_with(expected_image, 900)
    environment._run_docker_compose_command.assert_awaited_once_with(["up", "-d"])
    assert "SWEGEN_BUILD_SLOT_NONBLOCKING" not in os.environ


def test_remote_image_pull_retries_transient_registry_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    return_codes = iter([1, 1, 0])
    commands: list[tuple[object, ...]] = []

    class FakeProcess:
        def __init__(self, returncode: int) -> None:
            self.returncode = returncode

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"", b""

        def terminate(self) -> None:
            return None

        def kill(self) -> None:
            return None

    async def create_process(*command: object, **_kwargs: object) -> FakeProcess:
        commands.append(command)
        return FakeProcess(next(return_codes))

    sleep = AsyncMock()
    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_process)
    monkeypatch.setattr(asyncio, "sleep", sleep)

    environment = object.__new__(SwegenDockerEnvironment)

    assert asyncio.run(environment._pull_image("registry.example/image:tag", 30)) is True
    assert len(commands) == 3
    assert [await_call.args for await_call in sleep.await_args_list] == [
        (suffixed_docker.REMOTE_PULL_RETRY_SECONDS,),
        (suffixed_docker.REMOTE_PULL_RETRY_SECONDS,),
    ]


class _SweepConnection:
    def __init__(self, rows: list[dict[str, object]] | None = None) -> None:
        self.rows = rows or []
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, params: object = None) -> object:
        self.calls.append((" ".join(query.split()), tuple(params or ())))
        rows = self.rows

        class _Cursor:
            def fetchall(self) -> list[dict[str, object]]:
                return list(rows)

        return _Cursor()


def test_sweep_marks_only_rows_whose_worker_is_gone() -> None:
    connection = _SweepConnection(
        [{"request_id": "farm-1:swegen-a", "worker_id": "swegen-validate-old", "status": "running"}]
    )

    swept = sweep_orphaned_builds(
        connection,
        ["swegen-validate-live", "swegen-repair-live"],
        min_age_seconds=1800,
    )

    assert swept == [
        {"request_id": "farm-1:swegen-a", "worker_id": "swegen-validate-old", "status": "running"}
    ]
    query, parameters = connection.calls[0]
    assert f"SET status = '{ORPHANED_STATUS}'" in query
    assert "status IN ('submitting', 'queued', 'running')" in query
    assert "NOT (worker_id = ANY(%s))" in query
    assert parameters[0] == 1800.0
    # Live IDs are deduplicated and sorted so the statement is stable.
    assert parameters[1] == ["swegen-repair-live", "swegen-validate-live"]


def test_sweep_refuses_an_empty_live_set() -> None:
    connection = _SweepConnection()

    with pytest.raises(ValueError, match="refusing to sweep"):
        sweep_orphaned_builds(connection, [])

    assert connection.calls == []


def test_sweep_rejects_negative_min_age() -> None:
    with pytest.raises(ValueError, match="must not be negative"):
        sweep_orphaned_builds(_SweepConnection(), ["swegen-validate-live"], min_age_seconds=-1)


def test_sweep_ignores_blank_worker_ids_in_the_live_set() -> None:
    connection = _SweepConnection()

    sweep_orphaned_builds(connection, ["swegen-validate-live", "", "swegen-validate-live"])

    assert connection.calls[0][1][1] == ["swegen-validate-live"]


def test_orphaned_status_is_not_treated_as_terminal_by_the_client() -> None:
    # The client must never write it, and the dashboard's pending set must not
    # count it; both rely on it staying outside these constants.
    assert ORPHANED_STATUS not in _TERMINAL_STATUSES
    assert ORPHANED_STATUS not in _ACTIVE_STATUSES
    assert ORPHANED_STATUS not in REMOTE_BUILD_PENDING_STATUSES


def test_error_message_keeps_the_tail_of_an_oversized_build_log() -> None:
    # The farm returns whole build logs. The failing command is at the end, so a
    # head slice would store only the base-image pull noise every build shares.
    log = ("apt progress noise " * 500) + "ERROR: failed to solve: exit code: 1"
    message = _error_message({"error_message": log})

    assert message is not None
    assert message.endswith("ERROR: failed to solve: exit code: 1")
    assert len(message) <= 4000


def test_error_message_marks_truncation() -> None:
    message = _error_message({"error_message": "x" * 9000})

    assert message is not None
    assert message.startswith("[truncated; showing final characters]")
    assert len(message) <= 4000


def test_error_message_leaves_short_text_untouched() -> None:
    assert _error_message({"error_message": "  boom  "}) == "boom"
