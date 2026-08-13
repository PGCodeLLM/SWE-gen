from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from swegen.tools import trajectory_sync

_PLATFORM_TAG = (
    "swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/"
    "swesandbox/public/swe-gen/feature-implementation/generated:owner__repo-42"
)
_TRAJECTORY_TAG = (
    "swr-coder-data-trajectory-o84wch.swr-pro.myhuaweicloud.com/"
    "aifm.coder.exp/swegen/generated:owner__repo-42"
)
_SYNC_ENVIRONMENT_NAMES = (
    "SWEGEN_SWR_HOST",
    "SWEGEN_SWR_REPOSITORY",
    "SWEGEN_SWR_REGISTRY",
    "SWEGEN_TRAJECTORY_SYNC_THREADS",
    "SWEGEN_TRAJECTORY_SYNC_INTERVAL_SECONDS",
    "SWEGEN_TRAJECTORY_SYNC_PULL_TIMEOUT_SECONDS",
    "SWEGEN_TRAJECTORY_SYNC_LIMIT",
    "http_proxy",
    "https_proxy",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "no_proxy",
    "NO_PROXY",
)

NOW = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def clear_sync_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _SYNC_ENVIRONMENT_NAMES:
        monkeypatch.delenv(name, raising=False)


class Cursor:
    def __init__(self, rows: list[dict[str, object]] | None = None, *, rowcount: int = 0) -> None:
        self.rows = rows or []
        self.rowcount = rowcount

    def fetchall(self) -> list[dict[str, object]]:
        return self.rows


class Connection:
    """Records every statement so the query shape can be asserted."""

    def __init__(self, *results: Cursor) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.transactions = 0

    def execute(self, query: str, params: tuple[object, ...] = ()) -> Cursor:
        self.calls.append((" ".join(query.split()), params))
        return self.results.pop(0) if self.results else Cursor(rowcount=1)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.transactions += 1
        yield


def stub_connection(monkeypatch: pytest.MonkeyPatch, connection: Connection) -> None:
    @contextmanager
    def fake_connection() -> Iterator[Connection]:
        yield connection

    monkeypatch.setattr(trajectory_sync.db, "connection", fake_connection)


def settings(**overrides: object) -> trajectory_sync.SyncSettings:
    return trajectory_sync.SyncSettings(threads=2, **overrides)  # type: ignore[arg-type]


# ── Registry coordinates ────────────────────────────────────────────


def test_platform_and_trajectory_tags_use_their_own_repository_paths() -> None:
    # The registries do NOT share a repository path. Copying platform's path to
    # the trajectory host would push into a repository that has never held a
    # single one of the 18,770 existing trajectory images.
    coordinates = trajectory_sync.RegistryCoordinates()

    assert coordinates.platform_tag("owner__repo-42") == _PLATFORM_TAG
    assert coordinates.trajectory_tag("owner__repo-42") == _TRAJECTORY_TAG
    assert coordinates.trajectory_suffix == ""


def test_identical_source_and_target_coordinates_are_rejected() -> None:
    with pytest.raises(ValueError, match="must differ"):
        trajectory_sync.RegistryCoordinates(
            platform_host=trajectory_sync.TRAJECTORY_SWR_HOST,
            platform_repository=trajectory_sync.TRAJECTORY_SWR_REPOSITORY,
        )


def test_settings_default_to_eight_threads_and_hourly_scans() -> None:
    configured = trajectory_sync.SyncSettings.from_environment()

    assert configured.threads == 8
    assert configured.interval_seconds == 3600
    assert configured.limit == 0


def test_settings_read_thread_and_interval_overrides_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SWEGEN_TRAJECTORY_SYNC_THREADS", "4")
    monkeypatch.setenv("SWEGEN_TRAJECTORY_SYNC_INTERVAL_SECONDS", "900")

    configured = trajectory_sync.SyncSettings.from_environment()

    assert configured.threads == 4
    assert configured.interval_seconds == 900


def test_proxy_environment_forwards_only_the_push_proxy_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # SWR is only reachable when NO_PROXY carries .myhuaweicloud.com, so the
    # docker pull must inherit exactly the variables push_action forwards.
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example:8080")
    monkeypatch.setenv("NO_PROXY", ".myhuaweicloud.com")
    monkeypatch.setenv("SWEGEN_PG_PASSWORD", "must-not-leak")

    assert trajectory_sync.proxy_environment() == {
        "HTTPS_PROXY": "http://proxy.example:8080",
        "NO_PROXY": ".myhuaweicloud.com",
    }


# ── Backlog query ───────────────────────────────────────────────────


def test_backlog_selects_pushed_platform_instances_absent_from_trajectory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = Connection(Cursor([{"instance": "owner__repo-42"}, {"instance": "owner__repo-7"}]))

    backlog = trajectory_sync.load_backlog(connection, trajectory_sync.RegistryCoordinates())

    assert backlog == ["owner__repo-42", "owner__repo-7"]
    query, params = connection.calls[0]
    assert "FROM pushed_images" in query
    assert "registry = %s AND pushed" in query
    assert "WHERE instance NOT IN (SELECT instance FROM trajectory_images)" in query
    # Keyed on registry, never on the file suffix: the two registries differ in
    # suffix today ('_platform' vs '') but that column is bookkeeping, not truth.
    assert params == ("platform", "trajectory")


def test_backlog_honours_a_limit_and_drops_blank_instances() -> None:
    connection = Connection(
        Cursor(
            [
                {"instance": "owner__repo-1"},
                {"instance": ""},
                {"instance": None},
                {"instance": "owner__repo-2"},
            ]
        )
    )

    assert trajectory_sync.load_backlog(
        connection,
        trajectory_sync.RegistryCoordinates(),
        limit=1,
    ) == ["owner__repo-1"]


# ── Ledger writes ───────────────────────────────────────────────────


def test_recording_a_push_writes_a_guarded_trajectory_row() -> None:
    connection = Connection(Cursor(rowcount=1))

    written = trajectory_sync.record_trajectory_push(
        connection,
        "owner__repo-42",
        coordinates=trajectory_sync.RegistryCoordinates(),
        now=NOW,
    )

    assert written is True
    query, params = connection.calls[0]
    assert query.startswith("INSERT INTO pushed_images")
    # Idempotent: a re-run, a second pod, or a restart must not add a duplicate
    # verified row, and pushed_images has no unique key to lean on.
    assert "WHERE NOT EXISTS" in query
    assert params[:4] == ("owner__repo-42", "trajectory", "", _TRAJECTORY_TAG)
    assert params[5:] == ("owner__repo-42", "trajectory", "")
    payload = json.loads(str(params[4]))
    assert payload["swr_url"] == _TRAJECTORY_TAG
    assert payload["pushed"] is True
    assert payload["source"] == "trajectory_sync"


def test_recording_a_push_reports_a_no_op_when_the_row_already_exists() -> None:
    connection = Connection(Cursor(rowcount=0))

    assert (
        trajectory_sync.record_trajectory_push(
            connection,
            "owner__repo-42",
            coordinates=trajectory_sync.RegistryCoordinates(),
        )
        is False
    )


# ── Per-instance copy ───────────────────────────────────────────────


def test_sync_pulls_pushes_records_and_removes_every_local_tag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = Connection(Cursor(rowcount=1))
    stub_connection(monkeypatch, connection)
    pulled: list[str] = []
    pushed: list[tuple[str, str]] = []
    removed: list[str] = []

    monkeypatch.setattr(trajectory_sync, "image_exists_in_registry", lambda tag: False)
    monkeypatch.setattr(
        trajectory_sync,
        "pull_image",
        lambda tag, timeout_seconds: bool(pulled.append(tag)) or True,
    )
    monkeypatch.setattr(
        trajectory_sync,
        "push_to_registry",
        lambda source, target, log: bool(pushed.append((source, target))) or True,
    )
    monkeypatch.setattr(trajectory_sync, "remove_local_image", removed.append)

    outcome = trajectory_sync.sync_instance("owner__repo-42", settings())

    assert outcome.status is trajectory_sync.SyncStatus.SYNCED
    assert pulled == [_PLATFORM_TAG]
    assert pushed == [(_PLATFORM_TAG, _TRAJECTORY_TAG)]
    # Hard requirement: the cluster runs near DiskPressure, so both aliases go.
    assert removed == [_PLATFORM_TAG, _TRAJECTORY_TAG]
    assert connection.calls[0][0].startswith("INSERT INTO pushed_images")
    # The ledger row is committed explicitly, as swegen.ledger_repo does.
    assert connection.transactions == 1


def test_sync_removes_local_images_even_when_the_push_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = Connection()
    stub_connection(monkeypatch, connection)
    removed: list[str] = []

    monkeypatch.setattr(trajectory_sync, "image_exists_in_registry", lambda tag: False)
    monkeypatch.setattr(trajectory_sync, "pull_image", lambda tag, timeout_seconds: True)
    monkeypatch.setattr(trajectory_sync, "push_to_registry", lambda source, target, log: False)
    monkeypatch.setattr(trajectory_sync, "remove_local_image", removed.append)

    outcome = trajectory_sync.sync_instance("owner__repo-42", settings())

    assert outcome.status is trajectory_sync.SyncStatus.FAILED
    assert removed == [_PLATFORM_TAG, _TRAJECTORY_TAG]
    # A failed push must never be recorded as a verified trajectory image.
    assert connection.calls == []


def test_sync_removes_local_images_even_when_a_helper_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    removed: list[str] = []

    def exploding_push(source: str, target: str, log: object) -> bool:
        raise RuntimeError("docker daemon unreachable")

    monkeypatch.setattr(trajectory_sync, "image_exists_in_registry", lambda tag: False)
    monkeypatch.setattr(trajectory_sync, "pull_image", lambda tag, timeout_seconds: True)
    monkeypatch.setattr(trajectory_sync, "push_to_registry", exploding_push)
    monkeypatch.setattr(trajectory_sync, "remove_local_image", removed.append)

    outcome = trajectory_sync.sync_instance("owner__repo-42", settings())

    assert outcome.status is trajectory_sync.SyncStatus.FAILED
    assert "docker daemon unreachable" in outcome.detail
    assert removed == [_PLATFORM_TAG, _TRAJECTORY_TAG]


def test_sync_skips_the_copy_when_trajectory_already_has_the_image(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = Connection(Cursor(rowcount=1))
    stub_connection(monkeypatch, connection)
    removed: list[str] = []

    monkeypatch.setattr(
        trajectory_sync, "image_exists_in_registry", lambda tag: tag == _TRAJECTORY_TAG
    )
    monkeypatch.setattr(
        trajectory_sync,
        "pull_image",
        lambda tag, timeout_seconds: pytest.fail("pull must be skipped"),
    )
    monkeypatch.setattr(
        trajectory_sync,
        "push_to_registry",
        lambda source, target, log: pytest.fail("push must be skipped"),
    )
    monkeypatch.setattr(trajectory_sync, "remove_local_image", removed.append)

    outcome = trajectory_sync.sync_instance("owner__repo-42", settings())

    assert outcome.status is trajectory_sync.SyncStatus.ALREADY_PRESENT
    # The registry and ledger disagreed; backfilling the row keeps this
    # instance out of every future backlog instead of re-checking it hourly.
    assert connection.calls[0][0].startswith("INSERT INTO pushed_images")
    assert removed == [_PLATFORM_TAG, _TRAJECTORY_TAG]


def test_a_ledger_failure_does_not_fail_a_completed_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @contextmanager
    def exploding_connection() -> Iterator[Connection]:
        raise RuntimeError("postgres is unreachable")
        yield  # pragma: no cover - unreachable

    monkeypatch.setattr(trajectory_sync.db, "connection", exploding_connection)
    monkeypatch.setattr(trajectory_sync, "image_exists_in_registry", lambda tag: False)
    monkeypatch.setattr(trajectory_sync, "pull_image", lambda tag, timeout_seconds: True)
    monkeypatch.setattr(trajectory_sync, "push_to_registry", lambda source, target, log: True)
    monkeypatch.setattr(trajectory_sync, "remove_local_image", lambda tag: None)

    outcome = trajectory_sync.sync_instance("owner__repo-42", settings())

    assert outcome.status is trajectory_sync.SyncStatus.SYNCED
    assert "ledger row was not written" in outcome.detail


# ── Cycle ───────────────────────────────────────────────────────────


def test_a_failing_instance_does_not_abort_the_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    backlog = [f"owner__repo-{index}" for index in range(6)]
    stub_connection(monkeypatch, Connection(Cursor([{"instance": i} for i in backlog])))
    attempted: list[str] = []
    lock = threading.Lock()

    def flaky_sync(instance: str, _settings: trajectory_sync.SyncSettings):
        with lock:
            attempted.append(instance)
        if instance.endswith(("1", "4")):
            return trajectory_sync.SyncOutcome(
                instance,
                trajectory_sync.SyncStatus.FAILED,
                "push to trajectory failed",
            )
        if instance.endswith("2"):
            return trajectory_sync.SyncOutcome(
                instance,
                trajectory_sync.SyncStatus.ALREADY_PRESENT,
            )
        return trajectory_sync.SyncOutcome(instance, trajectory_sync.SyncStatus.SYNCED)

    monkeypatch.setattr(trajectory_sync, "sync_instance", flaky_sync)

    summary = trajectory_sync.run_cycle(settings())

    assert sorted(attempted) == sorted(backlog)
    assert summary.attempted == 6
    assert summary.synced == 3
    assert summary.already_present == 1
    assert summary.failed == 2
    assert "failed=2" in summary.as_text()


def test_an_unexpected_exception_in_one_worker_is_counted_not_propagated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub_connection(monkeypatch, Connection(Cursor([{"instance": "owner__repo-1"}])))

    def exploding_sync(instance: str, _settings: trajectory_sync.SyncSettings):
        raise RuntimeError("thread blew up")

    monkeypatch.setattr(trajectory_sync, "sync_instance", exploding_sync)

    summary = trajectory_sync.run_cycle(settings())

    assert summary.attempted == 1
    assert summary.failed == 1


def test_an_empty_backlog_is_a_no_op(monkeypatch: pytest.MonkeyPatch) -> None:
    stub_connection(monkeypatch, Connection(Cursor([])))
    monkeypatch.setattr(
        trajectory_sync,
        "sync_instance",
        lambda instance, settings: pytest.fail("nothing to sync"),
    )

    summary = trajectory_sync.run_cycle(settings())

    assert summary.attempted == 0
    assert summary.as_text() == (
        "attempted=0 synced=0 skipped_already_present=0 failed=0 cancelled=0"
    )


def test_a_set_stop_event_prevents_the_cycle_from_starting_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # SIGTERM must not strand a local image: work that never starts cannot
    # leave one behind, and in-flight work is awaited by the pool shutdown.
    stub_connection(monkeypatch, Connection(Cursor([{"instance": "owner__repo-1"}])))
    monkeypatch.setattr(
        trajectory_sync,
        "sync_instance",
        lambda instance, settings: pytest.fail("stopped cycles must not sync"),
    )
    stop_event = threading.Event()
    stop_event.set()

    summary = trajectory_sync.run_cycle(settings(), stop_event=stop_event)

    assert summary.attempted == 0


def test_pull_reports_failure_instead_of_raising(monkeypatch: pytest.MonkeyPatch) -> None:
    def exploding_run(command, **kwargs):
        raise TimeoutError("command timed out after 1800 seconds")

    monkeypatch.setattr(trajectory_sync, "run_bounded_command", exploding_run)

    assert trajectory_sync.pull_image(_PLATFORM_TAG, timeout_seconds=1.0) is False


# ── Manifest ────────────────────────────────────────────────────────


def test_manifest_runs_one_docker_capable_pod_on_a_node_local_image() -> None:
    manifest = (
        Path(__file__).resolve().parents[1] / "deploy" / "k3s" / "swegen-trajectory-sync.yaml"
    )
    deployment = yaml.safe_load(manifest.read_text())
    pod_spec = deployment["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]
    env = {item["name"]: item.get("value") for item in container["env"]}

    assert deployment["kind"] == "Deployment"
    assert deployment["metadata"]["name"] == "swegen-trajectory-sync"
    assert deployment["metadata"]["namespace"] == "swegen-pipeline"
    # Exactly one replica: a second pod would duplicate every pull and double
    # the disk churn this job exists to bound.
    assert deployment["spec"]["replicas"] == 1
    assert deployment["spec"]["strategy"]["type"] == "Recreate"
    assert pod_spec["automountServiceAccountToken"] is False
    # imagePullPolicy=Never means the tag must already resolve on every node.
    assert container["image"] == "swegen-worker:e2e"
    assert container["imagePullPolicy"] == "Never"
    assert container["command"] == ["python", "-m", "swegen.tools.trajectory_sync"]
    assert env["SWEGEN_TRAJECTORY_SYNC_THREADS"] == "8"
    assert env["SWEGEN_TRAJECTORY_SYNC_INTERVAL_SECONDS"] == "3600"
    # SIGTERM must leave time for an in-flight multi-GB copy to run its
    # cleanup, or the pod dies holding a local image.
    assert pod_spec["terminationGracePeriodSeconds"] >= 600

    envfrom = {
        entry[key]["name"]
        for entry in container["envFrom"]
        for key in ("configMapRef", "secretRef")
        if key in entry
    }
    assert {"swegen-pipeline-config", "swegen-database", "swegen-runtime-proxy"} <= envfrom
    # NO_PROXY must carry .myhuaweicloud.com or every SWR pull/push fails.
    no_proxy = next(item for item in container["env"] if item["name"] == "NO_PROXY")
    assert no_proxy["valueFrom"]["configMapKeyRef"]["key"] == "SWEGEN_NO_PROXY"

    mounts = {mount["mountPath"] for mount in container["volumeMounts"]}
    assert "/var/run/docker.sock" in mounts
    # Anonymous pushes to SWR are 401; the credential file must be mounted.
    assert "/root/.docker/config.json" in mounts
    assert any(
        volume.get("hostPath", {}).get("path") == "/var/run/docker.sock"
        for volume in pod_spec["volumes"]
    )
