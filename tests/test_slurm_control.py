import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

import slurm_control as control

NODES = ("node-1", "node-2", "node-3", "node-4")
MODEL_ENDPOINTS = ("https://models.example/v1",)
DEFAULT_TEST_MODELS = {
    "opus": control.DEFAULT_OPUS_MODEL,
    "sonnet": control.DEFAULT_SONNET_MODEL,
}


def model_catalog_text(models, endpoints=MODEL_ENDPOINTS):
    names = list(dict.fromkeys(models.values()))
    return yaml.safe_dump(
        {
            "model_list": [
                {
                    "model_name": name,
                    "litellm_params": {
                        "api_base": endpoint,
                        "api_key": "test-private-key",
                    },
                }
                for endpoint in endpoints
                for name in names
            ]
        },
        sort_keys=False,
    )


MODEL_CATALOG_TEXT = model_catalog_text(DEFAULT_TEST_MODELS)
MODEL_CATALOG_SHA256 = hashlib.sha256(MODEL_CATALOG_TEXT.encode()).hexdigest()


class FakeSlurmClient:
    def __init__(self, states: dict[str, str]):
        self.states = dict(states)
        self.suspended: list[str] = []
        self.resumed: list[str] = []
        self.held: list[str] = []
        self.released: list[str] = []
        self.cancelled: list[str] = []

    def job_state(self, job_id: str | None) -> str:
        return "NOT_SUBMITTED" if job_id is None else self.states.get(job_id, "UNKNOWN")

    def suspend(self, job_ids):
        self.suspended.extend(job_ids)
        for job_id in job_ids:
            self.states[job_id] = "SUSPENDED"

    def resume(self, job_ids):
        self.resumed.extend(job_ids)
        for job_id in job_ids:
            self.states[job_id] = "RUNNING"

    def hold(self, job_ids):
        self.held.extend(job_ids)

    def release(self, job_ids):
        self.released.extend(job_ids)

    def cancel(self, job_ids):
        self.cancelled.extend(job_ids)
        for job_id in job_ids:
            self.states[job_id] = "CANCELLED"


class FakeServiceManager(control.ServiceManager):
    def __init__(self):
        self.stopped: list[str] = []
        self.started: list[str] = []
        self._active: set[str] = set(control.STAGE_WORKER_SERVICES)

    def stop(self, services):
        stopped = [service for service in services if service in self._active]
        self.stopped.extend(stopped)
        self._active -= set(stopped)
        return stopped

    def start(self, services):
        started = [service for service in services if service not in self._active]
        self.started.extend(started)
        self._active |= set(started)
        return started

    def is_active(self, service):
        return service in self._active


def write_plan(
    path: Path,
    routes=None,
    job_ids=None,
    models=None,
    *,
    legacy_models=False,
    profile_roles_only=False,
    staged=False,
    preflight=True,
    model_hash=None,
    endpoints=None,
) -> None:
    routes = routes or {"sg": 4, "hk": 4, "de": 4}
    job_ids = job_ids if job_ids is not None else ["101", "102", "103", "104"]
    models = models or {
        "opus": control.DEFAULT_OPUS_MODEL,
        "sonnet": control.DEFAULT_SONNET_MODEL,
    }
    models_yaml = next(
        (parent / "models.yaml" for parent in path.parents if (parent / "models.yaml").is_file()),
        None,
    )
    if model_hash is None:
        model_hash = (
            control.model_config_sha256(models_yaml)
            if models_yaml is not None
            else MODEL_CATALOG_SHA256
        )
    if endpoints is None:
        endpoints = (
            tuple(control.model_config_endpoints(models_yaml, set(models.values())))
            if models_yaml is not None
            else MODEL_ENDPOINTS
        )
    model_backend = {
        "models": sorted(set(models.values())),
        "config_sha256": model_hash,
        "endpoints": list(endpoints),
        "profiles": [
            {
                "name": f"backend-{index:03d}",
                "api_base": endpoint,
                "models": sorted(set(models.values())),
                "roles": models,
            }
            for index, endpoint in enumerate(endpoints)
        ],
    }
    if not legacy_models and not profile_roles_only:
        model_backend["roles"] = models
    node_records = []
    for index, (node, job_id) in enumerate(zip(NODES, job_ids, strict=True), start=1):
        record = {
            "node": node,
            "index": index,
            "job_id": job_id,
            "remote_run_dir": f"/remote/{node}/run",
            "expected_workers": sum(routes.values()),
            "shards": [
                f"r9-{route}-n{index}-{suffix}"
                for route, workers in routes.items()
                for suffix in (["a"] if 0 < workers <= 4 else ["a", "b"] if workers > 4 else [])
            ],
        }
        if staged:
            record.update(
                {
                    "stage": "ready",
                    "staged_at": "2026-07-22T06:20:00+00:00",
                    "preflight": (
                        ["models=200 count_tokens=200 messages=200"] if preflight else []
                    ),
                }
            )
        node_records.append(record)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "revision": "r9",
                "action": "stage" if staged else "submit",
                "expected_workers": 4 * sum(routes.values()),
                "proxy_workers_per_node": routes,
                "model_backend": model_backend,
                "nodes": node_records,
            }
        )
        + "\n"
    )


def write_config(
    tmp_path: Path,
    *,
    desired="running",
    routes=None,
    models=None,
    threshold=10,
    include_models=True,
):
    workspace = tmp_path / "workspace"
    run_dir = workspace / "runs" / "run"
    run_dir.mkdir(parents=True)
    plan_path = run_dir / "slurm-stage1-r9-4n-plan.json"
    routes = routes or {"sg": 4, "hk": 4, "de": 4}
    config = {
        "version": 1,
        "desired_state": desired,
        "run": {
            "name": "run",
            "dir": str(run_dir),
            "input_jsonl": str(workspace / "input.jsonl"),
            "models_yaml": str(workspace / "models.yaml"),
            "plan_path": str(plan_path),
            "revision": "r9",
        },
        "slurm": {"node_count": 4, "routes": routes},
        "controller": {
            "poll_interval_seconds": 15,
            "status_path": str(run_dir / ".slurm-control" / "status.json"),
        },
        "circuit_breaker": {
            "enabled": True,
            "window_seconds": 300,
            "failure_threshold": threshold,
            "cooldown_seconds": 900,
        },
        "metadata": {
            "updated_at": "2026-07-22T06:00:00+00:00",
            "updated_by": "test",
        },
    }
    models = models or DEFAULT_TEST_MODELS
    if include_models:
        config["models"] = models
    config_path = tmp_path / "swegen-config.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False))
    (workspace / "models.yaml").write_text(model_catalog_text(models))
    write_plan(plan_path)
    return config_path, workspace, run_dir, plan_path


def reconciler(config_path, workspace, client, now, **kwargs):
    def default_health(plan_path, _window_seconds):
        plan = control.load_plan(plan_path)
        nodes = []
        for record in plan["nodes"]:
            job_id = str(record["job_id"]) if record.get("job_id") else None
            state = client.job_state(job_id)
            workers = (
                int(record.get("expected_workers", 0)) if state in control.RUNNING_STATES else 0
            )
            nodes.append(
                {
                    "node": record["node"],
                    "job_id": job_id,
                    "state": state,
                    "collected": state in control.RUNNING_STATES,
                    "active_worker_processes": workers,
                    "orchestrator_processes": len(record.get("shards", [])),
                    "recent_api_failure_events": 0,
                }
            )
        return {
            "active_workers": sum(item["active_worker_processes"] for item in nodes),
            "expected_workers": plan["expected_workers"],
            "nodes": nodes,
        }

    return control.Reconciler(
        config_path,
        workspace,
        client=client,
        now_fn=lambda: now,
        collect_action=kwargs.get("collect_action", lambda _path: {}),
        health_action=kwargs.get("health_action", default_health),
        launch_action=kwargs.get("launch_action"),
        sleep_fn=lambda _seconds: None,
        service_manager=kwargs.get("service_manager"),
    )


def test_config_accepts_each_route_concurrency_from_zero_to_eight(tmp_path) -> None:
    config_path, _workspace, _run_dir, _plan = write_config(
        tmp_path, desired="paused", routes={"sg": 0, "hk": 6, "de": 8}
    )

    loaded = control.load_config(config_path)

    assert loaded.slurm.routes == {"sg": 0, "hk": 6, "de": 8}
    assert loaded.slurm.total_workers == 56


def test_running_config_rejects_all_zero_routes(tmp_path) -> None:
    config_path, _workspace, _run_dir, _plan = write_config(
        tmp_path, routes={"sg": 0, "hk": 0, "de": 0}
    )

    with pytest.raises(control.ConfigError, match="positive concurrency"):
        control.load_config(config_path)


def test_pause_commands_require_root_controller_privileges(monkeypatch) -> None:
    monkeypatch.setattr(control.os, "geteuid", lambda: 1000)
    client = control.SlurmClient()

    with pytest.raises(control.ReconcileError, match="run as root"):
        client.suspend(["123"])


def test_root_controller_runs_scontrol_without_dropping_privileges(monkeypatch) -> None:
    calls = []

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        return control.subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(control.os, "geteuid", lambda: 0)
    monkeypatch.setattr(control.subprocess, "run", fake_run)
    client = control.SlurmClient()

    client.suspend(["123"])

    assert calls[0][0] == ["scontrol", "suspend", "123"]


def test_config_loads_explicit_role_models_and_defaults_legacy_documents(tmp_path) -> None:
    config_path, _workspace, _run_dir, _plan = write_config(
        tmp_path,
        models={"opus": "custom-opus", "sonnet": "custom-sonnet"},
    )

    assert control.load_config(config_path).models.as_dict() == {
        "opus": "custom-opus",
        "sonnet": "custom-sonnet",
    }

    legacy_path, _workspace, _run_dir, _plan = write_config(
        tmp_path / "legacy",
        include_models=False,
    )
    assert control.load_config(legacy_path).models.as_dict() == {
        "opus": control.DEFAULT_OPUS_MODEL,
        "sonnet": control.DEFAULT_SONNET_MODEL,
    }


def test_config_rejects_empty_role_model(tmp_path) -> None:
    config_path, _workspace, _run_dir, _plan = write_config(
        tmp_path,
        models={"opus": "", "sonnet": "custom-sonnet"},
    )

    with pytest.raises(control.ConfigError, match=r"models\.opus"):
        control.load_config(config_path)


def test_controller_launcher_preflights_stage_and_skips_validated_submit(tmp_path) -> None:
    config_path, workspace, run_dir, _plan_path = write_config(
        tmp_path,
        models={"opus": "custom-opus", "sonnet": "custom-sonnet"},
    )
    calls: list[list[str]] = []

    class LaunchClient:
        def run_privileged(self, argv, **_kwargs):
            calls.append(list(argv))

    instance = control.Reconciler(config_path, workspace, client=LaunchClient())
    candidate = run_dir / ".slurm-control" / "candidate.json"

    config = control.load_config(config_path)
    assert instance._launch(config, "stage", candidate) == candidate
    assert instance._launch(config, "submit", None) == config.run.plan_path.resolve()

    stage_argv, submit_argv = calls
    assert stage_argv[stage_argv.index("--opus-model") + 1] == "custom-opus"
    assert stage_argv[stage_argv.index("--sonnet-model") + 1] == "custom-sonnet"
    assert stage_argv[stage_argv.index("--stage-transport") + 1] == "ssh"
    assert stage_argv[stage_argv.index("--remote-root") + 1] == str(
        control.DEFAULT_CANDIDATE_REMOTE_ROOT
    )
    assert "--skip-preflight" not in stage_argv
    assert "--remote-root" not in submit_argv
    assert submit_argv[-1] == "--skip-preflight"


def test_api_failure_counter_is_global_but_revision_and_reason_scoped(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    run_dir = tmp_path / "run"
    for index, node in enumerate(NODES[:2]):
        path = (
            run_dir / "slurm-nodes" / node / f"orchestrator-instance-status-sg-n{index}-a-r9.jsonl"
        )
        path.parent.mkdir(parents=True)
        records = [
            {
                "timestamp": control.now_iso(now - timedelta(seconds=30 + index)),
                "status": "failure",
                "failure_reason": "Transient network/API error",
                "instance": f"task-{index}",
                "slurm_node": node,
            },
            {
                "timestamp": control.now_iso(now - timedelta(seconds=20)),
                "status": "failure",
                "failure_reason": "Validation failed (NOP or Oracle)",
            },
            {
                "timestamp": control.now_iso(now - timedelta(seconds=400)),
                "status": "failure",
                "failure_reason": "Transient network/API error",
            },
        ]
        path.write_text("".join(json.dumps(record) + "\n" for record in records))
    old_revision = run_dir / "orchestrator-instance-status-sg-a-r8.jsonl"
    old_revision.write_text(
        json.dumps(
            {
                "timestamp": control.now_iso(now),
                "status": "failure",
                "failure_reason": "Transient network/API error",
            }
        )
        + "\n"
    )

    count, recent = control.api_failures_in_window(run_dir, "r9", window_seconds=300, now=now)

    assert count == 2
    assert {item["node"] for item in recent} == set(NODES[:2])


def test_api_failure_counter_uses_only_current_plan_shards_without_duplicate_copies(
    tmp_path,
) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    run_dir = tmp_path / "run"
    plan_path = run_dir / "plan.json"
    write_plan(plan_path, {"sg": 4, "hk": 0, "de": 0})
    plan = control.load_plan(plan_path)
    record = {
        "timestamp": control.now_iso(now),
        "status": "failure",
        "failure_reason": "Transient network/API error",
        "instance": "active-task",
        "slurm_group": "r9-sg-n1-a",
        "worker_id": 1,
    }
    node_dir = run_dir / "slurm-nodes" / NODES[0]
    node_dir.mkdir(parents=True)
    active_name = "orchestrator-instance-status-sg-n1-a-r9.jsonl"
    (node_dir / active_name).write_text(json.dumps(record) + "\n")
    (run_dir / active_name).write_text(json.dumps(record) + "\n")
    inactive = node_dir / "orchestrator-instance-status-sg-n1-b-r9.jsonl"
    inactive.write_text(
        json.dumps(
            {
                **record,
                "instance": "inactive-task",
                "slurm_group": "r9-sg-n1-b",
            }
        )
        + "\n"
    )

    count, recent = control.api_failures_in_window(
        run_dir, "r9", window_seconds=300, now=now, plan=plan
    )

    assert count == 1
    assert recent[0]["instance"] == "active-task"


def test_pause_and_resume_use_scontrol_semantics(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    config_path, workspace, _run_dir, _plan = write_config(tmp_path, desired="paused")
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})

    paused = reconciler(config_path, workspace, client, now).reconcile_once()

    assert paused["controller"]["last_action"] == "paused"
    assert paused["controller"]["applied_state"] == "paused"
    assert client.suspended == ["101", "102", "103", "104"]

    value = yaml.safe_load(config_path.read_text())
    value["desired_state"] = "running"
    value["metadata"] = {
        "updated_at": control.now_iso(now + timedelta(seconds=1)),
        "updated_by": "dashboard",
    }
    control.atomic_yaml(config_path, value)
    resumed = reconciler(
        config_path, workspace, client, now + timedelta(seconds=2)
    ).reconcile_once()

    assert resumed["controller"]["last_action"] == "resumed"
    assert client.resumed == ["101", "102", "103", "104"]

    settled = reconciler(
        config_path, workspace, client, now + timedelta(seconds=3)
    ).reconcile_once()
    assert settled["slurm"]["active_workers"] == 48


def test_circuit_breaker_latches_paused_until_explicit_dashboard_resume(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    config_path, workspace, run_dir, _plan = write_config(tmp_path, threshold=2)
    status_path = (
        run_dir / "slurm-nodes" / NODES[0] / "orchestrator-instance-status-sg-n1-a-r9.jsonl"
    )
    status_path.parent.mkdir(parents=True)
    status_path.write_text(
        "".join(
            json.dumps(
                {
                    "timestamp": control.now_iso(now - timedelta(seconds=index)),
                    "status": "failure",
                    "failure_reason": "Transient network/API error",
                    "instance": f"task-{index}",
                }
            )
            + "\n"
            for index in (1, 2)
        )
    )
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})

    tripped = reconciler(config_path, workspace, client, now).reconcile_once()

    assert tripped["controller"]["last_action"] == "circuit_breaker_tripped"
    assert tripped["circuit_breaker"]["tripped"] is True
    assert yaml.safe_load(config_path.read_text())["desired_state"] == "paused"
    assert client.suspended == ["101", "102", "103", "104"]

    config = yaml.safe_load(config_path.read_text())
    config["desired_state"] = "running"
    config["metadata"] = {
        "updated_at": control.now_iso(now + timedelta(seconds=1)),
        "updated_by": "dashboard",
    }
    control.atomic_yaml(config_path, config)
    still_open = reconciler(
        config_path, workspace, client, now + timedelta(seconds=2)
    ).reconcile_once()

    assert still_open["circuit_breaker"]["tripped"] is True
    assert still_open["circuit_breaker"]["explicit_reset_required"] is True
    assert client.resumed == []

    config = yaml.safe_load(config_path.read_text())
    config["metadata"]["circuit_breaker_reset_requested_at"] = control.now_iso(
        now + timedelta(seconds=3)
    )
    control.atomic_yaml(config_path, config)
    reset = reconciler(
        config_path, workspace, client, now + timedelta(seconds=4)
    ).reconcile_once()

    assert reset["circuit_breaker"]["tripped"] is False
    assert reset["circuit_breaker"]["suppressed_until"] is not None
    assert reset["controller"]["last_action"] == "resumed"
    assert client.resumed == ["101", "102", "103", "104"]


def test_circuit_breaker_ignores_a_reset_marker_older_than_the_trip(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    config_path, workspace, _run_dir, _plan = write_config(tmp_path)
    config = yaml.safe_load(config_path.read_text())
    config["metadata"]["circuit_breaker_reset_requested_at"] = control.now_iso(now)
    control.atomic_yaml(config_path, config)
    instance = reconciler(config_path, workspace, FakeSlurmClient({}), now)

    assert (
        instance._explicit_breaker_reset(
            control.load_config(config_path),
            {
                "tripped": True,
                "tripped_at": control.now_iso(now + timedelta(seconds=1)),
            },
        )
        is False
    )


def test_breaker_reset_is_published_before_a_slow_topology_restart(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    config_path, workspace, run_dir, _plan = write_config(
        tmp_path,
        routes={"sg": 2, "hk": 2, "de": 2},
    )
    config = yaml.safe_load(config_path.read_text())
    config["metadata"]["circuit_breaker_reset_requested_at"] = control.now_iso(now)
    control.atomic_yaml(config_path, config)
    status_path = run_dir / ".slurm-control" / "status.json"
    status_path.parent.mkdir(parents=True)
    control.atomic_json(
        status_path,
        {
            "timestamp": control.now_iso(now - timedelta(seconds=1)),
            "controller": {"state": "running", "applied_state": "paused"},
            "circuit_breaker": {
                "tripped": True,
                "tripped_at": control.now_iso(now - timedelta(seconds=1)),
            },
        },
    )
    client = FakeSlurmClient({str(101 + index): "SUSPENDED" for index in range(4)})
    published: list[dict] = []

    def launch_action(_config, _action, _plan_path):
        published.append(json.loads(status_path.read_text()))
        raise control.ReconcileError("staging intentionally blocked")

    result = reconciler(
        config_path,
        workspace,
        client,
        now,
        launch_action=launch_action,
    ).reconcile_once()

    assert published[0]["controller"]["state"] == "reconciling"
    assert published[0]["controller"]["last_action"] == "circuit_breaker_reset"
    assert published[0]["circuit_breaker"]["tripped"] is False
    assert published[0]["circuit_breaker"]["explicit_reset_required"] is False
    assert result["controller"]["last_action"] == "error"
    assert result["circuit_breaker"]["tripped"] is False


def test_concurrency_change_collects_cancels_and_reuses_fixed_plan(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    desired_routes = {"sg": 2, "hk": 6, "de": 0}
    config_path, workspace, _run_dir, fixed_plan = write_config(tmp_path, routes=desired_routes)
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})
    collections: list[Path] = []
    launches: list[tuple[str, Path | None]] = []

    def collect_action(path):
        collections.append(path)
        return {}

    def launch_action(config, action, plan_path):
        launches.append((action, plan_path))
        destination = plan_path or fixed_plan
        new_ids = [None] * 4 if action == "stage" else ["201", "202", "203", "204"]
        write_plan(destination, desired_routes, new_ids, staged=action == "stage")
        if action == "submit":
            client.states.update(dict.fromkeys(new_ids, "PENDING"))
        return destination

    result = reconciler(
        config_path,
        workspace,
        client,
        now,
        collect_action=collect_action,
        launch_action=launch_action,
    ).reconcile_once()

    assert result["controller"]["last_action"] == "restarted"
    assert [action for action, _path in launches] == ["stage", "submit"]
    assert launches[1][1] is None
    assert collections == [fixed_plan, fixed_plan]
    assert client.cancelled == ["101", "102", "103", "104"]
    assert control.plan_routes(control.load_plan(fixed_plan)) == desired_routes
    assert result["slurm"]["expected_workers"] == 32


def test_role_model_change_uses_staged_safe_relaunch_and_updates_status(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    desired_models = {"opus": "new-opus", "sonnet": "new-sonnet"}
    config_path, workspace, _run_dir, fixed_plan = write_config(
        tmp_path,
        models=desired_models,
    )
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})
    launches: list[str] = []

    def launch_action(config, action, plan_path):
        launches.append(action)
        destination = plan_path or fixed_plan
        new_ids = [None] * 4 if action == "stage" else ["201", "202", "203", "204"]
        write_plan(
            destination,
            job_ids=new_ids,
            models=config.models.as_dict(),
            staged=action == "stage",
        )
        if action == "submit":
            client.states.update(dict.fromkeys(new_ids, "PENDING"))
        return destination

    result = reconciler(
        config_path,
        workspace,
        client,
        now,
        launch_action=launch_action,
    ).reconcile_once()

    assert launches == ["stage", "submit"]
    assert client.cancelled == ["101", "102", "103", "104"]
    assert result["controller"]["last_action"] == "restarted"
    assert result["models"] == {"desired": desired_models, "observed": desired_models}
    assert result["observed"]["models"] == desired_models


def test_endpoint_only_catalog_change_uses_staged_safe_relaunch(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    config_path, workspace, _run_dir, fixed_plan = write_config(tmp_path)
    endpoints = ("https://models.example/v1", "http://second.example:3000/v1")
    public_endpoints = ["https://models.example", "http://second.example:3000"]
    (workspace / "models.yaml").write_text(model_catalog_text(DEFAULT_TEST_MODELS, endpoints))
    desired_hash = control.model_config_sha256(workspace / "models.yaml")
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})
    launches: list[str] = []

    def launch_action(config, action, plan_path):
        launches.append(action)
        destination = plan_path or fixed_plan
        new_ids = [None] * 4 if action == "stage" else ["201", "202", "203", "204"]
        write_plan(
            destination,
            job_ids=new_ids,
            models=config.models.as_dict(),
            staged=action == "stage",
        )
        if action == "submit":
            client.states.update(dict.fromkeys(new_ids, "PENDING"))
        return destination

    result = reconciler(
        config_path,
        workspace,
        client,
        now,
        launch_action=launch_action,
    ).reconcile_once()

    assert launches == ["stage", "submit"]
    assert client.cancelled == ["101", "102", "103", "104"]
    assert result["controller"]["last_action"] == "restarted"
    assert result["model_backend"] == {
        "desired_config_sha256": desired_hash,
        "observed_config_sha256": desired_hash,
        "desired_endpoints": public_endpoints,
        "observed_endpoints": public_endpoints,
    }


def test_status_exposes_sanitized_model_endpoints_and_hashes(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    config_path, workspace, _run_dir, plan_path = write_config(tmp_path)
    private_endpoint = "https://user:embedded-secret@models.example/v1?token=hidden"
    (workspace / "models.yaml").write_text(
        model_catalog_text(DEFAULT_TEST_MODELS, (private_endpoint,))
    )
    write_plan(plan_path)
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})

    result = reconciler(config_path, workspace, client, now).reconcile_once()

    desired_hash = control.model_config_sha256(workspace / "models.yaml")
    assert result["desired"]["model_config_sha256"] == desired_hash
    assert result["observed"]["model_config_sha256"] == desired_hash
    assert result["desired"]["model_endpoints"] == ["https://models.example"]
    assert result["observed"]["model_endpoints"] == ["https://models.example"]
    assert "embedded-secret" not in json.dumps(result)
    assert "token=hidden" not in json.dumps(result)


def test_default_role_models_recognize_legacy_plan_without_job_churn(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    config_path, workspace, _run_dir, plan_path = write_config(
        tmp_path,
        include_models=False,
    )
    write_plan(plan_path, legacy_models=True)
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})

    def launch_action(_config, _action, _plan_path):
        raise AssertionError("a no-op default model selection must not relaunch jobs")

    result = reconciler(
        config_path,
        workspace,
        client,
        now,
        launch_action=launch_action,
    ).reconcile_once()

    assert result["controller"]["last_action"] == "none"
    assert client.cancelled == []


def test_plan_models_reads_consistent_profile_roles_and_rejects_ambiguity() -> None:
    roles = {"opus": "profile-opus", "sonnet": "profile-sonnet"}
    profiles = [
        {"name": "backend-000", "roles": roles},
        {"name": "backend-001", "roles": dict(roles)},
    ]

    assert control.plan_models({"model_backend": {"profiles": profiles}}) == roles
    assert control.plan_models(
        {"model_backend": {"roles": roles, "profiles": profiles}}
    ) == roles
    assert (
        control.plan_models(
            {
                "model_backend": {
                    "profiles": [
                        profiles[0],
                        {
                            "name": "backend-001",
                            "roles": {"opus": "other-opus", "sonnet": "profile-sonnet"},
                        },
                    ]
                }
            }
        )
        == {}
    )
    assert control.plan_models(
        {
            "model_backend": {
                "roles": roles,
                "profiles": [
                    {
                        "name": "backend-000",
                        "roles": {"opus": "other-opus", "sonnet": "profile-sonnet"},
                    }
                ],
            }
        }
    ) == {}
    assert control.plan_models(
        {"model_backend": {"profiles": [{"name": "backend-000", "roles": {"opus": "x"}}]}}
    ) == {}


def test_controller_collects_fresh_remote_health_and_reports_actual_workers(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    config_path, workspace, _run_dir, plan_path = write_config(tmp_path)
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})
    calls: list[tuple[Path, int]] = []

    def health_action(path, window_seconds):
        calls.append((path, window_seconds))
        return {
            "active_workers": 47,
            "nodes": [
                {
                    "node": node,
                    "job_id": str(101 + index),
                    "state": "RUNNING",
                    "collected": True,
                    "active_worker_processes": 12 if index < 3 else 11,
                    "recent_api_failure_events": 0,
                }
                for index, node in enumerate(NODES)
            ],
        }

    result = reconciler(
        config_path,
        workspace,
        client,
        now,
        health_action=health_action,
    ).reconcile_once()

    assert calls == [(plan_path, 300)]
    assert result["slurm"]["active_workers"] == 47
    assert result["slurm"]["active_workers_source"] == "remote_processes"


def test_recent_remote_api_events_trip_global_breaker_without_terminal_record(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    config_path, workspace, _run_dir, _plan = write_config(tmp_path, threshold=3)
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})

    def health_action(_path, _window_seconds):
        return {
            "active_workers": 48,
            "nodes": [
                {
                    "node": node,
                    "job_id": str(101 + index),
                    "state": "RUNNING",
                    "collected": True,
                    "active_worker_processes": 12,
                    "recent_api_failure_events": 2 if index == 0 else 1 if index == 1 else 0,
                }
                for index, node in enumerate(NODES)
            ],
        }

    result = reconciler(
        config_path,
        workspace,
        client,
        now,
        health_action=health_action,
    ).reconcile_once()

    assert result["controller"]["last_action"] == "circuit_breaker_tripped"
    assert result["circuit_breaker"]["failure_count"] == 3
    assert result["circuit_breaker"]["diagnostic_failure_count"] == 3
    assert client.suspended == ["101", "102", "103", "104"]


def test_pending_jobs_are_held_on_pause_and_released_on_resume(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    config_path, workspace, _run_dir, _plan = write_config(tmp_path, desired="paused")
    client = FakeSlurmClient({str(101 + index): "PENDING" for index in range(4)})

    paused = reconciler(config_path, workspace, client, now).reconcile_once()

    assert client.held == ["101", "102", "103", "104"]
    assert paused["controller"]["applied_state"] == "paused"
    assert paused["controller"]["held_job_ids"] == ["101", "102", "103", "104"]

    value = yaml.safe_load(config_path.read_text())
    value["desired_state"] = "running"
    value["metadata"] = {
        "updated_at": control.now_iso(now + timedelta(seconds=1)),
        "updated_by": "dashboard",
    }
    control.atomic_yaml(config_path, value)
    resumed = reconciler(
        config_path, workspace, client, now + timedelta(seconds=2)
    ).reconcile_once()

    assert resumed["controller"]["last_action"] == "resumed"
    assert client.released == ["101", "102", "103", "104"]
    assert resumed["controller"]["held_job_ids"] == []


def test_topology_change_refuses_to_duplicate_unknown_existing_jobs(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    desired_routes = {"sg": 2, "hk": 2, "de": 2}
    config_path, workspace, _run_dir, _plan = write_config(tmp_path, routes=desired_routes)
    client = FakeSlurmClient({str(101 + index): "UNKNOWN" for index in range(4)})
    launches: list[str] = []

    def launch_action(_config, action, _plan_path):
        launches.append(action)
        raise AssertionError("unknown existing jobs must block launch")

    result = reconciler(
        config_path,
        workspace,
        client,
        now,
        launch_action=launch_action,
    ).reconcile_once()

    assert result["controller"]["last_action"] == "error"
    assert "state is unknown" in result["controller"]["last_error"]
    assert client.cancelled == []
    assert launches == []


def test_failed_candidate_stage_never_cancels_existing_jobs(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    desired_routes = {"sg": 2, "hk": 2, "de": 2}
    config_path, workspace, _run_dir, _plan = write_config(tmp_path, routes=desired_routes)
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})
    actions: list[str] = []

    def launch_action(_config, action, _plan_path):
        actions.append(action)
        raise control.ReconcileError("candidate preflight failed")

    result = reconciler(
        config_path,
        workspace,
        client,
        now,
        launch_action=launch_action,
    ).reconcile_once()

    assert result["controller"]["last_action"] == "error"
    assert "candidate preflight failed" in result["controller"]["last_error"]
    assert actions == ["stage"]
    assert client.cancelled == []


def test_matching_fully_staged_candidate_is_reused_before_cancelling_jobs(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    desired_routes = {"sg": 2, "hk": 6, "de": 0}
    config_path, workspace, run_dir, fixed_plan = write_config(tmp_path, routes=desired_routes)
    candidate = run_dir / ".slurm-control" / "candidate-r9-plan.json"
    write_plan(
        candidate,
        desired_routes,
        [None] * 4,
        profile_roles_only=True,
        staged=True,
    )
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})
    launches: list[str] = []

    def launch_action(config, action, plan_path):
        launches.append(action)
        assert action == "submit"
        assert plan_path is None
        new_ids = ["201", "202", "203", "204"]
        write_plan(fixed_plan, desired_routes, new_ids, models=config.models.as_dict())
        client.states.update(dict.fromkeys(new_ids, "PENDING"))
        return fixed_plan

    result = reconciler(
        config_path,
        workspace,
        client,
        now,
        launch_action=launch_action,
    ).reconcile_once()

    assert result["controller"]["last_action"] == "restarted"
    assert launches == ["submit"]
    assert client.cancelled == ["101", "102", "103", "104"]
    assert not candidate.exists()


def test_cached_candidate_without_preflight_is_restaged(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    desired_routes = {"sg": 2, "hk": 6, "de": 0}
    config_path, workspace, run_dir, fixed_plan = write_config(tmp_path, routes=desired_routes)
    candidate = run_dir / ".slurm-control" / "candidate-r9-plan.json"
    write_plan(candidate, desired_routes, [None] * 4, staged=True, preflight=False)
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})
    launches: list[str] = []

    def launch_action(config, action, plan_path):
        launches.append(action)
        destination = plan_path or fixed_plan
        new_ids = [None] * 4 if action == "stage" else ["201", "202", "203", "204"]
        write_plan(
            destination,
            desired_routes,
            new_ids,
            models=config.models.as_dict(),
            staged=action == "stage",
        )
        if action == "submit":
            client.states.update(dict.fromkeys(new_ids, "PENDING"))
        return destination

    result = reconciler(
        config_path,
        workspace,
        client,
        now,
        launch_action=launch_action,
    ).reconcile_once()

    assert result["controller"]["last_action"] == "restarted"
    assert launches == ["stage", "submit"]
    assert client.cancelled == ["101", "102", "103", "104"]


def test_fresh_candidate_without_preflight_never_cancels_existing_jobs(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    desired_routes = {"sg": 2, "hk": 6, "de": 0}
    config_path, workspace, _run_dir, _fixed_plan = write_config(
        tmp_path, routes=desired_routes
    )
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})

    def launch_action(config, action, plan_path):
        assert action == "stage"
        assert plan_path is not None
        write_plan(
            plan_path,
            desired_routes,
            [None] * 4,
            models=config.models.as_dict(),
            staged=True,
            preflight=False,
        )
        return plan_path

    result = reconciler(
        config_path,
        workspace,
        client,
        now,
        launch_action=launch_action,
    ).reconcile_once()

    assert result["controller"]["last_action"] == "error"
    assert "preflight evidence" in result["controller"]["last_error"]
    assert client.cancelled == []


def test_candidate_with_wrong_role_models_never_cancels_existing_jobs(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    desired_models = {"opus": "new-opus", "sonnet": "new-sonnet"}
    config_path, workspace, _run_dir, _plan = write_config(
        tmp_path,
        models=desired_models,
    )
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})

    def launch_action(_config, action, plan_path):
        assert action == "stage"
        assert plan_path is not None
        write_plan(
            plan_path,
            job_ids=[None] * 4,
            models={"opus": "wrong-opus", "sonnet": "wrong-sonnet"},
            staged=True,
        )
        return plan_path

    result = reconciler(
        config_path,
        workspace,
        client,
        now,
        launch_action=launch_action,
    ).reconcile_once()

    assert result["controller"]["last_action"] == "error"
    assert "role models" in result["controller"]["last_error"]
    assert client.cancelled == []


def test_candidate_with_stale_model_hash_never_cancels_existing_jobs(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    config_path, workspace, _run_dir, _plan = write_config(tmp_path)
    endpoints = ("https://models.example/v1", "http://second.example:3000/v1")
    (workspace / "models.yaml").write_text(model_catalog_text(DEFAULT_TEST_MODELS, endpoints))
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})

    def launch_action(_config, action, plan_path):
        assert action == "stage"
        assert plan_path is not None
        write_plan(
            plan_path,
            job_ids=[None] * 4,
            staged=True,
            model_hash=MODEL_CATALOG_SHA256,
            endpoints=endpoints,
        )
        return plan_path

    result = reconciler(
        config_path,
        workspace,
        client,
        now,
        launch_action=launch_action,
    ).reconcile_once()

    assert result["controller"]["last_action"] == "error"
    assert "model endpoints" in result["controller"]["last_error"]
    assert client.cancelled == []


def test_reconfiguring_paused_jobs_never_resumes_old_topology(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    desired_routes = {"sg": 2, "hk": 6, "de": 0}
    config_path, workspace, _run_dir, fixed_plan = write_config(tmp_path, routes=desired_routes)
    client = FakeSlurmClient({str(101 + index): "SUSPENDED" for index in range(4)})
    collections: list[Path] = []

    def collect_action(path):
        collections.append(path)
        return {}

    def launch_action(_config, action, plan_path):
        destination = plan_path or fixed_plan
        new_ids = [None] * 4 if action == "stage" else ["201", "202", "203", "204"]
        write_plan(destination, desired_routes, new_ids, staged=action == "stage")
        if action == "submit":
            client.states.update(dict.fromkeys(new_ids, "PENDING"))
        return destination

    result = reconciler(
        config_path,
        workspace,
        client,
        now,
        collect_action=collect_action,
        launch_action=launch_action,
    ).reconcile_once()

    assert result["controller"]["last_action"] == "restarted"
    assert client.resumed == []
    assert client.cancelled == ["101", "102", "103", "104"]
    assert collections == [fixed_plan]


def test_controller_yaml_writes_use_shared_lock_and_mode_0600(tmp_path, monkeypatch) -> None:
    path = tmp_path / "swegen-config.yaml"
    calls: list[int] = []
    real_flock = control.fcntl.flock

    def recording_flock(fd, operation):
        calls.append(operation)
        return real_flock(fd, operation)

    monkeypatch.setattr(control.fcntl, "flock", recording_flock)
    control.atomic_yaml(path, {"version": 1})

    assert path.stat().st_mode & 0o777 == 0o600
    assert path.with_name(path.name + ".lock").is_file()
    assert control.fcntl.LOCK_EX in calls
    assert control.fcntl.LOCK_UN in calls


def test_circuit_breaker_trip_stops_stage_workers(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    config_path, workspace, run_dir, _plan = write_config(tmp_path, threshold=2)
    status_path = (
        run_dir / "slurm-nodes" / NODES[0] / "orchestrator-instance-status-sg-n1-a-r9.jsonl"
    )
    status_path.parent.mkdir(parents=True)
    status_path.write_text(
        "".join(
            json.dumps(
                {
                    "timestamp": control.now_iso(now - timedelta(seconds=index)),
                    "status": "failure",
                    "failure_reason": "Transient network/API error",
                    "instance": f"task-{index}",
                }
            )
            + "\n"
            for index in (1, 2)
        )
    )
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})
    services = FakeServiceManager()

    result = reconciler(
        config_path, workspace, client, now, service_manager=services
    ).reconcile_once()

    assert result["controller"]["last_action"] == "circuit_breaker_tripped"
    assert result["controller"]["stage_workers_action"] == "stopped"
    assert services.stopped == list(control.STAGE_WORKER_SERVICES)


def test_explicit_breaker_reset_starts_stage_workers(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    config_path, workspace, run_dir, _plan = write_config(tmp_path, threshold=2)
    status_path = (
        run_dir / "slurm-nodes" / NODES[0] / "orchestrator-instance-status-sg-n1-a-r9.jsonl"
    )
    status_path.parent.mkdir(parents=True)
    status_path.write_text(
        "".join(
            json.dumps(
                {
                    "timestamp": control.now_iso(now - timedelta(seconds=index)),
                    "status": "failure",
                    "failure_reason": "Transient network/API error",
                    "instance": f"task-{index}",
                }
            )
            + "\n"
            for index in (1, 2)
        )
    )
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})
    services = FakeServiceManager()

    reconciler(config_path, workspace, client, now, service_manager=services).reconcile_once()
    assert services.stopped == list(control.STAGE_WORKER_SERVICES)

    config = yaml.safe_load(config_path.read_text())
    config["desired_state"] = "running"
    config["metadata"]["circuit_breaker_reset_requested_at"] = control.now_iso(
        now + timedelta(seconds=3)
    )
    control.atomic_yaml(config_path, config)

    result = reconciler(
        config_path,
        workspace,
        client,
        now + timedelta(seconds=4),
        service_manager=services,
    ).reconcile_once()

    assert result["controller"]["stage_workers_action"] == "started"
    assert services.started == list(control.STAGE_WORKER_SERVICES)


def test_desired_state_paused_stops_stage_workers(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    config_path, workspace, _run_dir, _plan = write_config(tmp_path, desired="paused")
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})
    services = FakeServiceManager()

    result = reconciler(
        config_path, workspace, client, now, service_manager=services
    ).reconcile_once()

    assert result["controller"]["last_action"] == "paused"
    assert result["controller"]["stage_workers_action"] == "stopped"
    assert services.stopped == list(control.STAGE_WORKER_SERVICES)


def test_resume_starts_stage_workers(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    config_path, workspace, _run_dir, _plan = write_config(tmp_path, desired="paused")
    client = FakeSlurmClient({str(101 + index): "SUSPENDED" for index in range(4)})
    services = FakeServiceManager()

    result = reconciler(
        config_path, workspace, client, now, service_manager=services
    ).reconcile_once()
    assert result["controller"]["stage_workers_action"] == "none"

    config = yaml.safe_load(config_path.read_text())
    config["desired_state"] = "running"
    control.atomic_yaml(config_path, config)

    result = reconciler(
        config_path,
        workspace,
        client,
        now + timedelta(seconds=1),
        service_manager=services,
    ).reconcile_once()

    assert result["controller"]["last_action"] == "resumed"
    assert result["controller"]["stage_workers_action"] == "started"


def test_enforce_breaker_pause_stops_stage_workers(tmp_path) -> None:
    now = datetime(2026, 7, 22, 6, 30, tzinfo=UTC)
    config_path, workspace, run_dir, _plan = write_config(tmp_path, threshold=2)
    status_path = (
        run_dir / "slurm-nodes" / NODES[0] / "orchestrator-instance-status-sg-n1-a-r9.jsonl"
    )
    status_path.parent.mkdir(parents=True)
    status_path.write_text(
        "".join(
            json.dumps(
                {
                    "timestamp": control.now_iso(now - timedelta(seconds=index)),
                    "status": "failure",
                    "failure_reason": "Transient network/API error",
                    "instance": f"task-{index}",
                }
            )
            + "\n"
            for index in (1, 2)
        )
    )
    client = FakeSlurmClient({str(101 + index): "RUNNING" for index in range(4)})
    services = FakeServiceManager()

    reconciler(config_path, workspace, client, now, service_manager=services).reconcile_once()

    client.states["101"] = "RUNNING"
    services._active = set(control.STAGE_WORKER_SERVICES)
    services.stopped.clear()

    result = reconciler(
        config_path,
        workspace,
        client,
        now + timedelta(seconds=5),
        service_manager=services,
    ).reconcile_once()

    assert result["controller"]["last_action"] == "enforce_breaker_pause"
    assert result["controller"]["stage_workers_action"] == "stopped"
    assert services.stopped == list(control.STAGE_WORKER_SERVICES)
