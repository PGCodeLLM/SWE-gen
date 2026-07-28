import copy
import json
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import MethodType, SimpleNamespace

import pytest

import slurm_validation_worker as worker
from reward_hacking_detector.hacking import HackCheckResult, LLMConfig
from swegen.tools import harbor_runner


def completed_stage(state: str, reward=None, error=None):
    return {
        "state": state,
        "reward": reward,
        "exit_code": 0,
        "error": error,
        "job_result": None,
    }


def test_load_latest_postchecks_prefers_attempt_and_ignores_partial_lines(tmp_path) -> None:
    path = tmp_path / "postcheck-status.jsonl"
    path.write_text(
        json.dumps({"instance": "a", "attempt": 1, "timestamp": "9", "status": "accepted"})
        + "\n{partial\n"
        + json.dumps({"instance": "a", "attempt": 2, "timestamp": "1", "status": "error"})
        + "\n"
    )

    latest = worker.load_latest_postchecks(path)

    assert latest["a"]["attempt"] == 2
    assert latest["a"]["status"] == "error"


def test_harbor_command_fallback_uses_running_python(monkeypatch) -> None:
    monkeypatch.setattr(harbor_runner.shutil, "which", lambda _name: None)
    monkeypatch.setattr(harbor_runner.Path, "is_file", lambda _path: False)

    assert harbor_runner.harbor_cmd_base() == [
        sys.executable,
        "-c",
        "from harbor.cli.main import app; app()",
    ]


def test_harbor_command_prefers_sibling_console_script(monkeypatch) -> None:
    monkeypatch.setattr(harbor_runner.shutil, "which", lambda _name: None)
    monkeypatch.setattr(harbor_runner.Path, "is_file", lambda _path: True)
    monkeypatch.setattr(harbor_runner.os, "access", lambda *_args: True)

    assert harbor_runner.harbor_cmd_base() == [str(Path(sys.executable).with_name("harbor"))]


def test_harbor_wall_timeout_terminates_complete_client_process_group(
    tmp_path, monkeypatch
) -> None:
    signals: list[tuple[int, int]] = []

    class FakeChild:
        pid = 4321
        returncode = None

        def __init__(self):
            self.calls = 0

        def communicate(self, timeout=None):
            self.calls += 1
            if self.calls == 1:
                raise subprocess.TimeoutExpired(["harbor"], timeout)
            self.returncode = -15
            return "", ""

    monkeypatch.setattr(harbor_runner, "harbor_cmd_base", lambda: ["harbor"])
    monkeypatch.setattr(
        harbor_runner.subprocess,
        "Popen",
        lambda *_args, **_kwargs: FakeChild(),
    )
    monkeypatch.setattr(
        harbor_runner.os,
        "killpg",
        lambda pid, signum: signals.append((pid, signum)),
    )
    # The wall-timeout path reaps orphaned containers; stub it so the test does
    # not shell out to a real docker on the timeout branch.
    reaped: list[str] = []
    monkeypatch.setattr(
        harbor_runner,
        "_reap_harbor_containers",
        lambda task_id, environment: reaped.append(task_id),
    )

    with pytest.raises(TimeoutError, match="Harbor nop timed out"):
        harbor_runner.run_harbor_agent(
            "owner__repo-1",
            tmp_path / "tasks",
            tmp_path / "jobs",
            "nop",
            capture_output=True,
            wall_timeout_seconds=10,
        )

    assert signals == [(4321, worker.signal.SIGTERM)]
    # the timed-out task's containers must be reaped so they don't leak
    assert reaped == ["owner__repo-1"]


def test_parse_args_configures_a_safe_default_heartbeat(tmp_path) -> None:
    required = ["--run-dir", str(tmp_path), "--plan", str(tmp_path / "plan.json")]

    assert worker.parse_args(required).heartbeat_interval == 60.0
    assert worker.parse_args(required).baseline_concurrency == 1
    assert worker.parse_args([*required, "--baseline-only"]).baseline_only is True
    assert (
        worker.parse_args(
            [*required, "--baseline-only", "--baseline-concurrency", "20"]
        ).baseline_concurrency
        == 20
    )
    with pytest.raises(SystemExit):
        worker.parse_args([*required, "--heartbeat-interval", "0"])
    with pytest.raises(SystemExit):
        worker.parse_args([*required, "--baseline-concurrency", "0"])


def test_baseline_only_constructor_does_not_require_reward_network(tmp_path, monkeypatch) -> None:
    proxy_path = tmp_path / "proxy.env"
    proxy_path.write_text(
        "HTTP_PROXY=http://proxy.example:8080\n"
        "HTTPS_PROXY=http://proxy.example:8080\n"
        "ALL_PROXY=socks5h://proxy.example:1080\n"
        "NO_PROXY=127.0.0.1,localhost\n"
    )
    ca_bundle = tmp_path / "combined-ca.crt"
    ca_bundle.write_text("test ca\n")
    missing_credentials = tmp_path / "must-not-be-read.env"
    for key in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "SSL_CERT_FILE",
        "REQUESTS_CA_BUNDLE",
        "CURL_CA_BUNDLE",
    ):
        monkeypatch.delenv(key, raising=False)
    args = worker.parse_args(
        [
            "--run-dir",
            str(tmp_path / "run"),
            "--plan",
            str(tmp_path / "plan.json"),
            "--baseline-only",
            "--baseline-concurrency",
            "20",
            "--once",
            "--proxy-env",
            str(proxy_path),
            "--ca-bundle",
            str(ca_bundle),
            "--credentials-file",
            str(missing_credentials),
        ]
    )
    monkeypatch.setattr(worker.ValidationWorker, "_claim_pid", lambda _self: None)
    monkeypatch.setattr(worker, "collect_latest_statuses", lambda _run_dir: ({}, []))

    def unexpected_reward_llm(_self):
        raise AssertionError("baseline-only worker must not read reward credentials")

    monkeypatch.setattr(
        worker.ValidationWorker,
        "_configure_reward_llm",
        unexpected_reward_llm,
    )

    value = worker.ValidationWorker(args)

    assert value.reward_executor is None
    assert value.reward_futures == {}
    assert value.baseline_executor is not None
    assert value.baseline_executor._max_workers == 20
    assert value.llm_config is None
    assert worker.os.environ["HTTP_PROXY"] == "http://proxy.example:8080"
    assert worker.os.environ["HTTPS_PROXY"] == "http://proxy.example:8080"
    assert worker.os.environ["ALL_PROXY"] == "socks5h://proxy.example:1080"
    assert worker.os.environ["NO_PROXY"] == "127.0.0.1,localhost"
    assert worker.os.environ["SSL_CERT_FILE"] == str(ca_bundle)
    assert not missing_credentials.exists()
    assert value.run() == 0
    status = json.loads(value.status_path.read_text())
    assert status["mode"] == "baseline_only"
    assert status["baseline_concurrency"] == 20
    assert status["reward_concurrency"] == 0


def test_prepare_attempt_preserves_terminal_stages_and_resets_errors() -> None:
    record = worker.queued_snapshot("owner__repo-1", {"node": "node-a"}, "postcheck-0")
    record["attempt"] = 2
    record["status"] = "error"
    record["nop"] = completed_stage("pass", 0)
    record["oracle"] = completed_stage("error", error="docker unavailable")
    record["reward_hack"].update({"state": "pass", "is_hacking": False})

    retried = worker.prepare_attempt(record)

    assert retried["attempt"] == 3
    assert retried["status"] == "running"
    assert retried["nop"]["state"] == "pass"
    assert retried["nop"]["reward"] == 0
    assert retried["oracle"]["state"] == "pending"
    assert retried["reward_hack"]["state"] == "pass"


def test_stage_counts_keeps_errors_separate_from_hacking() -> None:
    accepted = worker.queued_snapshot("accepted", {}, "w")
    accepted.update({"status": "accepted"})
    accepted["nop"] = completed_stage("pass", 0)
    accepted["oracle"] = completed_stage("pass", 1)
    accepted["reward_hack"].update({"state": "pass", "is_hacking": False})
    errored = copy.deepcopy(accepted)
    errored["instance"] = "errored"
    errored["status"] = "error"
    errored["reward_hack"].update(
        {"state": "error", "is_hacking": None, "error": "endpoint unavailable"}
    )

    counts = worker.stage_counts(
        {"accepted": accepted, "errored": errored}, {"accepted", "errored", "missing"}
    )

    assert counts["accepted"] == 1
    assert counts["errors"] == 1
    assert counts["not_queued"] == 1
    assert counts["reward_hack"]["pass"] == 1
    assert counts["reward_hack"]["fail"] == 0
    assert counts["reward_hack"]["error"] == 1
    assert counts["baseline_valid"] == 2


def test_baseline_discovery_includes_local_tasks_and_uses_oldest_first(
    tmp_path, monkeypatch
) -> None:
    newest = "owner__repo-3"
    local = "owner__repo-2"
    oldest = "owner__repo-1"
    local_tests = tmp_path / "tasks" / local / "tests"
    local_tests.mkdir(parents=True)
    (local_tests / "test.sh").write_text("#!/bin/sh\n")
    latest = {
        newest: {"status": "success", "node_scope": "slurm"},
        local: {"status": "success"},
        oldest: {"status": "success", "node_scope": "slurm"},
        "missing__repo-4": {"status": "success"},
    }
    monkeypatch.setattr(
        worker,
        "collect_latest_statuses",
        lambda _run_dir: (latest, [newest, local, oldest, "missing__repo-4"]),
    )

    value = object.__new__(worker.ValidationWorker)
    value.run_dir = tmp_path
    value.args = SimpleNamespace(instance=None)
    value.records = {}
    value.worker_id = "stage2"
    value.current_instance = None
    value.reward_futures = {}
    value.append = MethodType(
        lambda self, record, publish=True: self.records.update({record["instance"]: record}), value
    )
    value.publish_status = MethodType(lambda self, *_args, **_kwargs: None, value)

    value.discover()

    assert value.generation_order == [oldest, local, newest]
    assert set(value.generations) == {oldest, local, newest}


def test_fetch_uses_retained_local_task_when_no_source_node(tmp_path) -> None:
    instance = "owner__repo-1"
    task = tmp_path / "run" / "tasks" / instance
    (task / "tests").mkdir(parents=True)
    (task / "tests" / "test.sh").write_text("#!/bin/sh\n")
    value = object.__new__(worker.ValidationWorker)
    value.run_dir = tmp_path / "run"
    value.tasks_dir = tmp_path / "cache"

    assert value.fetch_task(instance, {}) == task


def test_fetch_prefers_staged_task_source_over_srun(tmp_path) -> None:
    # A pre-staged task dir (e.g. NFS) must be used instead of pulling the task
    # over Slurm, even when the record names a source node.
    instance = "owner__repo-2"
    staged = tmp_path / "nfs-tasks" / instance
    (staged / "tests").mkdir(parents=True)
    (staged / "tests" / "test.sh").write_text("#!/bin/sh\n")
    value = object.__new__(worker.ValidationWorker)
    value.run_dir = tmp_path / "run"
    value.tasks_dir = tmp_path / "cache"
    value.task_source_dir = tmp_path / "nfs-tasks"

    def _no_srun(*_args, **_kwargs):
        raise AssertionError("srun fetch must not run when a staged task exists")

    value.node_records = _no_srun
    # record has a source node, which would otherwise trigger the srun path
    assert value.fetch_task(instance, {"source_node": "node-a"}) == staged


def test_fetch_falls_back_when_task_source_dir_missing_instance(tmp_path) -> None:
    # When the staged dir lacks the instance, fetch_task still uses the
    # no-source-node local fallback (backward compatible).
    instance = "owner__repo-3"
    task = tmp_path / "run" / "tasks" / instance
    (task / "tests").mkdir(parents=True)
    (task / "tests" / "test.sh").write_text("#!/bin/sh\n")
    value = object.__new__(worker.ValidationWorker)
    value.run_dir = tmp_path / "run"
    value.tasks_dir = tmp_path / "cache"
    value.task_source_dir = tmp_path / "nfs-tasks-empty"

    assert value.fetch_task(instance, {}) == task


def make_pipeline_worker(tmp_path, monkeypatch, hack_result: HackCheckResult):
    instance = "owner__repo-1"
    task_dir = tmp_path / "tasks" / instance
    (task_dir / "tests").mkdir(parents=True)
    (task_dir / "tests" / "test.sh").write_text("#!/bin/sh\nexit 0\n")
    value = object.__new__(worker.ValidationWorker)
    value.records = {instance: worker.queued_snapshot(instance, {"node": "node-a"}, "postcheck-0")}
    value.generations = {instance: {"node": "node-a", "status": "success"}}
    value.current_instance = None
    value.current_stage = None
    value.worker_id = "postcheck-0"
    value.jobs_dir = tmp_path / "jobs"
    value.reward_logs_dir = tmp_path / "reward-logs"
    value.llm_config = LLMConfig("test", "https://endpoint", "model", "key")
    value.args = SimpleNamespace(timeout_multiplier=None, retry_delay=600)
    value.fetch_task = lambda _instance, _record: task_dir

    def append(self, record, publish=True):
        self.records[record["instance"]] = copy.deepcopy(record)

    value.append = MethodType(append, value)
    value.publish_status = MethodType(lambda self, *_args, **_kwargs: None, value)
    monkeypatch.setattr(worker, "validate_task_structure", lambda _path: True)
    monkeypatch.setattr(
        worker,
        "run_harbor_agent",
        lambda **kwargs: (0, Path(f"/{kwargs['agent']}.json")),
    )
    monkeypatch.setattr(
        worker,
        "parse_harbor_outcome",
        lambda path: SimpleNamespace(
            reward=0 if path.name == "nop.json" else 1,
            error=None,
        ),
    )
    monkeypatch.setattr(worker, "build_test_bundle", lambda _path: "tests")

    async def fake_check(_bundle, configs, task_id=""):
        return [(configs[0], hack_result)]

    monkeypatch.setattr(worker, "check_instance", fake_check)
    monkeypatch.setattr(worker, "write_instance_log", lambda *args, **kwargs: None)
    return value, instance


def test_process_accepts_only_nop_zero_oracle_one_and_clean_checker(tmp_path, monkeypatch) -> None:
    value, instance = make_pipeline_worker(
        tmp_path,
        monkeypatch,
        HackCheckResult(is_hacking=False, reason="meaningful runtime assertions"),
    )

    value.process(instance)

    result = value.records[instance]
    assert result["status"] == "accepted"
    assert result["nop"]["reward"] == 0
    assert result["oracle"]["reward"] == 1
    assert result["reward_hack"]["state"] == "pass"


def test_baseline_only_process_stops_after_exact_nop_and_oracle(tmp_path, monkeypatch) -> None:
    value, instance = make_pipeline_worker(
        tmp_path,
        monkeypatch,
        HackCheckResult(is_hacking=False, reason="must not run"),
    )
    value.args.baseline_only = True
    value.records[instance]["reward_hack"].update(
        {"state": "pass", "is_hacking": False, "model": "legacy-local"}
    )
    harbor_calls: list[str] = []

    def fake_harbor(**kwargs):
        harbor_calls.append(kwargs["agent"])
        return 0, Path(f"/{kwargs['agent']}.json")

    monkeypatch.setattr(worker, "run_harbor_agent", fake_harbor)

    def unexpected_reward(*_args, **_kwargs):
        raise AssertionError("baseline-only worker must not run or consume Stage 3")

    value.run_reward_hack_stage = unexpected_reward
    value.sync_backfill_reward = unexpected_reward

    value.process(instance)

    result = value.records[instance]
    assert harbor_calls == ["nop", "oracle"]
    assert result["status"] == "baseline_valid"
    assert result["stage"] == "baseline_complete"
    assert result["nop"]["reward"] == 0
    assert result["oracle"]["reward"] == 1
    assert result["reward_hack"] == worker.pending_reward_stage()


def test_baseline_only_does_not_run_oracle_after_nop_infrastructure_error(
    tmp_path, monkeypatch
) -> None:
    value, instance = make_pipeline_worker(
        tmp_path,
        monkeypatch,
        HackCheckResult(is_hacking=False, reason="unused"),
    )
    value.args.baseline_only = True
    calls: list[str] = []

    def fake_harbor(**kwargs):
        calls.append(kwargs["agent"])
        return 1, Path("/nop.json")

    monkeypatch.setattr(worker, "run_harbor_agent", fake_harbor)
    monkeypatch.setattr(
        worker,
        "parse_harbor_outcome",
        lambda _path: SimpleNamespace(reward=None, error="environment timeout"),
    )

    value.process(instance)

    assert calls == ["nop"]
    assert value.records[instance]["status"] == "error"
    assert value.records[instance]["nop"]["state"] == "error"
    assert value.records[instance]["oracle"]["state"] == "pending"


def test_baseline_only_blacklists_instance_after_max_attempts(
    tmp_path, monkeypatch
) -> None:
    value, instance = make_pipeline_worker(
        tmp_path,
        monkeypatch,
        HackCheckResult(is_hacking=False, reason="unused"),
    )
    value.args.baseline_only = True
    value.args.max_attempts = 3
    value.worker_dir = tmp_path / ".validation-worker"
    value.worker_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(worker, "run_harbor_agent", lambda **k: (1, Path("/nop.json")))
    monkeypatch.setattr(
        worker,
        "parse_harbor_outcome",
        lambda _path: SimpleNamespace(reward=None, error="Docker compose failed"),
    )

    now = datetime.now(timezone.utc)
    # Attempts 1 and 2 stay retryable (kicked back to unresolved, not blacklisted).
    for expected_attempt in (1, 2):
        value.process(instance)
        record = value.records[instance]
        assert record["status"] == "error"
        assert record["attempt"] == expected_attempt
        assert worker.is_ready(record, now + timedelta(hours=1), baseline_only=True)
        # Clear the retry delay so the next process() call is eligible immediately.
        record["next_retry_at"] = None

    # Third failure exhausts the budget and retires the instance.
    value.process(instance)
    record = value.records[instance]
    assert record["status"] == worker.BLACKLISTED_STATUS
    assert record["nop"]["state"] == worker.BLACKLISTED_STATUS
    assert record["next_retry_at"] is None
    # A blacklisted instance is never selected again.
    assert not worker.is_ready(record, now + timedelta(days=1), baseline_only=True)

    # The blacklist ledger records the retired instance for auditing.
    ledger = value.worker_dir / "blacklist.jsonl"
    assert ledger.is_file()
    entries = [json.loads(line) for line in ledger.read_text().splitlines() if line.strip()]
    assert entries and entries[-1]["instance"] == instance
    assert entries[-1]["attempts"] == 3


@pytest.mark.parametrize(
    ("nop_reward", "oracle_reward"),
    [(1, 1), (False, 1), (0, 0), (0, True)],
)
def test_baseline_only_rejects_non_exact_harbor_rewards(
    tmp_path, monkeypatch, nop_reward, oracle_reward
) -> None:
    value, instance = make_pipeline_worker(
        tmp_path,
        monkeypatch,
        HackCheckResult(is_hacking=False, reason="must not run"),
    )
    value.args.baseline_only = True
    monkeypatch.setattr(
        worker,
        "parse_harbor_outcome",
        lambda path: SimpleNamespace(
            reward=nop_reward if path.name == "nop.json" else oracle_reward,
            error=None,
        ),
    )
    value.run_reward_hack_stage = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("reward stage ran")
    )

    value.process(instance)

    result = value.records[instance]
    assert result["status"] == "baseline_rejected"
    assert result["stage"] == "baseline_complete"
    assert result["reward_hack"] == worker.pending_reward_stage()


def test_process_retries_checker_infrastructure_error_without_flagging_hacking(
    tmp_path, monkeypatch
) -> None:
    value, instance = make_pipeline_worker(
        tmp_path,
        monkeypatch,
        HackCheckResult(
            is_hacking=True,
            reason="LLM check unavailable",
            error="ConnectTimeout",
        ),
    )

    value.process(instance)

    result = value.records[instance]
    assert result["status"] == "error"
    assert result["reward_hack"]["state"] == "error"
    assert result["reward_hack"]["is_hacking"] is None
    assert result["next_retry_at"] is not None


def test_harbor_stage_heartbeats_while_agent_is_blocked(tmp_path, monkeypatch) -> None:
    value, instance = make_pipeline_worker(
        tmp_path,
        monkeypatch,
        HackCheckResult(is_hacking=False, reason="clean"),
    )
    value.args.heartbeat_interval = 0.01
    heartbeat_seen = threading.Event()
    publications: list[tuple[str, str | None, str | None]] = []

    def publish(self, state, error=None):
        publications.append((state, self.current_instance, self.current_stage))
        heartbeat_seen.set()

    value.publish_status = MethodType(publish, value)

    def blocked_harbor(**_kwargs):
        assert heartbeat_seen.wait(timeout=1)
        return 0, Path("/nop.json")

    monkeypatch.setattr(worker, "run_harbor_agent", blocked_harbor)
    value.current_instance = instance

    value.run_harbor_stage(value.records[instance], tmp_path / "tasks" / instance, "nop")

    assert publications
    assert publications[0] == ("running", instance, "nop")


def test_process_heartbeats_while_task_fetch_is_blocked(tmp_path, monkeypatch) -> None:
    value, instance = make_pipeline_worker(
        tmp_path,
        monkeypatch,
        HackCheckResult(is_hacking=False, reason="clean"),
    )
    value.args.heartbeat_interval = 0.01
    heartbeat_seen = threading.Event()
    publications: list[tuple[str, str | None, str | None]] = []
    task_dir = tmp_path / "tasks" / instance

    def publish(self, state, error=None):
        publications.append((state, self.current_instance, self.current_stage))
        heartbeat_seen.set()

    def blocked_fetch(_instance, _record):
        assert heartbeat_seen.wait(timeout=1)
        return task_dir

    value.publish_status = MethodType(publish, value)
    value.fetch_task = blocked_fetch

    value.process(instance)

    assert ("running", instance, "fetch") in publications
    assert value.records[instance]["status"] == "accepted"


@pytest.mark.parametrize(
    ("backfill_state", "expected_status", "is_hacking"),
    [("pass", "accepted", False), ("fail", "rejected", True)],
)
def test_process_consumes_terminal_backfill_without_local_reward_call(
    tmp_path,
    monkeypatch,
    backfill_state,
    expected_status,
    is_hacking,
) -> None:
    value, instance = make_pipeline_worker(
        tmp_path,
        monkeypatch,
        HackCheckResult(is_hacking=False, reason="must not run"),
    )
    backfill_ledger = tmp_path / "reward-backfill-status.jsonl"
    backfill_ledger.write_text(
        json.dumps(
            {
                "instance": instance,
                "attempt": 1,
                "timestamp": "2026-07-19T00:00:00+00:00",
                "status": backfill_state,
                "reward_hack": {
                    "state": backfill_state,
                    "is_hacking": is_hacking,
                    "model": "spark",
                    "reason": "backfill verdict",
                },
            }
        )
        + "\n"
    )
    value.reward_backfill_ledger_path = backfill_ledger

    def unexpected_reward(self, _record, _task_dir):
        raise AssertionError("local reward checker must not run")

    value.run_reward_hack_stage = MethodType(unexpected_reward, value)

    value.process(instance)

    result = value.records[instance]
    assert result["status"] == expected_status
    assert result["nop"]["reward"] == 0
    assert result["oracle"]["reward"] == 1
    assert result["reward_hack"]["state"] == backfill_state
    assert result["reward_hack"]["source"] == "reward_backfill"
    assert result["reward_hack"]["model"] == "spark"


@pytest.mark.parametrize("backfill_state", ["queued", "running"])
def test_inflight_backfill_allows_baseline_then_waits_without_duplicate_reward(
    tmp_path, monkeypatch, backfill_state
) -> None:
    value, instance = make_pipeline_worker(
        tmp_path,
        monkeypatch,
        HackCheckResult(is_hacking=False, reason="must not run"),
    )
    value.generation_order = [instance]
    backfill_ledger = tmp_path / "reward-backfill-status.jsonl"
    backfill_ledger.write_text(
        json.dumps(
            {
                "instance": instance,
                "attempt": 1,
                "timestamp": "2026-07-19T00:00:00+00:00",
                "status": backfill_state,
                "reward_hack": {"state": "pending" if backfill_state == "queued" else "running"},
            }
        )
        + "\n"
    )
    value.reward_backfill_ledger_path = backfill_ledger
    harbor_calls: list[str] = []

    def fake_harbor(**kwargs):
        harbor_calls.append(kwargs["agent"])
        return 0, Path(f"/{kwargs['agent']}.json")

    monkeypatch.setattr(worker, "run_harbor_agent", fake_harbor)

    def unexpected_reward(self, _record, _task_dir):
        raise AssertionError("local reward checker must not run")

    value.run_reward_hack_stage = MethodType(unexpected_reward, value)

    value.process(instance)

    waiting = value.records[instance]
    assert harbor_calls == ["nop", "oracle"]
    assert waiting["status"] == "running"
    assert waiting["nop"]["reward"] == 0
    assert waiting["oracle"]["reward"] == 1
    assert waiting["reward_hack"]["state"] == "pending"
    assert waiting["reward_hack"]["backfill_state"] == backfill_state
    assert value.next_ready() is None

    with backfill_ledger.open("a") as stream:
        stream.write(
            json.dumps(
                {
                    "instance": instance,
                    "attempt": 1,
                    "timestamp": "2026-07-19T00:01:00+00:00",
                    "status": "pass",
                    "reward_hack": {
                        "state": "pass",
                        "is_hacking": False,
                        "model": "terra",
                        "reason": "clean",
                    },
                }
            )
            + "\n"
        )

    assert value.next_ready() == instance
    value.process(instance)

    result = value.records[instance]
    assert harbor_calls == ["nop", "oracle"]
    assert result["status"] == "accepted"
    assert result["reward_hack"]["source"] == "reward_backfill"
    assert result["reward_hack"]["model"] == "terra"


def test_next_ready_prioritizes_clean_backfill_before_generation_order(tmp_path) -> None:
    ordinary = "owner__repo-ordinary"
    clean = "owner__repo-clean"
    value = object.__new__(worker.ValidationWorker)
    value.generation_order = [ordinary, clean]
    value.records = {
        instance: worker.queued_snapshot(instance, {}, "postcheck-0")
        for instance in value.generation_order
    }
    value.reward_futures = {}
    ledger = tmp_path / "reward-backfill-status.jsonl"
    ledger.write_text(
        json.dumps(
            {
                "instance": clean,
                "attempt": 1,
                "timestamp": "2026-07-19T00:00:00+00:00",
                "status": "pass",
                "reward_hack": {"state": "pass", "is_hacking": False},
            }
        )
        + "\n"
    )
    value.reward_backfill_ledger_path = ledger

    assert value.next_ready() == clean


def test_baseline_only_next_ready_ignores_reward_state_and_skips_completed() -> None:
    completed_valid = "owner__repo-valid"
    completed_rejected = "owner__repo-rejected"
    accepted_legacy = "owner__repo-accepted"
    queued = "owner__repo-queued"
    value = object.__new__(worker.ValidationWorker)
    value.args = SimpleNamespace(baseline_only=True)
    value.generation_order = [completed_valid, completed_rejected, accepted_legacy, queued]
    value.records = {
        instance: worker.queued_snapshot(instance, {}, "postcheck-0")
        for instance in value.generation_order
    }
    value.records[completed_valid]["status"] = "baseline_valid"
    value.records[completed_rejected]["status"] = "baseline_rejected"
    value.records[accepted_legacy]["status"] = "accepted"

    def unexpected_reward_read(*_args, **_kwargs):
        raise AssertionError("baseline-only scheduling consulted Stage 3")

    value.load_reward_backfill = unexpected_reward_read
    value.active_reward_instances = unexpected_reward_read

    assert value.next_ready() == queued
    value.records[queued]["status"] = "baseline_valid"
    assert value.next_ready() is None


def test_full_mode_can_continue_a_baseline_valid_record_to_stage_three() -> None:
    now = worker.datetime.now(worker.UTC)
    valid = worker.queued_snapshot("owner__repo-1", {}, "postcheck-0")
    valid["status"] = "baseline_valid"
    rejected = copy.deepcopy(valid)
    rejected["status"] = "baseline_rejected"

    assert worker.is_ready(valid, now) is True
    assert worker.is_ready(valid, now, baseline_only=True) is False
    assert worker.is_ready(rejected, now) is False


def test_next_ready_falls_back_to_ordinary_when_clean_retry_is_delayed(tmp_path) -> None:
    ordinary = "owner__repo-ordinary"
    clean = "owner__repo-clean"
    value = object.__new__(worker.ValidationWorker)
    value.generation_order = [ordinary, clean]
    value.records = {
        instance: worker.queued_snapshot(instance, {}, "postcheck-0")
        for instance in value.generation_order
    }
    value.records[clean].update(
        {
            "status": "error",
            "next_retry_at": "2999-01-01T00:00:00+00:00",
        }
    )
    value.reward_futures = {}
    ledger = tmp_path / "reward-backfill-status.jsonl"
    ledger.write_text(
        json.dumps(
            {
                "instance": clean,
                "attempt": 1,
                "timestamp": "2026-07-19T00:00:00+00:00",
                "status": "pass",
                "reward_hack": {"state": "pass", "is_hacking": False},
            }
        )
        + "\n"
    )
    value.reward_backfill_ledger_path = ledger

    assert value.next_ready() == ordinary


@pytest.mark.parametrize(
    ("nop_reward", "oracle_reward", "is_hacking"),
    [(1, 1, False), (False, 1, False), (0, True, False), (0, 1, None)],
)
def test_finalize_requires_exact_evidence_even_when_states_pass(
    tmp_path, nop_reward, oracle_reward, is_hacking
) -> None:
    value = object.__new__(worker.ValidationWorker)
    value.records = {}
    value.generations = {}
    value.current_instance = None
    value.current_stage = None
    record = worker.queued_snapshot("owner__repo-1", {}, "postcheck-0")
    record["nop"] = completed_stage("pass", nop_reward)
    record["oracle"] = completed_stage("pass", oracle_reward)
    record["reward_hack"].update({"state": "pass", "is_hacking": is_hacking})

    def append(self, updated, publish=True):
        self.records[updated["instance"]] = copy.deepcopy(updated)

    value.append = MethodType(append, value)

    value.finalize(record)

    assert value.records["owner__repo-1"]["status"] == "rejected"


def test_parallel_baseline_pool_runs_twenty_ordered_restart_safe_pipelines(
    tmp_path, monkeypatch
) -> None:
    concurrency = 20
    work_instances = [f"owner__repo-{index}" for index in range(concurrency)]
    terminal_instances = ["owner__repo-valid", "owner__repo-rejected"]
    all_instances = [*terminal_instances, *work_instances]
    tasks_dir = tmp_path / "tasks"
    for instance in work_instances:
        tests_dir = tasks_dir / instance / "tests"
        tests_dir.mkdir(parents=True)
        (tests_dir / "test.sh").write_text("#!/bin/sh\nexit 0\n")

    value = object.__new__(worker.ValidationWorker)
    value.args = SimpleNamespace(
        baseline_only=True,
        baseline_concurrency=concurrency,
        heartbeat_interval=0.05,
        harbor_wall_timeout=60.0,
        max_tasks=0,
        once=True,
        poll_interval=1,
        retry_delay=600,
        timeout_multiplier=None,
    )
    value.run_dir = tmp_path
    value.worker_dir = tmp_path / ".validation-worker"
    value.ledger_path = value.worker_dir / "postcheck-status.jsonl"
    value.status_path = value.worker_dir / "worker-status.json"
    value.jobs_dir = value.worker_dir / "harbor-jobs"
    value.worker_id = "stage2"
    value.stop_requested = False
    value._coordinator_thread_ident = threading.get_ident()
    value._state_lock = threading.RLock()
    value._ledger_lock = threading.Lock()
    value._status_lock = threading.Lock()
    value.current_instance = None
    value.current_stage = None
    value.active_stages = {}
    value.records = {
        instance: worker.queued_snapshot(instance, {"node": "node-a"}, "stage2")
        for instance in all_instances
    }
    value.records[terminal_instances[0]]["status"] = "baseline_valid"
    value.records[terminal_instances[1]]["status"] = "baseline_rejected"
    interrupted = value.records[work_instances[0]]
    interrupted.update({"attempt": 4, "status": "running", "stage": "nop"})
    interrupted["nop"]["state"] = "running"
    value.generations = {
        instance: {"node": "node-a", "status": "success"} for instance in all_instances
    }
    value.generation_order = all_instances
    value.llm_config = None
    value.fallback_llm_config = None
    value.reward_executor = None
    value.reward_futures = {}
    value.baseline_executor = ThreadPoolExecutor(max_workers=concurrency)
    value.baseline_futures = {}
    value.fetch_task = lambda instance, _record: tasks_dir / instance
    value.discover = lambda: None
    value.close = lambda: None

    monkeypatch.setattr(worker, "validate_task_structure", lambda _path: True)
    monkeypatch.setattr(worker.os, "fsync", lambda _descriptor: None)
    order_lock = threading.Lock()
    orders: dict[str, list[str]] = {}
    active = 0
    peak = 0
    nop_started: set[str] = set()
    all_started = threading.Event()
    release = threading.Event()

    def fake_harbor(**kwargs):
        nonlocal active, peak
        task_id = kwargs["task_id"]
        agent = kwargs["agent"]
        with order_lock:
            orders.setdefault(task_id, []).append(agent)
            active += 1
            peak = max(peak, active)
            if agent == "nop":
                nop_started.add(task_id)
                if len(nop_started) == concurrency:
                    all_started.set()
        assert release.wait(timeout=10)
        with order_lock:
            active -= 1
        return 0, Path(f"/{task_id}.{agent}.json")

    monkeypatch.setattr(worker, "run_harbor_agent", fake_harbor)
    monkeypatch.setattr(
        worker,
        "parse_harbor_outcome",
        lambda path: SimpleNamespace(
            reward=0 if path.name.endswith(".nop.json") else 1,
            error=None,
        ),
    )

    observed_status: dict[str, object] = {}
    observer_errors: list[str] = []

    def observe_full_pool() -> None:
        try:
            if not all_started.wait(timeout=10):
                observer_errors.append(
                    f"twenty NOP workers did not start: {len(nop_started)} {sorted(orders)}"
                )
                return
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                try:
                    status = json.loads(value.status_path.read_text())
                except (OSError, json.JSONDecodeError):
                    time.sleep(0.01)
                    continue
                if status.get("baseline_active_count") == concurrency:
                    observed_status.update(status)
                    return
                time.sleep(0.01)
            observer_errors.append("worker status never reported a full baseline pool")
        finally:
            release.set()

    observer = threading.Thread(target=observe_full_pool, daemon=True)
    observer.start()
    assert value.run_parallel_baseline() == 0
    observer.join(timeout=10)

    assert observer_errors == []
    assert peak == concurrency
    assert set(orders) == set(work_instances)
    assert all(orders[instance] == ["nop", "oracle"] for instance in work_instances)
    assert not (set(orders) & set(terminal_instances))
    assert value.records[work_instances[0]]["attempt"] == 5
    assert all(value.records[instance]["status"] == "baseline_valid" for instance in work_instances)
    assert observed_status["baseline_concurrency"] == concurrency
    assert observed_status["baseline_active_count"] == concurrency
    assert set(observed_status["current_instances"]) == set(work_instances)
    assert set(observed_status["active_stages"]) == set(work_instances)

    lines = value.ledger_path.read_text().splitlines()
    assert lines
    assert all(isinstance(json.loads(line), dict) for line in lines)
    latest = worker.load_latest_postchecks(value.ledger_path)
    assert set(latest) == set(work_instances)
    assert all(latest[instance]["status"] == "baseline_valid" for instance in work_instances)


def test_reward_only_concurrency_reaches_twenty_without_parallel_harbor(
    tmp_path, monkeypatch
) -> None:
    concurrency = 20
    instances = [f"owner__repo-{index}" for index in range(concurrency)]
    tasks_dir = tmp_path / "tasks"
    for instance in instances:
        tests_dir = tasks_dir / instance / "tests"
        tests_dir.mkdir(parents=True)
        (tests_dir / "test.sh").write_text("#!/bin/sh\nexit 0\n")

    value = object.__new__(worker.ValidationWorker)
    value.records = {
        instance: worker.queued_snapshot(instance, {"node": "node-a"}, "postcheck-0")
        for instance in instances
    }
    value.generations = {
        instance: {"node": "node-a", "status": "success"} for instance in instances
    }
    value.generation_order = instances
    value.current_instance = None
    value.current_stage = None
    value.worker_id = "postcheck-0"
    value.jobs_dir = tmp_path / "jobs"
    value.reward_logs_dir = tmp_path / "reward-logs"
    value.llm_config = LLMConfig("spark", "https://endpoint", "spark", "key")
    value.fallback_llm_config = LLMConfig("terra", "https://endpoint", "terra", "key")
    value.args = SimpleNamespace(
        timeout_multiplier=None,
        retry_delay=600,
        reward_concurrency=concurrency,
    )
    value.fetch_task = lambda instance, _record: tasks_dir / instance
    value.reward_executor = ThreadPoolExecutor(max_workers=concurrency)
    value.reward_futures = {}

    def append(self, record, publish=True):
        self.records[record["instance"]] = copy.deepcopy(record)

    value.append = MethodType(append, value)
    value.publish_status = MethodType(lambda self, *_args, **_kwargs: None, value)
    monkeypatch.setattr(worker, "validate_task_structure", lambda _path: True)

    harbor_active = 0
    harbor_peak = 0

    def fake_harbor(**kwargs):
        nonlocal harbor_active, harbor_peak
        harbor_active += 1
        harbor_peak = max(harbor_peak, harbor_active)
        harbor_active -= 1
        return 0, Path(f"/{kwargs['agent']}.json")

    monkeypatch.setattr(worker, "run_harbor_agent", fake_harbor)
    monkeypatch.setattr(
        worker,
        "parse_harbor_outcome",
        lambda path: SimpleNamespace(
            reward=0 if path.name == "nop.json" else 1,
            error=None,
        ),
    )
    monkeypatch.setattr(worker, "build_test_bundle", lambda _path: "tests")
    monkeypatch.setattr(worker, "write_instance_log", lambda *args, **kwargs: None)

    reward_lock = threading.Lock()
    reward_active = 0
    reward_peak = 0
    all_started = threading.Event()
    release = threading.Event()

    def fake_execute(self, _bundle, _task_id, _task_dir=None):
        nonlocal reward_active, reward_peak
        with reward_lock:
            reward_active += 1
            reward_peak = max(reward_peak, reward_active)
            if reward_active == concurrency:
                all_started.set()
        assert release.wait(timeout=10)
        with reward_lock:
            reward_active -= 1
        result = HackCheckResult(is_hacking=False, reason="meaningful assertions")
        return self.llm_config, result, [(self.llm_config, result)]

    value.execute_reward_check = MethodType(fake_execute, value)

    try:
        for instance in instances:
            value.process(instance)

        assert all_started.wait(timeout=10)
        assert len(value.reward_futures) == concurrency
        assert reward_peak == concurrency
        assert harbor_peak == 1
        assert value.next_ready() is None

        release.set()
        while value.reward_futures:
            value.drain_reward_futures(block=True)

        assert all(value.records[instance]["status"] == "accepted" for instance in instances)
        assert all(
            value.records[instance]["reward_hack"]["model"] == "spark" for instance in instances
        )
    finally:
        release.set()
        value.reward_executor.shutdown(wait=True, cancel_futures=False)


def test_finish_reward_stage_records_actual_fallback_model(tmp_path) -> None:
    instance = "owner__repo-1"
    task_dir = tmp_path / instance
    task_dir.mkdir()
    value = object.__new__(worker.ValidationWorker)
    value.records = {}
    value.generations = {}
    value.current_instance = None
    value.current_stage = None
    value.reward_logs_dir = tmp_path / "logs"
    value.reward_futures = {}
    record = worker.queued_snapshot(instance, {}, "postcheck-0")
    record["reward_hack"]["state"] = "running"
    primary = LLMConfig("spark", "https://endpoint", "spark", "key")
    fallback = LLMConfig("terra", "https://endpoint", "terra", "key")
    primary_error = HackCheckResult(
        is_hacking=True,
        reason="usage exhausted",
        error="HTTP 429: usage exhausted",
        http_status=429,
    )
    fallback_result = HackCheckResult(is_hacking=False, reason="meaningful assertions")

    def append(self, updated, publish=True):
        self.records[updated["instance"]] = copy.deepcopy(updated)

    value.append = MethodType(append, value)
    value.finish_reward_hack_stage(
        record,
        task_dir,
        (fallback, fallback_result, [(primary, primary_error), (fallback, fallback_result)]),
    )

    stage = value.records[instance]["reward_hack"]
    assert stage["state"] == "pass"
    assert stage["model"] == "terra"
    assert stage["attempted_models"] == ["spark", "terra"]
    assert stage["fallback_used"] is True
    assert stage["fallback_reason"] == "HTTP 429: usage exhausted"


# ── Queue sharding (multi-node validation) ──────────────────────────────────


def test_instance_shard_is_disjoint_complete_and_stable() -> None:
    instances = [f"owner__repo-{i}" for i in range(500)]
    for shard_count in (1, 2, 4, 8):
        shards = [worker.instance_shard(i, shard_count) for i in instances]
        # every instance maps into range(shard_count)
        assert all(0 <= s < shard_count for s in shards)
        # deterministic / stable across calls (cross-process safe)
        assert shards == [worker.instance_shard(i, shard_count) for i in instances]
    # shard_count<=1 is the no-sharding backward-compatible default
    assert all(worker.instance_shard(i, 1) == 0 for i in instances)
    assert all(worker.instance_shard(i, 0) == 0 for i in instances)


def test_instance_shard_partition_covers_every_instance_exactly_once() -> None:
    instances = [f"owner__repo-{i}" for i in range(300)]
    shard_count = 4
    owned: dict[int, set] = {s: set() for s in range(shard_count)}
    for inst in instances:
        owned[worker.instance_shard(inst, shard_count)].add(inst)
    union: set = set()
    for s in range(shard_count):
        # shards are disjoint
        assert union.isdisjoint(owned[s])
        union |= owned[s]
    # and complete
    assert union == set(instances)


def test_worker_loads_only_its_shard_from_shared_ledger(tmp_path, monkeypatch) -> None:
    # A shared ledger holds records for all shards; a shard-1-of-3 worker must
    # load only instances it owns, so it never resumes another shard's in-flight
    # work (which would double-process across nodes).
    instances = [f"owner__repo-{i}" for i in range(120)]
    proxy_env = tmp_path / "proxy.env"
    proxy_env.write_text("")
    ca_bundle = tmp_path / "combined-ca.crt"
    ca_bundle.write_text("test ca\n")
    ledger = tmp_path / "postcheck-status.jsonl"
    with ledger.open("w") as fh:
        for inst in instances:
            fh.write(
                json.dumps(
                    {
                        "instance": inst,
                        "attempt": 1,
                        "status": "queued",
                        "worker_id": "someone",
                    }
                )
                + "\n"
            )
    args = worker.parse_args(
        [
            "--run-dir", str(tmp_path / "run"),
            "--plan", str(tmp_path / "plan.json"),
            "--baseline-only",
            "--ledger-path", str(ledger),
            "--worker-dir", str(tmp_path / "wd"),
            "--shard-index", "1",
            "--shard-count", "3",
            "--proxy-env", str(proxy_env),
            "--ca-bundle", str(ca_bundle),
            "--once",
        ]
    )
    monkeypatch.setattr(worker.ValidationWorker, "_claim_pid", lambda _self: None)
    monkeypatch.setattr(worker, "collect_latest_statuses", lambda _run_dir: ({}, []))
    value = worker.ValidationWorker(args)

    assert value.records, "worker should load its own shard's records"
    # every loaded instance is owned by shard 1 of 3
    assert all(worker.instance_shard(i, 3) == 1 for i in value.records)
    # and it loaded exactly the shard-1 subset (nothing owned was dropped)
    expected = {i for i in instances if worker.instance_shard(i, 3) == 1}
    assert set(value.records) == expected


def test_parse_args_rejects_bad_shard_config(tmp_path) -> None:
    base = [
        "--run-dir", str(tmp_path),
        "--plan", str(tmp_path / "plan.json"),
        "--baseline-only",
    ]
    # valid: shard 2 of 4
    args = worker.parse_args(base + ["--shard-index", "2", "--shard-count", "4"])
    assert args.shard_index == 2 and args.shard_count == 4
    # default is single shard
    assert worker.parse_args(base).shard_count == 1
    # shard-index >= shard-count is rejected
    with pytest.raises(SystemExit):
        worker.parse_args(base + ["--shard-index", "4", "--shard-count", "4"])
    with pytest.raises(SystemExit):
        worker.parse_args(base + ["--shard-count", "0"])
