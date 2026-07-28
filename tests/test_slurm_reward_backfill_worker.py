import copy
import io
import json
import tarfile
import threading
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from types import MethodType, SimpleNamespace

import slurm_reward_backfill_worker as backfill
from reward_hacking_detector.hacking import HackCheckResult, LLMConfig


def baseline_record(nop_reward=0, oracle_reward=1):
    return {
        "nop": {"state": "pass", "reward": nop_reward},
        "oracle": {"state": "pass", "reward": oracle_reward},
        "reward_hack": {"state": "pending"},
    }


def test_prepare_attempt_is_restart_safe_and_terminal_results_are_not_ready() -> None:
    instance = "owner__repo-1"
    generation = {"node": "node-a", "timestamp": "2026-07-18T00:00:00+00:00"}
    running = backfill.queued_snapshot(instance, generation, "backfill-0")
    running.update({"status": "running", "attempt": 4})
    running["reward_hack"].update({"state": "running", "model": "spark"})

    restarted = backfill.prepare_attempt(running, instance, generation, "backfill-0")

    assert restarted["attempt"] == 5
    assert restarted["status"] == "queued"
    assert restarted["reward_hack"] == backfill.reward_stage()
    restarted["status"] = "pass"
    assert not backfill.is_retry_ready(restarted, backfill.datetime.now(backfill.UTC))


def test_discovery_uses_oldest_first_and_includes_retained_local_successes(
    tmp_path, monkeypatch
) -> None:
    newest = "owner__repo-3"
    middle = "owner__repo-2"
    oldest = "owner__repo-1"
    latest = {
        newest: {"instance": newest, "status": "success", "node_scope": "slurm"},
        middle: {"instance": middle, "status": "success", "node_scope": "slurm"},
        oldest: {"instance": oldest, "status": "success", "node_scope": "slurm"},
        "local__repo-1": {
            "instance": "local__repo-1",
            "status": "success",
            "node_scope": "local",
        },
        "failed__repo-1": {
            "instance": "failed__repo-1",
            "status": "failure",
            "node_scope": "slurm",
        },
    }
    local_tests = tmp_path / "tasks" / "local__repo-1" / "tests"
    local_tests.mkdir(parents=True)
    (local_tests / "test.sh").write_text("#!/bin/sh\n")
    monkeypatch.setattr(
        backfill,
        "collect_latest_statuses",
        lambda _run_dir: (latest, [newest, middle, oldest, "local__repo-1"]),
    )

    worker = object.__new__(backfill.RewardBackfillWorker)
    worker.args = SimpleNamespace(instance=None)
    worker.run_dir = tmp_path
    worker.generations = {}
    worker.generation_order = []
    worker.futures = {}
    worker.node_records = MethodType(lambda self: {"node-a": {}}, worker)
    worker._refresh_sequential_records = MethodType(lambda self, force=False: None, worker)
    worker.publish_status = MethodType(lambda self, *_args, **_kwargs: None, worker)

    worker.discover()

    assert worker.generation_order == ["local__repo-1", oldest, middle, newest]
    assert set(worker.generations) == {newest, middle, oldest, "local__repo-1"}


def test_next_ready_dedupes_terminal_and_sequential_claims(tmp_path) -> None:
    instances = [f"owner__repo-{number}" for number in range(1, 5)]
    worker = object.__new__(backfill.RewardBackfillWorker)
    worker.generation_order = instances
    worker.generations = {instance: {} for instance in instances}
    worker.futures = {}
    worker.records = {instances[0]: {"status": "pass", "reward_hack": {"state": "pass"}}}
    worker.sequential_records = {
        instances[1]: {
            **baseline_record(),
            "reward_hack": {"state": "running"},
        },
        instances[2]: {
            **baseline_record(),
            "reward_hack": {"state": "fail"},
        },
        instances[3]: baseline_record(),
    }
    worker._refresh_sequential_records = MethodType(lambda self, force=False: None, worker)

    assert worker.next_ready() == instances[3]


def test_baseline_admission_requires_exact_nop_zero_and_oracle_one() -> None:
    assert backfill.baseline_is_valid(baseline_record())
    assert not backfill.baseline_is_valid(baseline_record(nop_reward=False))
    assert not backfill.baseline_is_valid(baseline_record(oracle_reward=True))
    assert not backfill.baseline_is_valid(baseline_record(nop_reward=1))
    assert not backfill.baseline_is_valid(baseline_record(oracle_reward=0))
    assert not backfill.baseline_is_valid(
        {
            **baseline_record(),
            "nop": {"state": "running", "reward": 0},
        }
    )


def test_next_ready_waits_for_baseline_and_preserves_existing_reward_result() -> None:
    waiting = "owner__repo-waiting"
    valid = "owner__repo-valid"
    worker = object.__new__(backfill.RewardBackfillWorker)
    worker.generation_order = [waiting, valid]
    worker.generations = {waiting: {}, valid: {}}
    worker.futures = {}
    worker.records = {
        # A result produced by an older generation-first worker remains
        # restart-safe, but cannot bypass the Stage 2 admission boundary.
        waiting: {"status": "pass", "reward_hack": {"state": "pass"}},
    }
    worker.sequential_records = {valid: baseline_record()}
    worker._refresh_sequential_records = MethodType(lambda self, force=False: None, worker)

    assert worker.next_ready() == valid
    assert worker.records[waiting]["status"] == "pass"


def test_refreshing_postcheck_ledger_unlocks_stage_three(tmp_path) -> None:
    instance = "owner__repo-1"
    ledger = tmp_path / "postcheck-status.jsonl"
    ledger.write_text(json.dumps({"instance": instance, "attempt": 1, "status": "running"}) + "\n")
    worker = object.__new__(backfill.RewardBackfillWorker)
    worker.postcheck_ledger_path = ledger
    worker._postcheck_signature = None
    worker.sequential_records = {}
    worker.generation_order = [instance]
    worker.generations = {instance: {}}
    worker.futures = {}
    worker.records = {}

    assert worker.next_ready() is None

    with ledger.open("a") as stream:
        stream.write(
            json.dumps(
                {
                    "instance": instance,
                    "attempt": 1,
                    "timestamp": "2026-07-20T00:01:00+00:00",
                    **baseline_record(),
                }
            )
            + "\n"
        )

    assert worker.next_ready() == instance


def _tests_archive(instance: str, *, include_environment: bool = False) -> bytes:
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w:gz") as archive:
        script = b"#!/bin/sh\nexit 0\n"
        info = tarfile.TarInfo(f"{instance}/tests/test.sh")
        info.size = len(script)
        archive.addfile(info, io.BytesIO(script))
        if include_environment:
            dockerfile = b"FROM scratch\n"
            info = tarfile.TarInfo(f"{instance}/environment/Dockerfile")
            info.size = len(dockerfile)
            archive.addfile(info, io.BytesIO(dockerfile))
    return payload.getvalue()


def test_fetch_caches_only_tests_under_worker_data_dir(tmp_path, monkeypatch) -> None:
    instance = "owner__repo-1"
    captured: list[list[str]] = []
    worker = object.__new__(backfill.RewardBackfillWorker)
    worker.tasks_dir = tmp_path / "data" / "reward-backfill-tasks"
    worker.node_map = {
        "node-a": {
            "node": "node-a",
            "job_id": "42",
            "remote_run_dir": "/data/work/slurm-swegen/remote-run",
        }
    }
    worker.args = SimpleNamespace(fetch_timeout=600)
    monkeypatch.setattr(backfill, "job_state", lambda _job_id: "RUNNING")
    monkeypatch.setattr(
        backfill,
        "srun_base",
        lambda node, job_id, state: ["srun", node, str(job_id), state],
    )

    def fake_run(argv, timeout):
        captured.append(argv)
        return SimpleNamespace(stdout=_tests_archive(instance))

    monkeypatch.setattr(backfill, "run_bytes", fake_run)

    destination = worker.fetch_task(instance, {"source_node": "node-a"})

    assert destination == worker.tasks_dir / instance
    assert (destination / "tests" / "test.sh").is_file()
    assert sorted(path.name for path in destination.iterdir()) == ["tests"]
    assert str(worker.tasks_dir) in str(destination)
    assert captured[0][-1] == f"{instance}/tests"
    assert "docker" not in " ".join(captured[0]).lower()
    assert "harbor" not in " ".join(captured[0]).lower()
    assert not list(worker.tasks_dir.glob(f".{instance}.fetch-*"))


def test_fetch_uses_retained_local_tests_without_slurm(tmp_path) -> None:
    instance = "owner__repo-1"
    local_task = tmp_path / "run" / "tasks" / instance
    (local_task / "tests").mkdir(parents=True)
    (local_task / "tests" / "test.sh").write_text("#!/bin/sh\n")
    worker = object.__new__(backfill.RewardBackfillWorker)
    worker.run_dir = tmp_path / "run"
    worker.tasks_dir = tmp_path / "cache"

    assert worker.fetch_task(instance, {}) == local_task


def test_fetch_rejects_archive_that_contains_non_test_task_data(tmp_path, monkeypatch) -> None:
    instance = "owner__repo-1"
    worker = object.__new__(backfill.RewardBackfillWorker)
    worker.tasks_dir = tmp_path / "reward-backfill-tasks"
    worker.node_map = {
        "node-a": {
            "node": "node-a",
            "job_id": "42",
            "remote_run_dir": "/data/work/slurm-swegen/remote-run",
        }
    }
    worker.args = SimpleNamespace(fetch_timeout=600)
    monkeypatch.setattr(backfill, "job_state", lambda _job_id: "RUNNING")
    monkeypatch.setattr(backfill, "srun_base", lambda *_args: ["srun"])
    monkeypatch.setattr(
        backfill,
        "run_bytes",
        lambda *_args, **_kwargs: SimpleNamespace(
            stdout=_tests_archive(instance, include_environment=True)
        ),
    )

    try:
        worker.fetch_task(instance, {"source_node": "node-a"})
    except RuntimeError as error:
        assert "unexpected task entries" in str(error)
    else:
        raise AssertionError("non-test archive data was accepted")


def test_independent_pool_reaches_twenty_active_checks(tmp_path) -> None:
    concurrency = 20
    instances = [f"owner__repo-{number}" for number in range(concurrency)]
    worker = object.__new__(backfill.RewardBackfillWorker)
    worker.args = SimpleNamespace(reward_concurrency=concurrency, retry_delay=600)
    worker.worker_id = "reward-backfill-0"
    worker.generations = {
        instance: {"node": "node-a", "status": "success"} for instance in instances
    }
    worker.records = {}
    worker.futures = {}
    worker.sequential_records = {instance: baseline_record() for instance in instances}
    worker._refresh_sequential_records = MethodType(lambda self, force=False: None, worker)
    worker.llm_config = LLMConfig("spark", "https://endpoint", "spark", "key")
    worker.executor = ThreadPoolExecutor(max_workers=concurrency)
    worker.publish_status = MethodType(lambda self, *_args, **_kwargs: None, worker)

    def append(self, record, publish=True):
        self.records[record["instance"]] = copy.deepcopy(record)

    worker.append = MethodType(append, worker)
    lock = threading.Lock()
    all_started = threading.Event()
    release = threading.Event()
    active = 0
    peak = 0

    def fake_execute(self, instance, _record):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            if active == concurrency:
                all_started.set()
        release.wait(timeout=5)
        with lock:
            active -= 1
        return instance

    worker.execute = MethodType(fake_execute, worker)
    try:
        for instance in instances:
            assert worker.submit(instance)
        assert all_started.wait(timeout=5)
        assert len(worker.futures) == concurrency
        assert peak == concurrency
    finally:
        release.set()
        wait(tuple(worker.futures), timeout=5)
        worker.executor.shutdown(wait=True)


def test_submit_rechecks_baseline_before_writing_queue_record() -> None:
    instance = "owner__repo-1"
    worker = object.__new__(backfill.RewardBackfillWorker)
    worker.generations = {instance: {"node": "node-a", "status": "success"}}
    worker.records = {}
    worker.futures = {}
    worker.sequential_records = {}
    worker._refresh_sequential_records = MethodType(lambda self, force=False: None, worker)

    assert worker.submit(instance) is False
    assert worker.records == {}


def test_counts_use_baseline_valid_as_stage_three_denominator() -> None:
    valid_pending = "owner__repo-valid-pending"
    valid_pass = "owner__repo-valid-pass"
    waiting = "owner__repo-waiting"
    retained = "owner__repo-retained"
    instances = [valid_pending, valid_pass, waiting, retained]
    worker = object.__new__(backfill.RewardBackfillWorker)
    worker.generations = {instance: {} for instance in instances}
    worker.futures = {}
    worker.sequential_records = {
        valid_pending: baseline_record(),
        valid_pass: baseline_record(),
    }
    worker.records = {
        valid_pass: {"status": "pass"},
        retained: {"status": "fail"},
    }

    counts = worker.counts()

    assert counts["generated_success"] == 4
    assert counts["eligible"] == 2
    assert counts["waiting_for_baseline"] == 2
    assert counts["retained_before_baseline"] == 1
    assert counts["not_queued"] == 1
    assert counts["pass"] == 1


def test_finish_records_actual_fallback_model_and_separate_terminal_status(
    tmp_path, monkeypatch
) -> None:
    instance = "owner__repo-1"
    task_dir = tmp_path / "reward-backfill-tasks" / instance
    task_dir.mkdir(parents=True)
    spark = LLMConfig("spark", "https://endpoint", "spark", "key")
    terra = LLMConfig("terra", "https://endpoint", "terra", "key")
    spark_error = HackCheckResult(
        is_hacking=True,
        reason="quota exhausted",
        error="HTTP 429: insufficient_quota",
    )
    clean = HackCheckResult(
        is_hacking=False,
        reason="meaningful runtime assertions",
        test_framework="pytest",
    )
    result = backfill.BackfillWorkResult(
        task_dir=task_dir,
        reward=(terra, clean, [(spark, spark_error), (terra, clean)]),
    )
    record = backfill.queued_snapshot(instance, {"node": "node-a"}, "backfill-0")
    record.update({"status": "running"})
    record["reward_hack"]["state"] = "running"
    worker = object.__new__(backfill.RewardBackfillWorker)
    worker.logs_dir = tmp_path / "reward-backfill-logs"
    worker.records = {}
    worker.futures = {}
    worker.append = MethodType(
        lambda self, value, publish=True: self.records.update(
            {value["instance"]: copy.deepcopy(value)}
        ),
        worker,
    )
    monkeypatch.setattr(backfill, "write_instance_log", lambda *_args, **_kwargs: None)

    worker.finish(record, result)

    saved = worker.records[instance]
    assert saved["status"] == "pass"
    assert saved["reward_hack"]["state"] == "pass"
    assert saved["reward_hack"]["model"] == "terra"
    assert saved["reward_hack"]["attempted_models"] == ["spark", "terra"]
    assert saved["reward_hack"]["fallback_used"] is True


def test_default_cli_is_docker_free_twenty_concurrency(tmp_path) -> None:
    args = backfill.parse_args(
        [
            "--run-dir",
            str(tmp_path / "run"),
            "--plan",
            str(tmp_path / "plan.json"),
        ]
    )

    assert args.reward_concurrency == 20
    source = Path(backfill.__file__).read_text(encoding="utf-8")
    assert "run_harbor_agent" not in source
    assert "import docker" not in source
