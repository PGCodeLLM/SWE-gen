import http.client
import json
import stat
import subprocess
import tarfile
import threading
from datetime import UTC, datetime, timedelta
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest
import yaml

from run_dashboard import (
    CONTROL_ROUTE_MAX_CONCURRENCY,
    DASHBOARD_HTML,
    LEGACY_DASHBOARD_HTML,
    ControlConfigError,
    ControlConfigStore,
    StatusCache,
    TaskExportManager,
    active_instances,
    build_slurm_node_stats,
    build_stage_i_throughput,
    calculate_status,
    ensure_control_token,
    load_historical_oracle_zero_instances,
    load_latest_postchecks,
    load_latest_reward_backfills,
    load_latest_statuses,
    load_model_catalog,
    load_postcheck_worker_status,
    load_reward_backfill_worker_status,
    make_handler,
    merge_reward_backfill_evidence,
    postcheck_journal_paths,
    reward_hack_accepted_records,
    reward_hack_filtered_records,
    status_journal_paths,
    success_ledger_paths,
)


def write_jsonl(path, records) -> None:
    path.write_text("".join(json.dumps(record) + "\n" for record in records))


def write_proc_command(proc_root, pid, cwd, *args) -> None:
    process = proc_root / str(pid)
    process.mkdir(parents=True)
    (process / "cmdline").write_bytes(b"\0".join(value.encode() for value in args) + b"\0")
    (process / "cwd").symlink_to(cwd, target_is_directory=True)


def test_active_instances_resolves_relative_state_dir_from_process_cwd(tmp_path) -> None:
    controller = tmp_path / "controller"
    controller_run = controller / "runs" / "shared-name"
    controller_run.mkdir(parents=True)
    remote = tmp_path / "remote"
    remote_run = remote / "runs" / "shared-name"
    remote_run.mkdir(parents=True)
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    write_proc_command(
        proc_root,
        101,
        remote,
        "/remote/.venv/bin/swegen",
        "create",
        "--repo",
        "owner/repo",
        "--pr",
        "1",
        "--state-dir",
        "runs/shared-name",
    )

    assert active_instances(controller_run, proc_root) == []


def test_active_instances_counts_matching_relative_and_absolute_state_dirs(tmp_path) -> None:
    controller = tmp_path / "controller"
    run_dir = controller / "runs" / "run"
    run_dir.mkdir(parents=True)
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    write_proc_command(
        proc_root,
        101,
        controller,
        "/controller/.venv/bin/swegen",
        "create",
        "--repo",
        "Owner/Relative",
        "--pr",
        "2",
        "--state-dir",
        "runs/run",
    )
    write_proc_command(
        proc_root,
        102,
        tmp_path,
        "/controller/.venv/bin/swegen",
        "create",
        "--repo",
        "Owner/Absolute",
        "--pr",
        "3",
        "--state-dir",
        str(run_dir),
    )
    write_proc_command(
        proc_root,
        103,
        tmp_path,
        "/remote/.venv/bin/swegen",
        "create",
        "--repo",
        "Owner/Other",
        "--pr",
        "4",
        "--state-dir",
        str(tmp_path / "other-run"),
    )

    assert active_instances(run_dir, proc_root) == [
        "owner__absolute-3",
        "owner__relative-2",
    ]


def test_calculate_status_uses_latest_result_per_instance(tmp_path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    input_jsonl = tmp_path / "input.jsonl"
    input_jsonl.write_text("{}\n{}\n{}\n")
    records = [
        {"instance": "a", "status": "failure", "timestamp": "1"},
        {"instance": "b", "status": "success", "timestamp": "2"},
        {"instance": "a", "status": "success", "timestamp": "3"},
    ]
    write_jsonl(run_dir / "orchestrator-instance-status.jsonl", records)
    monkeypatch.setattr("run_dashboard.active_instances", lambda _run_dir: ["c"])

    status = calculate_status(run_dir, input_jsonl)

    assert status["success"] == 2
    assert status["failure"] == 0
    assert status["processed"] == 2
    assert status["total"] == 3
    assert status["yield_percent"] == 100.0
    assert status["completion_percent"] == 66.67
    assert status["active_workers"] == 1
    assert status["recent"][0]["instance"] == "a"
    assert status["validation_summary"] == {
        "baseline_valid": 0,
        "validation_failed": 0,
        "generated_unvalidated": 0,
        "generated_validation_unknown": 2,
        "other_failures": 0,
    }


def test_stage_i_throughput_stacks_status_colors_in_24_windows() -> None:
    now = datetime(2026, 7, 22, 12, 7, tzinfo=UTC)
    window_start = datetime(2026, 7, 22, 6, 15, tzinfo=UTC)
    latest = {
        "first": {
            "status": "success",
            "timestamp": window_start.isoformat(),
            "validation": {"nop_reward": 0, "oracle_reward": 1},
        },
        "previous": {
            "status": "success",
            "timestamp": (now.replace(minute=0) - timedelta(minutes=1)).isoformat(),
            "oracle_reward": 0,
        },
        "current": {
            "status": "failure",
            "timestamp": now.replace(minute=1).isoformat(),
            "failure_reason": "Transient network/API error",
        },
        "historical-oracle-zero": {
            "status": "failure",
            "timestamp": now.replace(minute=2).isoformat(),
            "failure_reason": "Validation failed (NOP or Oracle)",
        },
        "outside": {
            "status": "failure",
            "timestamp": (window_start - timedelta(seconds=1)).isoformat(),
        },
        "reset": {
            "status": "unprocessed",
            "timestamp": now.isoformat(),
            "source_timestamp": (
                now.replace(minute=0) - timedelta(minutes=2)
            ).isoformat(),
        },
    }

    throughput = build_stage_i_throughput(latest, now=now)

    assert throughput["bucket_minutes"] == 15
    assert throughput["bucket_count"] == 24
    assert len(throughput["buckets"]) == 24
    assert throughput["buckets"][0]["green"] == 1
    assert throughput["buckets"][-2]["green"] == 1
    assert throughput["buckets"][-2]["gray"] == 1
    assert throughput["buckets"][-1] == {
        "start": "2026-07-22T12:00:00+00:00",
        "end": "2026-07-22T12:15:00+00:00",
        "processed": 2,
        "green": 0,
        "red": 2,
        "gray": 0,
    }
    assert sum(bucket["processed"] for bucket in throughput["buckets"]) == 5
    assert all(
        bucket["processed"] == bucket["green"] + bucket["red"] + bucket["gray"]
        for bucket in throughput["buckets"]
    )


def test_unprocessed_tombstone_hides_failure_until_a_later_retry(tmp_path) -> None:
    failure_journal = tmp_path / "orchestrator-instance-status-old.jsonl"
    reset_journal = tmp_path / "orchestrator-instance-status-stage1-api-reset-r9.jsonl"
    retry_journal = tmp_path / "orchestrator-instance-status-r9.jsonl"
    write_jsonl(
        failure_journal,
        [
            {
                "instance": "owner__repo-1",
                "status": "failure",
                "timestamp": "2026-07-22T00:00:00+00:00",
            }
        ],
    )
    write_jsonl(
        reset_journal,
        [
            {
                "instance": "owner__repo-1",
                "status": "unprocessed",
                "timestamp": "2026-07-22T01:00:00+00:00",
            }
        ],
    )

    latest, order = load_latest_statuses([failure_journal, reset_journal, retry_journal])

    assert latest == {}
    assert order == []
    latest_with_reset, reset_order = load_latest_statuses(
        [failure_journal, reset_journal, retry_journal],
        include_unprocessed=True,
    )
    assert latest_with_reset["owner__repo-1"]["status"] == "unprocessed"
    assert reset_order == ["owner__repo-1"]

    write_jsonl(
        retry_journal,
        [
            {
                "instance": "owner__repo-1",
                "status": "success",
                "timestamp": "2026-07-22T02:00:00+00:00",
            }
        ],
    )

    latest, order = load_latest_statuses([failure_journal, reset_journal, retry_journal])

    assert latest["owner__repo-1"]["status"] == "success"
    assert order == ["owner__repo-1"]


def test_unprocessed_tombstone_resets_failure_and_beats_older_ledger(
    tmp_path, monkeypatch
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    input_jsonl = tmp_path / "input.jsonl"
    input_jsonl.write_text("{}\n" * 3)
    write_jsonl(
        run_dir / "orchestrator-instance-status.jsonl",
        [
            {"instance": "retry", "status": "failure", "timestamp": "1"},
            {"instance": "done", "status": "failure", "timestamp": "1"},
        ],
    )
    write_jsonl(
        run_dir / "orchestrator-instance-status-reset-r9.jsonl",
        [{"instance": "retry", "status": "unprocessed", "timestamp": "3"}],
    )
    write_jsonl(
        run_dir / "create.jsonl",
        [
            {"task_id": "retry", "ts": "2", "harbor": "/tasks/retry"},
            {"task_id": "done", "ts": "2", "harbor": "/tasks/done"},
        ],
    )
    monkeypatch.setattr("run_dashboard.active_instances", lambda _run_dir: [])

    status = calculate_status(run_dir, input_jsonl)

    assert status["success"] == 1
    assert status["failure"] == 0
    assert status["processed"] == 1
    assert status["remaining"] == 2
    assert status["completion_percent"] == 33.33
    assert status["recent"][0]["instance"] == "retry"
    assert status["recent"][0]["status"] == "unprocessed"


def test_recent_success_keeps_postcheck_evidence_alongside_visible_tombstone(
    tmp_path, monkeypatch
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    input_jsonl = tmp_path / "input.jsonl"
    input_jsonl.write_text("{}\n" * 2)
    write_jsonl(
        run_dir / "orchestrator-instance-status.jsonl",
        [
            {"instance": "accepted", "status": "success", "timestamp": "2"},
            {"instance": "retry", "status": "failure", "timestamp": "1"},
        ],
    )
    write_jsonl(
        run_dir / "orchestrator-instance-status-reset-r9.jsonl",
        [{"instance": "retry", "status": "unprocessed", "timestamp": "3"}],
    )
    worker_dir = run_dir / ".validation-worker"
    worker_dir.mkdir()
    write_jsonl(
        worker_dir / "postcheck-status.jsonl",
        [
            {
                "instance": "accepted",
                "attempt": 1,
                "status": "accepted",
                "timestamp": "4",
                "nop": {"state": "pass", "reward": 0},
                "oracle": {"state": "pass", "reward": 1},
                "reward_hack": {
                    "state": "pass",
                    "is_hacking": False,
                    "reason": "clean",
                },
            }
        ],
    )
    monkeypatch.setattr("run_dashboard.active_instances", lambda _run_dir: [])

    status = calculate_status(run_dir, input_jsonl)

    recent = {record["instance"]: record for record in status["recent"]}
    assert status["recent"][0]["instance"] == "retry"
    assert recent["retry"]["status"] == "unprocessed"
    assert recent["accepted"]["validation_outcome"] == "baseline_valid"
    assert status["postcheck"]["accepted"] == 1


def test_calculate_status_merges_journals_and_success_ledger(tmp_path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    input_jsonl = tmp_path / "input.jsonl"
    input_jsonl.write_text("{}\n" * 6)
    write_jsonl(
        run_dir / "orchestrator-instance-status.jsonl",
        [
            {"instance": "a", "status": "failure", "timestamp": "1"},
            {"instance": "b", "status": "success", "timestamp": "2"},
        ],
    )
    write_jsonl(
        run_dir / "orchestrator-instance-status-sg-a-r3.jsonl",
        [
            {"instance": "a", "status": "success", "timestamp": "3"},
            {"instance": "c", "status": "failure", "timestamp": "4"},
            {"instance": "d", "status": "failure", "timestamp": "5"},
        ],
    )
    write_jsonl(
        run_dir / "orchestrator-instance-status.before-cleanup.jsonl",
        [{"instance": "backup-only", "status": "success", "timestamp": "9"}],
    )
    write_jsonl(
        run_dir / "create.jsonl",
        [
            {"task_id": "a", "ts": "3", "harbor": "/tasks/a"},
            {"task_id": "b", "ts": "2", "harbor": "/tasks/b"},
            {"task_id": "c", "ts": "6", "harbor": "/tasks/c"},
            {"task_id": "ledger-only", "ts": "7", "harbor": "/tasks/ledger-only"},
        ],
    )
    monkeypatch.setattr("run_dashboard.active_instances", lambda _run_dir: [])

    status = calculate_status(run_dir, input_jsonl)

    assert status["success"] == 4
    assert status["failure"] == 1
    assert status["processed"] == 5
    assert status["recent"][0]["instance"] == "ledger-only"
    assert "backup-only" not in {record["instance"] for record in status["recent"]}
    assert status["validation_summary"] == {
        "baseline_valid": 0,
        "validation_failed": 0,
        "generated_unvalidated": 0,
        "generated_validation_unknown": 4,
        "other_failures": 1,
    }


def test_validation_summary_requires_explicit_baseline_evidence(tmp_path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    node_dir = run_dir / "slurm-nodes" / "node-a"
    node_dir.mkdir(parents=True)
    input_jsonl = tmp_path / "input.jsonl"
    input_jsonl.write_text("{}\n" * 6)
    write_jsonl(
        node_dir / "orchestrator-instance-status.jsonl",
        [
            {
                "instance": "journal-valid",
                "status": "success",
                "timestamp": "1",
                "nop_passed": True,
                "oracle_passed": True,
            },
            {
                "instance": "unvalidated",
                "status": "success",
                "timestamp": "2",
                "validation_status": "validation_skipped",
            },
            {
                "instance": "unknown",
                "status": "success",
                "timestamp": "3",
                "validated": True,
            },
            {
                "instance": "validation-failed",
                "status": "failure",
                "timestamp": "4",
                "failure_reason": "Validation failed (NOP or Oracle)",
            },
            {
                "instance": "other-failed",
                "status": "failure",
                "timestamp": "5",
                "failure_reason": "Git checkout failed",
            },
        ],
    )
    write_jsonl(
        node_dir / "create.jsonl",
        [
            {"task_id": "journal-valid", "ts": "6"},
            {
                "task_id": "ledger-valid",
                "ts": "7",
                "validation": {"nop_reward": 0, "oracle_reward": 1},
            },
        ],
    )
    monkeypatch.setattr("run_dashboard.active_instances", lambda _run_dir: [])

    status = calculate_status(run_dir, input_jsonl)

    assert status["success"] == 4
    assert status["failure"] == 2
    assert status["validation_summary"] == {
        "baseline_valid": 2,
        "validation_failed": 1,
        "generated_unvalidated": 1,
        "generated_validation_unknown": 1,
        "other_failures": 1,
    }
    outcomes = {record["instance"]: record["validation_outcome"] for record in status["recent"]}
    assert outcomes == {
        "journal-valid": "baseline_valid",
        "ledger-valid": "baseline_valid",
        "other-failed": "other_failure",
        "unknown": "generated_validation_unknown",
        "unvalidated": "generated_unvalidated",
        "validation-failed": "validation_failed",
    }


def test_status_journal_paths_excludes_backups(tmp_path) -> None:
    live = tmp_path / "orchestrator-instance-status-hk-a-r3.jsonl"
    legacy = tmp_path / "orchestrator-instance-status.jsonl"
    backup = tmp_path / "orchestrator-instance-status.before-cleanup.jsonl"
    unrelated = tmp_path / "other.jsonl"
    for path in (live, legacy, backup, unrelated):
        path.write_text("")

    assert status_journal_paths(tmp_path) == [live, legacy]


def test_calculate_status_aggregates_nested_slurm_nodes(tmp_path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    node_a = run_dir / "slurm-nodes" / "node-a"
    node_b = run_dir / "slurm-nodes" / "node-b"
    node_a.mkdir(parents=True)
    node_b.mkdir(parents=True)
    input_jsonl = tmp_path / "input.jsonl"
    input_jsonl.write_text("{}\n" * 5)
    write_jsonl(
        node_a / "orchestrator-instance-status-a.jsonl",
        [
            {"instance": "a", "status": "success", "timestamp": "2"},
            {"instance": "duplicate", "status": "failure", "timestamp": "3"},
            {"instance": "failed", "status": "failure", "timestamp": "7"},
        ],
    )
    write_jsonl(
        node_b / "orchestrator-instance-status-b.jsonl",
        [{"instance": "b", "status": "success", "timestamp": "4"}],
    )
    write_jsonl(
        node_b / "create.jsonl",
        [
            {"task_id": "duplicate", "ts": "5"},
            {"task_id": "ledger-only", "ts": "6"},
        ],
    )
    (run_dir / "slurm-health.json").write_text(
        json.dumps(
            {
                "active_workers": 5,
                "expected_workers": 12,
                "nodes": [
                    {
                        "node": "node-a",
                        "state": "RUNNING",
                        "active_worker_processes": 3,
                        "expected_workers": 4,
                    },
                    {
                        "node": "node-b",
                        "state": "COMPLETING",
                        "active_worker_processes": 2,
                        "expected_workers": 4,
                    },
                    {
                        "node": "node-zero",
                        "state": "PENDING",
                        "active_worker_processes": 0,
                        "expected_workers": 4,
                    },
                ],
            }
        )
    )
    monkeypatch.setattr("run_dashboard.active_instances", lambda _run_dir: [])

    status = calculate_status(run_dir, input_jsonl)

    assert status["success"] == 4
    assert status["failure"] == 1
    assert status["active_workers"] == 5
    assert status["active_workers_local"] == 0
    assert status["active_workers_slurm"] == 5
    assert status["success_by_slurm_node"] == {"node-a": 1, "node-b": 3}
    assert status["slurm_nodes"] == [
        {
            "node": "node-a",
            "success": 1,
            "failure": 1,
            "active_workers": 3,
            "expected_workers": 4,
            "state": "RUNNING",
            "processed": 2,
            "yield_percent": 50.0,
        },
        {
            "node": "node-b",
            "success": 3,
            "failure": 0,
            "active_workers": 2,
            "expected_workers": 4,
            "state": "COMPLETING",
            "processed": 3,
            "yield_percent": 100.0,
        },
        {
            "node": "node-zero",
            "success": 0,
            "failure": 0,
            "active_workers": 0,
            "expected_workers": 4,
            "state": "PENDING",
            "processed": 0,
            "yield_percent": 0.0,
        },
    ]
    assert {record["node"] for record in status["recent"]} == {"node-a", "node-b"}


def test_slurm_node_table_excludes_historical_nodes_not_in_current_health() -> None:
    latest = {
        "current": {
            "node_scope": "slurm",
            "node": "node-current",
            "status": "success",
        },
        "historical": {
            "node_scope": "slurm",
            "node": "node-cancelled",
            "status": "success",
        },
    }
    health = {
        "nodes": [
            {
                "node": "node-current",
                "state": "RUNNING",
                "active_worker_processes": 24,
                "expected_workers": 24,
            }
        ]
    }

    rows = build_slurm_node_stats(latest, health)

    assert [row["node"] for row in rows] == ["node-current"]
    assert rows[0]["success"] == 1


def test_dashboard_renders_slurm_nodes_with_text_content() -> None:
    assert "SWE-gen pipeline" in DASHBOARD_HTML
    assert "Slurm control" in DASHBOARD_HTML
    assert 'id="resumeButton"' in DASHBOARD_HTML
    assert 'id="pauseButton"' in DASHBOARD_HTML
    assert 'id="routeSg"' in DASHBOARD_HTML
    assert 'id="routeHk"' in DASHBOARD_HTML
    assert 'id="routeDe"' in DASHBOARD_HTML
    assert 'id="modelOpus"' in DASHBOARD_HTML
    assert 'id="modelSonnet"' in DASHBOARD_HTML
    assert 'id="applyModelsButton"' in DASHBOARD_HTML
    assert 'id="controlToken" type="password"' in DASHBOARD_HTML
    assert "sessionStorage" in DASHBOARD_HTML
    assert "X-SWEGEN-Control-Token" in DASHBOARD_HTML
    assert "Stage 1" in DASHBOARD_HTML
    assert "Stage 2" in DASHBOARD_HTML
    assert "Stage 3" in DASHBOARD_HTML
    assert "Reward Hack Filter" in DASHBOARD_HTML
    assert "NOP=0 and Oracle=1" in DASHBOARD_HTML
    assert "Runs only on Stage 2 green" in DASHBOARD_HTML
    assert 'id="stageSwegenGreenBar"' in DASHBOARD_HTML
    assert 'id="stageSwegenRedBar"' in DASHBOARD_HTML
    assert 'id="stageSwegenGrayBar"' in DASHBOARD_HTML
    assert 'id="stageOneThroughput"' in DASHBOARD_HTML
    assert "Processed per 15 minutes" in DASHBOARD_HTML
    assert "throughput-segment" in DASHBOARD_HTML
    assert "Oracle passed" in DASHBOARD_HTML
    assert "Oracle failed" in DASHBOARD_HTML
    assert 'id="stageNopOracleGreenBar"' in DASHBOARD_HTML
    assert 'id="stageNopOracleRedBar"' in DASHBOARD_HTML
    assert 'id="stageNopOracleGrayBar"' in DASHBOARD_HTML
    assert 'id="stageRewardGreenBar"' in DASHBOARD_HTML
    assert 'id="stageRewardRedBar"' in DASHBOARD_HTML
    assert 'id="stageRewardGrayBar"' in DASHBOARD_HTML
    assert "Accepted" in DASHBOARD_HTML
    assert "Filtered" in DASHBOARD_HTML
    assert "renderWorkerRow('stageSwegenWorker'" in DASHBOARD_HTML
    assert "renderWorkerRow('stageNopOracleWorker'" in DASHBOARD_HTML
    assert "renderWorkerRow('stageRewardWorker'" in DASHBOARD_HTML
    assert "NOP and Oracle remain sequential" not in LEGACY_DASHBOARD_HTML
    assert "worker.baseline_active_count" in LEGACY_DASHBOARD_HTML
    assert "worker.baseline_concurrency" in LEGACY_DASHBOARD_HTML
    assert (
        "NOP/Oracle ${fmt(baselineActive)}/${fmt(baselineCapacity)} active" in LEGACY_DASHBOARD_HTML
    )
    assert 'id="downloadFilteredPackButton"' in DASHBOARD_HTML
    assert "/api/export/reward-hack-accepted.tar.gz" in DASHBOARD_HTML
    assert 'id="generatedCount"' not in DASHBOARD_HTML
    assert 'id="baselineCount"' not in DASHBOARD_HTML
    assert 'id="acceptedCount"' not in DASHBOARD_HTML
    assert 'id="pipelineNodeRows"' not in DASHBOARD_HTML
    assert 'id="postcheckStageRows"' not in DASHBOARD_HTML
    assert 'id="postcheckRows"' not in DASHBOARD_HTML
    assert 'id="recent"' not in DASHBOARD_HTML


def make_filtered_export_fixture(tmp_path: Path) -> tuple[Path, Path]:
    run_dir = tmp_path / "run"
    node_dir = run_dir / "slurm-nodes" / "node-a"
    task_dir = node_dir / "tasks_voyager_postprocessed" / "owner__repo-1"
    task_dir.mkdir(parents=True)
    (task_dir / "task.toml").write_text("[task]\n", encoding="utf-8")
    (task_dir / "instruction.md").write_text("filtered\n", encoding="utf-8")
    write_jsonl(
        node_dir / "orchestrator-instance-status.jsonl",
        [
            {
                "instance": "owner__repo-1",
                "status": "success",
                "timestamp": "2026-07-22T00:00:00+00:00",
                "node": "node-a",
            }
        ],
    )
    (run_dir / ".validation-worker").mkdir(parents=True, exist_ok=True)
    write_jsonl(
        run_dir / ".validation-worker" / "postcheck-status.jsonl",
        [
            {
                "instance": "owner__repo-1",
                "status": "rejected",
                "timestamp": "2026-07-22T00:01:00+00:00",
                "nop": {"state": "pass", "reward": 0},
                "oracle": {"state": "pass", "reward": 1},
                "reward_hack": {
                    "state": "fail",
                    "is_hacking": True,
                    "reason": "test-only assertions",
                },
            }
        ],
    )
    return run_dir, task_dir


def test_reward_hack_filtered_records_require_valid_baseline(tmp_path) -> None:
    run_dir, _task_dir = make_filtered_export_fixture(tmp_path)

    records = reward_hack_filtered_records(run_dir)

    assert list(records) == ["owner__repo-1"]
    assert records["owner__repo-1"]["source_node"] == "node-a"


def make_accepted_export_fixture(tmp_path: Path) -> tuple[Path, Path]:
    run_dir, task_dir = make_filtered_export_fixture(tmp_path)
    postcheck_path = run_dir / ".validation-worker" / "postcheck-status.jsonl"
    record = json.loads(postcheck_path.read_text(encoding="utf-8"))
    record["status"] = "accepted"
    record["reward_hack"] = {
        "state": "pass",
        "is_hacking": False,
        "reason": "clean runtime assertions",
    }
    postcheck_path.write_text(json.dumps(record) + "\n", encoding="utf-8")
    return run_dir, task_dir


def test_reward_hack_accepted_records_require_clean_filter_result(tmp_path) -> None:
    run_dir, _task_dir = make_accepted_export_fixture(tmp_path)

    records = reward_hack_accepted_records(run_dir)

    assert list(records) == ["owner__repo-1"]
    assert records["owner__repo-1"]["reward_hack"]["is_hacking"] is False


def test_task_export_sync_mirrors_every_node_component(tmp_path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    plan = {
        "run_name": "run",
        "nodes": [
            {
                "node": "node-a",
                "node_ip": "192.0.2.10",
                "remote_run_dir": "/remote/run",
            },
            {
                "node": "node-b",
                "node_ip": "192.0.2.11",
                "remote_run_dir": "/remote/run",
            },
        ],
    }
    (run_dir / "slurm-stage1-r9-4n-plan.json").write_text(json.dumps(plan), encoding="utf-8")
    commands = []

    def fake_rsync(command, **_kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    manager = TaskExportManager(run_dir, rsync_runner=fake_rsync)

    status = manager.sync_once()

    assert status["state"] == "ready"
    assert len(commands) == 4
    assert all(command[0] == "rsync" for command in commands)
    assert {command[-2] for command in commands} == {
        "alex@192.0.2.10:/remote/run/tasks/",
        "alex@192.0.2.10:/remote/run/tasks_voyager_postprocessed/",
        "alex@192.0.2.11:/remote/run/tasks/",
        "alex@192.0.2.11:/remote/run/tasks_voyager_postprocessed/",
    }


def test_task_export_falls_back_to_collected_local_node_mirror(tmp_path) -> None:
    run_dir = tmp_path / "run"
    local_source = run_dir / "slurm-nodes" / "node-a" / "tasks"
    local_postprocessed = run_dir / "slurm-nodes" / "node-a" / "tasks_voyager_postprocessed"
    local_source.mkdir(parents=True)
    local_postprocessed.mkdir(parents=True)
    (local_source / "owner__repo-1").mkdir()
    plan = {
        "run_name": "run",
        "nodes": [{
            "node": "node-a",
            "node_ip": "192.0.2.10",
            "remote_run_dir": "/remote/run",
        }],
    }
    (run_dir / "slurm-stage1-r9-4n-plan.json").write_text(json.dumps(plan), encoding="utf-8")
    commands = []

    def fake_rsync(command, **_kwargs):
        commands.append(command)
        if command[-2].startswith("alex@"):
            return subprocess.CompletedProcess(command, 255, "", "SSH unavailable")
        return subprocess.CompletedProcess(command, 0, "", "")

    manager = TaskExportManager(run_dir, rsync_runner=fake_rsync)

    status = manager.sync_once()

    assert status["state"] == "ready_with_warnings"
    assert len(status["warnings"]) == 2
    assert len(commands) == 4


def test_task_export_creates_manifest_and_keeps_five_packs(tmp_path) -> None:
    run_dir, _task_dir = make_accepted_export_fixture(tmp_path)
    manager = TaskExportManager(run_dir, tmp_path / "export")

    paths = [manager.create_pack() for _ in range(6)]

    assert not paths[0].exists()
    assert len(manager.pack_paths()) == 5
    with tarfile.open(paths[-1], mode="r:gz") as archive:
        names = archive.getnames()
    assert "owner__repo-1/task.toml" in names
    assert "manifest.json" in names


def test_postcheck_ledger_updates_validation_and_live_worker_summary(tmp_path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    node_dir = run_dir / "slurm-nodes" / "node-a"
    node_dir.mkdir(parents=True)
    input_jsonl = tmp_path / "input.jsonl"
    input_jsonl.write_text("{}\n" * 4)
    write_jsonl(
        node_dir / "orchestrator-instance-status.jsonl",
        [
            {"instance": instance, "status": "success", "timestamp": str(index)}
            for index, instance in enumerate(("accepted", "rejected", "error", "missing"), 1)
        ],
    )
    worker_dir = run_dir / ".validation-worker"
    worker_dir.mkdir()
    ledger = worker_dir / "postcheck-status.jsonl"
    accepted = {
        "event": "postcheck_status",
        "timestamp": "10",
        "instance": "accepted",
        "attempt": 1,
        "status": "accepted",
        "stage": "complete",
        "nop": {"state": "pass", "reward": 0},
        "oracle": {"state": "pass", "reward": 1},
        "reward_hack": {"state": "pass", "is_hacking": False, "reason": "clean"},
    }
    rejected = {
        **accepted,
        "timestamp": "11",
        "instance": "rejected",
        "status": "rejected",
        "nop": {"state": "fail", "reward": 1},
    }
    errored = {
        **accepted,
        "timestamp": "12",
        "instance": "error",
        "status": "error",
        "stage": "reward_hack",
        "reward_hack": {
            "state": "error",
            "is_hacking": None,
            "error": "endpoint unavailable",
        },
        "error": "endpoint unavailable",
    }
    ledger.write_text(
        json.dumps({**accepted, "status": "running", "timestamp": "09"})
        + "\n{truncated\n"
        + json.dumps(accepted)
        + "\n"
        + json.dumps(rejected)
        + "\n"
        + json.dumps(errored)
        + "\n"
    )
    (worker_dir / "worker-status.json").write_text(
        json.dumps(
            {
                "state": "running",
                "current_instance": "error",
                "current_stage": "reward_hack",
                "baseline_concurrency": 20,
                "baseline_active_count": 2,
                "current_instances": ["error", "accepted"],
                "active_stages": {"error": "oracle", "accepted": "nop"},
                "timestamp": "2999-01-01T00:00:00Z",
                "reward_model": "gpt-5.3-codex-spark",
                "reward_fallback_model": "gpt-5.6-terra",
                "reward_concurrency": 20,
                "reward_active_count": 7,
            }
        )
    )
    monkeypatch.setattr("run_dashboard.active_instances", lambda _run_dir: [])

    status = calculate_status(run_dir, input_jsonl)

    assert status["validation_summary"] == {
        "baseline_valid": 2,
        "validation_failed": 1,
        "generated_unvalidated": 0,
        "generated_validation_unknown": 1,
        "other_failures": 0,
    }
    postcheck = status["postcheck"]
    assert postcheck["eligible"] == 4
    assert postcheck["not_queued"] == 1
    assert postcheck["accepted"] == 1
    assert postcheck["rejected"] == 1
    assert postcheck["errors"] == 1
    assert postcheck["reward_hack"] == {
        "pass": 2,
        "fail": 0,
        "error": 1,
        "pending": 1,
        "running": 0,
    }
    assert postcheck["worker"]["current_instance"] == "error"
    assert postcheck["worker"]["baseline_concurrency"] == 20
    assert postcheck["worker"]["baseline_active_count"] == 2
    assert postcheck["worker"]["current_instances"] == ["error", "accepted"]
    assert postcheck["worker"]["active_stages"] == {
        "error": "oracle",
        "accepted": "nop",
    }
    assert status["health"]["baseline"]["baseline_concurrency"] == 20
    assert status["health"]["baseline"]["baseline_active_count"] == 2
    assert status["health"]["baseline"]["current_instances"] == ["error", "accepted"]
    assert postcheck["worker"]["reward_model"] == "gpt-5.3-codex-spark"
    assert postcheck["worker"]["reward_fallback_model"] == "gpt-5.6-terra"
    assert postcheck["worker"]["reward_concurrency"] == 20
    assert postcheck["worker"]["reward_active_count"] == 7
    assert "recent" not in postcheck
    assert status["funnel"] == {
        "generated": {
            "count": 4,
            "processed": 4,
            "denominator": 4,
            "failures": 0,
            "yield_percent": 100.0,
        },
        "baseline_valid": {
            "count": 2,
            "denominator": 4,
            "yield_percent": 50.0,
            "conversion_percent": 50.0,
            "pending": 1,
            "running": 0,
            "rejected": 1,
            "errors": 0,
        },
        "fully_accepted": {
            "count": 1,
            "denominator": 2,
            "yield_percent": 50.0,
            "conversion_percent": 50.0,
            "overall_percent": 25.0,
            "pending": 0,
            "running": 0,
            "flagged": 0,
            "errors": 1,
        },
    }
    assert status["pipeline"] == {
        "stage_i": {
            "denominator": 4,
            "green": 2,
            "red": 0,
            "gray": 2,
            "breakdown": {
                "unprocessed": 0,
                "transient_api": 0,
                "incomplete_or_other": 0,
                "validation_unresolved": 2,
            },
            "red_breakdown": {
                "current_exact": 0,
                "historical_recovered": 0,
                "historical_evidence_instances": 0,
            },
        },
        "stage_ii": {
            "denominator": 2,
            "green": 1,
            "red": 0,
            "gray": 1,
            "breakdown": {"pending": 0, "running": 0, "errors": 1},
        },
    }


def test_pipeline_stage_i_red_requires_explicit_oracle_zero(tmp_path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    input_jsonl = tmp_path / "input.jsonl"
    input_jsonl.write_text("{}\n" * 4)
    write_jsonl(
        run_dir / "orchestrator-instance-status.jsonl",
        [
            {"instance": "oracle-zero", "status": "success", "timestamp": "1"},
            {
                "instance": "ambiguous-validation",
                "status": "failure",
                "timestamp": "2",
                "failure_reason": "Validation failed (NOP or Oracle)",
            },
            {
                "instance": "transient",
                "status": "failure",
                "timestamp": "3",
                "failure_reason": "Transient network/API error",
            },
        ],
    )
    worker_dir = run_dir / ".validation-worker"
    worker_dir.mkdir()
    write_jsonl(
        worker_dir / "postcheck-status.jsonl",
        [
            {
                "instance": "oracle-zero",
                "timestamp": "4",
                "status": "rejected",
                "stage": "complete",
                "nop": {"state": "pass", "reward": 0},
                "oracle": {"state": "fail", "reward": 0},
                "reward_hack": {
                    "state": "pass",
                    "is_hacking": False,
                    "reason": "clean",
                },
            }
        ],
    )
    monkeypatch.setattr("run_dashboard.active_instances", lambda _run_dir: [])

    status = calculate_status(run_dir, input_jsonl)

    assert status["pipeline"]["stage_i"] == {
        "denominator": 4,
        "green": 0,
        "red": 1,
        "gray": 3,
        "breakdown": {
            "unprocessed": 1,
            "transient_api": 1,
            "incomplete_or_other": 0,
            "validation_unresolved": 1,
        },
        "red_breakdown": {
            "current_exact": 1,
            "historical_recovered": 0,
            "historical_evidence_instances": 0,
        },
    }
    assert status["postcheck"]["reward_clean"] == 0
    assert status["pipeline"]["stage_ii"] == {
        "denominator": 0,
        "green": 0,
        "red": 0,
        "gray": 0,
        "breakdown": {"pending": 0, "running": 0, "errors": 0},
    }


def test_pipeline_stage_ii_partitions_baseline_valid_reward_outcomes(tmp_path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    input_jsonl = tmp_path / "input.jsonl"
    instances = ("clean", "flagged", "pending", "error")
    input_jsonl.write_text("{}\n" * len(instances))
    write_jsonl(
        run_dir / "orchestrator-instance-status.jsonl",
        [
            {"instance": instance, "status": "success", "timestamp": str(index)}
            for index, instance in enumerate(instances, 1)
        ],
    )
    worker_dir = run_dir / ".validation-worker"
    worker_dir.mkdir()
    baseline_valid = {
        "status": "running",
        "stage": "reward_hack",
        "nop": {"state": "pass", "reward": 0},
        "oracle": {"state": "pass", "reward": 1},
    }
    write_jsonl(
        worker_dir / "postcheck-status.jsonl",
        [
            {
                **baseline_valid,
                "instance": "clean",
                "timestamp": "10",
                "reward_hack": {"state": "pass", "is_hacking": False},
            },
            {
                **baseline_valid,
                "instance": "flagged",
                "timestamp": "11",
                "reward_hack": {"state": "fail", "is_hacking": True},
            },
            {
                **baseline_valid,
                "instance": "pending",
                "timestamp": "12",
                "reward_hack": {"state": "pending"},
            },
            {
                **baseline_valid,
                "instance": "error",
                "timestamp": "13",
                "reward_hack": {"state": "error", "error": "rate limited"},
            },
        ],
    )
    monkeypatch.setattr("run_dashboard.active_instances", lambda _run_dir: [])

    pipeline = calculate_status(run_dir, input_jsonl)["pipeline"]

    assert pipeline["stage_i"] == {
        "denominator": 4,
        "green": 4,
        "red": 0,
        "gray": 0,
        "breakdown": {
            "unprocessed": 0,
            "transient_api": 0,
            "incomplete_or_other": 0,
            "validation_unresolved": 0,
        },
        "red_breakdown": {
            "current_exact": 0,
            "historical_recovered": 0,
            "historical_evidence_instances": 0,
        },
    }
    assert pipeline["stage_ii"] == {
        "denominator": 4,
        "green": 1,
        "red": 1,
        "gray": 2,
        "breakdown": {"pending": 1, "running": 0, "errors": 1},
    }
    assert (
        pipeline["stage_ii"]["green"] + pipeline["stage_ii"]["red"] + pipeline["stage_ii"]["gray"]
        == pipeline["stage_ii"]["denominator"]
        == pipeline["stage_i"]["green"]
    )


def test_historical_oracle_zero_evidence_recovers_only_validation_failures(
    tmp_path, monkeypatch
) -> None:
    run_dir = tmp_path / "run"
    worker_dir = run_dir / ".validation-worker"
    worker_dir.mkdir(parents=True)
    input_jsonl = tmp_path / "input.jsonl"
    instances = ("valid", "ambiguous", "incomplete", "transient", "unprocessed")
    input_jsonl.write_text("{}\n" * len(instances))
    write_jsonl(
        run_dir / "orchestrator-instance-status.jsonl",
        [
            {"instance": "valid", "status": "success", "timestamp": "1"},
            {
                "instance": "ambiguous",
                "status": "failure",
                "timestamp": "2",
                "failure_reason": "Validation failed (NOP or Oracle)",
            },
            {
                "instance": "incomplete",
                "status": "failure",
                "timestamp": "3",
                "failure_reason": "Claude/Harbor validation incomplete",
            },
            {
                "instance": "transient",
                "status": "failure",
                "timestamp": "4",
                "failure_reason": "Transient network/API error",
            },
        ],
    )
    write_jsonl(
        worker_dir / "postcheck-status.jsonl",
        [
            {
                "instance": "valid",
                "timestamp": "5",
                "status": "baseline_valid",
                "nop": {"state": "pass", "reward": 0},
                "oracle": {"state": "pass", "reward": 1},
                "reward_hack": {"state": "pending"},
            }
        ],
    )
    evidence_path = worker_dir / "historical-oracle-zero-evidence.jsonl"
    evidence_path.write_text(
        "not-json\n"
        + "\n".join(
            json.dumps({"instance": instance, "oracle_reward": 0})
            for instance in ("valid", "ambiguous", "incomplete", "transient")
        )
        + "\n"
        + json.dumps({"instance": "must-ignore", "oracle_reward": 1})
        + "\n"
    )
    monkeypatch.setattr("run_dashboard.active_instances", lambda _run_dir: [])

    assert load_historical_oracle_zero_instances(run_dir) == {
        "valid",
        "ambiguous",
        "incomplete",
        "transient",
    }
    stage = calculate_status(run_dir, input_jsonl)["pipeline"]["stage_i"]

    assert stage == {
        "denominator": 5,
        "green": 1,
        "red": 1,
        "gray": 3,
        "breakdown": {
            "unprocessed": 1,
            "transient_api": 1,
            "incomplete_or_other": 1,
            "validation_unresolved": 0,
        },
        "red_breakdown": {
            "current_exact": 0,
            "historical_recovered": 1,
            "historical_evidence_instances": 4,
        },
    }


def test_funnel_ratios_are_zero_safe_when_nothing_has_been_processed(tmp_path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    input_jsonl = tmp_path / "input.jsonl"
    input_jsonl.write_text("")
    monkeypatch.setattr("run_dashboard.active_instances", lambda _run_dir: [])

    funnel = calculate_status(run_dir, input_jsonl)["funnel"]

    assert funnel["generated"] == {
        "count": 0,
        "processed": 0,
        "denominator": 0,
        "failures": 0,
        "yield_percent": 0.0,
    }
    assert funnel["baseline_valid"]["count"] == 0
    assert funnel["baseline_valid"]["denominator"] == 0
    assert funnel["baseline_valid"]["yield_percent"] == 0.0
    assert funnel["fully_accepted"]["count"] == 0
    assert funnel["fully_accepted"]["denominator"] == 0
    assert funnel["fully_accepted"]["yield_percent"] == 0.0
    pipeline = calculate_status(run_dir, input_jsonl)["pipeline"]
    assert (
        pipeline["stage_i"]["green"] + pipeline["stage_i"]["red"] + pipeline["stage_i"]["gray"]
        == pipeline["stage_i"]["denominator"]
        == 0
    )
    assert (
        pipeline["stage_ii"]["green"] + pipeline["stage_ii"]["red"] + pipeline["stage_ii"]["gray"]
        == pipeline["stage_ii"]["denominator"]
        == 0
    )


def test_postcheck_discovery_and_latest_attempt_ignore_backups_and_malformed_lines(
    tmp_path,
) -> None:
    live = tmp_path / ".validation-worker" / "postcheck-status.jsonl"
    backup = tmp_path / "backups" / "postcheck-status.jsonl"
    task_copy = tmp_path / "tasks" / "task" / "postcheck-status.jsonl"
    for path in (live, backup, task_copy):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("")
    live.write_text(
        json.dumps({"instance": "a", "attempt": 1, "timestamp": "9", "status": "accepted"})
        + "\nnot-json\n"
        + json.dumps({"instance": "a", "attempt": 2, "timestamp": "1", "status": "error"})
        + "\n"
    )

    assert postcheck_journal_paths(tmp_path) == [live]
    latest = load_latest_postchecks([live])
    assert latest["a"]["attempt"] == 2
    assert latest["a"]["status"] == "error"


def test_reward_backfill_merges_stage_evidence_without_bypassing_baseline(
    tmp_path, monkeypatch
) -> None:
    run_dir = tmp_path / "run"
    node_dir = run_dir / "slurm-nodes" / "node-a"
    node_dir.mkdir(parents=True)
    input_jsonl = tmp_path / "input.jsonl"
    instances = (
        "accepted",
        "reward-only",
        "flagged",
        "running",
        "bad-baseline",
    )
    input_jsonl.write_text("{}\n" * len(instances))
    write_jsonl(
        node_dir / "orchestrator-instance-status.jsonl",
        [
            {"instance": instance, "status": "success", "timestamp": str(index)}
            for index, instance in enumerate(instances, 1)
        ],
    )
    worker_dir = run_dir / ".validation-worker"
    worker_dir.mkdir()
    baseline_pass = {
        "status": "running",
        "stage": "reward_hack",
        "nop": {"state": "pass", "reward": 0},
        "oracle": {"state": "pass", "reward": 1},
        "reward_hack": {"state": "pending"},
    }
    write_jsonl(
        worker_dir / "postcheck-status.jsonl",
        [
            {**baseline_pass, "instance": "accepted", "timestamp": "2026-07-19T01:00:00Z"},
            {**baseline_pass, "instance": "flagged", "timestamp": "2026-07-19T01:00:00Z"},
            {**baseline_pass, "instance": "running", "timestamp": "2026-07-19T01:00:00Z"},
            {
                **baseline_pass,
                "instance": "bad-baseline",
                "timestamp": "2026-07-19T01:00:00Z",
                "nop": {"state": "pass", "reward": 1},
            },
        ],
    )
    write_jsonl(
        worker_dir / "reward-backfill-status.jsonl",
        [
            {
                "instance": "accepted",
                "timestamp": "2026-07-19T01:01:00Z",
                "status": "pass",
                "reward_hack": {"state": "pass", "is_hacking": False},
            },
            {
                "instance": "reward-only",
                "timestamp": "2026-07-19T01:02:00Z",
                "status": "pass",
                "reward_hack": {"state": "pass", "is_hacking": False},
            },
            {
                "instance": "flagged",
                "timestamp": "2026-07-19T01:03:00Z",
                "status": "fail",
                "reward_hack": {"state": "fail", "is_hacking": True},
            },
            {
                "instance": "running",
                "timestamp": "2026-07-19T01:04:00Z",
                "status": "running",
                "reward_hack": {"state": "running"},
            },
            {
                "instance": "bad-baseline",
                "timestamp": "2026-07-19T01:05:00Z",
                "status": "pass",
                "reward_hack": {"state": "pass", "is_hacking": False},
            },
        ],
    )
    (worker_dir / "reward-backfill-worker-status.json").write_text(
        json.dumps(
            {
                "state": "running",
                "timestamp": "2026-07-19T01:06:00Z",
                "reward_concurrency": 20,
                "reward_active_count": 20,
                "reward_primary_model": "gpt-5.3-codex-spark",
                "reward_fallback_model": "gpt-5.6-terra",
            }
        )
    )
    monkeypatch.setattr("run_dashboard.active_instances", lambda _run_dir: [])
    monkeypatch.setattr(
        "run_dashboard.datetime",
        type(
            "FixedDateTime",
            (datetime,),
            {"now": classmethod(lambda cls, tz=None: cls(2026, 7, 19, 1, 6, 30, tzinfo=tz))},
        ),
    )

    status = calculate_status(run_dir, input_jsonl)

    postcheck = status["postcheck"]
    assert postcheck["accepted"] == 1
    assert postcheck["rejected"] == 2
    assert postcheck["running"] == 1
    assert postcheck["queued"] == 1
    assert postcheck["reward_hack"] == {
        "pass": 3,
        "fail": 1,
        "error": 0,
        "pending": 0,
        "running": 1,
    }
    assert postcheck["nop"]["pending"] == 1
    assert postcheck["reward_backfill_worker"]["reward_active_count"] == 20
    assert postcheck["reward_backfill_worker"]["reward_concurrency"] == 20
    assert status["funnel"]["generated"]["count"] == 5
    assert status["funnel"]["baseline_valid"]["count"] == 3
    assert status["funnel"]["fully_accepted"]["count"] == 1
    assert status["funnel"]["fully_accepted"]["flagged"] == 1
    assert status["funnel"]["fully_accepted"]["running"] == 1


def test_reward_backfill_missing_corrupt_and_stale_artifacts_are_safe(tmp_path) -> None:
    run_dir = tmp_path / "run"
    worker_dir = run_dir / ".validation-worker"
    worker_dir.mkdir(parents=True)

    missing = load_reward_backfill_worker_status(run_dir)
    assert missing["state"] == "not_running"
    assert missing["reward_active_count"] == 0
    assert missing["reward_concurrency"] == 20

    (worker_dir / "reward-backfill-status.jsonl").write_text(
        "not-json\n"
        + json.dumps({"instance": "valid", "status": "pass", "timestamp": "2"})
        + "\n{partial"
    )
    assert set(load_latest_reward_backfills(run_dir)) == {"valid"}

    status_path = worker_dir / "reward-backfill-worker-status.json"
    status_path.write_text("{partial")
    corrupt = load_reward_backfill_worker_status(run_dir)
    assert corrupt["state"] == "unavailable"
    assert corrupt["reward_active_count"] == 0
    assert corrupt["reward_concurrency"] == 20

    status_path.write_text(
        json.dumps(
            {
                "state": "running",
                "timestamp": "2026-07-19T00:00:00Z",
                "reward_concurrency": 20,
                "reward_active_count": 20,
                "recent": [{"instance": "must-not-leak"}],
            }
        )
    )
    stale = load_reward_backfill_worker_status(
        run_dir,
        now=datetime(2026, 7, 19, 0, 10, tzinfo=UTC),
    )
    assert stale["state"] == "stale"
    assert stale["stale"] is True
    assert stale["reward_active_count"] == 0
    assert stale["reward_concurrency"] == 20
    assert "recent" not in stale

    status_path.write_text(
        json.dumps(
            {
                "state": "running",
                "reward_concurrency": 20,
                "reward_active_count": 20,
            }
        )
    )
    missing_heartbeat = load_reward_backfill_worker_status(run_dir)
    assert missing_heartbeat["state"] == "stale"
    assert missing_heartbeat["reward_active_count"] == 0


def test_baseline_worker_status_sanitizes_parallel_fields_and_marks_stale(tmp_path) -> None:
    run_dir = tmp_path / "run"
    worker_dir = run_dir / ".validation-worker"
    worker_dir.mkdir(parents=True)
    (worker_dir / "worker-status.json").write_text(
        json.dumps(
            {
                "state": "running",
                "timestamp": "2026-07-19T00:00:00Z",
                "current_instance": "task",
                "current_stage": "oracle",
                "baseline_concurrency": 20,
                "baseline_active_count": 7,
                "current_instances": ["task", " ", 3, "task", "second"],
                "active_stages": {
                    "task": " oracle ",
                    "second": "nop",
                    " ": "nop",
                    "invalid": 3,
                },
                "reward_active_count": 7,
                "recent": [{"instance": "must-not-leak"}],
            }
        )
    )

    status = load_postcheck_worker_status(
        run_dir,
        now=datetime(2026, 7, 19, 0, 10, tzinfo=UTC),
    )

    assert status["state"] == "stale"
    assert status["stale"] is True
    assert status["baseline_concurrency"] == 20
    assert status["baseline_active_count"] == 0
    assert status["current_instances"] == []
    assert status["active_stages"] == {}
    assert status["reward_active_count"] == 0
    assert status["current_instance"] == "task"
    assert "recent" not in status


def test_baseline_worker_status_preserves_fresh_parallel_fields(tmp_path) -> None:
    run_dir = tmp_path / "run"
    worker_dir = run_dir / ".validation-worker"
    worker_dir.mkdir(parents=True)
    (worker_dir / "worker-status.json").write_text(
        json.dumps(
            {
                "state": "running",
                "timestamp": "2026-07-19T00:00:00Z",
                "baseline_concurrency": 20,
                "baseline_active_count": 2,
                "current_instances": ["task", " ", 3, "task", "second"],
                "active_stages": {
                    "task": " oracle ",
                    "second": "nop",
                    " ": "nop",
                    "invalid": 3,
                },
            }
        )
    )

    status = load_postcheck_worker_status(
        run_dir,
        now=datetime(2026, 7, 19, 0, 1, tzinfo=UTC),
    )

    assert status["baseline_concurrency"] == 20
    assert status["baseline_active_count"] == 2
    assert status["current_instances"] == ["task", "second"]
    assert status["active_stages"] == {"task": "oracle", "second": "nop"}

    (worker_dir / "worker-status.json").write_text(
        json.dumps(
            {
                "state": "stopped",
                "timestamp": "2026-07-19T00:00:00Z",
                "baseline_concurrency": 0,
                "baseline_active_count": -1,
                "current_instances": "not-a-list",
                "active_stages": ["not-a-map"],
            }
        )
    )
    disabled = load_postcheck_worker_status(run_dir)
    assert disabled["baseline_concurrency"] == 0
    assert disabled["baseline_active_count"] == 0
    assert disabled["current_instances"] == []
    assert disabled["active_stages"] == {}


def test_backfill_error_does_not_replace_a_terminal_local_reward_verdict() -> None:
    postchecks = {
        "task": {
            "instance": "task",
            "timestamp": "2026-07-19T01:00:00Z",
            "reward_hack": {
                "state": "pass",
                "is_hacking": False,
                "model": "gpt-5.6-terra",
            },
        }
    }
    backfills = {
        "task": {
            "instance": "task",
            "timestamp": "2026-07-19T01:05:00Z",
            "status": "error",
            "reward_hack": {"state": "error", "error": "proxy reset"},
        }
    }

    merged = merge_reward_backfill_evidence(postchecks, backfills)

    assert merged["task"]["reward_hack"]["state"] == "pass"
    assert merged["task"]["reward_hack"]["is_hacking"] is False


def test_recursive_discovery_prunes_backups_and_task_outputs(tmp_path) -> None:
    live = tmp_path / "slurm-nodes" / "node-a" / "orchestrator-instance-status.jsonl"
    ledger = tmp_path / "slurm-nodes" / "node-a" / "create.jsonl"
    backup = tmp_path / "backups" / "orchestrator-instance-status.jsonl"
    task_ledger = tmp_path / "tasks" / "task" / "create.jsonl"
    for path in (live, ledger, backup, task_ledger):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("")

    assert status_journal_paths(tmp_path) == [live]
    assert success_ledger_paths(tmp_path) == [ledger]


def test_funnel_includes_pre_slurm_successes_and_local_pipeline_row(tmp_path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    node_dir = run_dir / "slurm-nodes" / "node-a"
    node_dir.mkdir(parents=True)
    write_jsonl(
        run_dir / "orchestrator-instance-status.jsonl",
        [
            {"instance": "local-ok", "status": "success", "timestamp": "1"},
            {"instance": "local-bad", "status": "failure", "timestamp": "2"},
        ],
    )
    write_jsonl(
        node_dir / "orchestrator-instance-status.jsonl",
        [
            {"instance": "slurm-ok", "status": "success", "timestamp": "3"},
            {"instance": "slurm-bad", "status": "failure", "timestamp": "4"},
        ],
    )
    worker_dir = run_dir / ".validation-worker"
    worker_dir.mkdir()
    write_jsonl(
        worker_dir / "postcheck-status.jsonl",
        [
            {
                "instance": instance,
                "attempt": 1,
                "timestamp": "5",
                "status": "baseline_valid",
                "nop": {"state": "pass", "reward": 0},
                "oracle": {"state": "pass", "reward": 1},
                "reward_hack": {"state": "pending"},
            }
            for instance in ("local-ok", "slurm-ok")
        ],
    )
    input_jsonl = tmp_path / "input.jsonl"
    input_jsonl.write_text("{}\n" * 4)
    monkeypatch.setattr("run_dashboard.active_instances", lambda _run_dir: [])

    status = calculate_status(run_dir, input_jsonl)

    assert status["funnel"]["generated"] == {
        "count": 2,
        "processed": 4,
        "denominator": 4,
        "failures": 2,
        "yield_percent": 50.0,
    }
    assert status["funnel"]["baseline_valid"]["count"] == 2
    local_row = next(row for row in status["pipeline_nodes"] if row["node"] == "pre-slurm/local")
    assert local_row["generated"] == 1
    assert local_row["baseline_valid"] == 1


def test_stale_ledger_running_snapshot_is_counted_as_pending(tmp_path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    node_dir = run_dir / "slurm-nodes" / "node-a"
    node_dir.mkdir(parents=True)
    write_jsonl(
        node_dir / "orchestrator-instance-status.jsonl",
        [{"instance": "task", "status": "success", "timestamp": "1"}],
    )
    worker_dir = run_dir / ".validation-worker"
    worker_dir.mkdir()
    write_jsonl(
        worker_dir / "postcheck-status.jsonl",
        [
            {
                "instance": "task",
                "attempt": 1,
                "timestamp": "2",
                "status": "running",
                "nop": {"state": "running", "reward": None},
                "oracle": {"state": "pending", "reward": None},
                "reward_hack": {"state": "pending"},
            }
        ],
    )
    (worker_dir / "worker-status.json").write_text(
        json.dumps({"state": "stopped", "timestamp": "3"})
    )
    input_jsonl = tmp_path / "input.jsonl"
    input_jsonl.write_text("{}\n")
    monkeypatch.setattr("run_dashboard.active_instances", lambda _run_dir: [])

    status = calculate_status(run_dir, input_jsonl)

    assert status["funnel"]["baseline_valid"]["running"] == 0
    assert status["funnel"]["baseline_valid"]["pending"] == 1


def test_parallel_baseline_heartbeat_keeps_all_active_snapshots_running(
    tmp_path, monkeypatch
) -> None:
    run_dir = tmp_path / "run"
    node_dir = run_dir / "slurm-nodes" / "node-a"
    node_dir.mkdir(parents=True)
    instances = ["task-a", "task-b"]
    write_jsonl(
        node_dir / "orchestrator-instance-status.jsonl",
        [
            {"instance": instance, "status": "success", "timestamp": str(index)}
            for index, instance in enumerate(instances, 1)
        ],
    )
    worker_dir = run_dir / ".validation-worker"
    worker_dir.mkdir()
    write_jsonl(
        worker_dir / "postcheck-status.jsonl",
        [
            {
                "instance": "task-a",
                "attempt": 1,
                "timestamp": "3",
                "status": "running",
                "nop": {"state": "running", "reward": None},
                "oracle": {"state": "pending", "reward": None},
                "reward_hack": {"state": "pending"},
            },
            {
                "instance": "task-b",
                "attempt": 1,
                "timestamp": "3",
                "status": "running",
                "nop": {"state": "pass", "reward": 0},
                "oracle": {"state": "running", "reward": None},
                "reward_hack": {"state": "pending"},
            },
        ],
    )
    (worker_dir / "worker-status.json").write_text(
        json.dumps(
            {
                "state": "draining",
                "timestamp": "2999-01-01T00:00:00Z",
                "baseline_concurrency": 20,
                "baseline_active_count": 2,
                "current_instance": "task-a",
                "current_stage": "nop",
                "current_instances": instances,
                "active_stages": {"task-a": "nop", "task-b": "oracle"},
            }
        )
    )
    input_jsonl = tmp_path / "input.jsonl"
    input_jsonl.write_text("{}\n" * len(instances))
    monkeypatch.setattr("run_dashboard.active_instances", lambda _run_dir: [])

    status = calculate_status(run_dir, input_jsonl)

    assert status["postcheck"]["nop"]["running"] == 1
    assert status["postcheck"]["oracle"]["running"] == 1
    assert status["postcheck"]["baseline_running"] == 2
    assert status["postcheck"]["running"] == 2


def test_status_cache_serves_last_completed_snapshot(tmp_path, monkeypatch) -> None:
    calls = 0

    def fake_calculate(*_args):
        nonlocal calls
        calls += 1
        return {"success": calls}

    monkeypatch.setattr("run_dashboard.calculate_status", fake_calculate)
    cache = StatusCache(tmp_path, tmp_path / "input.jsonl", 0)

    assert json.loads(cache.body()) == {"success": 1}
    assert json.loads(cache.body()) == {"success": 1}
    assert calls == 1

    cache.refresh()
    assert json.loads(cache.body()) == {"success": 2}


def control_config(tmp_path, *, state="running", routes=None):
    run_dir = tmp_path / "run"
    run_dir.mkdir(exist_ok=True)
    status_path = run_dir / ".slurm-control" / "status.json"
    return {
        "version": 1,
        "desired_state": state,
        "run": {
            "name": "test-run",
            "dir": str(run_dir),
            "input_jsonl": str(tmp_path / "input.jsonl"),
            "models_yaml": str(tmp_path / "models.yaml"),
            "plan_path": str(run_dir / "slurm-stage1-r9-4n-plan.json"),
            "revision": "r9",
        },
        "models": {
            "opus": "gpt-5.6-sol",
            "sonnet": "gpt-5.6-terra",
        },
        "slurm": {
            "node_count": 4,
            "routes": routes or {"sg": 4, "hk": 4, "de": 4},
        },
        "controller": {
            "poll_interval_seconds": 15,
            "status_path": str(status_path),
        },
        "circuit_breaker": {
            "enabled": True,
            "window_seconds": 300,
            "failure_threshold": 10,
            "cooldown_seconds": 900,
        },
        "metadata": {"created_by": "test"},
    }


def test_model_catalog_maps_exact_names_without_exposing_credentials(tmp_path) -> None:
    models_yaml = tmp_path / "models.yaml"
    models_yaml.write_text(
        yaml.safe_dump(
            {
                "model_list": [
                    {
                        "model_name": "gpt-5.6-sol",
                        "litellm_params": {
                            "model": "openai/sol",
                            "api_base": "https://user:embedded-secret@sol.example/v1?token=hidden",
                            "api_key": "never-expose-this-key",
                        },
                    },
                    {
                        "model_name": "gpt-5.6-terra",
                        "litellm_params": {
                            "api_base": "https://terra.example/v1",
                            "api_key": "another-secret",
                        },
                    },
                    {
                        "model_name": "gpt-5.6-sol",
                        "litellm_params": {"api_base": "https://duplicate.example/v1"},
                    },
                ]
            },
            sort_keys=False,
        )
    )

    catalog, error = load_model_catalog(models_yaml)

    assert error is None
    assert catalog == [
        {
            "model_name": "gpt-5.6-sol",
            "api_base": "https://sol.example",
            "api_bases": [
                "https://sol.example",
                "https://duplicate.example",
            ],
            "endpoint_count": 2,
        },
        {
            "model_name": "gpt-5.6-terra",
            "api_base": "https://terra.example",
            "api_bases": ["https://terra.example"],
            "endpoint_count": 1,
        },
    ]
    assert all(
        set(item) == {"model_name", "api_base", "api_bases", "endpoint_count"}
        for item in catalog
    )
    assert "secret" not in json.dumps(catalog)


def test_control_store_updates_atomically_and_reports_observed_status(tmp_path) -> None:
    config_path = tmp_path / "swegen-config.yaml"
    config = control_config(tmp_path)
    (tmp_path / "models.yaml").write_text(
        yaml.safe_dump(
            {
                "model_list": [
                    {
                        "model_name": "gpt-5.6-sol",
                        "litellm_params": {
                            "api_base": "https://models.example/v1",
                            "api_key": "private-key",
                        },
                    },
                    {
                        "model_name": "gpt-5.6-terra",
                        "litellm_params": {"api_base": "https://models.example/v1"},
                    },
                ]
            },
            sort_keys=False,
        )
    )
    config_path.write_text(yaml.safe_dump(config, sort_keys=False))
    status_path = tmp_path / "run" / ".slurm-control" / "status.json"
    status_path.parent.mkdir()
    status_path.write_text(
        json.dumps(
            {
                "controller": {"state": "running", "applied_state": "running"},
                "run": {"active_plan": config["run"]["plan_path"]},
                "slurm": {"active_workers": 48},
                "circuit_breaker": {"state": "closed", "failure_count": 2},
            }
        )
    )
    store = ControlConfigStore(config_path)

    initial = store.snapshot()

    assert initial["available"] is True
    assert initial["desired_total_workers"] == 48
    assert initial["observed"]["slurm"]["active_workers"] == 48
    assert initial["model_catalog_error"] is None
    assert "private-key" not in json.dumps(initial)

    updated = store.update(
        {
            "routes": {"sg": 2, "hk": 3, "de": 4},
            "models": {"sonnet": "gpt-5.6-sol"},
            "circuit_breaker": {"failure_threshold": 12},
        }
    )

    assert updated["slurm"]["routes"] == {"sg": 2, "hk": 3, "de": 4}
    assert updated["models"] == {"opus": "gpt-5.6-sol", "sonnet": "gpt-5.6-sol"}
    assert updated["circuit_breaker"]["failure_threshold"] == 12
    assert updated["run"] == config["run"]
    assert updated["metadata"]["created_by"] == "test"
    assert updated["metadata"]["updated_by"] == "dashboard"
    assert "circuit_breaker_reset_requested_at" not in updated["metadata"]
    assert stat.S_IMODE(config_path.stat().st_mode) == 0o600
    assert not list(tmp_path.glob(".swegen-config.yaml.*.tmp"))
    assert yaml.safe_load(config_path.read_text()) == updated

    resumed = store.update(
        {"desired_state": "running", "reset_circuit_breaker": True}
    )

    assert resumed["desired_state"] == "running"
    assert resumed["metadata"]["circuit_breaker_reset_requested_at"] == resumed["metadata"][
        "updated_at"
    ]
    assert "reset_circuit_breaker" not in resumed


def test_topology_controls_warn_that_allocations_will_be_relaunched() -> None:
    assert DASHBOARD_HTML.count("window.confirm") == 2
    assert DASHBOARD_HTML.count("cancels the current Stage-I allocations") == 2
    assert "Apply SG/HK/DE" in DASHBOARD_HTML
    assert "Apply Opus" in DASHBOARD_HTML
    assert "reset_circuit_breaker:true" in DASHBOARD_HTML
    assert "RESET REQUESTED" in DASHBOARD_HTML


def test_control_store_allows_zero_routes_only_while_paused(tmp_path) -> None:
    config_path = tmp_path / "swegen-config.yaml"
    config_path.write_text(yaml.safe_dump(control_config(tmp_path), sort_keys=False))
    store = ControlConfigStore(config_path)

    paused = store.update(
        {
            "desired_state": "paused",
            "routes": {"sg": 0, "hk": 0, "de": 0},
        }
    )

    assert paused["desired_state"] == "paused"
    assert sum(paused["slurm"]["routes"].values()) == 0
    before = config_path.read_bytes()
    with pytest.raises(ControlConfigError, match="at least one route worker"):
        store.update({"desired_state": "running"})
    assert config_path.read_bytes() == before

    with pytest.raises(ControlConfigError, match="requires desired_state to be running"):
        store.update({"desired_state": "paused", "reset_circuit_breaker": True})
    with pytest.raises(ControlConfigError, match="must be true when supplied"):
        store.update({"reset_circuit_breaker": False})


def test_control_store_rejects_unsafe_or_legacy_config_without_exposing_it(tmp_path) -> None:
    config_path = tmp_path / "swegen-config.yaml"
    config_path.write_text("- proxies:\n  - https://user:secret@example.invalid\n")
    store = ControlConfigStore(config_path)

    snapshot = store.snapshot()

    assert snapshot["available"] is False
    assert snapshot["config"] is None
    assert "secret" not in json.dumps(snapshot)

    config_path.write_text(yaml.safe_dump(control_config(tmp_path), sort_keys=False))
    before = config_path.read_bytes()
    with pytest.raises(ControlConfigError, match=f"between 0 and {CONTROL_ROUTE_MAX_CONCURRENCY}"):
        store.update({"routes": {"sg": CONTROL_ROUTE_MAX_CONCURRENCY + 1}})
    assert config_path.read_bytes() == before


def test_ensure_control_token_creates_private_stable_token(tmp_path) -> None:
    token_path = tmp_path / ".control-token"

    first = ensure_control_token(token_path)
    second = ensure_control_token(token_path)

    assert len(first) >= 16
    assert second == first
    assert stat.S_IMODE(token_path.stat().st_mode) == 0o600


class StaticStatusCache:
    def body(self) -> bytes:
        return b'{"pipeline":{}}'


def test_export_download_endpoint_builds_a_datestamped_pack(tmp_path) -> None:
    run_dir, _task_dir = make_accepted_export_fixture(tmp_path)
    manager = TaskExportManager(run_dir, tmp_path / "export")
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        make_handler(StaticStatusCache(), export_manager=manager),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(*server.server_address, timeout=5)
        connection.request("GET", "/api/export/reward-hack-accepted.tar.gz")
        response = connection.getresponse()
        body = response.read()
        headers = dict(response.getheaders())
        connection.close()
        assert response.status == 200
        assert headers["Content-Disposition"].startswith("attachment; filename=\"reward-hack-accepted-")
        assert headers["Content-Disposition"].endswith(".tar.gz\"")
        assert body[:2] == b"\x1f\x8b"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def request_json(server, method, path, body=None, headers=None):
    connection = http.client.HTTPConnection(*server.server_address, timeout=5)
    payload = None if body is None else json.dumps(body)
    request_headers = dict(headers or {})
    if payload is not None:
        request_headers.setdefault("Content-Type", "application/json")
    connection.request(method, path, body=payload, headers=request_headers)
    response = connection.getresponse()
    value = json.loads(response.read())
    connection.close()
    return response.status, value


def test_control_api_is_public_read_authenticated_write(tmp_path) -> None:
    config_path = tmp_path / "swegen-config.yaml"
    (tmp_path / "models.yaml").write_text(
        yaml.safe_dump(
            {
                "model_list": [
                    {
                        "model_name": "gpt-5.6-sol",
                        "litellm_params": {
                            "api_base": "https://models.example/v1",
                            "api_key": "api-secret",
                        },
                    },
                    {
                        "model_name": "gpt-5.6-terra",
                        "litellm_params": {"api_base": "https://models.example/v1"},
                    },
                ]
            },
            sort_keys=False,
        )
    )
    config = control_config(tmp_path)
    config_path.write_text(yaml.safe_dump(config, sort_keys=False))
    status_path = Path(config["controller"]["status_path"])
    status_path.parent.mkdir(parents=True)
    status_path.write_text(
        json.dumps(
            {
                "controller": {"state": "not_running", "applied_state": "paused"},
                "circuit_breaker": {
                    "tripped": True,
                    "tripped_at": "2026-07-22T00:00:00+00:00",
                    "failure_count": 10,
                },
            }
        )
    )
    store = ControlConfigStore(config_path)
    token = "a-secure-control-token"
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        make_handler(StaticStatusCache(), store, token),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, public = request_json(server, "GET", "/api/control")
        assert status == 200
        assert public["config"]["desired_state"] == "running"
        assert public["model_catalog"] == [
            {
                "model_name": "gpt-5.6-sol",
                "api_base": "https://models.example",
                "api_bases": ["https://models.example"],
                "endpoint_count": 1,
            },
            {
                "model_name": "gpt-5.6-terra",
                "api_base": "https://models.example",
                "api_bases": ["https://models.example"],
                "endpoint_count": 1,
            },
        ]
        assert token not in json.dumps(public)
        assert "api-secret" not in json.dumps(public)

        status, unauthorized = request_json(
            server,
            "POST",
            "/api/control",
            {"desired_state": "paused"},
        )
        assert status == 401
        assert unauthorized["error"] == "invalid control token"
        assert store.load()["desired_state"] == "running"

        status, changed = request_json(
            server,
            "POST",
            "/api/control",
            {
                "desired_state": "paused",
                "routes": {"sg": 1, "hk": 2, "de": 3},
                "models": {"opus": "gpt-5.6-terra"},
            },
            {"X-SWEGEN-Control-Token": token},
        )
        assert status == 200
        assert changed["config"]["desired_state"] == "paused"
        assert changed["config"]["models"]["opus"] == "gpt-5.6-terra"
        assert changed["desired_total_workers"] == 24

        status, resumed = request_json(
            server,
            "POST",
            "/api/control",
            {"desired_state": "running", "reset_circuit_breaker": True},
            {"X-SWEGEN-Control-Token": token},
        )
        assert status == 200
        assert resumed["config"]["desired_state"] == "running"
        assert resumed["breaker_reset_pending"] is True
        assert resumed["config"]["metadata"]["circuit_breaker_reset_requested_at"]

        status, invalid = request_json(
            server,
            "POST",
            "/api/control",
            {"routes": {"sg": 9}},
            {"X-SWEGEN-Control-Token": token},
        )
        assert status == 422
        assert "between 0 and 8" in invalid["error"]
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
