import json

from run_dashboard import calculate_status, status_journal_paths


def write_jsonl(path, records) -> None:
    path.write_text("".join(json.dumps(record) + "\n" for record in records))


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


def test_status_journal_paths_excludes_backups(tmp_path) -> None:
    live = tmp_path / "orchestrator-instance-status-hk-a-r3.jsonl"
    legacy = tmp_path / "orchestrator-instance-status.jsonl"
    backup = tmp_path / "orchestrator-instance-status.before-cleanup.jsonl"
    unrelated = tmp_path / "other.jsonl"
    for path in (live, legacy, backup, unrelated):
        path.write_text("")

    assert status_journal_paths(tmp_path) == [live, legacy]
