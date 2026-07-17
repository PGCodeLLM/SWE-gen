import json

from failure_mode_monitor import detect_failure_modes, monitor_once


def test_detect_failure_modes() -> None:
    text = """
    API Error: Connection error.
    fatal: server certificate verification failed
    patch: **** malformed patch at line 12
    """

    assert detect_failure_modes(text) == {
        "claude_connection_error",
        "tls_or_certificate_error",
        "patch_apply_failure",
    }


def test_monitor_records_baseline_then_reports_only_new_modes(tmp_path) -> None:
    run_dir = tmp_path / "run"
    log_dir = run_dir / "orchestrator-logs"
    log_dir.mkdir(parents=True)
    status_path = run_dir / "orchestrator-instance-status.jsonl"
    status_path.write_text(
        json.dumps(
            {
                "instance": "owner__repo-1",
                "status": "failure",
                "failure_reason": "Validation failed (NOP or Oracle)",
            }
        )
        + "\n"
    )
    worker_log = log_dir / "worker-0.log"
    worker_log.write_text("Harbor oracle: expected reward=1, actual reward=0\n")
    state_path = run_dir / "monitor-state.json"

    baseline = monitor_once(run_dir, state_path, 900)
    assert baseline["event"] == "baseline"
    assert baseline["new_modes"] == []
    assert "oracle_validation_failure" in baseline["known_modes"]

    with worker_log.open("a") as fh:
        fh.write("API Error: Connection error.\n")
    second = monitor_once(run_dir, state_path, 900)
    assert second["new_modes"] == ["claude_connection_error"]

    third = monitor_once(run_dir, state_path, 900)
    assert third["new_modes"] == []
    assert third["message"] == "no new failure modes; waiting for next interval"
