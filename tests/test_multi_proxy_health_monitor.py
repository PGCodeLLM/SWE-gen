import json
from pathlib import Path

import multi_proxy_health_monitor as monitor


def test_parse_group_spec_supports_key_value_aliases_and_json() -> None:
    key_value = monitor.parse_group_spec(
        "name=SG-main,tmux=swegen-sg,input_marker=sg.jsonl,"
        "log_dir=orchestrator-logs-sg,expected_workers=4,required=yes"
    )

    assert key_value == monitor.GroupSpec(
        name="SG-main",
        tmux_session="swegen-sg",
        input_marker="sg.jsonl",
        log_dir=Path("orchestrator-logs-sg"),
        expected_workers=4,
        enabled=True,
        required=True,
    )

    as_json = monitor.parse_group_spec(
        json.dumps(
            {
                "name": "HK-B",
                "session": "swegen-hk-b",
                "input": "hk-b.jsonl",
                "logs": "orchestrator-logs-hk-b",
                "workers": 8,
                "enabled": False,
                "required": False,
                "progress": "orchestrator-progress-hk-b.jsonl",
            }
        )
    )

    assert as_json.name == "HK-B"
    assert as_json.expected_workers == 8
    assert as_json.enabled is False
    assert as_json.required is False
    assert as_json.progress_log == Path("orchestrator-progress-hk-b.jsonl")


def test_count_error_patterns_counts_transport_errors_without_plain_numbers() -> None:
    text = """
    [api_retry] retry 2 of 10 after API Error: 504 Gateway Timeout
    cause=ECONNRESET; socket connection was closed unexpectedly
    request timed out with ETIMEDOUT and another request_timeout label
    HTTP 429 Too Many Requests
    response: 502 Bad Gateway
    status code=503 Service Unavailable
    ordinary values 429 502 503 504 and pull request #502 should not count
    """

    assert monitor.count_error_patterns(text) == {
        "api_retry": 1,
        "econnreset": 1,
        "request_timeout": 1,
        "socket_error": 1,
        "http_429": 1,
        "http_502": 1,
        "http_503": 1,
        "http_504": 1,
    }


def test_pattern_accumulator_reports_cumulative_and_new_counts(tmp_path) -> None:
    log = tmp_path / "worker-0.log"
    log.write_text("API Error: 502 Bad Gateway\n")
    accumulator = monitor.PatternAccumulator()

    cumulative, new = accumulator.scan([log])
    assert cumulative["http_502"] == 1
    assert new["http_502"] == 1

    with log.open("a") as fh:
        fh.write("ECONNRESET\n")
    cumulative, new = accumulator.scan([log])

    assert cumulative["http_502"] == 1
    assert cumulative["econnreset"] == 1
    assert new["http_502"] == 0
    assert new["econnreset"] == 1


def test_count_error_patterns_recognizes_sdk_error_status_field() -> None:
    counts = monitor.count_error_patterns(
        "SystemMessage(subtype='api_retry', data={'error_status': 504})\n"
    )

    assert counts["api_retry"] == 1
    assert counts["http_504"] == 1


def test_count_task_events_uses_launch_starts_and_progress_results(tmp_path) -> None:
    launch = tmp_path / "orchestrator-hk-launch.log"
    launch.write_text(
        "[worker 0] (1/2) owner/repo#1 starting [token masked]\n"
        "[worker 1] (1/2) owner/repo#2 starting [token masked]\n"
        "[worker 0] (1/2) owner/repo#1 OK (log: worker-0.log)\n"
        "[worker 1] (1/2) owner/repo#2 failed (rc=1)\n"
    )
    progress = tmp_path / "orchestrator-progress-hk.jsonl"
    progress.write_text(
        json.dumps({"event": "task_finished", "status": "success"})
        + "\n"
        + "not-json\n"
        + json.dumps({"event": "task_finished", "status": "failure"})
        + "\n"
    )

    assert monitor.count_task_events(launch, progress) == {
        "started": 2,
        "ok": 1,
        "failed": 1,
    }


def test_evaluate_group_health_requires_runtime_fresh_logs_and_no_new_errors() -> None:
    base = {
        "enabled": True,
        "tmux_active": True,
        "orchestrator_process_count": 1,
        "worker_process_count": 4,
        "expected_worker_count": 4,
        "worker_log_file_count": 4,
        "recent_log_activity_age_seconds": 12.0,
        "max_log_age_seconds": 300.0,
        "new_error_counts": monitor.zero_filled_counts({}),
    }

    assert monitor.evaluate_group_health(**base) is True
    # Child processes legitimately dip below the configured concurrency while
    # a worker post-processes one task and starts the next.
    assert monitor.evaluate_group_health(**{**base, "worker_process_count": 3}) is True
    assert monitor.evaluate_group_health(**{**base, "worker_log_file_count": 3}) is False
    assert (
        monitor.evaluate_group_health(**{**base, "recent_log_activity_age_seconds": 301.0}) is False
    )
    assert (
        monitor.evaluate_group_health(
            **{**base, "new_error_counts": {**base["new_error_counts"], "http_504": 1}}
        )
        is False
    )
    assert monitor.evaluate_group_health(**{**base, "enabled": False}) is False


def test_process_counts_attribute_only_matching_orchestrator_descendants() -> None:
    processes = {
        10: monitor.ProcessInfo(10, 1, ("python", "src/orchestrator.py", "sg.jsonl")),
        11: monitor.ProcessInfo(11, 10, ("/venv/bin/swegen", "create")),
        12: monitor.ProcessInfo(12, 11, ("/sdk/_bundled/claude",)),
        20: monitor.ProcessInfo(20, 1, ("python", "src/orchestrator.py", "hk.jsonl")),
        21: monitor.ProcessInfo(21, 20, ("/venv/bin/swegen", "create")),
        22: monitor.ProcessInfo(22, 21, ("/sdk/_bundled/claude",)),
    }
    spec = monitor.GroupSpec(
        "SG-main",
        "swegen-sg",
        "sg.jsonl",
        Path("logs"),
        1,
    )

    assert monitor.process_counts(spec, processes) == (1, 1, 1)
