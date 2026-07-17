import json
from datetime import UTC, datetime

import sg_extra_stability_gate as gate


def health_record(now_epoch: float, *, overall_healthy: bool = True) -> dict:
    zero_errors = {
        "api_retry": 0,
        "econnreset": 0,
        "http_429": 0,
        "http_502": 0,
        "http_503": 0,
        "http_504": 0,
        "request_timeout": 0,
        "socket_error": 0,
    }
    groups = [
        {
            "name": name,
            "enabled": True,
            "required": True,
            "healthy": True,
            "error_counts": dict(zero_errors),
            "new_error_counts": dict(zero_errors),
        }
        for name in sorted(gate.REQUIRED_GROUP_NAMES)
    ]
    return {
        "timestamp": datetime.fromtimestamp(now_epoch, UTC).isoformat(timespec="seconds"),
        "event": "multi_proxy_health",
        "overall_healthy": overall_healthy,
        "expected_group_count": 5,
        "healthy_expected_group_count": 5 if overall_healthy else 4,
        "groups": groups,
    }


def evaluate(record: object, now_epoch: float = 10_000.0) -> gate.HealthEvaluation:
    return gate.evaluate_health_record(
        record,
        now_epoch=now_epoch,
        max_health_age_seconds=90.0,
        max_future_skew_seconds=5.0,
    )


def test_health_evaluation_requires_fresh_overall_health_and_all_five_groups() -> None:
    record = health_record(9_970.0)
    assert evaluate(record).acceptable is True

    unhealthy = health_record(9_970.0, overall_healthy=False)
    result = evaluate(unhealthy)
    assert result.acceptable is False
    assert result.reason == "source_unhealthy"

    retry = health_record(9_970.0)
    retry["groups"][0]["new_error_counts"]["api_retry"] = 1
    assert evaluate(retry).acceptable is False

    reset = health_record(9_970.0)
    reset["groups"][1]["new_error_counts"]["econnreset"] = 1
    assert evaluate(reset).acceptable is False

    gateway_error = health_record(9_970.0)
    gateway_error["groups"][2]["new_error_counts"]["http_504"] = 1
    assert evaluate(gateway_error).acceptable is False

    missing_group = health_record(9_970.0)
    missing_group["groups"].pop()
    assert evaluate(missing_group).reason == "source_groups_missing"

    stale = health_record(9_800.0)
    stale_result = evaluate(stale)
    assert stale_result.acceptable is False
    assert stale_result.reason == "source_stale"


def test_missing_or_invalid_health_file_is_unacceptable(tmp_path) -> None:
    missing = gate.read_health_status(
        tmp_path / "missing.json",
        now_epoch=10_000.0,
        max_health_age_seconds=90.0,
        max_future_skew_seconds=5.0,
    )
    assert missing.acceptable is False
    assert missing.reason == "source_missing"

    invalid_path = tmp_path / "health.json"
    invalid_path.write_text("not-json")
    invalid = gate.read_health_status(
        invalid_path,
        now_epoch=10_000.0,
        max_health_age_seconds=90.0,
        max_future_skew_seconds=5.0,
    )
    assert invalid.acceptable is False
    assert invalid.reason == "source_invalid_json"


def test_stability_clock_starts_on_first_good_poll_and_resets() -> None:
    tracker = gate.StabilityTracker(required_seconds=1800.0)

    first = tracker.observe(True, now_epoch=1_000.0, now_monotonic=50.0)
    assert first.stable_since_epoch == 1_000.0
    assert first.stable_elapsed_seconds == 0.0
    assert first.stable_window_satisfied is False

    almost = tracker.observe(True, now_epoch=2_799.0, now_monotonic=1849.0)
    assert almost.stable_elapsed_seconds == 1799.0
    assert almost.stable_window_satisfied is False

    reset = tracker.observe(False, now_epoch=2_800.0, now_monotonic=1850.0)
    assert reset.reset_this_poll is True
    assert reset.stable_since_epoch is None
    assert reset.stable_elapsed_seconds == 0.0

    restarted = tracker.observe(True, now_epoch=4_000.0, now_monotonic=2000.0)
    assert restarted.stable_since_epoch == 4_000.0
    assert restarted.stable_window_satisfied is False

    ready = tracker.observe(True, now_epoch=5_800.0, now_monotonic=3800.0)
    assert ready.stable_elapsed_seconds == 1800.0
    assert ready.stable_window_satisfied is True


def test_cumulative_error_change_catches_a_missed_unhealthy_snapshot() -> None:
    tracker = gate.ErrorCountTracker()
    first = evaluate(health_record(9_970.0))
    assert tracker.observe(first).acceptable is True

    later_record = health_record(9_980.0)
    later_record["groups"][0]["error_counts"]["api_retry"] = 1
    # The latest monitor snapshot may already have new_error_counts back at
    # zero, but its cumulative total still proves an error occurred.
    later = gate.evaluate_health_record(
        later_record,
        now_epoch=10_000.0,
        max_health_age_seconds=90.0,
        max_future_skew_seconds=5.0,
    )
    detected = tracker.observe(later)

    assert detected.acceptable is False
    assert detected.reason == "source_cumulative_errors_changed"


def test_launch_commands_use_fixed_r2_paths_env_and_four_workers(tmp_path) -> None:
    workspace = tmp_path / "repo"
    commands = gate.build_launch_commands(workspace)

    assert commands.worker.session_name == "swegen-sg-extra-r2"
    assert commands.worker.argv[:5] == (
        "tmux",
        "new-session",
        "-d",
        "-s",
        "swegen-sg-extra-r2",
    )
    worker_text = " ".join(commands.worker.argv)
    assert "SWEGEN_PROXY_ENV_FILE=.env" in worker_text
    assert "SWEGEN_WORKERS=4" in worker_text
    assert "SWEGEN_RUN_NAME=20260716-sol-max-full-16w" in worker_text
    assert "data_cache/orchestrator_shards/sg-extra-doubletons.jsonl" in worker_text
    assert "orchestrator-logs-sg-extra-r2-4w" in worker_text
    assert "orchestrator-progress-sg-extra-r2.jsonl" in worker_text
    assert "orchestrator-instance-status-sg-extra-r2.jsonl" in worker_text
    assert "orchestrator-sg-extra-r2-4w-launch.log" in worker_text

    assert commands.monitor.session_name == "swegen-sg-extra-monitor-r2"
    monitor_text = " ".join(commands.monitor.argv)
    assert "src/multi_proxy_health_monitor.py" in monitor_text
    assert "multi-proxy-health-sg-extra-r2.json" in monitor_text
    assert "multi-proxy-health-sg-extra-r2-monitor.log" in monitor_text
    assert '"name":"SG-extra"' in monitor_text
    assert '"workers":4' in monitor_text

    combined = f"{worker_text} {monitor_text}"
    assert "https://" not in combined
    assert "http://" not in combined
    assert "sk-" not in combined


def test_runtime_restore_never_restores_an_unfinished_stability_clock(tmp_path) -> None:
    status = tmp_path / "gate.json"
    status.write_text(
        json.dumps(
            {
                "event": "sg_extra_stability_gate",
                "scale_triggered": False,
                "stable_since": "2026-07-17T03:54:53+00:00",
                "stable_elapsed_seconds": 1799,
                "launch": {"worker_session_started": False},
            }
        )
    )

    runtime = gate.restore_runtime(status)
    tracker = gate.StabilityTracker(required_seconds=1800.0)
    first = tracker.observe(True, now_epoch=10_000.0, now_monotonic=100.0)

    assert runtime.scale_triggered is False
    assert first.stable_since_epoch == 10_000.0
    assert first.stable_elapsed_seconds == 0.0
