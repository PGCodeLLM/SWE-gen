from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import stage3_reward_guard as guard


def record(
    *,
    status: str = "pass",
    error: str | None = None,
    fallback_reason: str | None = None,
) -> dict[str, object]:
    return {
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
        "instance": "owner__repo-1",
        "status": status,
        "error": error,
        "reward_hack": {
            "error": error,
            "fallback_reason": fallback_reason,
        },
    }


def outcome(category: str, timestamp: float = 1.0) -> guard.Outcome:
    return guard.Outcome(timestamp, "owner__repo-1", "error", category)


def test_primary_429_fallback_is_immediate_error() -> None:
    value = guard.classify_record(
        record(
            fallback_reason=(
                "HTTP 429: All credentials for model spark are cooling down via provider codex"
            )
        )
    )

    assert value is not None
    assert value.category == "immediate_api_error"


def test_isolated_connect_error_is_tolerated() -> None:
    outcomes = [outcome("transient_api_error")]
    outcomes.extend(outcome("completed") for _ in range(19))

    tripped, details = guard.transient_trip(
        outcomes,
        concurrency=20,
        minimum=10,
        rate=0.25,
    )

    assert tripped is False
    assert details["transient_errors"] == 1
    assert details["transient_error_rate"] == 0.05


def test_batch_connect_outage_trips_ratio_and_minimum() -> None:
    outcomes = [outcome("transient_api_error") for _ in range(10)]
    outcomes.extend(outcome("completed") for _ in range(10))

    tripped, details = guard.transient_trip(
        outcomes,
        concurrency=20,
        minimum=10,
        rate=0.25,
    )

    assert tripped is True
    assert details["required_errors"] == 10
    assert details["transient_error_rate"] == 0.5


def test_small_concurrency_still_requires_configured_minimum() -> None:
    outcomes = [outcome("transient_api_error") for _ in range(9)]

    tripped, details = guard.transient_trip(
        outcomes,
        concurrency=4,
        minimum=10,
        rate=0.25,
    )

    assert tripped is False
    assert details["required_errors"] == 10


def test_read_appended_jsonl_retains_partial_line(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger.jsonl"
    first = json.dumps({"status": "pass"}).encode()
    second = json.dumps({"status": "error"}).encode()
    ledger.write_bytes(first + b"\n" + second)

    values, inode, offset = guard.read_appended_jsonl(ledger, inode=None, offset=0)

    assert values == [{"status": "pass"}]
    assert inode == ledger.stat().st_ino
    assert offset == len(first) + 1

    with ledger.open("ab") as stream:
        stream.write(b"\n")
    values, _inode, new_offset = guard.read_appended_jsonl(
        ledger,
        inode=inode,
        offset=offset,
    )
    assert values == [{"status": "error"}]
    assert new_offset == ledger.stat().st_size


def test_funnel_event_records_stage_deltas() -> None:
    event = guard.funnel_event(
        {
            "counts": {
                "generated": 100,
                "baseline_valid": 80,
                "fully_accepted": 50,
            },
            "baseline_state": "running",
            "reward_state": "running",
            "reward_active_count": 20,
        },
        {"generated": 100, "baseline_valid": 79, "fully_accepted": 45},
    )

    assert event["delta"] == {
        "generated": 0,
        "baseline_valid": 1,
        "fully_accepted": 5,
    }
    assert event["stage2_increasing"] is True
    assert event["stage3_increasing"] is True


def test_funnel_event_marks_stalled_stage() -> None:
    event = guard.funnel_event(
        {"counts": {"baseline_valid": 80, "fully_accepted": 50}},
        {"baseline_valid": 80, "fully_accepted": 50},
    )

    assert event["stage2_increasing"] is False
    assert event["stage3_increasing"] is False


def test_funnel_event_does_not_claim_progress_when_dashboard_failed() -> None:
    event = guard.funnel_event({"error": "connection refused"}, None)

    assert event["stage2_increasing"] is None
    assert event["stage3_increasing"] is None
    assert event["error"] == "connection refused"
