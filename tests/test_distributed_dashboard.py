from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from subprocess import CompletedProcess, TimeoutExpired

NOW = datetime(2026, 7, 29, 12, 8, tzinfo=UTC)


def _only_kinds(document: dict[str, object], kinds: set[str]) -> dict[str, object]:
    """Return ``document`` narrowed to the requested kinds, as kubectl would."""

    items = [item for item in document.get("items", []) if item.get("kind") in kinds]
    return {**document, "items": items}


def test_aggregate_pipeline_snapshot_builds_queue_task_and_stage_timing() -> None:
    from swegen.dashboard.distributed_status import aggregate_pipeline_snapshot

    task_rows = [
        {
            "task_id": "owner__repo-1",
            "task_version": 1,
            "repo": "owner/repo",
            "pr": 1,
            "trace_id": "11111111-1111-4111-8111-111111111111",
            "state": "queued",
            "current_stage": "reward",
            "created_at": NOW - timedelta(minutes=8),
            "updated_at": NOW - timedelta(minutes=3),
            "finished_at": None,
            "last_error": None,
            "last_reason": None,
        },
        {
            "task_id": "owner__repo-2",
            "task_version": 1,
            "repo": "owner/repo",
            "pr": 2,
            "trace_id": "22222222-2222-4222-8222-222222222222",
            "state": "queued",
            "current_stage": "generate",
            "created_at": NOW - timedelta(minutes=1),
            "updated_at": NOW - timedelta(minutes=1),
            "finished_at": None,
            "last_error": None,
            "last_reason": None,
        },
    ]
    result_rows = [
        {
            "task_id": "owner__repo-1",
            "task_version": 1,
            "stage": "generate",
            "attempt": 1,
            "status": "succeeded",
            "pgmq_msg_id": 10,
            "pgmq_read_count": 2,
            "worker_id": "generate-pod",
            "node_name": "node-generate",
            "started_at": NOW - timedelta(minutes=7),
            "finished_at": NOW - timedelta(minutes=5),
            "result": {},
            "error": None,
        },
        {
            "task_id": "owner__repo-1",
            "task_version": 1,
            "stage": "validate",
            "attempt": 1,
            "status": "succeeded",
            "pgmq_msg_id": 11,
            "pgmq_read_count": 1,
            "worker_id": "validate-pod",
            "node_name": "node-validate",
            "started_at": NOW - timedelta(minutes=4),
            "finished_at": NOW - timedelta(minutes=3),
            "result": {"nop_reward": 0, "oracle_reward": 1},
            "error": None,
        },
    ]
    activity_rows = [
        {
            "task_id": "owner__repo-1",
            "task_version": 1,
            "stage": "reward",
            "attempt": 1,
            "pgmq_msg_id": 12,
            "pgmq_read_count": 1,
            "worker_id": "reward-pod",
            "node_name": "node-reward",
            "started_at": NOW - timedelta(minutes=2),
            "heartbeat_at": NOW - timedelta(seconds=30),
        }
    ]
    queue_rows = [
        {
            "queue_name": "swegen_generate",
            "queue_length": 5,
            "queue_visible_length": 3,
            "total_messages": 100,
            "newest_msg_age_sec": 10,
            "oldest_msg_age_sec": 90,
            "scrape_time": NOW,
        },
        {
            "queue_name": "swegen_validate",
            "queue_length": 4,
            "queue_visible_length": 4,
            "total_messages": 80,
            "newest_msg_age_sec": 20,
            "oldest_msg_age_sec": 120,
            "scrape_time": NOW,
        },
        {
            "queue_name": "swegen_validate_repaired",
            "queue_length": 3,
            "queue_visible_length": 2,
            "total_messages": 12,
            "newest_msg_age_sec": 5,
            "oldest_msg_age_sec": 45,
            "scrape_time": NOW,
        },
        {
            "queue_name": "swegen_reward",
            "queue_length": 2,
            "queue_visible_length": 1,
            "total_messages": 70,
            "newest_msg_age_sec": 30,
            "oldest_msg_age_sec": 180,
            "scrape_time": NOW,
        },
        {
            "queue_name": "swegen_push",
            "queue_length": 0,
            "queue_visible_length": 0,
            "total_messages": 60,
            "newest_msg_age_sec": None,
            "oldest_msg_age_sec": None,
            "scrape_time": NOW,
        },
        {
            "queue_name": "swegen_dead",
            "queue_length": 2,
            "queue_visible_length": 2,
            "total_messages": 9,
            "newest_msg_age_sec": 40,
            "oldest_msg_age_sec": 400,
            "scrape_time": NOW,
        },
    ]

    snapshot = aggregate_pipeline_snapshot(
        task_rows,
        result_rows,
        activity_rows,
        queue_rows,
        now=NOW,
    )

    assert snapshot["queues"]["stages"]["generate"] == {
        "queue": "swegen_generate",
        "length": 5,
        "visible": 3,
        "in_flight": 2,
        "total_messages": 100,
        "newest_message_age_seconds": 10,
        "oldest_message_age_seconds": 90,
        "scraped_at": NOW.isoformat(),
    }
    assert snapshot["queues"]["dead"]["length"] == 2
    assert snapshot["queues"]["dead"]["visible"] == 2
    assert snapshot["queues"]["stages"]["validate"] == {
        "queue": "swegen_validate_repaired+swegen_validate",
        "length": 7,
        "visible": 6,
        "in_flight": 1,
        "total_messages": 92,
        "newest_message_age_seconds": 5,
        "oldest_message_age_seconds": 120,
        "scraped_at": NOW.isoformat(),
    }
    assert snapshot["queues"]["validate_repaired"]["length"] == 3
    assert snapshot["queues"]["validate_repaired"]["visible"] == 2
    assert snapshot["queues"]["validate_repaired"]["in_flight"] == 1
    assert snapshot["queues"]["validate_new"] == {
        "queue": "swegen_validate",
        "length": 4,
        "visible": 4,
        "in_flight": 0,
        "total_messages": 80,
        "newest_message_age_seconds": 20,
        "oldest_message_age_seconds": 120,
        "scraped_at": NOW.isoformat(),
    }
    assert snapshot["task_counts"] == {
        "total": 2,
        "by_state": {"queued": 2},
        "by_stage": {"generate": 1, "reward": 1},
    }

    first = snapshot["tasks"][0]
    assert first["task_id"] == "owner__repo-1"
    assert first["total_elapsed_seconds"] == 480.0
    stages = {stage["stage"]: stage for stage in first["stages"]}
    # The stage view carries only the five fields the dashboard's Timeline column
    # renders. attempt/deliveries/queued_at/started_at/finished_at/heartbeat_at/
    # queued_seconds/heartbeat_age_seconds/stale/node_name/error were dropped: at
    # 5 stages x 100 tasks they were ~245KB of a ~590KB payload and no front-end
    # code read any of them.
    assert stages["generate"] == {
        "stage": "generate",
        "state": "succeeded",
        "wait_seconds": 60.0,
        "run_seconds": 120.0,
        "worker_id": "generate-pod",
    }
    assert stages["validate"]["wait_seconds"] == 60.0
    assert stages["validate"]["run_seconds"] == 60.0
    assert stages["reward"]["state"] == "running"
    assert stages["reward"]["wait_seconds"] == 60.0
    assert stages["reward"]["run_seconds"] == 120.0
    assert stages["push"]["state"] == "not_started"

    second = snapshot["tasks"][1]
    assert second["task_id"] == "owner__repo-2"
    generate = second["stages"][0]
    assert generate["state"] == "queued"
    assert generate["wait_seconds"] is None
    assert generate["run_seconds"] is None


def test_aggregate_pipeline_snapshot_computes_completion_windows_and_stale_activity() -> None:
    from swegen.dashboard.distributed_status import aggregate_pipeline_snapshot

    task = {
        "task_id": "owner__repo-1",
        "task_version": 1,
        "repo": "owner/repo",
        "pr": 1,
        "trace_id": "11111111-1111-4111-8111-111111111111",
        "state": "queued",
        "current_stage": "reward",
        "created_at": NOW - timedelta(hours=1),
        "updated_at": NOW - timedelta(minutes=4),
        "finished_at": None,
        "last_error": None,
        "last_reason": None,
    }
    results = [
        {
            "task_id": "owner__repo-1",
            "task_version": 1,
            "stage": "generate",
            "attempt": 1,
            "status": "succeeded",
            "pgmq_msg_id": 1,
            "pgmq_read_count": 1,
            "worker_id": "pod-a",
            "node_name": "node-a",
            "started_at": NOW - timedelta(minutes=20),
            "finished_at": NOW - timedelta(minutes=14),
            "result": {},
            "error": None,
        },
        {
            "task_id": "owner__repo-1",
            "task_version": 1,
            "stage": "validate",
            "attempt": 1,
            "status": "failed",
            "pgmq_msg_id": 2,
            "pgmq_read_count": 3,
            "worker_id": "pod-b",
            "node_name": "node-b",
            "started_at": NOW - timedelta(minutes=6),
            "finished_at": NOW - timedelta(minutes=4),
            "result": {},
            "error": "validation failed",
        },
    ]
    activity = [
        {
            "task_id": "owner__repo-1",
            "task_version": 1,
            "stage": "reward",
            "attempt": 1,
            "pgmq_msg_id": 3,
            "pgmq_read_count": 1,
            "worker_id": "pod-c",
            "node_name": "node-c",
            "started_at": NOW - timedelta(minutes=3),
            "heartbeat_at": NOW - timedelta(minutes=2),
        }
    ]

    snapshot = aggregate_pipeline_snapshot(
        [task],
        results,
        activity,
        [],
        now=NOW,
        activity_stale_after_seconds=90,
    )

    assert snapshot["throughput"]["windows"]["60"]["validate"] == {
        "completed": 0,
        "succeeded": 0,
        "instances_per_second": 0.0,
    }
    assert snapshot["throughput"]["windows"]["300"]["validate"] == {
        "completed": 1,
        "succeeded": 0,
        "instances_per_second": 0.0,
    }
    assert snapshot["throughput"]["windows"]["900"]["generate"] == {
        "completed": 1,
        "succeeded": 1,
        "instances_per_second": round(1 / 900, 6),
    }
    reward = next(stage for stage in snapshot["tasks"][0]["stages"] if stage["stage"] == "reward")
    assert reward["state"] == "running"


def test_task_views_carry_only_the_fields_the_dashboard_renders() -> None:
    """The tasks section is the payload's biggest line item, so it stays minimal.

    At a 100-task limit the old task view shipped ~345KB per response: full
    tracebacks in ``last_error``, plus 11 unread per-stage timing fields across 5
    stages. Nothing in the front-end read any of them. This pins the emitted key
    sets so a new field is added deliberately rather than by accident.
    """

    from swegen.dashboard.distributed_status import aggregate_pipeline_snapshot

    task_rows = [
        {
            "task_id": "owner__repo-1",
            "task_version": 1,
            "state": "queued",
            "current_stage": "generate",
            "created_at": NOW - timedelta(minutes=8),
            "finished_at": None,
            "stored_file_count": 3,
            "stored_bytes": 4096,
        }
    ]
    snapshot = aggregate_pipeline_snapshot(task_rows, [], [], [], now=NOW)
    task = snapshot["tasks"][0]

    # Exactly what the Recent tasks table renders: identity, state, elapsed, the
    # storage cell, and the per-stage timeline.
    assert set(task) == {
        "task_id",
        "task_version",
        "state",
        "current_stage",
        "total_elapsed_seconds",
        "storage",
        "stages",
    }
    # Dropped because nothing read them; last_error carried whole tracebacks.
    for dropped in ("repo", "pr", "trace_id", "created_at", "updated_at", "last_error"):
        assert dropped not in task

    assert set(task["storage"]) == {
        "stored_file_count",
        "stored_bytes",
        "generated_on_node",
        "runtime_path_pattern",
        "runtime_directory_state",
    }
    # The constant strings the UI never showed are gone.
    for dropped in ("durable_source", "runtime_path_is_exact", "durability"):
        assert dropped not in task["storage"]

    for stage_view in task["stages"]:
        assert set(stage_view) == {
            "stage",
            "state",
            "wait_seconds",
            "run_seconds",
            "worker_id",
        }


def test_task_query_selects_only_the_columns_the_snapshot_emits() -> None:
    # The trim reaches the SQL too: the dropped columns are never fetched, so the
    # saving lands on the database round-trip as well as the JSON body.
    import inspect

    from swegen.dashboard.distributed_status import PipelineStatusCollector

    source = inspect.getsource(PipelineStatusCollector.collect)
    task_query = source.split("FROM pipeline_tasks t", 1)[0]
    for dropped in ("t.repo", "t.pr", "t.trace_id", "t.last_error", "t.last_reason"):
        assert dropped not in task_query
    for kept in ("t.task_id", "t.task_version", "t.state", "t.current_stage"):
        assert kept in task_query


def test_aggregate_pipeline_snapshot_separates_fresh_and_stale_global_activity() -> None:
    from swegen.dashboard.distributed_status import aggregate_pipeline_snapshot

    snapshot = aggregate_pipeline_snapshot(
        [],
        [],
        [],
        [],
        now=NOW,
        activity_stale_after_seconds=90,
        activity_count_rows=[
            {"stage": "validate", "fresh": 144, "stale": 21},
            {"stage": "repair", "fresh": 143, "stale": 182},
            {"stage": "reward_repair", "fresh": 5, "stale": 0},
        ],
    )

    assert snapshot["activity"] == {
        "stale_after_seconds": 90,
        "stages": {
            "generate": {"fresh": 0, "stale": 0, "total": 0},
            "validate": {"fresh": 144, "stale": 21, "total": 165},
            "repair": {"fresh": 143, "stale": 182, "total": 325},
            "reward": {"fresh": 0, "stale": 0, "total": 0},
            "push": {"fresh": 0, "stale": 0, "total": 0},
        },
    }


def test_aggregate_pipeline_snapshot_clamps_invalid_negative_durations() -> None:
    from swegen.dashboard.distributed_status import aggregate_pipeline_snapshot

    task = {
        "task_id": "owner__repo-1",
        "task_version": 1,
        "repo": "owner/repo",
        "pr": 1,
        "trace_id": "11111111-1111-4111-8111-111111111111",
        "state": "completed",
        "current_stage": "push",
        "created_at": NOW,
        "updated_at": NOW,
        "finished_at": NOW - timedelta(seconds=1),
        "last_error": None,
        "last_reason": None,
    }
    result = {
        "task_id": "owner__repo-1",
        "task_version": 1,
        "stage": "generate",
        "attempt": 1,
        "status": "succeeded",
        "pgmq_msg_id": 1,
        "pgmq_read_count": 1,
        "worker_id": "pod-a",
        "node_name": "node-a",
        "started_at": NOW - timedelta(seconds=2),
        "finished_at": NOW - timedelta(seconds=3),
        "result": {},
        "error": None,
    }

    snapshot = aggregate_pipeline_snapshot([task], [result], [], [], now=NOW)

    assert snapshot["tasks"][0]["total_elapsed_seconds"] == 0.0
    generate = snapshot["tasks"][0]["stages"][0]
    assert generate["wait_seconds"] == 0.0
    assert generate["run_seconds"] == 0.0


def test_aggregate_pipeline_snapshot_exposes_15_minute_stage_outcomes() -> None:
    from swegen.dashboard.distributed_status import aggregate_pipeline_snapshot

    snapshot = aggregate_pipeline_snapshot(
        [],
        [],
        [],
        [],
        now=NOW,
        hourly_yield_rows=[
            {
                "stage": "validate",
                "bucket": NOW - timedelta(hours=1),
                "succeeded": 8,
                "processed": 10,
            }
        ],
        lifetime_stage_rows=[
            {"stage": "generate", "processed": 1234},
            {"stage": "validate", "processed": 987},
            {"stage": "unknown", "processed": 9999},
        ],
        remote_build_rows=[
            {
                "status": "submitting",
                "recent_count": 2,
                "stale_count": 10,
                "latest_updated_at": NOW - timedelta(minutes=1),
            },
            {
                "status": "queued",
                "recent_count": 3,
                "stale_count": 20,
                "latest_updated_at": NOW - timedelta(minutes=2),
            },
            {
                "status": "running",
                "recent_count": 4,
                "stale_count": 30,
                "latest_updated_at": NOW,
            },
        ],
        remote_build_tracking_available=True,
    )

    assert "stage_time_series" not in snapshot
    assert snapshot["hourly_yield"]["stages"]["validate"] == [
        {
            "bucket": (NOW - timedelta(hours=1)).isoformat(),
            "succeeded": 8,
            "processed": 10,
            "yield_percent": 80.0,
        }
    ]
    assert snapshot["hourly_yield"]["stages"]["push"] == []
    assert snapshot["throughput"]["lifetime_processed"] == {
        "generate": 1234,
        "validate": 987,
        "repair": 0,
        "reward": 0,
        "push": 0,
    }
    assert snapshot["remote_builds"] == {
        "available": True,
        "recent": 9,
        "stale": 60,
        "recent_status_counts": {
            "queued": 3,
            "running": 4,
            "submitting": 2,
        },
        "stale_status_counts": {
            "queued": 20,
            "running": 30,
            "submitting": 10,
        },
        "recent_window_seconds": 4200,
        "latest_updated_at": NOW.isoformat(),
        "authoritative": False,
    }


def test_aggregate_pipeline_snapshot_exposes_per_stage_instance_coverage() -> None:
    from swegen.dashboard.distributed_status import STAGES, aggregate_pipeline_snapshot

    snapshot = aggregate_pipeline_snapshot(
        [],
        [],
        [],
        [],
        now=NOW,
        unique_instance_rows=[
            {"stage": "generate", "unique_instances": 68334},
            {"stage": "validate", "unique_instances": 50338},
            {"stage": "repair", "unique_instances": 17276},
            {"stage": "reward", "unique_instances": 11568},
            {"stage": "push", "unique_instances": 6217},
            # A stage the dashboard does not track must be ignored, not leak in.
            {"stage": "reward_repair", "unique_instances": 42},
        ],
        instance_universe_total=208659,
    )

    coverage = snapshot["instance_coverage"]
    assert coverage["universe_total"] == 208659
    # Every tracked stage carries a distinct-task_id count, defaulting to 0.
    assert set(coverage["unique_instances_processed"]) == set(STAGES)
    assert coverage["unique_instances_processed"] == {
        "generate": 68334,
        "validate": 50338,
        "repair": 17276,
        "reward": 11568,
        "push": 6217,
    }
    # The whole payload still serializes for the /api/pipeline/status response.
    json.dumps(snapshot)


def test_aggregate_pipeline_snapshot_instance_coverage_defaults_and_degrades() -> None:
    from swegen.dashboard.distributed_status import STAGES, aggregate_pipeline_snapshot

    snapshot = aggregate_pipeline_snapshot([], [], [], [], now=NOW)

    coverage = snapshot["instance_coverage"]
    # A mindforge outage leaves the denominator None; the UI shows "—" for it.
    assert coverage["universe_total"] is None
    # Every stage is present and defaults to zero processed instances.
    assert coverage["unique_instances_processed"] == dict.fromkeys(STAGES, 0)


def test_deployment_name_from_worker_id_strips_replicaset_and_pod_hash() -> None:
    from swegen.dashboard.distributed_status import deployment_name_from_worker_id

    # The two trailing dash-delimited hash tokens (ReplicaSet + pod) are removed.
    assert (
        deployment_name_from_worker_id("swegen-generate-9b75d9789-27tw2")
        == "swegen-generate"
    )
    assert (
        deployment_name_from_worker_id("swegen-generate-deepseek-exp-6c4f8b9d5-abc12")
        == "swegen-generate-deepseek-exp"
    )
    # A worker_id that is not a pod name is returned unchanged, not dropped.
    assert deployment_name_from_worker_id("custom-worker") == "custom-worker"
    assert deployment_name_from_worker_id(None) is None
    assert deployment_name_from_worker_id("") is None


def test_resolve_worker_model_handles_63_char_truncated_dynamic_pod_names() -> None:
    from swegen.dashboard.distributed_status import resolve_worker_model

    deployment_to_model = {
        "swegen-generate-dyn-deepseek-v4-flash-1-95-77-23": "deepseek-v4-flash",
        "swegen-generate-dyn-glm-5-2-moedsa-7-244-3-251": "glm-5.2-moedsa",
    }
    # Real dynamic-pool pod names hit the 63-char cap: k8s truncates the tail so
    # the "-<hash>-<rand>" suffix collapses into ONE dashless-in-the-middle blob
    # (here "-54bfbb78785246"), which the regex strip leaves unchanged. The
    # prefix match against the known deployment name still resolves the model.
    truncated = "swegen-generate-dyn-deepseek-v4-flash-1-95-77-23-54bfbb78785246"
    assert len(truncated) == 63
    assert resolve_worker_model(truncated, deployment_to_model) == "deepseek-v4-flash"
    assert (
        resolve_worker_model(
            "swegen-generate-dyn-deepseek-v4-flash-1-95-77-23-54bfbb7872dmwm",
            deployment_to_model,
        )
        == "deepseek-v4-flash"
    )
    # A well-formed (short) pod name resolves too, via the same prefix match.
    assert (
        resolve_worker_model(
            "swegen-generate-dyn-glm-5-2-moedsa-7-244-3-251-abc12",
            deployment_to_model,
        )
        == "glm-5.2-moedsa"
    )
    # Longest-prefix wins so a deployment that is a prefix of another can't steal
    # the other's pods.
    d2m = {"swegen-gen": "short-model", "swegen-gen-big": "big-model"}
    assert resolve_worker_model("swegen-gen-big-abc12-def34", d2m) == "big-model"
    # Unknown worker still resolves to *something* (its derived name), not dropped.
    assert resolve_worker_model("swegen-generate-9b75d9789-27tw2", {}) == "swegen-generate"
    assert resolve_worker_model(None, deployment_to_model) is None


def test_aggregate_generate_model_timeseries_folds_worker_to_model_up_and_down() -> None:
    from swegen.dashboard.distributed_status import aggregate_generate_model_timeseries

    bucket_a = datetime(2026, 8, 7, 10, 0, tzinfo=UTC)
    bucket_b = datetime(2026, 8, 7, 10, 15, tzinfo=UTC)
    rows = [
        # glm pods (swegen-generate) — two distinct pods in the same bucket fold
        # into one model_id total.
        {"bucket": bucket_a, "worker_id": "swegen-generate-9b75d9789-27tw2",
         "status": "succeeded", "n": 6},
        {"bucket": bucket_a, "worker_id": "swegen-generate-9b75d9789-aa000",
         "status": "succeeded", "n": 4},
        {"bucket": bucket_a, "worker_id": "swegen-generate-9b75d9789-27tw2",
         "status": "failed", "n": 2},
        # deepseek pods (swegen-generate-deepseek-exp).
        {"bucket": bucket_a, "worker_id": "swegen-generate-deepseek-exp-6c4f8b9d5-abc12",
         "status": "succeeded", "n": 3},
        {"bucket": bucket_a, "worker_id": "swegen-generate-deepseek-exp-6c4f8b9d5-abc12",
         "status": "error", "n": 5},
        # A 'rejected' status is carried in its own third series (neither up nor
        # down); it never inflates the failure total.
        {"bucket": bucket_a, "worker_id": "swegen-generate-9b75d9789-27tw2",
         "status": "rejected", "n": 99},
        # A later bucket with only glm failures.
        {"bucket": bucket_b, "worker_id": "swegen-generate-9b75d9789-27tw2",
         "status": "failed", "n": 3},
    ]
    deployment_to_model = {
        "swegen-generate": "glm-5.2-pretrain-v1",
        "swegen-generate-deepseek-exp": "deepseek-v4-flash",
    }

    result = aggregate_generate_model_timeseries(rows, deployment_to_model)

    assert result["models"] == ["deepseek-v4-flash", "glm-5.2-pretrain-v1"]
    assert [bucket["t"] for bucket in result["buckets"]] == [
        bucket_a.isoformat(),
        bucket_b.isoformat(),
    ]
    first = result["buckets"][0]["by_model"]
    # glm: 6+4 up, 2 down; the 'rejected' row lands in its own third series and
    # is never folded into the failure total.
    assert first["glm-5.2-pretrain-v1"] == {"succeeded": 10, "failed": 2, "rejected": 99}
    # deepseek: 'error' counts as a failure (down) alongside 'failed'.
    assert first["deepseek-v4-flash"] == {"succeeded": 3, "failed": 5, "rejected": 0}
    second = result["buckets"][1]["by_model"]
    assert second == {"glm-5.2-pretrain-v1": {"succeeded": 0, "failed": 3, "rejected": 0}}


def test_aggregate_generate_model_timeseries_labels_unknown_deployment_by_name() -> None:
    from swegen.dashboard.distributed_status import aggregate_generate_model_timeseries

    bucket = datetime(2026, 8, 7, 10, 0, tzinfo=UTC)
    # An empty deployment->model map (kubectl unavailable) still charts the data,
    # labelling each series by its derived deployment name.
    result = aggregate_generate_model_timeseries(
        [{"bucket": bucket, "worker_id": "swegen-generate-9b75d9789-27tw2",
          "status": "succeeded", "n": 7}],
        {},
    )
    assert result["models"] == ["swegen-generate"]
    assert result["buckets"][0]["by_model"] == {
        "swegen-generate": {"succeeded": 7, "failed": 0, "rejected": 0}
    }


def test_aggregate_downstream_stage_model_timeseries_attributes_to_generating_model() -> None:
    from swegen.dashboard.distributed_status import (
        aggregate_downstream_stage_model_timeseries,
        build_task_generating_model_map,
    )

    bucket = datetime(2026, 8, 7, 11, 0, tzinfo=UTC)
    # task-1 generated by pretrain, task-2 by deepseek, task-3 generated outside
    # the 48h window (absent from the map) -> "unknown".
    task_to_model = build_task_generating_model_map(
        [
            {"task_id": "task-1", "worker_id": "swegen-generate-9b75d9789-27tw2"},
            {"task_id": "task-2", "worker_id": "swegen-generate-deepseek-exp-6c4f8b9d5-abc12"},
        ],
        {
            "swegen-generate": "glm-5.2-pretrain-v1",
            "swegen-generate-deepseek-exp": "deepseek-v4-flash",
        },
    )

    rows = [
        # A deepseek-generated task that FAILS validate must show under
        # deepseek's failed/down bucket at validate.
        {"bucket": bucket, "task_id": "task-2", "stage": "validate", "status": "failed", "n": 1},
        # A pretrain-generated task that succeeds validate -> pretrain up.
        {"bucket": bucket, "task_id": "task-1", "stage": "validate", "status": "succeeded", "n": 1},
        # A validate 'rejected' (nop-oracle) -> pretrain's rejected series.
        {"bucket": bucket, "task_id": "task-1", "stage": "validate", "status": "rejected", "n": 1},
        # A task with no known generate row -> unknown model, at reward.
        {"bucket": bucket, "task_id": "task-3", "stage": "reward", "status": "succeeded", "n": 1},
        # 'error' folds into failed/down at repair for deepseek.
        {"bucket": bucket, "task_id": "task-2", "stage": "repair", "status": "error", "n": 1},
    ]

    result = aggregate_downstream_stage_model_timeseries(rows, task_to_model)

    # All four downstream stages are present even when empty (push here).
    assert set(result) == {"validate", "repair", "reward", "push"}
    assert result["push"] == {"models": [], "buckets": []}

    validate = result["validate"]["buckets"][0]["by_model"]
    assert validate["deepseek-v4-flash"] == {"succeeded": 0, "failed": 1, "rejected": 0}
    assert validate["glm-5.2-pretrain-v1"] == {"succeeded": 1, "failed": 0, "rejected": 1}
    # Unknown generating model is charted, not dropped.
    reward = result["reward"]["buckets"][0]["by_model"]
    assert reward["unknown"] == {"succeeded": 1, "failed": 0, "rejected": 0}
    repair = result["repair"]["buckets"][0]["by_model"]
    assert repair["deepseek-v4-flash"] == {"succeeded": 0, "failed": 1, "rejected": 0}


def test_aggregate_pipeline_snapshot_embeds_unified_stage_model_timeseries() -> None:
    from swegen.dashboard.distributed_status import STAGES, aggregate_pipeline_snapshot

    snapshot = aggregate_pipeline_snapshot(
        [],
        [],
        [],
        [],
        now=NOW,
        generate_model_bucket_rows=[
            {"bucket": datetime(2026, 8, 7, 10, 0, tzinfo=UTC),
             "worker_id": "swegen-generate-9b75d9789-27tw2",
             "status": "succeeded", "n": 5},
        ],
        generate_deployment_to_model={"swegen-generate": "glm-5.2-pretrain-v1"},
        generate_task_model_rows=[
            {"task_id": "task-1", "worker_id": "swegen-generate-9b75d9789-27tw2"},
        ],
        downstream_model_bucket_rows=[
            {"bucket": datetime(2026, 8, 7, 10, 0, tzinfo=UTC),
             "task_id": "task-1", "stage": "validate", "status": "succeeded", "n": 3},
        ],
    )

    unified = snapshot["stage_model_timeseries"]
    assert unified["bucket_seconds"] == 900
    assert unified["lookback_hours"] == 48
    # All five stages are present in the unified structure.
    assert set(unified["stages"]) == set(STAGES)
    generate = unified["stages"]["generate"]
    assert generate["models"] == ["glm-5.2-pretrain-v1"]
    assert generate["buckets"][0]["by_model"] == {
        "glm-5.2-pretrain-v1": {"succeeded": 5, "failed": 0, "rejected": 0}
    }
    # Downstream validate is attributed to task-1's GENERATING model (pretrain).
    validate = unified["stages"]["validate"]
    assert validate["models"] == ["glm-5.2-pretrain-v1"]
    assert validate["buckets"][0]["by_model"] == {
        "glm-5.2-pretrain-v1": {"succeeded": 3, "failed": 0, "rejected": 0}
    }
    # The legacy generate-only key is gone; nothing should read it anymore.
    assert "generate_model_timeseries" not in snapshot
    # Defaults to empty per-stage breakdowns when no rows are provided at all.
    empty = aggregate_pipeline_snapshot([], [], [], [], now=NOW)
    assert set(empty["stage_model_timeseries"]["stages"]) == set(STAGES)
    for stage in STAGES:
        assert empty["stage_model_timeseries"]["stages"][stage] == {
            "models": [],
            "buckets": [],
        }
    json.dumps(snapshot)


def test_resolve_generate_models_from_deployments_reads_last_model_secret() -> None:
    from swegen.dashboard.distributed_status import (
        resolve_generate_models_from_deployments,
    )

    def generate_deployment(name: str, secret: str) -> dict[str, object]:
        return {
            "kind": "Deployment",
            "metadata": {"name": name, "labels": {"swegen.pgcode/stage": "generate"}},
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "envFrom": [
                                    {"configMapRef": {"name": "swegen-pipeline-config"}},
                                    {"secretRef": {"name": "swegen-database"}},
                                    # A non-model secret before the model one must
                                    # not shadow the trailing model credential.
                                    {"secretRef": {"name": "swegen-runtime-proxy"}},
                                    {"secretRef": {"name": secret}},
                                ]
                            }
                        ]
                    }
                }
            },
        }

    items = [
        generate_deployment(
            "swegen-generate", "swegen-model-credentials-glm52-pretrain-v1-20260807"
        ),
        generate_deployment(
            "swegen-generate-deepseek-exp",
            "swegen-model-credentials-deepseek-v4-flash-soldirect-20260807",
        ),
        # A non-generate deployment is ignored entirely.
        {
            "kind": "Deployment",
            "metadata": {"name": "swegen-reward", "labels": {"swegen.pgcode/stage": "reward"}},
            "spec": {"template": {"spec": {"containers": []}}},
        },
    ]
    secret_models = {
        "swegen-model-credentials-glm52-pretrain-v1-20260807": "glm-5.2-pretrain-v1",
        "swegen-model-credentials-deepseek-v4-flash-soldirect-20260807": "deepseek-v4-flash",
    }

    mapping = resolve_generate_models_from_deployments(items, secret_models)
    assert mapping == {
        "swegen-generate": "glm-5.2-pretrain-v1",
        "swegen-generate-deepseek-exp": "deepseek-v4-flash",
    }
    # A deployment whose secret value could not be read is left unmapped so the
    # chart falls back to its deployment-name label.
    partial = resolve_generate_models_from_deployments(items, {})
    assert partial == {}


def test_resolve_generate_models_from_deployments_reads_inline_env_for_dynamic() -> None:
    from swegen.dashboard.distributed_status import (
        resolve_generate_models_from_deployments,
    )

    # A dynamic endpoint deployment (swegen-generate-dyn-<slug>) delivers the
    # model as INLINE container env, with NO swegen-model-credentials-* envFrom.
    dyn = {
        "kind": "Deployment",
        "metadata": {
            "name": "swegen-generate-dyn-deepseek-v4-flash-1-95-77-23",
            "labels": {"swegen.pgcode/stage": "generate"},
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "worker",
                            "envFrom": [
                                {"configMapRef": {"name": "swegen-pipeline-config"}},
                                {"secretRef": {"name": "swegen-database"}},
                            ],
                            "env": [
                                {"name": "ANTHROPIC_BASE_URL", "value": "http://1.95.77.23"},
                                {"name": "ANTHROPIC_MODEL", "value": "deepseek-v4-flash"},
                            ],
                        }
                    ]
                }
            }
        },
    }

    # No secret_models are available for a dynamic deployment; the inline env is
    # the source of truth and must still resolve.
    mapping = resolve_generate_models_from_deployments([dyn], {})
    assert mapping == {
        "swegen-generate-dyn-deepseek-v4-flash-1-95-77-23": "deepseek-v4-flash"
    }


def test_resolve_generate_models_from_deployments_inline_env_wins_over_secret() -> None:
    from swegen.dashboard.distributed_status import (
        resolve_generate_models_from_deployments,
    )

    # A single deployment carrying BOTH a model-credential secret and an inline
    # ANTHROPIC_MODEL env. Inline env overrides envFrom at runtime, so the inline
    # value must win.
    item = {
        "kind": "Deployment",
        "metadata": {
            "name": "swegen-generate-conflict",
            "labels": {"swegen.pgcode/stage": "generate"},
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "worker",
                            "envFrom": [
                                {
                                    "secretRef": {
                                        "name": "swegen-model-credentials-secret-model-20260807"
                                    }
                                },
                            ],
                            "env": [
                                {"name": "ANTHROPIC_MODEL", "value": "inline-model"},
                            ],
                        }
                    ]
                }
            }
        },
    }

    mapping = resolve_generate_models_from_deployments(
        [item],
        {"swegen-model-credentials-secret-model-20260807": "secret-model"},
    )
    assert mapping == {"swegen-generate-conflict": "inline-model"}


def test_aggregate_generate_model_timeseries_keys_dynamic_series_by_model() -> None:
    from swegen.dashboard.distributed_status import aggregate_generate_model_timeseries

    bucket = datetime(2026, 8, 7, 10, 0, tzinfo=UTC)
    # A dynamic-endpoint worker pod. Its worker_id strips the <replicaset-hash>-
    # <pod-hash> suffix down to the dyn deployment name, which the map resolves
    # to the model; the resulting by_model key must be the MODEL, not the
    # deployment name.
    worker_id = "swegen-generate-dyn-deepseek-v4-flash-1-95-77-23-54bfbb7878-5246k"
    deployment = "swegen-generate-dyn-deepseek-v4-flash-1-95-77-23"
    result = aggregate_generate_model_timeseries(
        [{"bucket": bucket, "worker_id": worker_id, "status": "succeeded", "n": 4}],
        {deployment: "deepseek-v4-flash"},
    )
    assert result["models"] == ["deepseek-v4-flash"]
    by_model = result["buckets"][0]["by_model"]
    assert set(by_model) == {"deepseek-v4-flash"}
    assert deployment not in by_model
    assert by_model["deepseek-v4-flash"] == {"succeeded": 4, "failed": 0, "rejected": 0}


def test_summarize_swr_push_sync_computes_registry_counts_and_out_of_sync_ids() -> None:
    from swegen.dashboard.distributed_status import summarize_swr_push_sync

    summary = summarize_swr_push_sync(
        {
            "platform_count": 11372,
            "trajectory_count": 10023,
            "in_sync": 10158,
            "platform_only": 1214,
            "trajectory_only": 135,
        },
        [
            {"instance": "img-a", "on_platform": True, "on_trajectory": False},
            {"instance": "img-b", "on_platform": False, "on_trajectory": True},
        ],
        available=True,
    )

    assert summary["available"] is True
    assert summary["platform_count"] == 11372
    assert summary["trajectory_count"] == 10023
    assert summary["in_sync"] == 10158
    assert summary["platform_only"] == 1214
    assert summary["trajectory_only"] == 135
    # platform_only + trajectory_only, independent of the truncated id list.
    assert summary["out_of_sync_total"] == 1349
    assert summary["out_of_sync_instances"] == [
        {"instance": "img-a", "registry": "platform"},
        {"instance": "img-b", "registry": "trajectory"},
    ]
    # The list was capped well below the true total, so it is flagged truncated.
    assert summary["out_of_sync_list_truncated"] is True


def test_summarize_swr_push_sync_degrades_when_pushed_images_absent() -> None:
    from swegen.dashboard.distributed_status import summarize_swr_push_sync

    summary = summarize_swr_push_sync(None, available=False)

    assert summary == {
        "available": False,
        "platform_count": 0,
        "trajectory_count": 0,
        "in_sync": 0,
        "platform_only": 0,
        "trajectory_only": 0,
        "out_of_sync_total": 0,
        "out_of_sync_instances": [],
        "out_of_sync_list_limit": 500,
        "out_of_sync_list_truncated": False,
    }


def test_aggregate_pipeline_snapshot_surfaces_swr_push_sync_section() -> None:
    from swegen.dashboard.distributed_status import aggregate_pipeline_snapshot

    snapshot = aggregate_pipeline_snapshot(
        [],
        [],
        [],
        [],
        now=NOW,
        swr_push_sync_row={
            "platform_count": 11372,
            "trajectory_count": 10023,
            "in_sync": 10158,
            "platform_only": 1214,
            "trajectory_only": 135,
        },
        swr_push_sync_out_of_sync_rows=[
            {"instance": "img-a", "on_platform": True, "on_trajectory": False},
        ],
        swr_push_sync_available=True,
    )

    section = snapshot["swr_push_sync"]
    assert section["available"] is True
    assert section["platform_count"] == 11372
    assert section["trajectory_count"] == 10023
    assert section["in_sync"] == 10158
    assert section["platform_only"] == 1214
    assert section["trajectory_only"] == 135
    assert section["out_of_sync_total"] == 1349
    assert section["out_of_sync_instances"] == [
        {"instance": "img-a", "registry": "platform"},
    ]
    # JSON-serializable so it can ride the same payload the other sections use.
    json.dumps(snapshot)


def test_aggregate_pipeline_snapshot_swr_push_sync_defaults_to_unavailable() -> None:
    from swegen.dashboard.distributed_status import aggregate_pipeline_snapshot

    snapshot = aggregate_pipeline_snapshot([], [], [], [], now=NOW)

    assert snapshot["swr_push_sync"]["available"] is False
    assert snapshot["swr_push_sync"]["platform_count"] == 0
    assert snapshot["swr_push_sync"]["out_of_sync_instances"] == []


def test_remote_buildkit_resources_support_worker_local_schema() -> None:
    from swegen.dashboard.distributed_status import summarize_remote_buildkit_resources

    summary = summarize_remote_buildkit_resources(
        {
            "success": True,
            "scope": "local_buildkit_worker",
            "owner_pod": "buildkit-worker-6",
            "queue_depth": 7,
            "queue_capacity": 2000,
            "running_count": 3,
            "queue": {"api_capacity": {"running": 4, "max": 50}},
        },
        sampled_workers=("buildkit-worker-2",),
    )

    assert summary == {
        "available": True,
        "scope": "local_buildkit_worker",
        "is_global": False,
        "sampled_worker": "buildkit-worker-6",
        "sampled_workers": ["buildkit-worker-2", "buildkit-worker-6"],
        "sampled_worker_count": 2,
        "backend_count": None,
        "available_backend_count": None,
        "queue_length": 7,
        "queue_capacity": 2000,
        "running_builds": 3,
        "inflight_builds": 4,
        "node_disk_io": [],
        "schema_warning": (
            "Farm API returned a worker-local sample; queue and running counts are not global."
        ),
    }


def test_remote_buildkit_resources_support_documented_global_schema() -> None:
    from swegen.dashboard.distributed_status import summarize_remote_buildkit_resources

    summary = summarize_remote_buildkit_resources(
        {
            "count": 3,
            "global_queue": {"queued": 8, "max_queued": 1000},
            "global_backend_inflight": {"backend-a": 2, "backend-b": 1},
            "backends": [
                {"name": "a", "healthy_for_new_build": True},
                {"name": "b", "healthy_for_new_build": False},
                {"name": "c", "healthy_for_new_build": True},
            ],
        }
    )

    assert summary["is_global"] is True
    assert summary["backend_count"] == 3
    assert summary["available_backend_count"] == 2
    assert summary["queue_length"] == 8
    assert summary["queue_capacity"] == 1000
    assert summary["inflight_builds"] == 3
    assert summary["schema_warning"] is None


def test_remote_buildkit_resources_normalize_per_node_disk_io() -> None:
    from swegen.dashboard.distributed_status import summarize_remote_buildkit_resources

    summary = summarize_remote_buildkit_resources(
        {
            "global_queue": {"queued": 0, "max_queued": 1000},
            "nodes": [
                {
                    "name": "farm-node-a",
                    "resources": {
                        "disk_io": {
                            "read_bps": 1_048_576,
                            "write_bps": 2_097_152,
                            "read_iops": 12,
                            "write_iops": 34,
                            "utilization_percent": 81.5,
                            "io_current": 3,
                        }
                    },
                }
            ],
        }
    )

    assert summary["node_disk_io"] == [
        {
            "node": "farm-node-a",
            "read_bytes_per_second": 1_048_576.0,
            "write_bytes_per_second": 2_097_152.0,
            "read_iops": 12.0,
            "write_iops": 34.0,
            "busy_percent": 81.5,
            "io_current": 3.0,
        }
    ]


def test_cadvisor_disk_io_parser_avoids_parent_partition_iops_double_counting() -> None:
    from swegen.dashboard.distributed_status import parse_cadvisor_disk_io

    metrics = parse_cadvisor_disk_io(
        "\n".join(
            (
                'container_fs_reads_bytes_total{device="/dev/vda",id="/"} 100',
                'container_fs_writes_bytes_total{device="/dev/vda",id="/"} 200',
                'container_fs_reads_total{device="/dev/vda",id="/"} 10',
                'container_fs_reads_total{device="/dev/vda1",id="/"} 10',
                'container_fs_writes_total{device="/dev/vda",id="/"} 20',
                'container_fs_writes_total{device="/dev/vda1",id="/"} 20',
                'container_fs_io_current{device="/dev/vda1",id="/"} 2',
                'container_fs_io_time_seconds_total{device="/dev/vda1",id="/"} 5',
                'container_fs_reads_bytes_total{device="/dev/shm",id="/"} 999',
                'container_fs_reads_bytes_total{device="/dev/vda",id="/pod"} 999',
            )
        )
    )

    assert metrics == {
        "read_bytes": 100.0,
        "write_bytes": 200.0,
        "read_ops": 10.0,
        "write_ops": 20.0,
        "io_current": 2.0,
        "io_time_by_device": {"/dev/vda1": 5.0},
        "device_count": 1,
    }


def test_remote_buildkit_collector_enforces_safe_polling_and_plain_resources_path() -> None:
    from swegen.dashboard.distributed_status import RemoteBuildKitFarmCollector

    calls: list[tuple[str, float]] = []

    class Response:
        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        @staticmethod
        def getcode() -> int:
            return 200

        @staticmethod
        def read(_size: int) -> bytes:
            return b'{"scope":"local_buildkit_worker","queue_depth":0}'

    class Opener:
        @staticmethod
        def open(request: object, *, timeout: float) -> Response:
            calls.append((request.full_url, timeout))  # type: ignore[attr-defined]
            return Response()

    collector = RemoteBuildKitFarmCollector(
        base_url="http://7.156.122.134:32083/",
        poll_seconds=1,
        opener=Opener(),
    )

    assert collector.poll_seconds == 30
    status, payload = collector._fetch("/resources", 1200)
    assert status == 200
    assert payload == {"scope": "local_buildkit_worker", "queue_depth": 0}
    assert calls == [("http://7.156.122.134:32083/resources", 1200)]
    assert "force_refresh" not in calls[0][0]
    assert "include_cache_details" not in calls[0][0]


def test_remote_buildkit_collector_reports_unconfigured_instead_of_guessing_a_url(
    monkeypatch,
) -> None:
    """An unset farm URL degrades the panel; it must not poll a hardcoded address.

    The module used to carry a copy of the live farm IP, so the dashboard kept
    polling it and presenting it as "the farm" even when nothing configured one.
    Raising is not an option here: SnapshotCache builds this collector in its
    __init__, outside the per-collector error handling in refresh(), so a raise
    would take the whole status page down over one optional panel.
    """

    from swegen.dashboard.distributed_status import (
        REMOTE_BUILDKIT_URL_ENV,
        RemoteBuildKitFarmCollector,
    )

    monkeypatch.delenv(REMOTE_BUILDKIT_URL_ENV, raising=False)
    monkeypatch.delenv("SWEGEN_BUILDKIT_FARM_URL", raising=False)

    def forbidden_open(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("an unconfigured farm must never be polled")

    collector = RemoteBuildKitFarmCollector(opener=type("O", (), {"open": forbidden_open})())

    assert collector.configured is False
    assert collector.base_url == ""

    snapshot = collector.collect()

    assert snapshot["gateway"]["ok"] is False
    assert snapshot["ready"]["ok"] is False
    assert snapshot["resources"]["available"] is False
    # The rendered warning names the variable and the manifest that supplies it.
    for message in (
        snapshot["gateway"]["error"],
        snapshot["ready"]["error"],
        snapshot["resources"]["error"],
    ):
        assert REMOTE_BUILDKIT_URL_ENV in message
        assert "swegen-pipeline-config" in message


def test_remote_buildkit_collector_uses_a_configured_url_from_the_environment(
    monkeypatch,
) -> None:
    from swegen.dashboard.distributed_status import (
        REMOTE_BUILDKIT_URL_ENV,
        RemoteBuildKitFarmCollector,
    )

    monkeypatch.setenv(REMOTE_BUILDKIT_URL_ENV, "http://farm.example:32083/")

    collector = RemoteBuildKitFarmCollector()

    assert collector.configured is True
    assert collector.base_url == "http://farm.example:32083"


def test_k3s_collector_sums_multiple_deployments_for_the_same_stage() -> None:
    from swegen.dashboard.distributed_status import K3sStatusCollector

    nodes = {"items": []}
    workloads = {
        "items": [
            {
                "kind": "Deployment",
                "metadata": {"name": "swegen-generate"},
                "spec": {
                    "replicas": 92,
                    "selector": {"matchLabels": {"swegen.pgcode/stage": "generate"}},
                },
                "status": {"readyReplicas": 92, "availableReplicas": 92},
            },
            {
                "kind": "Deployment",
                "metadata": {"name": "swegen-generate-canary"},
                "spec": {
                    "replicas": 4,
                    "selector": {"matchLabels": {"swegen.pgcode/stage": "generate"}},
                },
                "status": {"readyReplicas": 4, "availableReplicas": 4},
            },
        ]
    }

    def runner(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        # The collector queries deployments and pods separately so the pod
        # query can carry a field selector; mirror that split here.
        if "nodes" in command:
            document = nodes
        elif "deployments" in command:
            document = _only_kinds(workloads, {"Deployment"})
        else:
            document = _only_kinds(workloads, {"Pod"})
        return CompletedProcess(command, 0, stdout=json.dumps(document), stderr="")

    stage = K3sStatusCollector(runner=runner).collect()["stages"]["generate"]

    assert stage["desired"] == 96
    assert "ready" not in stage
    assert "available" not in stage
    assert "pods_ready" not in stage
    assert stage["pod_phases"] == {}


def test_k3s_collector_counts_only_the_single_generate_deployment() -> None:
    # Generate is now one deployment (the overflow/moedsa split was removed and
    # STAGE_ALIASES is empty). Its replicas and pods count under "generate";
    # pods carrying stale generate-overflow / generate-moedsa labels from a
    # partly-drained cutover must NOT be folded into the generate count.
    from swegen.dashboard.distributed_status import K3sStatusCollector

    nodes = {"items": [{"metadata": {"name": "node-a"}, "status": {}}]}
    workloads = {
        "items": [
            {
                "kind": "Deployment",
                "metadata": {"name": "swegen-generate"},
                "spec": {
                    "replicas": 228,
                    "selector": {"matchLabels": {"swegen.pgcode/stage": "generate"}},
                },
                "status": {"readyReplicas": 228},
            },
            _pod("gen-1", "generate", {"phase": "Running"}),
            _pod("gen-2", "generate", {"phase": "Running"}),
            # Stale labels from a not-yet-removed overflow/moedsa pool: no alias
            # exists, so these are ignored rather than counted as generate.
            _pod("ovf-1", "generate-overflow", {"phase": "Running"}),
            _pod("moe-1", "generate-moedsa", {"phase": "Running"}),
        ]
    }

    def runner(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        if "nodes" in command:
            document = nodes
        elif "deployments" in command:
            document = _only_kinds(workloads, {"Deployment"})
        else:
            document = _only_kinds(workloads, {"Pod"})
        return CompletedProcess(command, 0, stdout=json.dumps(document), stderr="")

    stages = K3sStatusCollector(runner=runner).collect()["stages"]

    # Only the single deployment's 228 replicas and its two generate-labelled
    # Running pods count.
    assert stages["generate"]["desired"] == 228
    assert stages["generate"]["pod_phases"].get("Running") == 2
    assert stages["generate"]["nodes"].get("node-a") == 2
    # The stale-label pools are not counted and do not leak as stage keys.
    assert "generate-overflow" not in stages
    assert "generate-moedsa" not in stages


def _pod(name: str, stage: str, status: dict[str, object], **metadata: object) -> dict[str, object]:
    return {
        "kind": "Pod",
        "metadata": {"name": name, "labels": {"swegen.pgcode/stage": stage}, **metadata},
        "spec": {"nodeName": "node-a", "containers": [{}]},
        "status": {"containerStatuses": [{"ready": True, "restartCount": 0}], **status},
    }


def test_k3s_collector_counts_real_pod_phases_instead_of_a_ready_fraction() -> None:
    from swegen.dashboard.distributed_status import K3sStatusCollector

    nodes = {"items": [{"metadata": {"name": "node-a"}, "status": {"allocatable": {"cpu": "8"}}}]}
    workloads = {
        "items": [
            {
                "kind": "Deployment",
                "metadata": {"name": "swegen-validate"},
                "spec": {
                    "replicas": 128,
                    "selector": {"matchLabels": {"swegen.pgcode/stage": "validate"}},
                },
                "status": {"readyReplicas": 122, "availableReplicas": 122},
            },
            _pod("validate-running", "validate", {"phase": "Running"}),
            _pod("validate-pending", "validate", {"phase": "Pending"}),
            _pod("validate-succeeded", "validate", {"phase": "Succeeded"}),
            # Terminating is not a phase: a Running pod carrying a deletion timestamp.
            _pod(
                "validate-terminating",
                "validate",
                {"phase": "Running"},
                deletionTimestamp="2026-08-01T00:00:00Z",
            ),
            # A deletion timestamp on an already terminal pod must not read as Terminating.
            _pod(
                "validate-deleted-succeeded",
                "validate",
                {"phase": "Succeeded"},
                deletionTimestamp="2026-08-01T00:00:00Z",
            ),
            *(
                _pod(
                    f"validate-evicted-{index}",
                    "validate",
                    {"phase": "Failed", "reason": "Evicted", "containerStatuses": []},
                )
                for index in range(2_000)
            ),
        ]
    }

    def runner(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        if "top" in command:
            return CompletedProcess(command, 1, stdout="", stderr="metrics unavailable")
        # The collector queries deployments and pods separately so the pod
        # query can carry a field selector; mirror that split here.
        if "nodes" in command:
            document = nodes
        elif "deployments" in command:
            document = _only_kinds(workloads, {"Deployment"})
        else:
            document = _only_kinds(workloads, {"Pod"})
        return CompletedProcess(command, 0, stdout=json.dumps(document), stderr="")

    snapshot = K3sStatusCollector(runner=runner).collect()
    stage = snapshot["stages"]["validate"]

    assert stage["desired"] == 128
    assert stage["pod_phases"] == {
        "Running": 1,
        "Pending": 1,
        "Terminating": 1,
        "Succeeded": 2,
    }
    assert list(stage["pod_phases"]) == ["Running", "Pending", "Terminating", "Succeeded"]
    assert stage["evicted"] == 2_000
    # Eviction records stay out of the node pod list and the per-stage node histogram.
    assert stage["nodes"] == {"node-a": 5}
    assert [pod["name"] for pod in snapshot["nodes"][0]["pods"]] == [
        "validate-deleted-succeeded",
        "validate-pending",
        "validate-running",
        "validate-succeeded",
        "validate-terminating",
    ]
    assert snapshot["nodes"][0]["pod_count"] == 5
    terminating = next(
        pod for pod in snapshot["nodes"][0]["pods"] if pod["name"] == "validate-terminating"
    )
    assert terminating["phase"] == "Terminating"


def test_pod_display_phase_separates_terminating_and_evicted_from_phases() -> None:
    from swegen.dashboard.distributed_status import pod_display_phase

    assert pod_display_phase({"status": {"phase": "Running"}}) == "Running"
    assert pod_display_phase({"status": {}}) == "Unknown"
    assert (
        pod_display_phase(
            {
                "metadata": {"deletionTimestamp": "2026-08-01T00:00:00Z"},
                "status": {"phase": "Pending"},
            }
        )
        == "Terminating"
    )
    assert pod_display_phase({"status": {"phase": "Failed", "reason": "Evicted"}}) == "Evicted"
    assert pod_display_phase({"status": {"phase": "Failed", "reason": "OOMKilled"}}) == "Failed"
    assert (
        pod_display_phase(
            {
                "metadata": {"deletionTimestamp": "2026-08-01T00:00:00Z"},
                "status": {"phase": "Failed"},
            }
        )
        == "Failed"
    )


def test_k3s_collector_reads_authoritative_local_build_slots() -> None:
    from swegen.dashboard.distributed_status import K3sStatusCollector

    calls: list[list[str]] = []

    def runner(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        calls.append(command)
        return CompletedProcess(
            command,
            0,
            stdout=json.dumps(
                {
                    "total": 48,
                    "used": 7,
                    "free": 41,
                    "waiters": None,
                    "waiters_source": None,
                }
            ),
            stderr="",
        )

    metrics = K3sStatusCollector(runner=runner)._collect_build_slot_metrics(
        {"node-a": "validate-a"}
    )

    assert metrics["node-a"] | {"sampled_at": None} == {
        "available": True,
        "used": 7,
        "total": 48,
        "free": 41,
        "utilization_percent": 14.6,
        "waiters": None,
        "waiters_source": None,
        "sampled_at": None,
        "stale": False,
        "error": None,
    }
    assert calls[0][:7] == [
        "kubectl",
        "--request-timeout=8s",
        "-n",
        "swegen-pipeline",
        "exec",
        "validate-a",
        "--",
    ]
    assert "flock" in calls[0][-1]


def test_k3s_collector_does_not_render_failed_slot_probe_as_zero() -> None:
    from swegen.dashboard.distributed_status import K3sStatusCollector

    def runner(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        return CompletedProcess(command, 1, stdout="", stderr="slot directory unavailable")

    metrics = K3sStatusCollector(runner=runner)._collect_build_slot_metrics(
        {"node-a": "validate-a"}
    )

    assert metrics["node-a"] == {
        "available": False,
        "used": None,
        "total": None,
        "free": None,
        "utilization_percent": None,
        "waiters": None,
        "waiters_source": None,
        "sampled_at": None,
        "stale": False,
        "error": "slot directory unavailable",
    }


def test_k3s_collector_contains_slot_probe_timeout_to_the_node() -> None:
    from swegen.dashboard.distributed_status import K3sStatusCollector

    def runner(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        raise TimeoutExpired(command, 10)

    metrics = K3sStatusCollector(runner=runner)._collect_build_slot_metrics(
        {"node-a": "swegen-buildkit-pruner-a"}
    )

    assert metrics["node-a"] == {
        "available": False,
        "used": None,
        "total": None,
        "free": None,
        "utilization_percent": None,
        "waiters": None,
        "waiters_source": None,
        "sampled_at": None,
        "stale": False,
        "error": "BuildKit slot probe timed out after 10 seconds",
    }


def test_k3s_collector_reports_cluster_resources_and_retains_stale_metrics() -> None:
    from swegen.dashboard.distributed_status import K3sStatusCollector

    nodes = {
        "items": [
            {
                "metadata": {"name": "node-a"},
                "status": {
                    "allocatable": {"cpu": "4", "memory": "8Gi"},
                    "addresses": [{"type": "InternalIP", "address": "10.0.0.1"}],
                },
            },
            {
                "metadata": {"name": "node-b"},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "16Gi"},
                    "addresses": [{"type": "InternalIP", "address": "10.0.0.2"}],
                },
            },
        ]
    }
    workloads = {
        "items": [
            {
                "kind": "Pod",
                "metadata": {
                    "name": "swegen-buildkit-pruner-node-a",
                    "labels": {"app.kubernetes.io/name": "swegen-buildkit-pruner"},
                },
                "spec": {
                    "nodeName": "node-a",
                    "containers": [
                        {
                            "volumeMounts": [
                                {
                                    "name": "build-slots",
                                    "mountPath": "/run/swegen-build-slots",
                                }
                            ]
                        }
                    ],
                },
                "status": {
                    "phase": "Running",
                    "containerStatuses": [{"ready": True, "restartCount": 0}],
                },
            },
            {
                "kind": "Pod",
                "metadata": {
                    "name": "generate-a",
                    "labels": {"swegen.pgcode/stage": "generate"},
                },
                "spec": {
                    "nodeName": "node-a",
                    "containers": [{"resources": {"requests": {"cpu": "500m"}}}],
                },
                "status": {
                    "phase": "Running",
                    "containerStatuses": [{"ready": True, "restartCount": 0}],
                },
            },
            {
                "kind": "Pod",
                "metadata": {
                    "name": "reward-a",
                    "labels": {"swegen.pgcode/stage": "reward"},
                },
                "spec": {
                    "nodeName": "node-a",
                    "containers": [
                        {
                            "resources": {"requests": {"cpu": "0.25"}},
                            "volumeMounts": [
                                {
                                    "name": "build-slots",
                                    "mountPath": "/run/swegen-build-slots",
                                }
                            ],
                        }
                    ],
                },
                "status": {
                    "phase": "Running",
                    "containerStatuses": [{"ready": False, "restartCount": 2}],
                },
            },
            {
                "kind": "Pod",
                "metadata": {
                    "name": "validate-b",
                    "labels": {"swegen.pgcode/stage": "validate"},
                },
                "spec": {
                    "nodeName": "node-b",
                    "containers": [{"resources": {"requests": {"cpu": "1500m"}}}],
                },
                "status": {
                    "phase": "Running",
                    "containerStatuses": [{"ready": True, "restartCount": 1}],
                },
            },
        ]
    }
    fail_top = False
    exec_calls: list[list[str]] = []

    def runner(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        if "exec" in command:
            exec_calls.append(command)
            return CompletedProcess(
                command,
                0,
                stdout=json.dumps(
                    {
                        "total": 48,
                        "used": 7,
                        "free": 41,
                        "waiters": None,
                        "waiters_source": None,
                    }
                ),
                stderr="",
            )
        if "top" in command:
            if fail_top:
                return CompletedProcess(command, 1, stdout="", stderr="metrics unavailable")
            return CompletedProcess(
                command,
                0,
                stdout="node-a 1000m 25% 2Gi 25%\nnode-b 2000m 25% 4096Mi 25%\n",
                stderr="",
            )
        # The collector queries deployments and pods separately so the pod
        # query can carry a field selector; mirror that split here.
        if "nodes" in command:
            document = nodes
        elif "deployments" in command:
            document = _only_kinds(workloads, {"Deployment"})
        else:
            document = _only_kinds(workloads, {"Pod"})
        return CompletedProcess(command, 0, stdout=json.dumps(document), stderr="")

    collector = K3sStatusCollector(runner=runner)
    first_snapshot = collector.collect()
    metrics = first_snapshot["resource_metrics"]

    assert metrics["available"] is True
    assert metrics["stale"] is False
    assert metrics["aggregate"]["cpu_used_millicores"] == 3_000
    assert metrics["aggregate"]["cpu_allocatable_millicores"] == 12_000
    assert metrics["aggregate"]["cpu_percent"] == 25.0
    assert metrics["aggregate"]["cpu_allocated_millicores"] == 2_250
    assert metrics["aggregate"]["cpu_allocated_percent"] == 18.8
    assert metrics["aggregate"]["memory_percent"] == 25.0
    assert metrics["nodes"][0]["ip"] == "10.0.0.1"
    assert metrics["nodes"][0]["cpu_allocated_millicores"] == 750
    assert metrics["nodes"][0]["cpu_allocated_percent"] == 18.8
    assert metrics["nodes"][0]["build_slots"]["used"] == 7
    assert metrics["nodes"][0]["build_slots"]["total"] == 48
    assert metrics["nodes"][0]["build_slots"]["waiters"] is None
    assert metrics["nodes"][1]["build_slots"]["available"] is False
    assert exec_calls[0][5] == "swegen-buildkit-pruner-node-a"
    assert first_snapshot["nodes"][0]["pod_count"] == 2
    assert first_snapshot["nodes"][0]["build_slot_probe_pod"] == ("swegen-buildkit-pruner-node-a")
    assert first_snapshot["nodes"][0]["build_slot_max"] == 48
    assert first_snapshot["nodes"][0]["pods_by_stage"] == {
        "generate": 1,
        "reward": 1,
    }
    assert first_snapshot["nodes"][0]["pods"][1] == {
        "name": "reward-a",
        "stage": "reward",
        "phase": "Running",
        "ready": False,
        "restarts": 2,
    }
    assert first_snapshot["scaling"] == {
        "max_replicas": 12,
        "basis": "sum of cluster node allocatable CPU, floored to whole CPUs",
        "allocatable_millicores": 12_000,
        "stale": False,
    }

    fail_top = True
    stale = collector.collect()["resource_metrics"]

    assert stale["available"] is True
    assert stale["stale"] is True
    assert "metrics unavailable" in stale["error"]
    assert stale["aggregate"] == metrics["aggregate"]


def test_k3s_collector_excludes_evicted_pods_and_keeps_deployments() -> None:
    """Evicted records must never reach the collector, and Deployments must survive.

    A node under disk pressure accumulates Failed/Evicted Pods by the thousand,
    which grew the old combined query past 140 MB and tripped its own timeout.
    The selector cannot be applied to a combined ``deployments,pods`` query:
    Deployments have no ``status.phase``, so kubectl silently drops them.
    """

    from swegen.dashboard.distributed_status import K3sStatusCollector

    commands: list[list[str]] = []
    nodes = {"items": []}
    workloads = {
        "items": [
            {
                "kind": "Deployment",
                "metadata": {"name": "swegen-validate", "labels": {}},
                "spec": {"replicas": 4},
                "status": {},
            },
            {
                "kind": "Pod",
                "metadata": {"name": "swegen-validate-a", "labels": {}},
                "status": {"phase": "Running"},
            },
        ]
    }

    def runner(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        commands.append(command)
        if "top" in command:
            return CompletedProcess(command, 1, stdout="", stderr="metrics unavailable")
        if "nodes" in command:
            document = nodes
        elif "deployments" in command:
            document = _only_kinds(workloads, {"Deployment"})
        else:
            document = _only_kinds(workloads, {"Pod"})
        return CompletedProcess(command, 0, stdout=json.dumps(document), stderr="")

    K3sStatusCollector(runner=runner).collect()

    pod_queries = [c for c in commands if "pods" in c and "top" not in c]
    assert pod_queries, "collector never queried pods"
    for command in pod_queries:
        assert "--field-selector=status.phase!=Failed" in command
    # Deployments are fetched on their own so the selector cannot drop them.
    deployment_queries = [c for c in commands if "deployments" in c]
    assert len(deployment_queries) == 1
    assert not any("--field-selector" in argument for argument in deployment_queries[0])


def test_summarize_generate_endpoints_builds_section_without_token() -> None:
    from swegen.dashboard.distributed_status import summarize_generate_endpoints

    tripped = datetime(2026, 8, 7, 11, 0, tzinfo=UTC)
    rows = [
        {
            "slug": "m1-alpha",
            "model_id": "model-one",
            "base_url": "https://alpha.example.com:8443/v1",
            "auth_token": "TOP-SECRET-TOKEN",
            "concurrency": 12,
            "enabled": True,
            "breaker_open": False,
            "breaker_reason": None,
            "tripped_at": None,
            "reset_at": None,
            "last_probe_status": 200,
            "last_probe_at": NOW,
            "consecutive_fail": 0,
        },
        {
            "slug": "m2-beta",
            "model_id": "model-two",
            "base_url": "https://beta.example.com",
            "auth_token": "OTHER-SECRET",
            "concurrency": 4,
            "enabled": True,
            "breaker_open": True,
            "breaker_reason": "endpoint unhealthy: http 503",
            "tripped_at": tripped,
            "reset_at": None,
            "last_probe_status": 503,
            "last_probe_at": NOW,
            "consecutive_fail": 3,
        },
    ]
    running = {"m1-alpha": 12, "m2-beta": 0}
    model_timeseries = {
        "models": ["model-one", "model-two"],
        "buckets": [
            {
                "t": "2026-08-07T11:45:00+00:00",
                "by_model": {
                    "model-one": {"succeeded": 9, "failed": 1, "rejected": 0},
                    "model-two": {"succeeded": 0, "failed": 5, "rejected": 0},
                },
            }
        ],
    }

    section = summarize_generate_endpoints(rows, running, model_timeseries)

    assert section["available"] is True
    assert "TOP-SECRET-TOKEN" not in json.dumps(section)
    assert "OTHER-SECRET" not in json.dumps(section)
    assert "auth_token" not in json.dumps(section)
    by_slug = {e["slug"]: e for e in section["endpoints"]}

    alpha = by_slug["m1-alpha"]
    assert alpha["model_id"] == "model-one"
    assert alpha["host"] == "alpha.example.com:8443"
    assert alpha["concurrency"] == 12
    # Running count is attributed to the deployment / endpoint label by slug.
    assert alpha["running"] == 12
    assert alpha["deployment"] == "swegen-generate-dyn-m1-alpha"
    assert alpha["breaker_open"] is False
    assert alpha["recent_succeeded"] == 9 and alpha["recent_failed"] == 1
    assert alpha["last_probe_status"] == 200

    beta = by_slug["m2-beta"]
    assert beta["running"] == 0
    assert beta["breaker_open"] is True
    assert beta["breaker_reason"] == "endpoint unhealthy: http 503"
    assert beta["last_probe_status"] == 503
    assert beta["tripped_at"] is not None
    assert beta["recent_failed"] == 5


def test_summarize_generate_endpoints_degrades_when_table_absent() -> None:
    from swegen.dashboard.distributed_status import summarize_generate_endpoints

    section = summarize_generate_endpoints([], available=False)
    assert section == {"available": False, "endpoints": []}


def test_aggregate_pipeline_snapshot_embeds_generate_endpoints_section() -> None:
    from swegen.dashboard.distributed_status import aggregate_pipeline_snapshot

    snapshot = aggregate_pipeline_snapshot(
        [],
        [],
        [],
        [],
        now=NOW,
        generate_endpoint_rows=[
            {
                "slug": "m1-alpha",
                "model_id": "model-one",
                "base_url": "https://alpha.example.com/v1",
                "concurrency": 3,
                "enabled": True,
                "breaker_open": False,
                "breaker_reason": None,
                "tripped_at": None,
                "reset_at": None,
                "last_probe_status": 200,
                "last_probe_at": NOW,
                "consecutive_fail": 0,
            }
        ],
        generate_endpoints_available=True,
    )

    section = snapshot["generate_endpoints"]
    assert section["available"] is True
    assert [e["slug"] for e in section["endpoints"]] == ["m1-alpha"]
    assert "auth_token" not in json.dumps(section)
    # Default (postgres-side) running count is a placeholder; the k3s collector
    # supplies the live count that the UI joins by slug.
    assert section["endpoints"][0]["running"] == 0

    empty = aggregate_pipeline_snapshot([], [], [], [], now=NOW)
    assert empty["generate_endpoints"] == {"available": False, "endpoints": []}


def test_k3s_collector_counts_running_endpoint_pods_by_label() -> None:
    from swegen.dashboard.distributed_status import K3sStatusCollector

    nodes = {
        "items": [
            {
                "metadata": {"name": "node-a"},
                "status": {
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "allocatable": {"cpu": "8", "memory": "16Gi"},
                    "addresses": [{"type": "InternalIP", "address": "10.0.0.1"}],
                },
            }
        ]
    }
    workloads = {
        "items": [
            {
                "kind": "Pod",
                "metadata": {
                    "name": "swegen-generate-dyn-m1-alpha-rs-a",
                    "labels": {
                        "swegen.pgcode/endpoint": "m1-alpha",
                        "swegen.pgcode/stage": "generate",
                    },
                },
                "spec": {"nodeName": "node-a", "containers": []},
                "status": {"phase": "Running", "containerStatuses": [{"ready": True}]},
            },
            {
                "kind": "Pod",
                "metadata": {
                    "name": "swegen-generate-dyn-m1-alpha-rs-b",
                    "labels": {
                        "swegen.pgcode/endpoint": "m1-alpha",
                        "swegen.pgcode/stage": "generate",
                    },
                },
                "spec": {"nodeName": "node-a", "containers": []},
                "status": {"phase": "Running", "containerStatuses": [{"ready": True}]},
            },
            {
                # Attributed by deployment name even without the endpoint label.
                "kind": "Pod",
                "metadata": {
                    "name": "swegen-generate-dyn-m2-beta-7d9f8c6b5-abc12",
                    "labels": {"swegen.pgcode/stage": "generate"},
                },
                "spec": {"nodeName": "node-a", "containers": []},
                "status": {"phase": "Running", "containerStatuses": [{"ready": True}]},
            },
            {
                # Pending endpoint pod: not counted as running.
                "kind": "Pod",
                "metadata": {
                    "name": "swegen-generate-dyn-m1-alpha-rs-d",
                    "labels": {"swegen.pgcode/endpoint": "m1-alpha"},
                },
                "spec": {"nodeName": "node-a", "containers": []},
                "status": {"phase": "Pending"},
            },
        ]
    }

    def runner(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        if "top" in command:
            return CompletedProcess(command, 1, stdout="", stderr="metrics unavailable")
        if "nodes" in command:
            document = nodes
        elif "deployments" in command:
            document = _only_kinds(workloads, {"Deployment"})
        else:
            document = _only_kinds(workloads, {"Pod"})
        return CompletedProcess(command, 0, stdout=json.dumps(document), stderr="")

    out = K3sStatusCollector(runner=runner).collect()
    assert out["generate_endpoint_pods"] == {"m1-alpha": 2, "m2-beta": 1}


def test_aggregate_snapshot_reports_the_selected_timeseries_lookback() -> None:
    from swegen.dashboard.distributed_status import aggregate_pipeline_snapshot

    snapshot = aggregate_pipeline_snapshot(
        [], [], [], [], now=NOW, timeseries_lookback_hours=72
    )
    assert snapshot["stage_model_timeseries"]["lookback_hours"] == 72
    # The hourly-yield window is deliberately left at its own 12h scope; the
    # range dropdown only governs the stacked-bar timeseries.
    assert snapshot["hourly_yield"]["lookback_hours"] == 12


class _RecordingResult:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[dict[str, object]]:
        return self._rows

    def fetchone(self) -> dict[str, object] | None:
        return self._rows[0] if self._rows else None


class _RecordingConnection:
    """Minimal psycopg stand-in that records every (sql, params) pair."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, object]] = []

    def __enter__(self) -> _RecordingConnection:
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    def transaction(self) -> _RecordingConnection:
        return self

    def execute(self, sql: str, params: object = None) -> _RecordingResult:
        self.executed.append((sql, params))
        # to_regclass() probes must report "table absent" so the optional
        # sections degrade cleanly and the collect() path stays exercised.
        if "to_regclass" in sql:
            return _RecordingResult([{"relation": None}])
        return _RecordingResult([])


def test_collect_threads_lookback_hours_into_the_timeseries_sql_and_snapshot(
    monkeypatch,
) -> None:
    from swegen.dashboard import distributed_status
    from swegen.dashboard.distributed_status import PipelineStatusCollector

    connection = _RecordingConnection()
    monkeypatch.setattr(distributed_status, "_database_dsn", lambda: "dsn")
    monkeypatch.setattr(
        distributed_status.psycopg, "connect", lambda *a, **k: connection
    )
    monkeypatch.setattr(
        distributed_status, "_fetch_instance_universe_total", lambda: None
    )
    monkeypatch.setattr(
        PipelineStatusCollector, "_resolve_generate_models", lambda self: {}
    )

    snapshot = PipelineStatusCollector().collect(lookback_hours=72)

    # Every timeseries query binds 72 as a positional %s param via make_interval,
    # never string-interpolated, and the fixed 48h literal is gone.
    timeseries = [
        (sql, params)
        for sql, params in connection.executed
        if "make_interval(hours =>" in sql
    ]
    assert len(timeseries) == 3
    for sql, params in timeseries:
        assert params == (72,)
        assert "INTERVAL '48 hours'" not in sql
    # The snapshot reports the chosen lookback for the per-model stacked bars.
    assert snapshot["stage_model_timeseries"]["lookback_hours"] == 72
    assert "stage_time_series" not in snapshot
    # The hourly-yield window keeps its independent 12h scope.
    assert snapshot["hourly_yield"]["lookback_hours"] == 12


def test_collect_rejects_a_non_positive_or_non_int_lookback() -> None:
    from swegen.dashboard.distributed_status import PipelineStatusCollector

    # A bad lookback can only reach collect() through validated code, but defend
    # in depth: non-int / non-positive values fall back to the 48h default.
    for bad in (0, -5, True, "72", None):
        assert PipelineStatusCollector._sanitized_lookback(bad) == 48
    assert PipelineStatusCollector._sanitized_lookback(72) == 72


class _FakeExportCursor:
    """Server-side-cursor stand-in that records itersize and yields fixed rows."""

    def __init__(self, rows: list[dict[str, object]], raises: Exception | None) -> None:
        self._rows = rows
        self._raises = raises
        self.itersize: int | None = None
        self.executed: list[tuple[str, object]] = []

    def __enter__(self) -> _FakeExportCursor:
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    def execute(self, sql: str, params: object = None) -> None:
        self.executed.append((sql, params))
        if self._raises is not None:
            raise self._raises

    def __iter__(self):
        return iter(self._rows)


class _FakeExportConnection:
    """psycopg stand-in exposing just the named-cursor surface the export uses."""

    def __init__(
        self,
        rows: list[dict[str, object]] | None = None,
        raises: Exception | None = None,
    ) -> None:
        self.cursor_obj = _FakeExportCursor(rows or [], raises)
        self.session_sql: list[str] = []
        self.cursor_names: list[str] = []

    def __enter__(self) -> _FakeExportConnection:
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    def transaction(self) -> _FakeExportConnection:
        return self

    def execute(self, sql: str, params: object = None) -> None:
        self.session_sql.append(sql)

    def cursor(self, name: str = "") -> _FakeExportCursor:
        self.cursor_names.append(name)
        return self.cursor_obj


_EXPORT_ROW = {
    "instance": "01mf02__jaq-100",
    "registry": "platform",
    "swr_url": "swr-coder-data-platform.example.com/swesandbox/generated:01mf02__jaq-100",
    "written_at": datetime(2026, 8, 2, 9, 35, 42, tzinfo=UTC),
    "repo": "01mf02/jaq",
    "pr": 100,
    "created_at": datetime(2026, 7, 31, 20, 29, 27, tzinfo=UTC),
}


def test_pushed_image_export_record_carries_repo_instance_path_and_both_stamps() -> None:
    from swegen.dashboard.distributed_status import pushed_image_export_record

    record = pushed_image_export_record(_EXPORT_ROW)

    # Every field the manifest promises, with ISO-8601 stamps for both dates.
    assert record == {
        "instance_id": "01mf02__jaq-100",
        "repo": "01mf02/jaq",
        "pr": 100,
        "registry": "platform",
        "registry_path": (
            "swr-coder-data-platform.example.com/swesandbox/generated:01mf02__jaq-100"
        ),
        "created_at": "2026-07-31T20:29:27+00:00",
        "pushed_at": "2026-08-02T09:35:42+00:00",
    }
    # Serializable as one JSONL line.
    assert json.loads(json.dumps(record))["instance_id"] == "01mf02__jaq-100"


def test_iter_pushed_image_export_streams_rows_off_a_batched_named_cursor() -> None:
    from swegen.dashboard.distributed_status import (
        PUSHED_IMAGE_EXPORT_BATCH,
        iter_pushed_image_export,
    )

    connection = _FakeExportConnection([_EXPORT_ROW, {**_EXPORT_ROW, "pr": 101}])
    records = list(iter_pushed_image_export("platform", connect=lambda: connection))

    assert [r["pr"] for r in records] == [100, 101]
    # The URL marker is bound as a param, never interpolated into the SQL text.
    sql, params = connection.cursor_obj.executed[0]
    assert params == ("%data-platform%",)
    assert "platform" not in sql
    # The clicked registry labels every record, because the legacy rows this
    # export deliberately includes carry an empty registry column.
    assert {r["registry"] for r in records} == {"platform"}
    # A *named* cursor keeps the result set server-side, fetched in batches.
    assert connection.cursor_names == ["pushed_image_export"]
    assert connection.cursor_obj.itersize == PUSHED_IMAGE_EXPORT_BATCH
    # Read-only with a statement timeout, like the other collectors.
    assert any("READ ONLY" in sql for sql in connection.session_sql)
    assert any("statement_timeout" in sql for sql in connection.session_sql)


def test_iter_pushed_image_export_yields_nothing_when_there_are_no_rows() -> None:
    from swegen.dashboard.distributed_status import iter_pushed_image_export

    connection = _FakeExportConnection([])
    assert list(iter_pushed_image_export("trajectory", connect=lambda: connection)) == []


def test_iter_pushed_image_export_degrades_to_empty_when_tables_are_absent() -> None:
    import psycopg

    from swegen.dashboard.distributed_status import iter_pushed_image_export

    # An unmigrated database must yield an empty manifest, not raise.
    connection = _FakeExportConnection(
        [_EXPORT_ROW], raises=psycopg.errors.UndefinedTable("no pushed_images")
    )
    assert list(iter_pushed_image_export("platform", connect=lambda: connection)) == []


def test_iter_pushed_image_export_rejects_an_unknown_registry() -> None:
    import pytest

    from swegen.dashboard.distributed_status import iter_pushed_image_export

    # Only the two known registries can ever reach the SQL.
    for bad in ("", "PLATFORM", "public", "platform; DROP TABLE pushed_images"):
        with pytest.raises(ValueError):
            list(iter_pushed_image_export(bad, connect=_FakeExportConnection))


def test_pushed_image_export_sql_filters_on_the_registry_url() -> None:
    from swegen.dashboard.distributed_status import (
        _PUSHED_IMAGE_EXPORT_SQL,
        PUSHED_IMAGE_REGISTRY_URL_MARKERS,
    )

    sql = _PUSHED_IMAGE_EXPORT_SQL
    # swr_url is the only complete discriminator. ~12.9k rows written on
    # 2026-07-28 predate the `registry` column and carry an empty value while
    # still holding a real URL and a matching task, and the summary card counts
    # them -- filtering on `registry` returned 11.6k lines under a card reading
    # 17.8k. `suffix` is no better: it defaults to '' so trajectory is
    # indistinguishable from unset.
    assert "p.swr_url LIKE %s" in sql
    assert "p.registry = %s" not in sql
    assert "suffix" not in sql
    assert "p.pushed" in sql
    assert PUSHED_IMAGE_REGISTRY_URL_MARKERS == {
        "platform": "%data-platform%",
        "trajectory": "%data-trajectory%",
    }
    # Joined to pipeline_tasks for owner/repo + pr + created_at, one row per
    # instance even when a task was retried into a second task_version.
    assert "JOIN public.pipeline_tasks t ON t.task_id = p.instance" in sql
    assert "DISTINCT ON (p.instance)" in sql
