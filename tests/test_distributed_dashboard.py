from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from subprocess import CompletedProcess

NOW = datetime(2026, 7, 29, 12, 8, tzinfo=UTC)


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
    assert snapshot["task_counts"] == {
        "total": 2,
        "by_state": {"queued": 2},
        "by_stage": {"generate": 1, "reward": 1},
    }

    first = snapshot["tasks"][0]
    assert first["task_id"] == "owner__repo-1"
    assert first["total_elapsed_seconds"] == 480.0
    stages = {stage["stage"]: stage for stage in first["stages"]}
    assert stages["generate"] | {} == {
        "stage": "generate",
        "state": "succeeded",
        "attempt": 1,
        "deliveries": 2,
        "queued_at": (NOW - timedelta(minutes=8)).isoformat(),
        "started_at": (NOW - timedelta(minutes=7)).isoformat(),
        "finished_at": (NOW - timedelta(minutes=5)).isoformat(),
        "heartbeat_at": None,
        "queued_seconds": 0.0,
        "wait_seconds": 60.0,
        "run_seconds": 120.0,
        "heartbeat_age_seconds": None,
        "stale": False,
        "worker_id": "generate-pod",
        "node_name": "node-generate",
        "error": None,
    }
    assert stages["validate"]["wait_seconds"] == 60.0
    assert stages["validate"]["run_seconds"] == 60.0
    assert stages["reward"]["state"] == "running"
    assert stages["reward"]["wait_seconds"] == 60.0
    assert stages["reward"]["run_seconds"] == 120.0
    assert stages["reward"]["heartbeat_age_seconds"] == 30.0
    assert stages["reward"]["stale"] is False
    assert stages["push"]["state"] == "not_started"

    second = snapshot["tasks"][1]
    assert second["task_id"] == "owner__repo-2"
    generate = second["stages"][0]
    assert generate["state"] == "queued"
    assert generate["queued_seconds"] == 60.0
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
    assert reward["heartbeat_age_seconds"] == 120.0
    assert reward["stale"] is True


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
        time_bucket_rows=[
            {
                "stage": "validate",
                "bucket": NOW - timedelta(minutes=15),
                "succeeded": 7,
                "failed": 2,
            }
        ],
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
            {"status": "submitting", "count": 2},
            {"status": "queued", "count": 3},
            {"status": "running", "count": 4},
            {"status": "success", "count": 11},
        ],
        remote_build_tracking_available=True,
    )

    assert snapshot["stage_time_series"]["bucket_seconds"] == 900
    assert snapshot["stage_time_series"]["stages"]["validate"] == [
        {
            "bucket": (NOW - timedelta(minutes=15)).isoformat(),
            "succeeded": 7,
            "failed": 2,
        }
    ]
    assert snapshot["stage_time_series"]["stages"]["generate"] == []
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
        "pending": 9,
        "status_counts": {
            "queued": 3,
            "running": 4,
            "submitting": 2,
            "success": 11,
        },
    }


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
                "metadata": {"name": "swegen-generate-overflow"},
                "spec": {
                    "replicas": 4,
                    "selector": {"matchLabels": {"swegen.pgcode/stage": "generate"}},
                },
                "status": {"readyReplicas": 4, "availableReplicas": 4},
            },
        ]
    }

    def runner(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        document = nodes if "nodes" in command else workloads
        return CompletedProcess(command, 0, stdout=json.dumps(document), stderr="")

    stage = K3sStatusCollector(runner=runner).collect()["stages"]["generate"]

    assert stage["desired"] == 96
    assert stage["ready"] == 96
    assert stage["available"] == 96


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
                    "containers": [{"resources": {"requests": {"cpu": "0.25"}}}],
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

    def runner(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        if "top" in command:
            if fail_top:
                return CompletedProcess(command, 1, stdout="", stderr="metrics unavailable")
            return CompletedProcess(
                command,
                0,
                stdout="node-a 1000m 25% 2Gi 25%\nnode-b 2000m 25% 4096Mi 25%\n",
                stderr="",
            )
        document = nodes if "nodes" in command else workloads
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
    assert first_snapshot["nodes"][0]["pod_count"] == 2
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
