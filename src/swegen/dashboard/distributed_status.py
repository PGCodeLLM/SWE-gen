"""Live PostgreSQL/PGMQ and k3s telemetry for the distributed pipeline."""

from __future__ import annotations

import json
import os
import re
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

STAGES = ("generate", "validate", "repair", "reward", "push")
QUEUE_BY_STAGE = {
    "generate": ("swegen_generate",),
    "validate": ("swegen_validate_repaired", "swegen_validate"),
    "repair": ("swegen_repair",),
    "reward": ("swegen_reward",),
    "push": ("swegen_push",),
}
DEAD_QUEUE = "swegen_dead"
REMOTE_BUILD_PENDING_STATUSES = frozenset({"submitting", "queued", "running"})
REMOTE_BUILDKIT_DEFAULT_URL = "http://7.156.122.134:32083"
REMOTE_BUILDKIT_MIN_POLL_SECONDS = 30.0
REMOTE_BUILDKIT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024
LOCAL_DISK_IO_POLL_SECONDS = 30.0


def _optional_nonnegative_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _optional_nonnegative_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _sum_inflight(value: object) -> int | None:
    direct = _optional_nonnegative_int(value)
    if direct is not None:
        return direct
    if isinstance(value, dict):
        values = []
        for item in value.values():
            if isinstance(item, dict):
                parsed = _optional_nonnegative_int(item.get("inflight"))
            else:
                parsed = _optional_nonnegative_int(item)
            if parsed is not None:
                values.append(parsed)
        return sum(values) if values else None
    if isinstance(value, list):
        values = [
            parsed for item in value if (parsed := _optional_nonnegative_int(item)) is not None
        ]
        return sum(values) if values else None
    return None


def _first_metric(payload: dict[str, Any], names: tuple[str, ...]) -> float | None:
    for name in names:
        value = _optional_nonnegative_float(payload.get(name))
        if value is not None:
            return value
    return None


def normalize_disk_io_metrics(payload: object) -> dict[str, float | None]:
    """Normalize common disk-I/O telemetry schemas without retaining raw payloads."""

    if not isinstance(payload, dict):
        return {
            "read_bytes_per_second": None,
            "write_bytes_per_second": None,
            "read_iops": None,
            "write_iops": None,
            "busy_percent": None,
            "io_current": None,
        }
    devices = payload.get("devices")
    if isinstance(devices, dict):
        device_rows = [row for row in devices.values() if isinstance(row, dict)]
    elif isinstance(devices, list):
        device_rows = [row for row in devices if isinstance(row, dict)]
    else:
        device_rows = []
    if device_rows:
        normalized = [normalize_disk_io_metrics(row) for row in device_rows]

        def total(name: str) -> float | None:
            values = [row[name] for row in normalized if row[name] is not None]
            return sum(values) if values else None

        busy_values = [row["busy_percent"] for row in normalized if row["busy_percent"] is not None]
        return {
            "read_bytes_per_second": total("read_bytes_per_second"),
            "write_bytes_per_second": total("write_bytes_per_second"),
            "read_iops": total("read_iops"),
            "write_iops": total("write_iops"),
            "busy_percent": max(busy_values) if busy_values else None,
            "io_current": total("io_current"),
        }
    return {
        "read_bytes_per_second": _first_metric(
            payload,
            ("read_bytes_per_second", "read_bytes_sec", "read_bps", "read_bytes_s"),
        ),
        "write_bytes_per_second": _first_metric(
            payload,
            ("write_bytes_per_second", "write_bytes_sec", "write_bps", "write_bytes_s"),
        ),
        "read_iops": _first_metric(
            payload,
            ("read_iops", "reads_per_second", "read_ops_per_second"),
        ),
        "write_iops": _first_metric(
            payload,
            ("write_iops", "writes_per_second", "write_ops_per_second"),
        ),
        "busy_percent": _first_metric(
            payload,
            ("busy_percent", "utilization_percent", "util_percent", "busy"),
        ),
        "io_current": _first_metric(payload, ("io_current", "inflight", "queue_depth")),
    }


def _remote_buildkit_node_disk_io(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    nodes = payload.get("nodes")
    if isinstance(nodes, list):
        for node in nodes:
            if not isinstance(node, dict):
                continue
            resources = node.get("resources") if isinstance(node.get("resources"), dict) else {}
            metrics = normalize_disk_io_metrics(node.get("disk_io") or resources.get("disk_io"))
            rows.append({"node": str(node.get("node") or node.get("name") or "unknown"), **metrics})
    if not rows:
        backends = payload.get("backends")
        for backend in backends if isinstance(backends, list) else []:
            if not isinstance(backend, dict):
                continue
            resources = (
                backend.get("resources") if isinstance(backend.get("resources"), dict) else {}
            )
            metrics = normalize_disk_io_metrics(resources.get("disk_io"))
            rows.append(
                {
                    "node": str(
                        backend.get("node") or backend.get("name") or backend.get("id") or "unknown"
                    ),
                    **metrics,
                }
            )
    if not rows:
        metrics = normalize_disk_io_metrics(
            payload.get("disk_io")
            or (
                payload.get("resources", {}).get("disk_io")
                if isinstance(payload.get("resources"), dict)
                else None
            )
        )
        if any(value is not None for value in metrics.values()):
            rows.append(
                {
                    "node": str(
                        payload.get("owner_pod")
                        or payload.get("buildkit_worker_id")
                        or "sampled worker"
                    ),
                    **metrics,
                }
            )
    return rows


_CADVISOR_METRIC = re.compile(r"^(container_fs_[a-z_]+)\{([^}]*)\}\s+([^\s]+)")
_PROMETHEUS_LABEL = re.compile(r'(\w+)="([^"]*)"')


def parse_cadvisor_disk_io(text: str) -> dict[str, Any]:
    """Extract node-root block-device counters from kubelet cAdvisor metrics."""

    accepted = {
        "container_fs_reads_bytes_total",
        "container_fs_writes_bytes_total",
        "container_fs_reads_total",
        "container_fs_writes_total",
        "container_fs_io_current",
        "container_fs_io_time_seconds_total",
    }
    by_metric: dict[str, dict[str, float]] = {name: {} for name in accepted}
    for line in text.splitlines():
        match = _CADVISOR_METRIC.match(line)
        if match is None or match.group(1) not in accepted:
            continue
        labels = dict(_PROMETHEUS_LABEL.findall(match.group(2)))
        device = labels.get("device", "")
        if labels.get("id") != "/" or not device.startswith("/dev/") or device == "/dev/shm":
            continue
        value = _optional_nonnegative_float(match.group(3))
        if value is not None:
            by_metric[match.group(1)][device] = value
    byte_devices = {
        *by_metric["container_fs_reads_bytes_total"],
        *by_metric["container_fs_writes_bytes_total"],
    }

    def total(metric: str, devices: set[str] | None = None) -> float:
        values = by_metric[metric]
        selected = devices if devices else set(values)
        return sum(values.get(device, 0.0) for device in selected)

    return {
        "read_bytes": total("container_fs_reads_bytes_total"),
        "write_bytes": total("container_fs_writes_bytes_total"),
        "read_ops": total("container_fs_reads_total", byte_devices),
        "write_ops": total("container_fs_writes_total", byte_devices),
        "io_current": total("container_fs_io_current"),
        "io_time_by_device": dict(by_metric["container_fs_io_time_seconds_total"]),
        "device_count": len(byte_devices),
    }


def summarize_remote_buildkit_resources(
    resources: dict[str, Any] | None,
    *,
    sampled_workers: Iterable[str] = (),
) -> dict[str, Any]:
    """Normalize both documented global and deployed worker-local resource schemas."""

    payload = resources or {}
    backends = payload.get("backends")
    backend_rows = backends if isinstance(backends, list) else []
    global_queue = payload.get("global_queue")
    queue = global_queue if isinstance(global_queue, dict) else payload.get("queue")
    queue = queue if isinstance(queue, dict) else {}
    is_global = bool(backend_rows) or isinstance(global_queue, dict)
    sampled = sorted({worker for worker in sampled_workers if worker})
    current_worker = payload.get("owner_pod") or payload.get("buildkit_worker_id")
    if isinstance(current_worker, str) and current_worker:
        sampled = sorted({*sampled, current_worker})
    if is_global:
        backend_count = _optional_nonnegative_int(payload.get("count"))
        if backend_count is None:
            backend_count = len(backend_rows)
        available_backends = sum(
            backend.get("healthy_for_new_build") is True
            for backend in backend_rows
            if isinstance(backend, dict)
        )
        inflight = _sum_inflight(payload.get("global_backend_inflight"))
        if inflight is None:
            inflight = sum(
                _optional_nonnegative_int(backend.get("inflight")) or 0
                for backend in backend_rows
                if isinstance(backend, dict)
            )
        scope = "global"
    else:
        backend_count = None
        available_backends = None
        inflight = _optional_nonnegative_int(
            (queue.get("api_capacity") or {}).get("running")
            if isinstance(queue.get("api_capacity"), dict)
            else None
        )
        if inflight is None:
            inflight = _optional_nonnegative_int(payload.get("running_count"))
        scope = str(payload.get("scope") or "worker_local_sample")
    queue_length = _optional_nonnegative_int(queue.get("queued"))
    if queue_length is None:
        queue_length = _optional_nonnegative_int(payload.get("queue_depth"))
    queue_capacity = _optional_nonnegative_int(queue.get("max_queued"))
    if queue_capacity is None:
        queue_capacity = _optional_nonnegative_int(payload.get("queue_capacity"))
    running_builds = _optional_nonnegative_int(payload.get("running_count"))
    status_counts = payload.get("build_status_counts")
    if running_builds is None and isinstance(status_counts, dict):
        running_builds = _optional_nonnegative_int(status_counts.get("running"))
    if running_builds is None and is_global:
        running_builds = inflight
    return {
        "available": bool(payload),
        "scope": scope,
        "is_global": is_global,
        "sampled_worker": current_worker if isinstance(current_worker, str) else None,
        "sampled_workers": sampled,
        "sampled_worker_count": len(sampled),
        "backend_count": backend_count,
        "available_backend_count": available_backends,
        "queue_length": queue_length,
        "queue_capacity": queue_capacity,
        "running_builds": running_builds,
        "inflight_builds": inflight,
        "node_disk_io": _remote_buildkit_node_disk_io(payload),
        "schema_warning": (
            None
            if is_global
            else "Farm API returned a worker-local sample; queue and running counts are not global."
        ),
    }


def summarize_remote_build_tracking(
    rows: Iterable[dict[str, Any]],
    *,
    available: bool,
) -> dict[str, Any]:
    counts = {
        str(row["status"]): int(row.get("count") or 0)
        for row in rows
        if row.get("status") is not None
    }
    return {
        "available": available,
        "pending": (
            sum(counts.get(status, 0) for status in REMOTE_BUILD_PENDING_STATUSES)
            if available
            else None
        ),
        "status_counts": dict(sorted(counts.items())),
    }


def _cpu_millicores(value: str) -> int:
    value = value.strip()
    if value.endswith("m"):
        return round(float(value[:-1]))
    if value.endswith("u"):
        return round(float(value[:-1]) / 1_000)
    if value.endswith("n"):
        return round(float(value[:-1]) / 1_000_000)
    return round(float(value) * 1_000)


def _memory_bytes(value: str) -> int:
    value = value.strip()
    suffixes = {
        "Ki": 1024,
        "Mi": 1024**2,
        "Gi": 1024**3,
        "Ti": 1024**4,
        "K": 1000,
        "M": 1000**2,
        "G": 1000**3,
        "T": 1000**4,
    }
    for suffix, multiplier in suffixes.items():
        if value.endswith(suffix):
            return round(float(value[: -len(suffix)]) * multiplier)
    return round(float(value))


def _pod_cpu_request_millicores(pod: dict[str, Any]) -> int:
    spec = pod.get("spec", {})
    regular = sum(
        _cpu_millicores(str(container.get("resources", {}).get("requests", {}).get("cpu", "0")))
        for container in spec.get("containers", [])
    )
    init_max = max(
        (
            _cpu_millicores(str(container.get("resources", {}).get("requests", {}).get("cpu", "0")))
            for container in spec.get("initContainers", [])
        ),
        default=0,
    )
    overhead = _cpu_millicores(str(spec.get("overhead", {}).get("cpu", "0")))
    return max(regular, init_max) + overhead


def _allocated_cpu_by_node(pod_doc: dict[str, Any]) -> dict[str, int]:
    allocated: Counter[str] = Counter()
    for pod in pod_doc.get("items", []):
        if pod.get("kind") not in {None, "Pod"}:
            continue
        if pod.get("status", {}).get("phase") in {"Succeeded", "Failed"}:
            continue
        node_name = pod.get("spec", {}).get("nodeName")
        if node_name:
            allocated[node_name] += _pod_cpu_request_millicores(pod)
    return dict(allocated)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _seconds(later: datetime, earlier: datetime) -> float:
    return max(0.0, round((later - earlier).total_seconds(), 3))


def _empty_stage(stage: str) -> dict[str, Any]:
    return {
        "stage": stage,
        "state": "not_started",
        "attempt": None,
        "deliveries": None,
        "queued_at": None,
        "started_at": None,
        "finished_at": None,
        "heartbeat_at": None,
        "queued_seconds": None,
        "wait_seconds": None,
        "run_seconds": None,
        "heartbeat_age_seconds": None,
        "stale": False,
        "worker_id": None,
        "node_name": None,
        "error": None,
    }


def aggregate_pipeline_snapshot(
    task_rows: Iterable[dict[str, Any]],
    result_rows: Iterable[dict[str, Any]],
    activity_rows: Iterable[dict[str, Any]],
    queue_rows: Iterable[dict[str, Any]],
    *,
    now: datetime | None = None,
    activity_stale_after_seconds: float = 120.0,
    time_bucket_rows: Iterable[dict[str, Any]] = (),
    hourly_yield_rows: Iterable[dict[str, Any]] = (),
    lifetime_stage_rows: Iterable[dict[str, Any]] = (),
    remote_build_rows: Iterable[dict[str, Any]] = (),
    remote_build_tracking_available: bool = False,
) -> dict[str, Any]:
    """Build a compact JSON-safe pipeline snapshot from database rows."""

    now = now or datetime.now(UTC)
    tasks = list(task_rows)
    results = list(result_rows)
    activities = list(activity_rows)
    queues = list(queue_rows)
    results_by_task: dict[tuple[str, int], dict[str, dict[str, Any]]] = {}
    activity_by_task: dict[tuple[str, int], dict[str, dict[str, Any]]] = {}
    for row in results:
        key = (row["task_id"], row["task_version"])
        existing = results_by_task.setdefault(key, {}).get(row["stage"])
        if existing is None or row["attempt"] >= existing["attempt"]:
            results_by_task[key][row["stage"]] = row
    for row in activities:
        activity_by_task.setdefault((row["task_id"], row["task_version"]), {})[row["stage"]] = row

    task_views: list[dict[str, Any]] = []
    for task in tasks:
        key = (task["task_id"], task["task_version"])
        stage_results = results_by_task.get(key, {})
        stage_activity = activity_by_task.get(key, {})
        predecessor_finished = task["created_at"]
        stage_views: list[dict[str, Any]] = []
        for stage in STAGES:
            view = _empty_stage(stage)
            queued_at = predecessor_finished
            result = stage_results.get(stage)
            activity = stage_activity.get(stage)
            if result is not None:
                started_at = result["started_at"]
                finished_at = result["finished_at"]
                view.update(
                    state=result["status"],
                    attempt=result["attempt"],
                    deliveries=result["pgmq_read_count"],
                    queued_at=_iso(queued_at),
                    started_at=_iso(started_at),
                    finished_at=_iso(finished_at),
                    queued_seconds=0.0,
                    wait_seconds=_seconds(started_at, queued_at),
                    run_seconds=_seconds(finished_at, started_at),
                    worker_id=result["worker_id"],
                    node_name=result["node_name"],
                    error=result.get("error"),
                )
                if result["status"] == "succeeded":
                    predecessor_finished = finished_at
            elif activity is not None:
                started_at = activity["started_at"]
                heartbeat_at = activity["heartbeat_at"]
                heartbeat_age = _seconds(now, heartbeat_at)
                view.update(
                    state="running",
                    attempt=activity["attempt"],
                    deliveries=activity["pgmq_read_count"],
                    queued_at=_iso(queued_at),
                    started_at=_iso(started_at),
                    heartbeat_at=_iso(heartbeat_at),
                    queued_seconds=0.0,
                    wait_seconds=_seconds(started_at, queued_at),
                    run_seconds=_seconds(now, started_at),
                    heartbeat_age_seconds=heartbeat_age,
                    stale=heartbeat_age > activity_stale_after_seconds,
                    worker_id=activity["worker_id"],
                    node_name=activity["node_name"],
                )
            elif task["current_stage"] == stage and task["state"] in {"queued", "running"}:
                view.update(
                    state="queued",
                    queued_at=_iso(queued_at),
                    queued_seconds=_seconds(now, queued_at),
                )
            stage_views.append(view)

        end = task.get("finished_at") or now
        generate_record = stage_activity.get("generate") or stage_results.get("generate")
        generate_message_id = (
            generate_record.get("pgmq_msg_id") if generate_record is not None else None
        )
        generate_node = generate_record.get("node_name") if generate_record is not None else None
        message_component = str(generate_message_id) if generate_message_id else "<pgmq-msg-id>"
        runtime_pattern = (
            f"/data/swegen-k3s/workspaces/generate-{message_component}-*/tasks/{task['task_id']}"
        )
        generate_running = "generate" in stage_activity
        task_views.append(
            {
                "task_id": task["task_id"],
                "task_version": task["task_version"],
                "repo": task["repo"],
                "pr": task["pr"],
                "trace_id": str(task["trace_id"]),
                "state": task["state"],
                "current_stage": task["current_stage"],
                "created_at": _iso(task["created_at"]),
                "updated_at": _iso(task["updated_at"]),
                "finished_at": _iso(task.get("finished_at")),
                "total_elapsed_seconds": _seconds(end, task["created_at"]),
                "last_error": task.get("last_error"),
                "last_reason": task.get("last_reason"),
                "storage": {
                    "durable_source": ("PostgreSQL swegen_distributed.public.pipeline_task_files"),
                    "stored_file_count": int(task.get("stored_file_count") or 0),
                    "stored_bytes": int(task.get("stored_bytes") or 0),
                    "generated_on_node": generate_node,
                    "runtime_path_pattern": runtime_pattern,
                    "runtime_path_is_exact": False,
                    "runtime_directory_state": (
                        "temporary directory may currently exist"
                        if generate_running
                        else "temporary directory is removed after stage completion"
                    ),
                    "durability": "persistent hostPath backing (node-local, not replicated)",
                },
                "stages": stage_views,
            }
        )

    queue_map = {row["queue_name"]: row for row in queues}

    def queue_view(names: tuple[str, ...]) -> dict[str, Any]:
        rows = [queue_map.get(name, {}) for name in names]
        length = sum(int(row.get("queue_length") or 0) for row in rows)
        visible = sum(int(row.get("queue_visible_length") or 0) for row in rows)
        newest_ages = [
            row.get("newest_msg_age_sec")
            for row in rows
            if row.get("newest_msg_age_sec") is not None
        ]
        oldest_ages = [
            row.get("oldest_msg_age_sec")
            for row in rows
            if row.get("oldest_msg_age_sec") is not None
        ]
        scrape_times = [row.get("scrape_time") for row in rows if row.get("scrape_time")]
        return {
            "queue": "+".join(names),
            "length": length,
            "visible": visible,
            "in_flight": max(0, length - visible),
            "total_messages": sum(int(row.get("total_messages") or 0) for row in rows),
            "newest_message_age_seconds": min(newest_ages) if newest_ages else None,
            "oldest_message_age_seconds": max(oldest_ages) if oldest_ages else None,
            "scraped_at": _iso(max(scrape_times)) if scrape_times else None,
        }

    throughput: dict[str, dict[str, dict[str, Any]]] = {}
    for window in (60, 300, 900):
        window_data: dict[str, dict[str, Any]] = {}
        for stage in STAGES:
            recent = [
                row
                for row in results
                if row["stage"] == stage
                and 0 <= (now - row["finished_at"]).total_seconds() <= window
            ]
            succeeded = sum(row["status"] == "succeeded" for row in recent)
            window_data[stage] = {
                "completed": len(recent),
                "succeeded": succeeded,
                "instances_per_second": round(succeeded / window, 6),
            }
        throughput[str(window)] = window_data

    by_state = Counter(task["state"] for task in tasks)
    by_stage = Counter(task["current_stage"] for task in tasks)
    stage_time_series = {stage: [] for stage in STAGES}
    for row in time_bucket_rows:
        stage = row.get("stage")
        if stage not in stage_time_series:
            continue
        stage_time_series[stage].append(
            {
                "bucket": _iso(row["bucket"]),
                "succeeded": int(row.get("succeeded") or 0),
                "failed": int(row.get("failed") or 0),
            }
        )
    for rows in stage_time_series.values():
        rows.sort(key=lambda row: row["bucket"] or "")
    hourly_yield = {stage: [] for stage in STAGES}
    for row in hourly_yield_rows:
        stage = row.get("stage")
        if stage not in hourly_yield:
            continue
        succeeded = int(row.get("succeeded") or 0)
        processed = int(row.get("processed") or 0)
        hourly_yield[stage].append(
            {
                "bucket": _iso(row["bucket"]),
                "succeeded": succeeded,
                "processed": processed,
                "yield_percent": round(succeeded * 100 / processed, 1) if processed else None,
            }
        )
    for rows in hourly_yield.values():
        rows.sort(key=lambda row: row["bucket"] or "")
    lifetime_processed = dict.fromkeys(STAGES, 0)
    for row in lifetime_stage_rows:
        stage = row.get("stage")
        if stage in lifetime_processed:
            lifetime_processed[stage] = int(row.get("processed") or 0)
    return {
        "generated_at": now.isoformat(),
        "queues": {
            "stages": {stage: queue_view(names) for stage, names in QUEUE_BY_STAGE.items()},
            "validate_repaired": queue_view(("swegen_validate_repaired",)),
            "dead": queue_view((DEAD_QUEUE,)),
        },
        "task_counts": {
            "total": len(tasks),
            "by_state": dict(sorted(by_state.items())),
            "by_stage": dict(sorted(by_stage.items())),
        },
        "throughput": {
            "windows": throughput,
            "lifetime_processed": lifetime_processed,
        },
        "stage_time_series": {
            "bucket_seconds": 900,
            "lookback_hours": 6,
            "stages": stage_time_series,
        },
        "hourly_yield": {
            "bucket_seconds": 3600,
            "lookback_hours": 12,
            "stages": hourly_yield,
        },
        "remote_builds": summarize_remote_build_tracking(
            remote_build_rows,
            available=remote_build_tracking_available,
        ),
        "tasks": task_views,
    }


def _database_dsn() -> str:
    password = os.environ.get("SWEGEN_PG_PASSWORD", "")
    if not password:
        raise RuntimeError("SWEGEN_PG_PASSWORD is not set")
    return " ".join(
        (
            f"host={os.environ.get('SWEGEN_PG_HOST', '7.237.95.141')}",
            f"port={os.environ.get('SWEGEN_PG_PORT', '5432')}",
            f"dbname={os.environ.get('SWEGEN_PG_DB', 'swegen_distributed')}",
            f"user={os.environ.get('SWEGEN_PG_USER', 'root')}",
            f"password={password}",
            "connect_timeout=5",
        )
    )


class PipelineStatusCollector:
    """Fetch bounded recent task telemetry and complete PGMQ queue metrics."""

    def __init__(self, *, recent_task_limit: int = 100) -> None:
        self.recent_task_limit = recent_task_limit

    def collect(self) -> dict[str, Any]:
        with psycopg.connect(_database_dsn(), row_factory=dict_row) as connection:
            with connection.transaction():
                connection.execute("SET LOCAL statement_timeout = '5s'")
                connection.execute("SET TRANSACTION READ ONLY")
                tasks = list(
                    connection.execute(
                        """
                        SELECT t.task_id, t.task_version, t.repo, t.pr, t.trace_id, t.state,
                               t.current_stage, t.created_at, t.updated_at, t.finished_at,
                               t.last_error, t.last_reason,
                               (
                                   SELECT count(*)
                                   FROM pipeline_task_files f
                                   WHERE f.task_id = t.task_id
                                     AND f.task_version = t.task_version
                               ) AS stored_file_count,
                               (
                                   SELECT COALESCE(sum(f.size_bytes), 0)
                                   FROM pipeline_task_files f
                                   WHERE f.task_id = t.task_id
                                     AND f.task_version = t.task_version
                               ) AS stored_bytes
                        FROM pipeline_tasks t
                        ORDER BY t.updated_at DESC
                        LIMIT %s
                        """,
                        (self.recent_task_limit,),
                    ).fetchall()
                )
                results = list(
                    connection.execute(
                        """
                        SELECT r.* FROM pipeline_stage_results r
                        JOIN (
                            SELECT task_id, task_version FROM pipeline_tasks
                            ORDER BY updated_at DESC LIMIT %s
                        ) t USING (task_id, task_version)
                        """,
                        (self.recent_task_limit,),
                    ).fetchall()
                )
                activity = list(
                    connection.execute(
                        """
                        SELECT a.* FROM pipeline_stage_activity a
                        JOIN (
                            SELECT task_id, task_version FROM pipeline_tasks
                            ORDER BY updated_at DESC LIMIT %s
                        ) t USING (task_id, task_version)
                        """,
                        (self.recent_task_limit,),
                    ).fetchall()
                )
                queues = list(connection.execute("SELECT * FROM pgmq.metrics_all()").fetchall())
                time_buckets = list(
                    connection.execute(
                        """
                        SELECT
                            stage,
                            date_bin(
                                INTERVAL '15 minutes',
                                finished_at,
                                TIMESTAMPTZ '2001-01-01 00:00:00+00'
                            ) AS bucket,
                            count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
                            count(*) FILTER (WHERE status <> 'succeeded') AS failed
                        FROM pipeline_stage_results
                        WHERE finished_at >= now() - INTERVAL '6 hours'
                        GROUP BY stage, bucket
                        ORDER BY bucket, stage
                        """
                    ).fetchall()
                )
                hourly_yields = list(
                    connection.execute(
                        """
                        SELECT
                            stage,
                            date_trunc('hour', finished_at) AS bucket,
                            count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
                            count(*) AS processed
                        FROM pipeline_stage_results
                        WHERE finished_at >= now() - INTERVAL '12 hours'
                        GROUP BY stage, bucket
                        ORDER BY bucket, stage
                        """
                    ).fetchall()
                )
                lifetime_stages = list(
                    connection.execute(
                        """
                        SELECT stage, count(*) AS processed
                        FROM pipeline_stage_results
                        WHERE status IN ('succeeded', 'rejected', 'failed')
                        GROUP BY stage
                        """
                    ).fetchall()
                )
                remote_build_rows: list[dict[str, Any]] = []
                remote_build_tracking_available = False
                relation = connection.execute(
                    "SELECT to_regclass('public.pipeline_remote_builds') AS relation"
                ).fetchone()
                if relation and relation["relation"] is not None:
                    try:
                        with connection.transaction():
                            remote_build_rows = list(
                                connection.execute(
                                    """
                                    SELECT status, count(*) AS count
                                    FROM pipeline_remote_builds
                                    WHERE route = 'remote'
                                      AND status IN ('submitting', 'queued', 'running')
                                    GROUP BY status
                                    ORDER BY status
                                    """
                                ).fetchall()
                            )
                        remote_build_tracking_available = True
                    except (psycopg.errors.UndefinedColumn, psycopg.errors.UndefinedTable):
                        remote_build_rows = []
        return aggregate_pipeline_snapshot(
            tasks,
            results,
            activity,
            queues,
            time_bucket_rows=time_buckets,
            hourly_yield_rows=hourly_yields,
            lifetime_stage_rows=lifetime_stages,
            remote_build_rows=remote_build_rows,
            remote_build_tracking_available=remote_build_tracking_available,
        )


class RemoteBuildKitFarmCollector:
    """Poll the read-only remote farm endpoints without blocking dashboard refreshes."""

    ENDPOINTS = (
        ("gateway", "/healthz", 120.0),
        ("ready", "/ready", 300.0),
        ("resources", "/resources", 1200.0),
    )

    def __init__(
        self,
        *,
        base_url: str | None = None,
        poll_seconds: float | None = None,
        opener: Any | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.base_url = (
            base_url
            or os.environ.get("SWEGEN_REMOTE_BUILDKIT_URL")
            or os.environ.get("SWEGEN_BUILDKIT_FARM_URL")
            or REMOTE_BUILDKIT_DEFAULT_URL
        ).rstrip("/")
        configured_poll = (
            poll_seconds
            if poll_seconds is not None
            else float(os.environ.get("SWEGEN_BUILDKIT_FARM_POLL_SECONDS", "30"))
        )
        self.poll_seconds = max(REMOTE_BUILDKIT_MIN_POLL_SECONDS, configured_poll)
        # Explicitly disable proxies; this is the urllib equivalent of NO_PROXY for the farm IP.
        self.opener = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.monotonic = monotonic
        self.now = now or (lambda: datetime.now(UTC))
        self._lock = threading.Lock()
        self._sampling = False
        self._last_started_monotonic: float | None = None
        self._sample_started_at: str | None = None
        self._sample_completed_at: str | None = None
        self._sampled_workers: set[str] = set()
        self._endpoints: dict[str, dict[str, Any]] = {
            name: {
                "payload": None,
                "http_status": None,
                "checked_at": None,
                "last_success_at": None,
                "error": None,
            }
            for name, _path, _timeout in self.ENDPOINTS
        }

    @staticmethod
    def _read_json_response(response: Any) -> dict[str, Any]:
        raw = response.read(REMOTE_BUILDKIT_MAX_RESPONSE_BYTES + 1)
        if len(raw) > REMOTE_BUILDKIT_MAX_RESPONSE_BYTES:
            raise ValueError("response exceeded 4 MiB monitoring limit")
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError("response was not a JSON object")
        return parsed

    def _fetch(self, path: str, timeout: float) -> tuple[int | None, dict[str, Any] | None]:
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            headers={"Accept": "application/json", "User-Agent": "swegen-dashboard/1"},
            method="GET",
        )
        try:
            with self.opener.open(request, timeout=timeout) as response:
                return response.getcode(), self._read_json_response(response)
        except urllib.error.HTTPError as error:
            with error:
                return error.code, self._read_json_response(error)

    def _record_endpoint(
        self,
        name: str,
        *,
        http_status: int | None,
        payload: dict[str, Any] | None,
        error: str | None,
    ) -> None:
        checked_at = self.now().isoformat()
        with self._lock:
            current = self._endpoints[name]
            if payload is not None:
                current["payload"] = payload
            current["http_status"] = http_status
            current["checked_at"] = checked_at
            current["error"] = error
            if payload is not None and error is None:
                current["last_success_at"] = checked_at
            if name == "resources" and payload is not None:
                worker = payload.get("owner_pod") or payload.get("buildkit_worker_id")
                if isinstance(worker, str) and worker:
                    self._sampled_workers.add(worker)

    def _refresh(self) -> None:
        try:
            for name, path, timeout in self.ENDPOINTS:
                try:
                    http_status, payload = self._fetch(path, timeout)
                    self._record_endpoint(
                        name,
                        http_status=http_status,
                        payload=payload,
                        error=None,
                    )
                except Exception as error:
                    self._record_endpoint(
                        name,
                        http_status=None,
                        payload=None,
                        error=f"{type(error).__name__}: {str(error)[:300]}",
                    )
        finally:
            with self._lock:
                self._sampling = False
                self._sample_completed_at = self.now().isoformat()

    @staticmethod
    def _endpoint_view(
        endpoint: dict[str, Any],
        *,
        accepted_statuses: frozenset[str],
    ) -> dict[str, Any]:
        payload = endpoint.get("payload")
        payload = payload if isinstance(payload, dict) else {}
        status = str(payload.get("status") or "unknown")
        http_status = endpoint.get("http_status")
        healthy = (
            endpoint.get("error") is None
            and isinstance(http_status, int)
            and 200 <= http_status < 300
            and status in accepted_statuses
            and payload.get("docker_available") is not False
        )
        return {
            "ok": healthy,
            "status": status,
            "http_status": http_status,
            "service": payload.get("service") if isinstance(payload.get("service"), str) else None,
            "checked_at": endpoint.get("checked_at"),
            "last_success_at": endpoint.get("last_success_at"),
            "error": endpoint.get("error"),
        }

    def _snapshot_locked(self) -> dict[str, Any]:
        gateway = self._endpoint_view(
            self._endpoints["gateway"],
            accepted_statuses=frozenset({"ok", "healthy", "ready"}),
        )
        ready = self._endpoint_view(
            self._endpoints["ready"],
            accepted_statuses=frozenset({"ready", "healthy", "ok"}),
        )
        resources_payload = self._endpoints["resources"].get("payload")
        resources = summarize_remote_buildkit_resources(
            resources_payload if isinstance(resources_payload, dict) else None,
            sampled_workers=self._sampled_workers,
        )
        resources.update(
            {
                "checked_at": self._endpoints["resources"].get("checked_at"),
                "last_success_at": self._endpoints["resources"].get("last_success_at"),
                "error": self._endpoints["resources"].get("error"),
            }
        )
        return {
            "poll_interval_seconds": self.poll_seconds,
            "sampling": self._sampling,
            "sample_started_at": self._sample_started_at,
            "sample_completed_at": self._sample_completed_at,
            "gateway": gateway,
            "ready": ready,
            "resources": resources,
        }

    def collect(self) -> dict[str, Any]:
        start_sample = False
        with self._lock:
            current = self.monotonic()
            due = (
                self._last_started_monotonic is None
                or current - self._last_started_monotonic >= self.poll_seconds
            )
            if due and not self._sampling:
                self._sampling = True
                self._last_started_monotonic = current
                self._sample_started_at = self.now().isoformat()
                start_sample = True
            snapshot = self._snapshot_locked()
        if start_sample:
            threading.Thread(
                target=self._refresh,
                name="remote-buildkit-farm-refresh",
                daemon=True,
            ).start()
        return snapshot


class K3sStatusCollector:
    """Read node, deployment, and pod health from kubectl JSON output."""

    def __init__(
        self,
        *,
        namespace: str = "swegen-pipeline",
        runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        self.namespace = namespace
        self.runner = runner
        self._last_resource_metrics: dict[str, Any] | None = None
        self._last_disk_io_poll_monotonic: float | None = None
        self._disk_io_counters: dict[str, dict[str, Any]] = {}
        self._disk_io_metrics: dict[str, dict[str, Any]] = {}

    def _get(self, args: list[str]) -> dict[str, Any]:
        completed = self.runner(
            ["kubectl", "--request-timeout=3s", *args, "-o", "json"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError((completed.stderr or "kubectl failed")[:500])
        return json.loads(completed.stdout)

    def collect(self) -> dict[str, Any]:
        node_doc = self._get(["get", "nodes"])
        workload_doc = self._get(
            [
                "-n",
                self.namespace,
                "get",
                "deployments,pods",
            ]
        )
        allocation_error = None
        try:
            all_pod_doc = self._get(["get", "pods", "-A"])
        except Exception as error:
            all_pod_doc = None
            allocation_error = f"{type(error).__name__}: {str(error)[:300]}"
        nodes = []
        for item in node_doc.get("items", []):
            conditions = {c["type"]: c for c in item.get("status", {}).get("conditions", [])}
            ready = conditions.get("Ready", {}).get("status") == "True"
            nodes.append(
                {
                    "name": item["metadata"]["name"],
                    "ready": ready,
                    "pressure": [
                        name
                        for name in ("MemoryPressure", "DiskPressure", "PIDPressure")
                        if conditions.get(name, {}).get("status") == "True"
                    ],
                }
            )

        stages = {
            stage: {
                "desired": 0,
                "ready": 0,
                "available": 0,
                "pods": 0,
                "pods_ready": 0,
                "restarts": 0,
                "nodes": {},
            }
            for stage in STAGES
        }
        pods_by_node: dict[str, list[dict[str, Any]]] = {}
        storage_mounts: list[dict[str, Any]] = []
        for item in workload_doc.get("items", []):
            stage = item.get("metadata", {}).get("labels", {}).get("swegen.pgcode/stage")
            if item.get("kind") == "Deployment" and stage is None:
                stage = (
                    item.get("spec", {})
                    .get("selector", {})
                    .get("matchLabels", {})
                    .get("swegen.pgcode/stage")
                )
            if stage not in stages:
                continue
            if item.get("kind") == "Deployment":
                spec = item.get("spec", {})
                status = item.get("status", {})
                stages[stage]["desired"] += int(spec.get("replicas") or 0)
                stages[stage]["ready"] += int(status.get("readyReplicas") or 0)
                stages[stage]["available"] += int(status.get("availableReplicas") or 0)
                pod_spec = spec.get("template", {}).get("spec", {})
                volumes = {volume.get("name"): volume for volume in pod_spec.get("volumes", [])}
                node_selector = pod_spec.get("nodeSelector", {})
                node_ip = node_selector.get("swegen.pgcode/node-ip")
                for container in pod_spec.get("containers", []):
                    for mount in container.get("volumeMounts", []):
                        mount_path = mount.get("mountPath", "")
                        if not mount_path.startswith("/data/swegen-k3s/"):
                            continue
                        volume = volumes.get(mount.get("name"), {})
                        if "hostPath" in volume:
                            kind = "hostPath"
                            source = volume["hostPath"].get("path")
                            durability = "persistent hostPath (node-local, not replicated)"
                        elif "persistentVolumeClaim" in volume:
                            kind = "persistentVolumeClaim"
                            source = volume["persistentVolumeClaim"].get("claimName")
                            durability = "persistent PVC"
                        elif "emptyDir" in volume:
                            kind = "emptyDir"
                            source = None
                            durability = "ephemeral emptyDir"
                        else:
                            kind = "containerFilesystem"
                            source = None
                            durability = "ephemeral container filesystem"
                        storage_mounts.append(
                            {
                                "deployment": item.get("metadata", {}).get("name"),
                                "stage": stage,
                                "node_ip": node_ip,
                                "mount_path": mount_path,
                                "source_path": source,
                                "kind": kind,
                                "durability": durability,
                            }
                        )
            elif item.get("kind") == "Pod":
                status = item.get("status", {})
                stages[stage]["pods"] += 1
                container_statuses = status.get("containerStatuses", [])
                pod_ready = status.get("phase") == "Running" and all(
                    entry.get("ready") for entry in container_statuses
                )
                stages[stage]["pods_ready"] += int(bool(container_statuses) and pod_ready)
                stages[stage]["restarts"] += sum(
                    int(entry.get("restartCount") or 0) for entry in container_statuses
                )
                node = item.get("spec", {}).get("nodeName") or "unscheduled"
                stages[stage]["nodes"][node] = stages[stage]["nodes"].get(node, 0) + 1
                if node != "unscheduled":
                    pods_by_node.setdefault(node, []).append(
                        {
                            "name": item.get("metadata", {}).get("name", "unknown"),
                            "stage": stage,
                            "phase": status.get("phase", "Unknown"),
                            "ready": bool(container_statuses) and pod_ready,
                            "restarts": sum(
                                int(entry.get("restartCount") or 0) for entry in container_statuses
                            ),
                        }
                    )
        for node in nodes:
            scheduled_pods = sorted(
                pods_by_node.get(node["name"], []),
                key=lambda pod: (pod["stage"], pod["name"]),
            )
            node["pods"] = scheduled_pods
            node["pod_count"] = len(scheduled_pods)
            node["pods_by_stage"] = dict(
                sorted(Counter(pod["stage"] for pod in scheduled_pods).items())
            )
        resource_metrics = self._collect_resource_metrics(
            node_doc,
            all_pod_doc=all_pod_doc,
            allocation_error=allocation_error,
        )
        workspace_mounts = [
            mount
            for mount in storage_mounts
            if mount["mount_path"] == "/data/swegen-k3s/workspaces"
        ]
        shared_persistent = bool(workspace_mounts) and all(
            mount["kind"] == "persistentVolumeClaim" for mount in workspace_mounts
        )
        storage = {
            "workspace_root": "/data/swegen-k3s/workspaces",
            "generated_task_relative_path": "tasks/<task_id>",
            "source_of_truth": "PostgreSQL swegen_distributed.public.pipeline_task_files",
            "exact_runtime_paths_recorded": False,
            "shared_persistent": shared_persistent,
            "mounts": storage_mounts,
            "warning": (
                None
                if shared_persistent
                else "Generated task directories are temporary stage workspaces backed by "
                "node-local hostPath storage. Identical absolute paths on different nodes are "
                "different disks; node/disk loss can lose scratch artifacts, and each stage "
                "deletes its temporary directory after completion. PostgreSQL task files are "
                "the durable source of truth. For durable on-disk exports, mount a shared RWX "
                "PVC at /data/swegen-k3s/task-exports and write exports there explicitly."
            ),
        }
        total_allocatable_millicores = sum(
            _cpu_millicores(str(item.get("status", {}).get("allocatable", {}).get("cpu", "0")))
            for item in node_doc.get("items", [])
        )
        scaling = {
            "max_replicas": total_allocatable_millicores // 1_000,
            "basis": "sum of cluster node allocatable CPU, floored to whole CPUs",
            "allocatable_millicores": total_allocatable_millicores,
            "stale": False,
        }
        return {
            "nodes": nodes,
            "stages": stages,
            "resource_metrics": resource_metrics,
            "storage": storage,
            "scaling": scaling,
        }

    def _collect_resource_metrics(
        self,
        node_doc: dict[str, Any],
        *,
        all_pod_doc: dict[str, Any] | None,
        allocation_error: str | None,
    ) -> dict[str, Any]:
        try:
            completed = self.runner(
                ["kubectl", "--request-timeout=3s", "top", "nodes", "--no-headers"],
                capture_output=True,
                text=True,
                timeout=4,
                check=False,
            )
            if completed.returncode != 0:
                raise RuntimeError((completed.stderr or "kubectl top nodes failed")[:500])
            usage_by_node: dict[str, tuple[int, int]] = {}
            for line in completed.stdout.splitlines():
                fields = line.split()
                if not fields:
                    continue
                if len(fields) < 4:
                    raise ValueError("kubectl top nodes returned an incomplete row")
                usage_by_node[fields[0]] = (
                    _cpu_millicores(fields[1]),
                    _memory_bytes(fields[3]),
                )
            allocated_by_node = (
                _allocated_cpu_by_node(all_pod_doc) if all_pod_doc is not None else None
            )
            disk_io_by_node = self._collect_disk_io_metrics(node_doc)

            per_node = []
            missing = []
            total_cpu_used = 0
            total_cpu_allocated = 0
            total_cpu_allocatable = 0
            total_memory_used = 0
            total_memory_allocatable = 0
            for item in node_doc.get("items", []):
                name = item.get("metadata", {}).get("name", "unknown")
                allocatable = item.get("status", {}).get("allocatable", {})
                cpu_allocatable = _cpu_millicores(str(allocatable.get("cpu", "0")))
                memory_allocatable = _memory_bytes(str(allocatable.get("memory", "0")))
                addresses = item.get("status", {}).get("addresses", [])
                internal_ip = next(
                    (
                        entry.get("address")
                        for entry in addresses
                        if entry.get("type") == "InternalIP"
                    ),
                    None,
                )
                usage = usage_by_node.get(name)
                cpu_allocated = (
                    allocated_by_node.get(name, 0) if allocated_by_node is not None else None
                )
                if usage is None:
                    missing.append(name)
                    cpu_used = None
                    memory_used = None
                else:
                    cpu_used, memory_used = usage
                    total_cpu_used += cpu_used
                    total_memory_used += memory_used
                total_cpu_allocatable += cpu_allocatable
                if cpu_allocated is not None:
                    total_cpu_allocated += cpu_allocated
                total_memory_allocatable += memory_allocatable
                per_node.append(
                    {
                        "name": name,
                        "ip": internal_ip,
                        "available": usage is not None,
                        "cpu_used_millicores": cpu_used,
                        "cpu_allocated_millicores": cpu_allocated,
                        "cpu_allocatable_millicores": cpu_allocatable,
                        "cpu_percent": (
                            round(cpu_used * 100 / cpu_allocatable, 1)
                            if cpu_used is not None and cpu_allocatable
                            else None
                        ),
                        "cpu_allocated_percent": (
                            round(cpu_allocated * 100 / cpu_allocatable, 1)
                            if cpu_allocated is not None and cpu_allocatable
                            else None
                        ),
                        "memory_used_bytes": memory_used,
                        "memory_allocatable_bytes": memory_allocatable,
                        "memory_percent": (
                            round(memory_used * 100 / memory_allocatable, 1)
                            if memory_used is not None and memory_allocatable
                            else None
                        ),
                        "disk_io": disk_io_by_node.get(
                            name,
                            {
                                "available": False,
                                "read_bytes_per_second": None,
                                "write_bytes_per_second": None,
                                "read_iops": None,
                                "write_iops": None,
                                "busy_percent": None,
                                "io_current": None,
                                "error": "disk I/O metrics unavailable",
                            },
                        ),
                    }
                )
            all_available = not missing
            result = {
                "available": all_available,
                "stale": False,
                "error": (
                    None if all_available else f"metrics missing for nodes: {', '.join(missing)}"
                ),
                "allocation_error": allocation_error,
                "collected_at": datetime.now(UTC).isoformat(),
                "aggregate": {
                    "cpu_used_millicores": total_cpu_used if all_available else None,
                    "cpu_allocated_millicores": (
                        total_cpu_allocated if allocated_by_node is not None else None
                    ),
                    "cpu_allocatable_millicores": total_cpu_allocatable,
                    "cpu_percent": (
                        round(total_cpu_used * 100 / total_cpu_allocatable, 1)
                        if all_available and total_cpu_allocatable
                        else None
                    ),
                    "cpu_allocated_percent": (
                        round(total_cpu_allocated * 100 / total_cpu_allocatable, 1)
                        if allocated_by_node is not None and total_cpu_allocatable
                        else None
                    ),
                    "memory_used_bytes": total_memory_used if all_available else None,
                    "memory_allocatable_bytes": total_memory_allocatable,
                    "memory_percent": (
                        round(total_memory_used * 100 / total_memory_allocatable, 1)
                        if all_available and total_memory_allocatable
                        else None
                    ),
                },
                "nodes": per_node,
            }
            if all_available:
                self._last_resource_metrics = result
            return result
        except Exception as error:
            message = f"{type(error).__name__}: {str(error)[:300]}"
            if self._last_resource_metrics is not None:
                stale = json.loads(json.dumps(self._last_resource_metrics))
                stale.update(available=True, stale=True, error=message)
                return stale
            return {
                "available": False,
                "stale": False,
                "error": message,
                "allocation_error": None,
                "collected_at": None,
                "aggregate": {},
                "nodes": [],
            }

    @staticmethod
    def _counter_rate(current: float, previous: float, elapsed: float) -> float | None:
        delta = current - previous
        if elapsed <= 0 or delta < 0:
            return None
        return round(delta / elapsed, 3)

    def _collect_disk_io_metrics(self, node_doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
        current_monotonic = time.monotonic()
        if (
            self._last_disk_io_poll_monotonic is not None
            and current_monotonic - self._last_disk_io_poll_monotonic
            < LOCAL_DISK_IO_POLL_SECONDS
        ):
            return self._disk_io_metrics
        self._last_disk_io_poll_monotonic = current_monotonic
        sampled_at = datetime.now(UTC).isoformat()
        updated = dict(self._disk_io_metrics)
        for item in node_doc.get("items", []):
            node = item.get("metadata", {}).get("name")
            if not isinstance(node, str) or not node:
                continue
            path = (
                f"/api/v1/nodes/{urllib.parse.quote(node, safe='')}/proxy/metrics/cadvisor"
            )
            completed = self.runner(
                ["kubectl", "--request-timeout=5s", "get", "--raw", path],
                capture_output=True,
                text=True,
                timeout=8,
                check=False,
            )
            if completed.returncode != 0:
                existing = dict(updated.get(node, {}))
                existing.update(
                    available=bool(existing.get("available")),
                    stale=bool(existing),
                    error=(completed.stderr or "cAdvisor disk I/O metrics unavailable")[:300],
                )
                updated[node] = existing
                continue
            counters = parse_cadvisor_disk_io(completed.stdout)
            previous = self._disk_io_counters.get(node)
            metrics: dict[str, Any] = {
                "available": False,
                "stale": False,
                "read_bytes_per_second": None,
                "write_bytes_per_second": None,
                "read_iops": None,
                "write_iops": None,
                "busy_percent": None,
                "io_current": counters["io_current"],
                "sampled_at": sampled_at,
                "error": "awaiting second cAdvisor sample",
            }
            if previous is not None and counters["device_count"]:
                elapsed = current_monotonic - float(previous["sampled_monotonic"])
                metrics.update(
                    available=True,
                    read_bytes_per_second=self._counter_rate(
                        counters["read_bytes"], previous["read_bytes"], elapsed
                    ),
                    write_bytes_per_second=self._counter_rate(
                        counters["write_bytes"], previous["write_bytes"], elapsed
                    ),
                    read_iops=self._counter_rate(
                        counters["read_ops"], previous["read_ops"], elapsed
                    ),
                    write_iops=self._counter_rate(
                        counters["write_ops"], previous["write_ops"], elapsed
                    ),
                    error=None,
                )
                busy_values = []
                previous_io_time = previous.get("io_time_by_device", {})
                for device, io_time in counters["io_time_by_device"].items():
                    old_io_time = previous_io_time.get(device)
                    if old_io_time is None:
                        continue
                    rate = self._counter_rate(io_time, old_io_time, elapsed)
                    if rate is not None:
                        busy_values.append(min(100.0, rate * 100))
                metrics["busy_percent"] = round(max(busy_values), 1) if busy_values else None
            self._disk_io_counters[node] = {
                **counters,
                "sampled_monotonic": current_monotonic,
            }
            updated[node] = metrics
        self._disk_io_metrics = updated
        return updated
