"""Live PostgreSQL/PGMQ and k3s telemetry for the distributed pipeline."""

from __future__ import annotations

import base64
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
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

STAGES = ("generate", "validate", "repair", "reward", "push")

# Maps an auxiliary deployment's pod-label stage onto its canonical stage so the
# dashboard counts the combined worker fleet. Generate used to fan out into
# generate-overflow / generate-moedsa pools that folded back here; it is now a
# single swegen-generate deployment, so no aliases are currently needed. Kept as
# an extension point for future split pools.
STAGE_ALIASES: dict[str, str] = {}
QUEUE_BY_STAGE = {
    "generate": ("swegen_generate",),
    "validate": ("swegen_validate_repaired", "swegen_validate"),
    "repair": ("swegen_repair",),
    "reward": ("swegen_reward",),
    "push": ("swegen_push",),
}
DEAD_QUEUE = "swegen_dead"
REMOTE_BUILD_PENDING_STATUSES = frozenset({"submitting", "queued", "running"})
# Remote build rows are best-effort submission tracking, not an authoritative
# farm queue. A worker killed before terminal writeback can leave one of these
# rows nonterminal forever. Keep only records updated within the client build
# deadline plus a small status-writeback grace window in the recent bucket.
REMOTE_BUILD_TRACKING_RECENT_SECONDS = 70 * 60
# No default farm URL: the literal that used to live here duplicated ConfigMap
# SWEGEN_REMOTE_BUILDKIT_URL, so the panel kept polling a hardcoded address and
# reporting it as the farm even when nothing configured one. Unlike the worker
# stages, an unset value here is reported as unconfigured rather than raised:
# this collector is built eagerly in SnapshotCache.__init__, outside the
# per-collector error handling in SnapshotCache.refresh(), so raising would take
# the entire status page down over one optional panel.
REMOTE_BUILDKIT_URL_ENV = "SWEGEN_REMOTE_BUILDKIT_URL"
REMOTE_BUILDKIT_URL_UNCONFIGURED = (
    f"{REMOTE_BUILDKIT_URL_ENV} is not set, so the remote BuildKit farm is not "
    "being polled. It is supplied by ConfigMap swegen-pipeline-config "
    "(deploy/k3s/swegen-pipeline.yaml); export it for the dashboard process to "
    "re-enable this panel."
)
REMOTE_BUILDKIT_MIN_POLL_SECONDS = 30.0
REMOTE_BUILDKIT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024
LOCAL_DISK_IO_POLL_SECONDS = 30.0
ACTIVITY_STALE_AFTER_SECONDS = 120.0
# Cap on how many out-of-sync instance ids the SWR push-sync panel returns; the
# summary still reports the true total so the list can be truncated safely.
SWR_PUSH_SYNC_LIST_LIMIT = 500
TERMINAL_POD_PHASES = frozenset({"Succeeded", "Failed"})
# Display order for pod states; anything kubectl reports outside this list sorts alphabetically.
POD_PHASE_ORDER = ("Running", "Pending", "Terminating", "Succeeded", "Failed", "Unknown")
# Phase carrying evicted Pods. Excluded at the kubectl query so their records
# never reach the collector: they are inert but unbounded in number.
_EVICTED_POD_PHASE = "Failed"


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
    rows = list(rows)
    recent_counts = {
        str(row["status"]): int(row.get("recent_count") or 0)
        for row in rows
        if row.get("status") is not None
    }
    stale_counts = {
        str(row["status"]): int(row.get("stale_count") or 0)
        for row in rows
        if row.get("status") is not None
    }
    latest_updates = [value for row in rows if (value := row.get("latest_updated_at")) is not None]
    latest_updated_at = max(latest_updates) if latest_updates else None
    if isinstance(latest_updated_at, datetime):
        latest_updated_at = latest_updated_at.isoformat()
    return {
        "available": available,
        "recent": (
            sum(recent_counts.get(status, 0) for status in REMOTE_BUILD_PENDING_STATUSES)
            if available
            else None
        ),
        "stale": (
            sum(stale_counts.get(status, 0) for status in REMOTE_BUILD_PENDING_STATUSES)
            if available
            else None
        ),
        "recent_status_counts": dict(sorted(recent_counts.items())),
        "stale_status_counts": dict(sorted(stale_counts.items())),
        "recent_window_seconds": REMOTE_BUILD_TRACKING_RECENT_SECONDS,
        "latest_updated_at": latest_updated_at,
        "authoritative": False,
    }


def summarize_swr_push_sync(
    summary_row: dict[str, Any] | None,
    out_of_sync_rows: Iterable[dict[str, Any]] = (),
    *,
    available: bool,
    list_limit: int = SWR_PUSH_SYNC_LIST_LIMIT,
) -> dict[str, Any]:
    """Fold the SWR push-registry sync counts and a capped out-of-sync id list.

    An instance is "in sync" only when it is pushed to BOTH the data-platform and
    the data-trajectory registry. ``platform_only`` / ``trajectory_only`` are the
    two out-of-sync directions. Degrades to zeros when ``pushed_images`` is absent.
    """

    if not available or summary_row is None:
        return {
            "available": False,
            "platform_count": 0,
            "trajectory_count": 0,
            "in_sync": 0,
            "platform_only": 0,
            "trajectory_only": 0,
            "out_of_sync_total": 0,
            "out_of_sync_instances": [],
            "out_of_sync_list_limit": list_limit,
            "out_of_sync_list_truncated": False,
        }

    platform_only = int(summary_row.get("platform_only") or 0)
    trajectory_only = int(summary_row.get("trajectory_only") or 0)
    out_of_sync_total = platform_only + trajectory_only
    instances = [
        {
            "instance": str(row["instance"]),
            "registry": ("platform" if row.get("on_platform") else "trajectory"),
        }
        for row in out_of_sync_rows
        if row.get("instance") is not None
    ]
    return {
        "available": True,
        "platform_count": int(summary_row.get("platform_count") or 0),
        "trajectory_count": int(summary_row.get("trajectory_count") or 0),
        "in_sync": int(summary_row.get("in_sync") or 0),
        "platform_only": platform_only,
        "trajectory_only": trajectory_only,
        "out_of_sync_total": out_of_sync_total,
        "out_of_sync_instances": instances,
        "out_of_sync_list_limit": list_limit,
        "out_of_sync_list_truncated": out_of_sync_total > len(instances),
    }


# The two SWR push registries the dashboard exports, mapped to the substring
# that identifies each one inside `pushed_images.swr_url`.
#
# The URL is the discriminator, not the `registry` column, because ~12.9k rows
# written on 2026-07-28 predate that column and carry an empty registry while
# still holding a real swr_url and a matching pipeline_tasks row. Filtering on
# `registry` would silently drop 6,136 platform and 6,436 trajectory images --
# and the summary card counts them, so the button would have handed back 11.6k
# lines while the number directly above it read 17.8k. `suffix` is no use
# either: it defaults to '' so trajectory is indistinguishable from unset.
PUSHED_IMAGE_REGISTRY_URL_MARKERS = {
    "platform": "%data-platform%",
    "trajectory": "%data-trajectory%",
}
PUSHED_IMAGE_REGISTRIES = tuple(PUSHED_IMAGE_REGISTRY_URL_MARKERS)
# Rows pulled per server-side FETCH while streaming an export. The export is
# ~12k rows; batching keeps both the DB round-trips and the resident row set
# bounded instead of materialising the whole result in the dashboard process.
PUSHED_IMAGE_EXPORT_BATCH = 500

# One row per pushed instance. DISTINCT ON collapses the rare instance that has
# more than one pipeline_tasks version (retries bump task_version), keeping the
# newest, so an export line count matches the card's distinct-image count.
_PUSHED_IMAGE_EXPORT_SQL = """
SELECT DISTINCT ON (p.instance)
       p.instance, p.swr_url, p.written_at,
       t.repo, t.pr, t.created_at
FROM public.pushed_images p
JOIN public.pipeline_tasks t ON t.task_id = p.instance
WHERE p.pushed AND p.swr_url LIKE %s
ORDER BY p.instance, t.task_version DESC
"""


def pushed_image_export_record(row: Mapping[str, Any], registry: str = "") -> dict[str, Any]:
    """Fold one export row into the JSON-safe object written as a JSONL line.

    ``registry`` is supplied by the caller rather than read from the row: the
    legacy rows this export deliberately includes have an empty registry column,
    and the value the operator asked for is the one they clicked.
    """

    return {
        "instance_id": str(row["instance"]),
        "repo": row.get("repo"),
        "pr": row.get("pr"),
        "registry": registry or row.get("registry") or "",
        "registry_path": row.get("swr_url"),
        "created_at": _iso(row.get("created_at")),
        "pushed_at": _iso(row.get("written_at")),
    }


def iter_pushed_image_export(
    registry: str,
    *,
    connect: Callable[[], Any] | None = None,
) -> Iterable[dict[str, Any]]:
    """Stream every instance pushed to ``registry`` as JSON-safe export records.

    Rows are pulled through a named (server-side) cursor in batches so a ~12k
    row export never materialises in the dashboard process at once. A missing
    ``pushed_images``/``pipeline_tasks`` relation degrades to an empty stream:
    an operator clicking Download on a database that has not been migrated gets
    an empty file, not a 500.
    """

    if registry not in PUSHED_IMAGE_REGISTRIES:
        raise ValueError(f"unknown push registry: {registry!r}")
    opener = connect or (lambda: psycopg.connect(_database_dsn(), row_factory=dict_row))
    with opener() as connection:
        try:
            with connection.transaction():
                connection.execute("SET LOCAL statement_timeout = '30s'")
                connection.execute("SET TRANSACTION READ ONLY")
                # Named cursor => the result set stays on the server and is
                # fetched in itersize batches as the response is written out.
                with connection.cursor(name="pushed_image_export") as cursor:
                    cursor.itersize = PUSHED_IMAGE_EXPORT_BATCH
                    cursor.execute(
                        _PUSHED_IMAGE_EXPORT_SQL,
                        (PUSHED_IMAGE_REGISTRY_URL_MARKERS[registry],),
                    )
                    for row in cursor:
                        yield pushed_image_export_record(row, registry)
        except (psycopg.errors.UndefinedColumn, psycopg.errors.UndefinedTable):
            return


GENERATE_ENDPOINT_DEPLOYMENT_PREFIX = "swegen-generate-dyn-"


def generate_endpoint_deployment_name(slug: str) -> str:
    """Deployment name the controller reconciles for a registry ``slug``."""

    return f"{GENERATE_ENDPOINT_DEPLOYMENT_PREFIX}{slug}"


def _endpoint_host(base_url: object) -> str | None:
    """Return the host[:port] of an endpoint URL for display, never the token."""

    if not isinstance(base_url, str) or not base_url:
        return None
    try:
        parsed = urllib.parse.urlsplit(base_url)
    except ValueError:
        return base_url
    return parsed.netloc or base_url


def summarize_generate_endpoints(
    rows: Iterable[dict[str, Any]],
    running_by_slug: dict[str, int] | None = None,
    generate_model_timeseries: dict[str, Any] | None = None,
    *,
    available: bool = True,
) -> dict[str, Any]:
    """Fold the dynamic generate-endpoint registry into a JSON-safe snapshot.

    Each row is a ``generate_endpoints`` record. ``running_by_slug`` is the live
    running-pod count per slug (attributed by the pod's
    ``swegen.pgcode/endpoint`` label / ``swegen-generate-dyn-<slug>`` deployment,
    supplied by the k3s collector). ``generate_model_timeseries`` is the existing
    per-generating-model diverging chart (``{"models":[...],"buckets":[...]}``);
    each endpoint's recent 5m succeeded/failed counts are pulled out of it keyed
    by the endpoint's ``model_id`` so the UI can show recent outcomes.

    SECURITY: the ``auth_token`` column is never read into the output. Missing
    registry table degrades to ``available: False`` with an empty list.
    """

    running_by_slug = running_by_slug or {}
    recent_by_model = _recent_outcomes_by_model(generate_model_timeseries)
    endpoints: list[dict[str, Any]] = []
    for row in rows:
        slug = row.get("slug")
        if not slug:
            continue
        model_id = row.get("model_id")
        recent = recent_by_model.get(model_id, {"succeeded": 0, "failed": 0})
        endpoints.append(
            {
                "slug": slug,
                "model_id": model_id,
                "host": _endpoint_host(row.get("base_url")),
                "base_url": row.get("base_url"),
                "concurrency": int(row.get("concurrency") or 0),
                "running": int(running_by_slug.get(slug, 0)),
                "deployment": generate_endpoint_deployment_name(slug),
                "enabled": bool(row.get("enabled")),
                "breaker_open": bool(row.get("breaker_open")),
                "breaker_reason": row.get("breaker_reason"),
                "tripped_at": _iso(row.get("tripped_at"))
                if isinstance(row.get("tripped_at"), datetime)
                else row.get("tripped_at"),
                "reset_at": _iso(row.get("reset_at"))
                if isinstance(row.get("reset_at"), datetime)
                else row.get("reset_at"),
                "last_probe_status": row.get("last_probe_status"),
                "last_probe_at": _iso(row.get("last_probe_at"))
                if isinstance(row.get("last_probe_at"), datetime)
                else row.get("last_probe_at"),
                "consecutive_fail": int(row.get("consecutive_fail") or 0),
                "recent_succeeded": recent["succeeded"],
                "recent_failed": recent["failed"],
            }
        )
    endpoints.sort(key=lambda entry: (str(entry.get("model_id") or ""), entry["slug"]))
    return {"available": bool(available), "endpoints": endpoints}


def _recent_outcomes_by_model(
    generate_model_timeseries: dict[str, Any] | None,
) -> dict[str, dict[str, int]]:
    """Sum the last (most recent) bucket's per-model succeeded/failed counts.

    Reuses the generate diverging chart so an endpoint's recent outcomes are the
    same numbers the chart draws, keyed by model_id.
    """

    if not isinstance(generate_model_timeseries, dict):
        return {}
    buckets = generate_model_timeseries.get("buckets") or []
    if not buckets:
        return {}
    latest = buckets[-1]
    by_model = latest.get("by_model") if isinstance(latest, dict) else None
    if not isinstance(by_model, dict):
        return {}
    out: dict[str, dict[str, int]] = {}
    for model_id, counts in by_model.items():
        if not isinstance(counts, dict):
            continue
        out[model_id] = {
            "succeeded": int(counts.get("succeeded") or 0),
            "failed": int(counts.get("failed") or 0),
        }
    return out


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


def _pod_phase_sort_key(phase: str) -> tuple[int, str]:
    order = POD_PHASE_ORDER.index(phase) if phase in POD_PHASE_ORDER else len(POD_PHASE_ORDER)
    return order, phase


def pod_display_phase(pod: dict[str, Any]) -> str:
    """Report the pod state kubectl prints, including the Terminating pseudo-phase.

    Terminating is not a phase: it is a non-terminal pod carrying a deletion timestamp.
    Evicted is reported separately because a node under memory pressure accumulates
    thousands of Failed/Evicted records that would otherwise dominate every count.
    """

    status = pod.get("status", {})
    phase = str(status.get("phase") or "Unknown")
    if phase == "Failed" and status.get("reason") == "Evicted":
        return "Evicted"
    if phase not in TERMINAL_POD_PHASES and pod.get("metadata", {}).get("deletionTimestamp"):
        return "Terminating"
    return phase


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


# Only the five fields the dashboard's Timeline column actually renders. The
# per-stage view used to carry 16 keys (attempt/deliveries/queued_at/started_at/
# finished_at/heartbeat_at/queued_seconds/heartbeat_age_seconds/stale/node_name/
# error); at 5 stages x 100 recent tasks that was ~245 KB of the ~590 KB status
# payload and not one of those keys was ever read by the front-end. Anything
# needed for deeper forensics is a direct query against pipeline_stage_results /
# pipeline_stage_activity, which hold the same values without shipping them to
# every browser on every 5s poll.
def _empty_stage(stage: str) -> dict[str, Any]:
    return {
        "stage": stage,
        "state": "not_started",
        "wait_seconds": None,
        "run_seconds": None,
        "worker_id": None,
    }


# A worker_id is a pod name shaped like
# ``<deployment>-<replicaset_hash>-<pod_hash>`` (e.g.
# ``swegen-generate-9b75d9789-27tw2``). The trailing two dash-delimited hash
# tokens are the ReplicaSet suffix and the per-pod suffix that Kubernetes
# appends; stripping them yields the owning Deployment name. The pod suffix is
# always five [a-z0-9] characters; the ReplicaSet suffix is a variable-length
# [a-z0-9] token.
_POD_SUFFIX_RE = re.compile(r"-[a-z0-9]+-[a-z0-9]{5}$")
# Diverging-chart direction mapping, shared by every stage. 'succeeded' stacks
# up (good outcome); 'failed'/'error' stack down (infra/terminal failure).
# 'rejected' (a validate/reward nop-oracle legitimate rejection) is neither a
# clean success nor an infra failure: it is counted into its own third series so
# the operator can see it, but the diverging bars only stack succeeded-up and
# failed-down. The renderer draws rejected as a thin neutral marker on the down
# side; it is never folded into the failure total. Any other status is ignored.
_UP_STATUSES = frozenset({"succeeded"})
_DOWN_STATUSES = frozenset({"failed", "error"})
_REJECTED_STATUSES = frozenset({"rejected"})
# Stages downstream of generate; their per-model split is attributed to the
# model that GENERATED each underlying task (see aggregate_stage_model_timeseries).
_DOWNSTREAM_STAGES = ("validate", "repair", "reward", "push")
# Label for a downstream result whose generating model could not be resolved
# (its generate row fell outside the 48h query window, or its worker_id did not
# resolve to a deployment). Charted under this bucket rather than dropped.
_UNKNOWN_MODEL = "unknown"


def _diverging_direction(status: object) -> str | None:
    """Map a stage-result status onto its diverging-chart series, or None."""

    if status in _UP_STATUSES:
        return "succeeded"
    if status in _DOWN_STATUSES:
        return "failed"
    if status in _REJECTED_STATUSES:
        return "rejected"
    return None


def deployment_name_from_worker_id(worker_id: str | None) -> str | None:
    """Derive the owning Deployment name from a worker/pod id, or None.

    Strips the ReplicaSet + pod hash suffix Kubernetes appends. A worker_id that
    does not match the pod-name shape (no suffix to strip) is returned unchanged
    so an operator-set custom id still maps to *something* rather than vanishing.

    NOTE: this regex strip is unreliable for long names. A pod name is capped at
    63 chars, and when ``<deployment>-<10char-hash>-<5char>`` would exceed that,
    Kubernetes truncates the tail -- often collapsing the ``-<hash>-<rand>`` into
    a single dashless-in-the-middle blob (e.g. the dynamic
    ``swegen-generate-dyn-...`` pools hit exactly 63 chars). The regex requires
    two trailing dash-groups and then leaves such names UNchanged. Prefer
    ``resolve_worker_model`` (below), which prefix-matches against the known
    deployment names and is truncation-proof; this helper remains for callers
    without a deployment set and as that resolver's fallback.
    """

    if not worker_id:
        return None
    stripped = _POD_SUFFIX_RE.sub("", worker_id)
    return stripped or worker_id


def resolve_worker_model(
    worker_id: str | None,
    deployment_to_model: Mapping[str, str],
) -> str | None:
    """Resolve a worker/pod id to its model_id via the known deployment names.

    The reliable signal is the set of real Deployment names in
    ``deployment_to_model``: a pod name always begins with
    ``<deployment-name>-`` (the ReplicaSet/pod suffix follows), so we
    prefix-match the worker_id against those names -- longest match wins, so a
    name that is a prefix of another can't steal its pods. This is immune to the
    63-char pod-name truncation that defeats the regex strip. Returns the model
    for the matched deployment, else falls back to the regex-derived deployment
    name (mapped or raw) so an unknown worker is still charted rather than
    dropped. ``None`` only when ``worker_id`` is empty.
    """

    if not worker_id:
        return None
    best: str | None = None
    for name in deployment_to_model:
        if name and (worker_id == name or worker_id.startswith(name + "-")):
            if best is None or len(name) > len(best):
                best = name
    if best is not None:
        return deployment_to_model[best]
    deployment = deployment_name_from_worker_id(worker_id)
    return deployment_to_model.get(deployment or "", deployment)


def _empty_model_counts() -> dict[str, int]:
    return {"succeeded": 0, "failed": 0, "rejected": 0}


def _fold_model_buckets(
    entries: Iterable[tuple[str, str, str, int]],
) -> dict[str, Any]:
    """Fold ``(bucket, model_id, direction, count)`` tuples into the chart shape.

    ``direction`` is one of succeeded/failed/rejected (already mapped from the
    raw status). Returns ``{"models": [...], "buckets": [{"t":..,"by_model":..}]}``
    sorted by bucket time and model_id for stable colour assignment.
    """

    buckets: dict[str, dict[str, dict[str, int]]] = {}
    models: set[str] = set()
    for bucket, model_id, direction, count in entries:
        models.add(model_id)
        counts = buckets.setdefault(bucket, {}).setdefault(model_id, _empty_model_counts())
        counts[direction] += count
    ordered_buckets = [{"t": bucket, "by_model": buckets[bucket]} for bucket in sorted(buckets)]
    return {"models": sorted(models), "buckets": ordered_buckets}


def aggregate_generate_model_timeseries(
    generate_bucket_rows: Iterable[dict[str, Any]],
    deployment_to_model: dict[str, str],
) -> dict[str, Any]:
    """Fold per-worker generate buckets into per-(bucket, model_id) up/down counts.

    ``generate_bucket_rows`` are per-(bucket, worker_id, status) counts from the
    generate time-bucket query. Generate is the one stage split by its OWN
    worker's model: each worker_id resolves through its Deployment name to a
    model_id via ``deployment_to_model``; a worker whose deployment is unknown is
    labelled by its derived deployment name so it is still charted rather than
    silently dropped. 'succeeded' stacks up; 'failed'/'error' stack down; a stray
    'rejected' never appears for generate but would land in its own series.
    """

    def entries() -> Iterable[tuple[str, str, str, int]]:
        for row in generate_bucket_rows:
            bucket = _iso(row.get("bucket"))
            if bucket is None:
                continue
            direction = _diverging_direction(row.get("status"))
            if direction is None:
                continue
            model_id = (
                resolve_worker_model(row.get("worker_id"), deployment_to_model)
                or _UNKNOWN_MODEL
            )
            yield bucket, model_id, direction, int(row.get("n") or 0)

    return _fold_model_buckets(entries())


def aggregate_downstream_stage_model_timeseries(
    downstream_bucket_rows: Iterable[dict[str, Any]],
    task_to_generating_model: dict[str, str],
) -> dict[str, dict[str, Any]]:
    """Split each downstream stage's buckets by the model that GENERATED the task.

    Validate/repair/reward/push each run under a single deployment, so splitting
    them by their own worker's model would give one trivial series. The useful
    breakdown is by the model that generated the underlying task: it answers
    "how do deepseek-generated vs pretrain-generated tasks fare downstream?".

    ``downstream_bucket_rows`` are per-(bucket, task_id, stage, status) rows;
    each task_id is looked up in ``task_to_generating_model`` (built from the
    generate-stage rows). A task whose generate row is outside the query window
    or unresolvable buckets under the "unknown" model rather than being dropped.
    Returns ``{stage: {"models": [...], "buckets": [...]}}`` for every downstream
    stage, present even when empty.
    """

    per_stage: dict[str, list[tuple[str, str, str, int]]] = {
        stage: [] for stage in _DOWNSTREAM_STAGES
    }
    for row in downstream_bucket_rows:
        stage = row.get("stage")
        if stage not in per_stage:
            continue
        bucket = _iso(row.get("bucket"))
        if bucket is None:
            continue
        direction = _diverging_direction(row.get("status"))
        if direction is None:
            continue
        model_id = task_to_generating_model.get(row.get("task_id"), _UNKNOWN_MODEL)
        per_stage[stage].append((bucket, model_id, direction, int(row.get("n") or 1)))
    return {stage: _fold_model_buckets(entries) for stage, entries in per_stage.items()}


def build_task_generating_model_map(
    generate_task_rows: Iterable[dict[str, Any]],
    deployment_to_model: dict[str, str],
) -> dict[str, str]:
    """Map each generate-stage task_id to the model_id that generated it.

    ``generate_task_rows`` are ``(task_id, worker_id)`` rows from the generate
    stage. worker_id resolves through its Deployment to a model_id, falling back
    to the deployment name when the model is unresolved. A task_id absent from
    this map (its generate row fell outside the window) is treated as "unknown"
    by the downstream aggregation.
    """

    mapping: dict[str, str] = {}
    for row in generate_task_rows:
        task_id = row.get("task_id")
        if not task_id:
            continue
        model_id = resolve_worker_model(row.get("worker_id"), deployment_to_model)
        if model_id:
            mapping[task_id] = model_id
    return mapping


def aggregate_pipeline_snapshot(
    task_rows: Iterable[dict[str, Any]],
    result_rows: Iterable[dict[str, Any]],
    activity_rows: Iterable[dict[str, Any]],
    queue_rows: Iterable[dict[str, Any]],
    *,
    now: datetime | None = None,
    activity_stale_after_seconds: float = ACTIVITY_STALE_AFTER_SECONDS,
    timeseries_lookback_hours: int = 48,
    activity_count_rows: Iterable[dict[str, Any]] = (),
    generate_model_bucket_rows: Iterable[dict[str, Any]] = (),
    generate_deployment_to_model: dict[str, str] | None = None,
    downstream_model_bucket_rows: Iterable[dict[str, Any]] = (),
    generate_task_model_rows: Iterable[dict[str, Any]] = (),
    hourly_yield_rows: Iterable[dict[str, Any]] = (),
    lifetime_stage_rows: Iterable[dict[str, Any]] = (),
    unique_instance_rows: Iterable[dict[str, Any]] = (),
    instance_universe_total: int | None = None,
    remote_build_rows: Iterable[dict[str, Any]] = (),
    remote_build_tracking_available: bool = False,
    swr_push_sync_row: dict[str, Any] | None = None,
    swr_push_sync_out_of_sync_rows: Iterable[dict[str, Any]] = (),
    swr_push_sync_available: bool = False,
    generate_endpoint_rows: Iterable[dict[str, Any]] = (),
    generate_endpoint_running_by_slug: dict[str, int] | None = None,
    generate_endpoints_available: bool = False,
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
                    wait_seconds=_seconds(started_at, queued_at),
                    run_seconds=_seconds(finished_at, started_at),
                    worker_id=result["worker_id"],
                )
                if result["status"] == "succeeded":
                    predecessor_finished = finished_at
            elif activity is not None:
                started_at = activity["started_at"]
                view.update(
                    state="running",
                    wait_seconds=_seconds(started_at, queued_at),
                    run_seconds=_seconds(now, started_at),
                    worker_id=activity["worker_id"],
                )
            elif task["current_stage"] == stage and task["state"] in {"queued", "running"}:
                view.update(state="queued")
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
        # Only what the Recent tasks table renders. Dropped here because nothing
        # reads them: repo, pr, trace_id, created_at, updated_at, finished_at,
        # last_error, last_reason, and the constant storage.durable_source /
        # runtime_path_is_exact / durability strings. last_error alone was ~48 KB
        # per response (full tracebacks x 100 tasks) for a column the UI does not
        # have. These are all still one query away in pipeline_tasks.
        task_views.append(
            {
                "task_id": task["task_id"],
                "task_version": task["task_version"],
                "state": task["state"],
                "current_stage": task["current_stage"],
                "total_elapsed_seconds": _seconds(end, task["created_at"]),
                "storage": {
                    "stored_file_count": int(task.get("stored_file_count") or 0),
                    "stored_bytes": int(task.get("stored_bytes") or 0),
                    "generated_on_node": generate_node,
                    "runtime_path_pattern": runtime_pattern,
                    "runtime_directory_state": (
                        "temporary directory may currently exist"
                        if generate_running
                        else "temporary directory is removed after stage completion"
                    ),
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
    # Unified diverging per-model breakdown for every stage. Generate splits by
    # its own worker's model; the four downstream stages split by the model that
    # GENERATED each task (joined via task_id -> generate worker -> model).
    deployment_to_model = generate_deployment_to_model or {}
    stage_model_timeseries = {
        "generate": aggregate_generate_model_timeseries(
            generate_model_bucket_rows, deployment_to_model
        ),
    }
    task_generating_model = build_task_generating_model_map(
        generate_task_model_rows, deployment_to_model
    )
    stage_model_timeseries.update(
        aggregate_downstream_stage_model_timeseries(
            downstream_model_bucket_rows, task_generating_model
        )
    )
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
    unique_instances_processed = dict.fromkeys(STAGES, 0)
    for row in unique_instance_rows:
        stage = row.get("stage")
        if stage in unique_instances_processed:
            unique_instances_processed[stage] = int(row.get("unique_instances") or 0)
    activity_counts = {stage: {"fresh": 0, "stale": 0, "total": 0} for stage in STAGES}
    for row in activity_count_rows:
        stage = row.get("stage")
        if stage not in activity_counts:
            continue
        fresh = int(row.get("fresh") or 0)
        stale = int(row.get("stale") or 0)
        activity_counts[stage] = {
            "fresh": fresh,
            "stale": stale,
            "total": fresh + stale,
        }
    return {
        "generated_at": now.isoformat(),
        "queues": {
            "stages": {stage: queue_view(names) for stage, names in QUEUE_BY_STAGE.items()},
            "validate_repaired": queue_view(("swegen_validate_repaired",)),
            "validate_new": queue_view(("swegen_validate",)),
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
        "instance_coverage": {
            # Feature-PR universe: rows in mindforge's feature-labelled PR table.
            # None means the denominator query failed and the UI shows "—".
            "universe_total": (
                int(instance_universe_total) if instance_universe_total is not None else None
            ),
            # Distinct pipeline task_ids each stage has processed (attempted, any
            # status). task_id already lives in the feature-PR namespace, so this
            # is a coverage numerator against universe_total directly.
            "unique_instances_processed": unique_instances_processed,
        },
        "activity": {
            "stale_after_seconds": activity_stale_after_seconds,
            "stages": activity_counts,
        },
        # Unified diverging per-model breakdown for ALL five stages. Every stage
        # card stacks success upward and failure downward, coloured by model_id.
        # Generate is coloured by its own worker's model; the four downstream
        # stages are coloured by the model that generated each task, so the
        # operator can compare how each model's tasks fare downstream. Rejected
        # results (validate/reward nop-oracle rejections) are carried as a third
        # per-model count and drawn as a thin neutral marker, not folded into
        # the failure total.
        "stage_model_timeseries": {
            "bucket_seconds": 900,
            "lookback_hours": timeseries_lookback_hours,
            "stages": stage_model_timeseries,
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
        "swr_push_sync": summarize_swr_push_sync(
            swr_push_sync_row,
            swr_push_sync_out_of_sync_rows,
            available=swr_push_sync_available,
        ),
        "generate_endpoints": summarize_generate_endpoints(
            generate_endpoint_rows,
            generate_endpoint_running_by_slug,
            stage_model_timeseries.get("generate"),
            available=generate_endpoints_available,
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


def _mindforge_dsn() -> str:
    """DSN for the feature-PR universe: same host/port/credentials, dbname=mindforge.

    The coverage denominator lives in a different logical database on the same
    PostgreSQL host as the pipeline. Only the dbname differs from the pipeline DSN.
    """

    password = os.environ.get("SWEGEN_PG_PASSWORD", "")
    if not password:
        raise RuntimeError("SWEGEN_PG_PASSWORD is not set")
    return " ".join(
        (
            f"host={os.environ.get('SWEGEN_PG_HOST', '7.237.95.141')}",
            f"port={os.environ.get('SWEGEN_PG_PORT', '5432')}",
            "dbname=mindforge",
            f"user={os.environ.get('SWEGEN_PG_USER', 'root')}",
            f"password={password}",
            "connect_timeout=5",
        )
    )


def _fetch_instance_universe_total() -> int | None:
    """Count feature-labelled PRs in the mindforge universe, or None on failure.

    A mindforge outage must not break the pipeline dashboard, so every failure
    mode (missing env, connection error, missing table) degrades to None and the
    UI falls back to showing the numerator with a "—" denominator.
    """

    try:
        with psycopg.connect(_mindforge_dsn(), row_factory=dict_row) as connection:
            with connection.transaction():
                connection.execute("SET LOCAL statement_timeout = '5s'")
                connection.execute("SET TRANSACTION READ ONLY")
                # All constants; no params, so a literal % would never appear here.
                row = connection.execute(
                    """
                    SELECT count(*) AS total
                    FROM mindforge.go_prs_prs_copy_lang_category_merged
                    WHERE pr_category = 'feature'
                    """
                ).fetchone()
        if row is None or row.get("total") is None:
            return None
        return int(row["total"])
    except Exception:
        return None


def resolve_generate_models_from_deployments(
    deployment_items: Iterable[dict[str, Any]],
    secret_models: dict[str, str],
) -> dict[str, str]:
    """Map generate Deployment name -> model_id from deployment specs + secrets.

    ``deployment_items`` are Kubernetes Deployment objects (kubectl JSON items);
    only those whose stage label is ``generate`` are considered. The static
    generate workers deliver their model via the ``swegen-model-credentials-*``
    Secret referenced in ``envFrom`` (whose ``ANTHROPIC_MODEL`` value is the
    model_id); ``secret_models`` maps that Secret name to its decoded
    ``ANTHROPIC_MODEL``. The dynamic endpoint deployments
    (``swegen-generate-dyn-<slug>``) instead carry the model as an inline
    container ``env`` entry ``{name: ANTHROPIC_MODEL, value: <model_id>}`` and
    reference no such secret. Both sources are read here; when both are present
    for one deployment the inline ``env`` value WINS (inline env overrides
    ``envFrom`` at runtime in k8s, and it is the source of truth for the dynamic
    pools). When NEITHER a secret model nor an inline value is available, the
    deployment is left out of the map and the chart falls back to labelling that
    series by its deployment name.
    """

    mapping: dict[str, str] = {}
    for item in deployment_items:
        if item.get("kind") != "Deployment":
            continue
        metadata = item.get("metadata", {})
        spec = item.get("spec", {})
        labels = metadata.get("labels", {})
        stage = labels.get("swegen.pgcode/stage") or (
            spec.get("selector", {}).get("matchLabels", {}).get("swegen.pgcode/stage")
        )
        if STAGE_ALIASES.get(stage, stage) != "generate":
            continue
        name = metadata.get("name")
        if not name:
            continue
        pod_spec = spec.get("template", {}).get("spec", {})
        secret_name = None
        inline_model: str | None = None
        for container in pod_spec.get("containers", []):
            for source in container.get("envFrom", []):
                ref = source.get("secretRef") or {}
                ref_name = ref.get("name")
                if isinstance(ref_name, str) and ref_name.startswith(
                    "swegen-model-credentials-"
                ):
                    # Last matching envFrom wins: envFrom later in the list
                    # overrides earlier sources, matching runtime precedence.
                    secret_name = ref_name
            for entry in container.get("env", []):
                if entry.get("name") != "ANTHROPIC_MODEL":
                    continue
                value = entry.get("value")
                if isinstance(value, str) and value:
                    # Last inline entry wins, mirroring runtime env precedence.
                    inline_model = value
        secret_model = secret_models.get(secret_name) if secret_name else None
        # Inline env overrides the secret-derived model when both exist.
        model_id = inline_model or secret_model
        if model_id:
            mapping[name] = model_id
    return mapping


class PipelineStatusCollector:
    """Fetch bounded recent task telemetry and complete PGMQ queue metrics."""

    def __init__(
        self,
        *,
        recent_task_limit: int = 100,
        namespace: str = "swegen-pipeline",
        runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        self.recent_task_limit = recent_task_limit
        self.namespace = namespace
        self.runner = runner

    def _resolve_generate_models(self) -> dict[str, str]:
        """Best-effort generate Deployment -> model_id map; {} on any failure.

        Reads generate Deployments and, for each distinct model-credential
        Secret they reference, the decoded ``ANTHROPIC_MODEL`` value. Any
        kubectl failure (RBAC, timeout, missing objects) degrades to an empty
        map so a k3s hiccup never breaks the pipeline snapshot.
        """

        try:
            deployment_doc = self._kubectl_json(
                ["-n", self.namespace, "get", "deployments"]
            )
        except Exception:
            return {}
        deployment_items = deployment_doc.get("items", [])
        secret_names = set()
        for item in deployment_items:
            if item.get("kind") != "Deployment":
                continue
            labels = item.get("metadata", {}).get("labels", {})
            spec = item.get("spec", {})
            stage = labels.get("swegen.pgcode/stage") or (
                spec.get("selector", {}).get("matchLabels", {}).get("swegen.pgcode/stage")
            )
            if STAGE_ALIASES.get(stage, stage) != "generate":
                continue
            pod_spec = spec.get("template", {}).get("spec", {})
            for container in pod_spec.get("containers", []):
                for source in container.get("envFrom", []):
                    ref_name = (source.get("secretRef") or {}).get("name")
                    if isinstance(ref_name, str) and ref_name.startswith(
                        "swegen-model-credentials-"
                    ):
                        secret_names.add(ref_name)
        secret_models: dict[str, str] = {}
        for secret_name in secret_names:
            model_id = self._read_secret_model(secret_name)
            if model_id:
                secret_models[secret_name] = model_id
        return resolve_generate_models_from_deployments(deployment_items, secret_models)

    def _read_secret_model(self, secret_name: str) -> str | None:
        """Decode a Secret's ANTHROPIC_MODEL value, or None if unavailable."""

        try:
            completed = self.runner(
                [
                    "kubectl",
                    "--request-timeout=3s",
                    "-n",
                    self.namespace,
                    "get",
                    "secret",
                    secret_name,
                    "-o",
                    "jsonpath={.data.ANTHROPIC_MODEL}",
                ],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
        except Exception:
            return None
        if completed.returncode != 0:
            return None
        encoded = (completed.stdout or "").strip()
        if not encoded:
            return None
        try:
            return base64.b64decode(encoded).decode("utf-8").strip() or None
        except Exception:
            return None

    def _kubectl_json(self, args: list[str]) -> dict[str, Any]:
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

    @staticmethod
    def _sanitized_lookback(lookback_hours: object) -> int:
        """Coerce the range-dropdown lookback to a positive int, else 48h.

        The value flows into SQL only as a bound ``%s`` param, but validating
        here keeps a bad or non-int argument from ever reaching the query.
        """

        if isinstance(lookback_hours, bool) or not isinstance(lookback_hours, int):
            return 48
        return lookback_hours if lookback_hours > 0 else 48

    def collect(self, *, lookback_hours: int = 48) -> dict[str, Any]:
        # The operator's range dropdown drives the stacked-bar timeseries window.
        lookback_hours = self._sanitized_lookback(lookback_hours)
        with psycopg.connect(_database_dsn(), row_factory=dict_row) as connection:
            with connection.transaction():
                connection.execute("SET LOCAL statement_timeout = '5s'")
                connection.execute("SET TRANSACTION READ ONLY")
                tasks = list(
                    connection.execute(
                        """
                        -- Only the columns the snapshot emits. repo/pr/trace_id/
                        -- last_error/last_reason are deliberately not selected:
                        -- nothing renders them, and last_error carries full
                        -- tracebacks that dominated the response body.
                        SELECT t.task_id, t.task_version, t.state,
                               t.current_stage, t.created_at, t.finished_at,
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
                activity_counts = list(
                    connection.execute(
                        """
                        SELECT
                            stage,
                            count(*) FILTER (
                                WHERE heartbeat_at >= now() - (%s * INTERVAL '1 second')
                            ) AS fresh,
                            count(*) FILTER (
                                WHERE heartbeat_at < now() - (%s * INTERVAL '1 second')
                            ) AS stale
                        FROM pipeline_stage_activity
                        GROUP BY stage
                        """,
                        (
                            ACTIVITY_STALE_AFTER_SECONDS,
                            ACTIVITY_STALE_AFTER_SECONDS,
                        ),
                    ).fetchall()
                )
                queues = list(connection.execute("SELECT * FROM pgmq.metrics_all()").fetchall())
                # Per-worker generate buckets feed the diverging per-model chart.
                # worker_id resolves to a deployment and then a model_id in
                # Python; keeping the split here means the SQL stays a plain
                # positional-placeholder query with no % literal.
                generate_model_buckets = list(
                    connection.execute(
                        """
                        SELECT
                            date_bin(
                                INTERVAL '15 minutes',
                                finished_at,
                                TIMESTAMPTZ '2001-01-01 00:00:00+00'
                            ) AS bucket,
                            worker_id,
                            status,
                            count(*) AS n
                        FROM pipeline_stage_results
                        WHERE stage = 'generate'
                          AND finished_at >= now() - make_interval(hours => %s)
                        GROUP BY bucket, worker_id, status
                        ORDER BY bucket, worker_id, status
                        """,
                        (lookback_hours,),
                    ).fetchall()
                )
                # Downstream stages split by the GENERATING model of each task:
                # task_id is carried so the fold can join to the generate-stage
                # worker->model resolution below. Bucketed to 15 min over the
                # operator-selected lookback window to match the generate chart.
                downstream_model_buckets = list(
                    connection.execute(
                        """
                        SELECT
                            date_bin(
                                INTERVAL '15 minutes',
                                finished_at,
                                TIMESTAMPTZ '2001-01-01 00:00:00+00'
                            ) AS bucket,
                            task_id,
                            stage,
                            status,
                            count(*) AS n
                        FROM pipeline_stage_results
                        WHERE stage IN ('validate', 'repair', 'reward', 'push')
                          AND finished_at >= now() - make_interval(hours => %s)
                        GROUP BY bucket, task_id, stage, status
                        ORDER BY bucket, task_id, stage, status
                        """,
                        (lookback_hours,),
                    ).fetchall()
                )
                # task_id -> generate worker_id, used to attribute each
                # downstream result to its generating model. Bounded to the same
                # lookback window; a task generated earlier resolves to "unknown".
                generate_task_models = list(
                    connection.execute(
                        """
                        SELECT DISTINCT ON (task_id) task_id, worker_id
                        FROM pipeline_stage_results
                        WHERE stage = 'generate'
                          AND status = 'succeeded'
                          AND finished_at >= now() - make_interval(hours => %s)
                        ORDER BY task_id, finished_at DESC
                        """,
                        (lookback_hours,),
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
                unique_instances = list(
                    connection.execute(
                        """
                        SELECT stage, count(DISTINCT task_id) AS unique_instances
                        FROM pipeline_stage_results
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
                                    SELECT
                                        status,
                                        count(*) FILTER (WHERE recent) AS recent_count,
                                        count(*) FILTER (WHERE NOT recent) AS stale_count,
                                        max(updated_at) AS latest_updated_at
                                    FROM (
                                        SELECT
                                            status,
                                            updated_at,
                                            updated_at >= now() - make_interval(secs => %s)
                                                AS recent
                                        FROM pipeline_remote_builds
                                        WHERE route = 'remote'
                                          AND status IN ('submitting', 'queued', 'running')
                                    ) AS tracked
                                    GROUP BY status
                                    ORDER BY status
                                    """,
                                    (REMOTE_BUILD_TRACKING_RECENT_SECONDS,),
                                ).fetchall()
                            )
                        remote_build_tracking_available = True
                    except (psycopg.errors.UndefinedColumn, psycopg.errors.UndefinedTable):
                        remote_build_rows = []
                swr_push_sync_row: dict[str, Any] | None = None
                swr_push_sync_out_of_sync_rows: list[dict[str, Any]] = []
                swr_push_sync_available = False
                pushed_images_relation = connection.execute(
                    "SELECT to_regclass('public.pushed_images') AS relation"
                ).fetchone()
                if pushed_images_relation and pushed_images_relation["relation"] is not None:
                    try:
                        with connection.transaction():
                            swr_push_sync_row = connection.execute(
                                """
                                WITH reg AS (
                                    SELECT instance,
                                        CASE
                                            WHEN swr_url LIKE '%data-platform%' THEN 'platform'
                                            WHEN swr_url LIKE '%data-trajectory%' THEN 'trajectory'
                                        END AS registry,
                                        bool_or(pushed) AS pushed
                                    FROM public.pushed_images
                                    WHERE swr_url LIKE '%data-platform%'
                                       OR swr_url LIKE '%data-trajectory%'
                                    -- positional refs: `registry` is a CASE
                                    -- expression, so it can't be named in
                                    -- GROUP BY directly (GroupingError).
                                    GROUP BY 1, 2
                                ),
                                piv AS (
                                    SELECT instance,
                                        bool_or(registry = 'platform' AND pushed) AS on_platform,
                                        bool_or(registry = 'trajectory' AND pushed) AS on_trajectory
                                    FROM reg GROUP BY instance
                                )
                                SELECT
                                    count(*) FILTER (WHERE on_platform) AS platform_count,
                                    count(*) FILTER (WHERE on_trajectory) AS trajectory_count,
                                    count(*) FILTER (WHERE on_platform AND on_trajectory)
                                        AS in_sync,
                                    count(*) FILTER (WHERE on_platform AND NOT on_trajectory)
                                        AS platform_only,
                                    count(*) FILTER (WHERE on_trajectory AND NOT on_platform)
                                        AS trajectory_only
                                FROM piv
                                """
                            ).fetchone()
                            swr_push_sync_out_of_sync_rows = list(
                                connection.execute(
                                    # This query is parameterized (LIMIT %s), so
                                    # literal % in the LIKE patterns must be
                                    # doubled to %% or psycopg reads '%d' as a
                                    # placeholder and raises ProgrammingError.
                                    """
                                    WITH reg AS (
                                        SELECT instance,
                                            CASE
                                                WHEN swr_url LIKE '%%data-platform%%'
                                                    THEN 'platform'
                                                WHEN swr_url LIKE '%%data-trajectory%%'
                                                    THEN 'trajectory'
                                            END AS registry,
                                            bool_or(pushed) AS pushed
                                        FROM public.pushed_images
                                        WHERE swr_url LIKE '%%data-platform%%'
                                           OR swr_url LIKE '%%data-trajectory%%'
                                        -- positional refs: `registry` is a CASE
                                        -- expression, so it can't be named in
                                        -- GROUP BY directly (GroupingError).
                                        GROUP BY 1, 2
                                    ),
                                    piv AS (
                                        SELECT instance,
                                            bool_or(registry = 'platform' AND pushed)
                                                AS on_platform,
                                            bool_or(registry = 'trajectory' AND pushed)
                                                AS on_trajectory
                                        FROM reg GROUP BY instance
                                    )
                                    SELECT instance, on_platform, on_trajectory
                                    FROM piv
                                    WHERE on_platform <> on_trajectory
                                    ORDER BY instance
                                    LIMIT %s
                                    """,
                                    (SWR_PUSH_SYNC_LIST_LIMIT,),
                                ).fetchall()
                            )
                        swr_push_sync_available = True
                    except (psycopg.errors.UndefinedColumn, psycopg.errors.UndefinedTable):
                        swr_push_sync_row = None
                        swr_push_sync_out_of_sync_rows = []
                # Dynamic generate-endpoint registry (read-only). The auth_token
                # column is deliberately NOT selected: it must never leave the DB
                # through the dashboard snapshot. A missing table (schema not yet
                # migrated) degrades to an empty, unavailable section.
                generate_endpoint_rows: list[dict[str, Any]] = []
                generate_endpoints_available = False
                try:
                    generate_endpoint_rows = list(
                        connection.execute(
                            """
                            SELECT slug, base_url, model_id, concurrency, enabled,
                                   breaker_open, breaker_reason, tripped_at, reset_at,
                                   last_probe_status, last_probe_at, consecutive_fail
                            FROM generate_endpoints
                            ORDER BY model_id, slug
                            """
                        ).fetchall()
                    )
                    generate_endpoints_available = True
                except (psycopg.errors.UndefinedColumn, psycopg.errors.UndefinedTable):
                    generate_endpoint_rows = []
        # Denominator lives in a separate database (mindforge) on the same host;
        # fetch it on its own connection, after the pipeline transaction closes,
        # so a mindforge outage cannot fail the pipeline read.
        instance_universe_total = _fetch_instance_universe_total()
        # Resolve which model_id each generate deployment runs. This is a tiny
        # map that changes only when a generate deployment is added/rolled, so
        # it is refreshed once per collect alongside the DB read. A kubectl
        # failure degrades to an empty map: the chart then labels each series by
        # its derived deployment name instead of the model_id.
        generate_deployment_to_model = self._resolve_generate_models()
        return aggregate_pipeline_snapshot(
            tasks,
            results,
            activity,
            queues,
            timeseries_lookback_hours=lookback_hours,
            activity_count_rows=activity_counts,
            generate_model_bucket_rows=generate_model_buckets,
            generate_deployment_to_model=generate_deployment_to_model,
            downstream_model_bucket_rows=downstream_model_buckets,
            generate_task_model_rows=generate_task_models,
            hourly_yield_rows=hourly_yields,
            lifetime_stage_rows=lifetime_stages,
            unique_instance_rows=unique_instances,
            instance_universe_total=instance_universe_total,
            remote_build_rows=remote_build_rows,
            remote_build_tracking_available=remote_build_tracking_available,
            swr_push_sync_row=swr_push_sync_row,
            swr_push_sync_out_of_sync_rows=swr_push_sync_out_of_sync_rows,
            swr_push_sync_available=swr_push_sync_available,
            generate_endpoint_rows=generate_endpoint_rows,
            # Live per-endpoint running-pod counts come from the k3s collector
            # (postgres has no pod visibility); the UI joins them onto this list
            # by slug. The registry section itself carries a running:0 placeholder.
            generate_endpoint_running_by_slug=None,
            generate_endpoints_available=generate_endpoints_available,
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
        configured_url = (
            base_url
            or os.environ.get(REMOTE_BUILDKIT_URL_ENV, "")
            or os.environ.get("SWEGEN_BUILDKIT_FARM_URL", "")
        ).strip()
        self.base_url = configured_url.rstrip("/")
        # Unconfigured is a reportable state, not a crash: every endpoint stays
        # in its initial "never sampled" shape and _refresh() is skipped, so the
        # panel renders the farm as unavailable with the variable named.
        self.configured = bool(self.base_url)
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
        if not self.configured:
            for endpoint in self._endpoints.values():
                endpoint["error"] = REMOTE_BUILDKIT_URL_UNCONFIGURED

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
        if not self.configured:
            # Nothing to poll: report the unconfigured state instead of
            # fabricating requests against a guessed address.
            with self._lock:
                return self._snapshot_locked()
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
        self._last_build_slot_metrics: dict[str, dict[str, Any]] = {}

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
        # Deployments and Pods are fetched separately so the Pod query can carry
        # a field selector. Evicted Pods linger as Failed records — a node under
        # disk pressure accumulates them by the thousand — and including them
        # grew this response past 140 MB, which blew the timeout above and took
        # the whole k3s panel down. A combined "deployments,pods" query cannot
        # be filtered: the selector applies to every type, and Deployments have
        # no status.phase, so they are silently dropped from the result.
        deployment_doc = self._get(["-n", self.namespace, "get", "deployments"])
        pod_doc = self._get(
            [
                "-n",
                self.namespace,
                "get",
                "pods",
                f"--field-selector=status.phase!={_EVICTED_POD_PHASE}",
            ]
        )
        workload_doc = {"items": [*deployment_doc.get("items", []), *pod_doc.get("items", [])]}
        allocation_error = None
        try:
            all_pod_doc = self._get(
                [
                    "get",
                    "pods",
                    "-A",
                    f"--field-selector=status.phase!={_EVICTED_POD_PHASE}",
                ]
            )
        except Exception as error:
            all_pod_doc = None
            allocation_error = f"{type(error).__name__}: {str(error)[:300]}"
        nodes = []
        for item in node_doc.get("items", []):
            conditions = {c["type"]: c for c in item.get("status", {}).get("conditions", [])}
            ready = conditions.get("Ready", {}).get("status") == "True"
            allocatable_millicores = _cpu_millicores(
                str(item.get("status", {}).get("allocatable", {}).get("cpu", "0"))
            )
            nodes.append(
                {
                    "name": item["metadata"]["name"],
                    "ready": ready,
                    "build_slot_max": max(1, allocatable_millicores // 1_000),
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
                "pod_phases": {},
                "evicted": 0,
                "restarts": 0,
                "nodes": {},
            }
            for stage in STAGES
        }
        pods_by_node: dict[str, list[dict[str, Any]]] = {}
        slot_probe_pod_by_node: dict[str, str] = {}
        storage_mounts: list[dict[str, Any]] = []
        # Live running-pod count per dynamic generate endpoint. Attributed by the
        # controller-set pod label ``swegen.pgcode/endpoint=<slug>`` (falling back
        # to the ``swegen-generate-dyn-<slug>`` deployment name derived from the
        # pod name). The dashboard joins this onto the registry section by slug.
        generate_endpoint_pods: Counter[str] = Counter()
        for item in workload_doc.get("items", []):
            if item.get("kind") == "Pod":
                status = item.get("status", {})
                labels = item.get("metadata", {}).get("labels", {}) or {}
                endpoint_slug = labels.get("swegen.pgcode/endpoint")
                if not endpoint_slug:
                    owner = deployment_name_from_worker_id(
                        item.get("metadata", {}).get("name")
                    )
                    if owner and owner.startswith(GENERATE_ENDPOINT_DEPLOYMENT_PREFIX):
                        endpoint_slug = owner[len(GENERATE_ENDPOINT_DEPLOYMENT_PREFIX):]
                if endpoint_slug and pod_display_phase(item) == "Running":
                    generate_endpoint_pods[endpoint_slug] += 1
                node = item.get("spec", {}).get("nodeName") or "unscheduled"
                pod_name = item.get("metadata", {}).get("name", "unknown")
                has_build_slot_mount = any(
                    mount.get("mountPath") == "/run/swegen-build-slots"
                    for container in item.get("spec", {}).get("containers", [])
                    for mount in container.get("volumeMounts", [])
                )
                if (
                    node != "unscheduled"
                    and status.get("phase") == "Running"
                    and pod_name != "unknown"
                    and has_build_slot_mount
                ):
                    is_pruner = (
                        item.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/name")
                        == "swegen-buildkit-pruner"
                    )
                    if node not in slot_probe_pod_by_node or is_pruner:
                        slot_probe_pod_by_node[node] = pod_name
            stage = item.get("metadata", {}).get("labels", {}).get("swegen.pgcode/stage")
            if item.get("kind") == "Deployment" and stage is None:
                stage = (
                    item.get("spec", {})
                    .get("selector", {})
                    .get("matchLabels", {})
                    .get("swegen.pgcode/stage")
                )
            stage = STAGE_ALIASES.get(stage, stage)
            if stage not in stages:
                continue
            if item.get("kind") == "Deployment":
                spec = item.get("spec", {})
                stages[stage]["desired"] += int(spec.get("replicas") or 0)
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
                phase = pod_display_phase(item)
                if phase == "Evicted":
                    # Eviction records survive their node indefinitely and reach five figures;
                    # they are counted once and kept out of every other pod tally.
                    stages[stage]["evicted"] += 1
                    continue
                phases = stages[stage]["pod_phases"]
                phases[phase] = phases.get(phase, 0) + 1
                container_statuses = status.get("containerStatuses", [])
                pod_ready = status.get("phase") == "Running" and all(
                    entry.get("ready") for entry in container_statuses
                )
                stages[stage]["restarts"] += sum(
                    int(entry.get("restartCount") or 0) for entry in container_statuses
                )
                node = item.get("spec", {}).get("nodeName") or "unscheduled"
                stages[stage]["nodes"][node] = stages[stage]["nodes"].get(node, 0) + 1
                if node != "unscheduled":
                    pod_name = item.get("metadata", {}).get("name", "unknown")
                    pods_by_node.setdefault(node, []).append(
                        {
                            "name": pod_name,
                            "stage": stage,
                            "phase": phase,
                            "ready": bool(container_statuses) and pod_ready,
                            "restarts": sum(
                                int(entry.get("restartCount") or 0) for entry in container_statuses
                            ),
                        }
                    )
        for stage_view in stages.values():
            phases = stage_view["pod_phases"]
            stage_view["pod_phases"] = {
                phase: phases[phase] for phase in sorted(phases, key=_pod_phase_sort_key)
            }
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
            node["build_slot_probe_pod"] = slot_probe_pod_by_node.get(node["name"])
        build_slots_by_node = self._collect_build_slot_metrics(slot_probe_pod_by_node)
        for node in nodes:
            total = build_slots_by_node.get(node["name"], {}).get("total")
            if isinstance(total, int):
                node["build_slot_max"] = max(node["build_slot_max"], total)
        resource_metrics = self._collect_resource_metrics(
            node_doc,
            all_pod_doc=all_pod_doc,
            allocation_error=allocation_error,
            build_slots_by_node=build_slots_by_node,
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
            "generate_endpoint_pods": dict(sorted(generate_endpoint_pods.items())),
        }

    def _collect_resource_metrics(
        self,
        node_doc: dict[str, Any],
        *,
        all_pod_doc: dict[str, Any] | None,
        allocation_error: str | None,
        build_slots_by_node: dict[str, dict[str, Any]],
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
                        "build_slots": build_slots_by_node.get(
                            name,
                            {
                                "available": False,
                                "used": None,
                                "total": None,
                                "free": None,
                                "utilization_percent": None,
                                "waiters": None,
                                "waiters_source": None,
                                "error": "node-local slot state unavailable",
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

    def _collect_build_slot_metrics(
        self,
        probe_pods: dict[str, str],
    ) -> dict[str, dict[str, Any]]:
        probe = """import fcntl,json,os,pathlib
d=pathlib.Path('/run/swegen-build-slots')
n=int((d/'count').read_text().strip())
used=0
for i in range(n):
 fd=os.open(d/str(i),os.O_RDWR)
 try:
  fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
  fcntl.flock(fd,fcntl.LOCK_UN)
 except BlockingIOError:
  used+=1
waiters=None
source=None
for name in ('waiters','waiting','queue_depth','queue'):
 path=d/name
 if path.is_file():
  try:
   waiters=max(0,int(path.read_text().strip()))
   source=name
   break
  except (OSError,ValueError):
   pass
print(json.dumps({'total':n,'used':used,'free':n-used,'waiters':waiters,'waiters_source':source}))"""
        sampled_at = datetime.now(UTC).isoformat()
        result: dict[str, dict[str, Any]] = {}
        for node, pod in probe_pods.items():
            command = [
                "kubectl",
                "--request-timeout=8s",
                "-n",
                self.namespace,
                "exec",
                pod,
                "--",
                "python",
                "-c",
                probe,
            ]
            try:
                completed = self.runner(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
            except (OSError, subprocess.SubprocessError) as error:
                if isinstance(error, subprocess.TimeoutExpired):
                    message = "BuildKit slot probe timed out after 10 seconds"
                else:
                    message = f"{type(error).__name__}: {str(error)[:240]}"
                previous = dict(self._last_build_slot_metrics.get(node, {}))
                if previous:
                    previous.update(stale=True, error=message)
                    result[node] = previous
                else:
                    result[node] = {
                        "available": False,
                        "used": None,
                        "total": None,
                        "free": None,
                        "utilization_percent": None,
                        "waiters": None,
                        "waiters_source": None,
                        "sampled_at": None,
                        "stale": False,
                        "error": message,
                    }
                continue
            if completed.returncode != 0:
                previous = dict(self._last_build_slot_metrics.get(node, {}))
                if previous:
                    previous.update(
                        stale=True,
                        error=(completed.stderr or "slot probe failed")[:300],
                    )
                    result[node] = previous
                else:
                    result[node] = {
                        "available": False,
                        "used": None,
                        "total": None,
                        "free": None,
                        "utilization_percent": None,
                        "waiters": None,
                        "waiters_source": None,
                        "sampled_at": None,
                        "stale": False,
                        "error": (completed.stderr or "slot probe failed")[:300],
                    }
                continue
            try:
                payload = json.loads(completed.stdout)
                total = _optional_nonnegative_int(payload.get("total"))
                used = _optional_nonnegative_int(payload.get("used"))
                free = _optional_nonnegative_int(payload.get("free"))
                if total is None or total <= 0 or used is None or used > total:
                    raise ValueError("invalid slot probe counts")
                waiters = _optional_nonnegative_int(payload.get("waiters"))
                waiters_source = payload.get("waiters_source")
                result[node] = {
                    "available": True,
                    "used": used,
                    "total": total,
                    "free": free if free is not None else total - used,
                    "utilization_percent": round(used * 100 / total, 1),
                    "waiters": waiters,
                    "waiters_source": (
                        waiters_source if isinstance(waiters_source, str) else None
                    ),
                    "sampled_at": sampled_at,
                    "stale": False,
                    "error": None,
                }
            except (AttributeError, json.JSONDecodeError, TypeError, ValueError) as error:
                result[node] = {
                    "available": False,
                    "used": None,
                    "total": None,
                    "free": None,
                    "utilization_percent": None,
                    "waiters": None,
                    "waiters_source": None,
                    "sampled_at": sampled_at,
                    "stale": False,
                    "error": f"{type(error).__name__}: {str(error)[:240]}",
                }
        self._last_build_slot_metrics = {
            node: metrics for node, metrics in result.items() if metrics.get("available")
        }
        return result

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
