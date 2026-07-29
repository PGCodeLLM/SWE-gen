"""Live PostgreSQL/PGMQ and k3s telemetry for the distributed pipeline."""

from __future__ import annotations

import json
import os
import subprocess
from collections import Counter
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

STAGES = ("generate", "validate", "reward", "push")
QUEUE_BY_STAGE = {stage: f"swegen_{stage}" for stage in STAGES}
DEAD_QUEUE = "swegen_dead"


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

    def queue_view(name: str) -> dict[str, Any]:
        row = queue_map.get(name, {})
        length = int(row.get("queue_length") or 0)
        visible = int(row.get("queue_visible_length") or 0)
        return {
            "queue": name,
            "length": length,
            "visible": visible,
            "in_flight": max(0, length - visible),
            "total_messages": int(row.get("total_messages") or 0),
            "newest_message_age_seconds": row.get("newest_msg_age_sec"),
            "oldest_message_age_seconds": row.get("oldest_msg_age_sec"),
            "scraped_at": _iso(row.get("scrape_time")),
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
    return {
        "generated_at": now.isoformat(),
        "queues": {
            "stages": {stage: queue_view(name) for stage, name in QUEUE_BY_STAGE.items()},
            "dead": queue_view(DEAD_QUEUE),
        },
        "task_counts": {
            "total": len(tasks),
            "by_state": dict(sorted(by_state.items())),
            "by_stage": dict(sorted(by_stage.items())),
        },
        "throughput": {"windows": throughput},
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
        return aggregate_pipeline_snapshot(
            tasks,
            results,
            activity,
            queues,
            time_bucket_rows=time_buckets,
            hourly_yield_rows=hourly_yields,
        )


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
        resource_metrics = self._collect_resource_metrics(node_doc)
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

    def _collect_resource_metrics(self, node_doc: dict[str, Any]) -> dict[str, Any]:
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

            per_node = []
            missing = []
            total_cpu_used = 0
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
                if usage is None:
                    missing.append(name)
                    cpu_used = None
                    memory_used = None
                else:
                    cpu_used, memory_used = usage
                    total_cpu_used += cpu_used
                    total_memory_used += memory_used
                total_cpu_allocatable += cpu_allocatable
                total_memory_allocatable += memory_allocatable
                per_node.append(
                    {
                        "name": name,
                        "ip": internal_ip,
                        "available": usage is not None,
                        "cpu_used_millicores": cpu_used,
                        "cpu_allocatable_millicores": cpu_allocatable,
                        "cpu_percent": (
                            round(cpu_used * 100 / cpu_allocatable, 1)
                            if cpu_used is not None and cpu_allocatable
                            else None
                        ),
                        "memory_used_bytes": memory_used,
                        "memory_allocatable_bytes": memory_allocatable,
                        "memory_percent": (
                            round(memory_used * 100 / memory_allocatable, 1)
                            if memory_used is not None and memory_allocatable
                            else None
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
                "collected_at": datetime.now(UTC).isoformat(),
                "aggregate": {
                    "cpu_used_millicores": total_cpu_used if all_available else None,
                    "cpu_allocatable_millicores": total_cpu_allocatable,
                    "cpu_percent": (
                        round(total_cpu_used * 100 / total_cpu_allocatable, 1)
                        if all_available and total_cpu_allocatable
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
                "collected_at": None,
                "aggregate": {},
                "nodes": [],
            }
