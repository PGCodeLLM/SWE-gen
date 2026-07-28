#!/usr/bin/env python3
"""Live WebUI for orchestrator outcomes and explicit validation evidence."""

from __future__ import annotations

import argparse
import copy
import fcntl
import hmac
import io
import json
import os
import re
import secrets
import signal
import subprocess
import tarfile
import tempfile
import threading
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

from swegen.ledger_repo import LedgerRepo

CREATE_INSTANCE_RE = re.compile(r"--repo\x00([^\x00]+)\x00--pr\x00([^\x00]+)")
STATUS_JOURNAL_GLOB = "orchestrator-instance-status*.jsonl"
BACKUP_MARKER = ".before-"
SUCCESS_LEDGER_NAME = "create.jsonl"
POSTCHECK_LEDGER_PREFIX = "postcheck-status"
POSTCHECK_WORKER_STATUS = Path(".validation-worker/worker-status.json")
REWARD_BACKFILL_LEDGER = Path(".validation-worker/reward-backfill-status.jsonl")
REWARD_BACKFILL_WORKER_STATUS = Path(".validation-worker/reward-backfill-worker-status.json")
HISTORICAL_ORACLE_ZERO_LEDGER = Path(".validation-worker/historical-oracle-zero-evidence.jsonl")
REWARD_BACKFILL_DEFAULT_CONCURRENCY = 20
POSTCHECK_STATUS_STALE_SECONDS = 300
REWARD_BACKFILL_STATUS_STALE_SECONDS = 300
SLURM_HEALTH_STALE_SECONDS = 180
SLURM_PLAN_NAME = "slurm-plan.json"
SLURM_HEALTH_NAME = "slurm-health.json"
SLURM_NODE_DIR_NAMES = {"slurm-nodes", "slurm_nodes"}
NODE_BASELINE_WORKER_HOSTS = ("7.244.2.110", "7.244.1.209")
NODE_BASELINE_WORKER_STATUS_GLOB = "/data/local-swegen-scratch/.validation-worker-shard-*/worker-status.json"
NODE_BASELINE_WORKER_STATUS_TTL_SECONDS = 10
LOCAL_BASELINE_WORKER_STATUS_GLOB = "/data/local-swegen-scratch/.validation-worker-shard-*/worker-status.json"
TASK_EXPORT_DIR_NAME = "export"
TASK_EXPORT_SOURCE_DIR_NAME = "sources"
TASK_EXPORT_PACK_DIR_NAME = "task-packs"
TASK_EXPORT_PACK_PREFIX = "reward-hack-accepted-"
TASK_EXPORT_PACK_SUFFIX = ".tar.gz"
TASK_EXPORT_KEEP = 5
TASK_EXPORT_SYNC_INTERVAL_SECONDS = 120
TASK_EXPORT_SYNC_TIMEOUT_SECONDS = 7200
TASK_EXPORT_COMPONENTS = ("tasks", "tasks_voyager_postprocessed")
TASK_EXPORT_REMOTE_USER = "alex"
DEFAULT_CONTROL_CONFIG_PATH = Path("/data/work/alex/SWE-gen/swegen-config.yaml")
DEFAULT_CONTROL_TOKEN_PATH = Path("/data/work/alex/SWE-gen/.swegen-control-token")
CONTROL_CONFIG_VERSION = 1
CONTROL_ROUTE_NAMES = ("sg", "hk", "de")
CONTROL_ROUTE_MAX_CONCURRENCY = 8
CONTROL_REQUEST_MAX_BYTES = 64 * 1024
CONTROL_MODEL_ROLES = ("opus", "sonnet")
DEFAULT_CONTROL_MODELS = {
    "opus": "gpt-5.6-sol",
    "sonnet": "gpt-5.6-terra",
}
STAGE_I_THROUGHPUT_BUCKET_MINUTES = 15
STAGE_I_THROUGHPUT_BUCKET_COUNT = 24
DISCOVERY_PRUNE_DIRS = {
    ".cache",
    ".git",
    ".swegen",
    "__pycache__",
    "cache",
    "caches",
    "data_cache",
    "harbor-jobs",
    "node_modules",
    "postprocessed-output",
    "repo-cache",
    "repo_cache",
    "repos",
    "tasks",
}
NODE_FIELDS = (
    "slurm_node",
    "slurm_node_name",
    "slurmd_nodename",
    "node",
    "node_name",
    "hostname",
    "host",
)
VALIDATION_EVIDENCE_FIELDS = (
    "validation",
    "validation_status",
    "nop_reward",
    "oracle_reward",
    "nop_passed",
    "oracle_passed",
    "postcheck_status",
    "reward_hack_status",
    "reward_hack_is_hacking",
    "reward_hack_reason",
    "reward_hack_error",
)
BASELINE_VALID_STATUSES = {
    "baseline_valid",
    "nop_oracle_passed",
    "nop=0_oracle=1",
}
UNVALIDATED_STATUSES = {
    "unvalidated",
    "not_validated",
    "not_run",
    "skipped",
    "validation_skipped",
}
VALIDATION_FAILED_STATUSES = {
    "failed",
    "validation_failed",
    "baseline_failed",
}
VALIDATION_FAILURE_MARKERS = (
    "validation failed",
    "harbor validation failed",
    "nop or oracle",
    "nop failed",
    "oracle failed",
)


def _is_backup_component(name: str) -> bool:
    lower = name.lower()
    return (
        BACKUP_MARKER in lower
        or lower in {"backup", "backups", "snapshot", "snapshots"}
        or lower.startswith(("backup-", "snapshot-"))
        or lower.endswith(("-backup", "-snapshot"))
    )


def _discover_run_files(run_dir: Path, filename_matches: Any) -> list[Path]:
    """Recursively find live run artifacts without entering output/cache trees."""
    paths: list[Path] = []
    if not run_dir.is_dir():
        return paths

    for root, dirnames, filenames in os.walk(run_dir):
        dirnames[:] = sorted(
            dirname
            for dirname in dirnames
            if dirname.lower() not in DISCOVERY_PRUNE_DIRS and not _is_backup_component(dirname)
        )
        root_path = Path(root)
        for filename in filenames:
            if _is_backup_component(filename) or not filename_matches(filename):
                continue
            paths.append(root_path / filename)
    return sorted(paths)


def status_journal_paths(run_dir: Path) -> list[Path]:
    """Return live local and Slurm status journals under the run directory."""
    return _discover_run_files(
        run_dir,
        lambda name: name.startswith("orchestrator-instance-status") and name.endswith(".jsonl"),
    )


def success_ledger_paths(run_dir: Path) -> list[Path]:
    """Return authoritative success ledgers from local and Slurm node state."""
    return _discover_run_files(run_dir, lambda name: name == SUCCESS_LEDGER_NAME)


def postcheck_journal_paths(run_dir: Path) -> list[Path]:
    """Return live append-only standalone validation-worker journals."""
    return _discover_run_files(
        run_dir,
        lambda name: name.startswith(POSTCHECK_LEDGER_PREFIX) and name.endswith(".jsonl"),
    )


# Name of the symlink that exposes the shared, synchronized postcheck ledger
# (-> /data/nfs_shared/swegen/postcheck-status.jsonl) inside the run dir.
SHARED_POSTCHECK_LEDGER_NAME = "postcheck-status-shared.jsonl"


def authoritative_postcheck_paths(run_dir: Path) -> list[Path]:
    """Postcheck journal paths ordered so the shared ledger is authoritative.

    ``load_latest_postchecks`` keeps the newest record per instance and breaks
    timestamp/attempt ties by path order (last wins). The shared ledger holds
    the synchronized terminal verdicts (accepted/rejected/blacklisted) that the
    one-shot merge lands; placing it last makes those verdicts win ties over the
    scattered, partly-stale per-worker journals, so the dashboard and the
    reward-hack export pack both reflect a single source of truth.
    """
    paths = postcheck_journal_paths(run_dir)
    shared = [p for p in paths if p.name == SHARED_POSTCHECK_LEDGER_NAME]
    worker = [p for p in paths if p.name != SHARED_POSTCHECK_LEDGER_NAME]
    return [*worker, *shared]


def shared_postcheck_ledger_path(run_dir: Path) -> Path | None:
    """Resolve the shared-ledger symlink to its real NFS path, if present."""
    for path in postcheck_journal_paths(run_dir):
        if path.name == SHARED_POSTCHECK_LEDGER_NAME:
            try:
                return path.resolve()
            except OSError:
                return path
    return None


def load_authoritative_postchecks(run_dir: Path) -> dict[str, dict[str, Any]]:
    """Newest postcheck record per instance from the authoritative source.

    With the Postgres backend all per-worker journals and the shared merged
    ledger are one table (postcheck_status), so this is a single
    LedgerRepo.load_latest() query — the old multi-path "shared wins ties"
    ordering is preserved because later writes (including merged verdicts) have
    higher ids and win the DISTINCT ON tiebreak. In jsonl mode it falls back
    to the original multi-path load_latest_postchecks over all journals.
    """
    paths = authoritative_postcheck_paths(run_dir)
    shared = run_dir / SHARED_POSTCHECK_LEDGER_NAME
    repo = LedgerRepo(shared)
    if repo.backend == "jsonl":
        # Original semantics: merge all journal paths, shared-ledger wins ties.
        if not paths:
            return {}
        return load_latest_postchecks(paths)
    # Postgres: one table for every journal + the shared ledger.
    return repo.load_latest()


def _string_value(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _node_from_record(record: dict[str, Any]) -> tuple[str | None, bool]:
    for field in NODE_FIELDS:
        node = _string_value(record.get(field))
        if node:
            return node, field.startswith("slurm")

    slurm = record.get("slurm")
    if isinstance(slurm, dict):
        for field in NODE_FIELDS:
            node = _string_value(slurm.get(field))
            if node:
                return node, True
    return None, False


def _node_from_path(path: Path, run_dir: Path | None) -> tuple[str | None, bool]:
    if run_dir is None:
        return None, False
    try:
        parts = path.relative_to(run_dir).parts
    except ValueError:
        return None, False

    for index, part in enumerate(parts[:-1]):
        if part.lower() in SLURM_NODE_DIR_NAMES and index + 1 < len(parts) - 1:
            return parts[index + 1], True
    # The legacy Slurm launcher stored one directory per node beneath this
    # exact folder. Do not interpret similarly named flat proxy log folders as
    # machines.
    for index, part in enumerate(parts[:-1]):
        if part == "orchestrator-logs" and index + 1 < len(parts) - 1:
            return parts[index + 1], True
    return None, False


def _enrich_node_identity(
    record: dict[str, Any], source_path: Path, run_dir: Path | None
) -> dict[str, Any]:
    enriched = dict(record)
    node, is_slurm = _node_from_record(enriched)
    if node is None:
        node, is_slurm = _node_from_path(source_path, run_dir)
    if node is not None:
        enriched["node"] = node
        if is_slurm:
            enriched["node_scope"] = "slurm"
    return enriched


def _copy_node_identity(target: dict[str, Any], source: dict[str, Any]) -> None:
    if not _string_value(target.get("node")) and _string_value(source.get("node")):
        target["node"] = source["node"]
    if target.get("node_scope") != "slurm" and source.get("node_scope") == "slurm":
        target["node_scope"] = "slurm"


def _copy_validation_evidence(target: dict[str, Any], source: dict[str, Any]) -> None:
    for field in VALIDATION_EVIDENCE_FIELDS:
        if field not in target and field in source:
            target[field] = source[field]


def load_latest_statuses(
    status_paths: Path | Iterable[Path],
    *,
    run_dir: Path | None = None,
    include_unprocessed: bool = False,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Return the latest status per instance across all journals.

    An append-only ``unprocessed`` record is a tombstone for an earlier
    success/failure journal entry. A later retry result supersedes the
    tombstone normally by timestamp, so operational resets do not destroy the
    original evidence or require in-place JSONL rewrites. Tombstones are
    omitted by default and retained for callers such as the throughput chart.
    """
    latest: dict[str, dict[str, Any]] = {}
    sequence: dict[str, tuple[str, int, int]] = {}
    paths = [status_paths] if isinstance(status_paths, Path) else list(status_paths)

    for path_index, status_path in enumerate(paths):
        if not status_path.exists():
            continue
        try:
            fh = status_path.open(encoding="utf-8", errors="replace")
        except OSError:
            continue
        with fh:
            for index, line in enumerate(fh):
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(record, dict):
                    continue
                instance = record.get("instance")
                status = record.get("status")
                if not isinstance(instance, str) or status not in {
                    "success",
                    "failure",
                    "unprocessed",
                }:
                    continue
                timestamp = record.get("timestamp")
                key = (
                    timestamp if isinstance(timestamp, str) else "",
                    path_index,
                    index,
                )
                if instance not in sequence or key >= sequence[instance]:
                    latest[instance] = _enrich_node_identity(record, status_path, run_dir)
                    sequence[instance] = key

    visible = (
        latest
        if include_unprocessed
        else {
            instance: record
            for instance, record in latest.items()
            if record.get("status") != "unprocessed"
        }
    )
    order = sorted(visible, key=sequence.__getitem__, reverse=True)
    return visible, order


def load_success_ledger(
    create_path: Path, *, run_dir: Path | None = None
) -> dict[str, dict[str, Any]]:
    """Load the run's authoritative successful-instance ledger."""
    successes: dict[str, dict[str, Any]] = {}
    repo = LedgerRepo(create_path)
    # Latest record per task_id (mirrors the old (ts, line_index) dedup).
    latest = repo.load_latest() if repo.backend == "postgres" else None
    if latest is None:
        # jsonl fallback: original file scan.
        if not create_path.exists():
            return successes
        try:
            fh = create_path.open(encoding="utf-8", errors="replace")
        except OSError:
            return successes
        records: list[dict[str, Any]] = []
        with fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(rec, dict):
                    records.append(rec)
    else:
        records = list(latest.values())

    for record in records:
        instance = record.get("task_id")
        if not isinstance(instance, str) or not instance:
            harbor = record.get("harbor")
            instance = Path(harbor).name if isinstance(harbor, str) else ""
        if not instance:
            continue
        timestamp = record.get("ts")
        if not isinstance(timestamp, str):
            timestamp = ""
        success_record = {
            "instance": instance,
            "status": "success",
            "timestamp": timestamp,
            "worker_id": None,
        }
        _copy_validation_evidence(success_record, record)
        successes[instance] = _enrich_node_identity(success_record, create_path, run_dir)
    return successes


def load_latest_postchecks(
    journal_paths: Path | Iterable[Path],
) -> dict[str, dict[str, Any]]:
    """Load the newest standalone post-check snapshot for every instance."""
    latest: dict[str, dict[str, Any]] = {}
    sequence: dict[str, tuple[int, str, int, int]] = {}
    paths = [journal_paths] if isinstance(journal_paths, Path) else list(journal_paths)
    for path_index, path in enumerate(paths):
        if not path.is_file():
            continue
        try:
            stream = path.open(encoding="utf-8", errors="replace")
        except OSError:
            continue
        with stream:
            for line_index, line in enumerate(stream):
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(record, dict):
                    continue
                instance = record.get("instance")
                if not isinstance(instance, str) or not instance:
                    continue
                attempt = record.get("attempt")
                attempt_number = attempt if isinstance(attempt, int) and attempt >= 1 else 1
                timestamp = record.get("timestamp")
                key = (
                    attempt_number,
                    timestamp if isinstance(timestamp, str) else "",
                    path_index,
                    line_index,
                )
                if instance not in sequence or key >= sequence[instance]:
                    latest[instance] = record
                    sequence[instance] = key
    return latest


def load_historical_oracle_zero_instances(run_dir: Path) -> set[str]:
    """Load exact Oracle=0 evidence recovered from pre-import Harbor jobs.

    The ledger is deliberately separate from the live post-check journal.  It
    records historical generation attempts, while current exact NOP/Oracle
    evidence remains authoritative when an instance later reaches Stage I
    green.
    """
    path = run_dir / HISTORICAL_ORACLE_ZERO_LEDGER
    instances: set[str] = set()
    try:
        stream = path.open(encoding="utf-8", errors="replace")
    except OSError:
        return instances
    with stream:
        for line in stream:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            instance = record.get("instance")
            reward = record.get("oracle_reward")
            if isinstance(instance, str) and instance and _reward_matches(reward, 0):
                instances.add(instance)
    return instances


def load_postcheck_worker_status(
    run_dir: Path,
    *,
    now: datetime | None = None,
    stale_after_seconds: int = POSTCHECK_STATUS_STALE_SECONDS,
) -> dict[str, Any]:
    path = run_dir / POSTCHECK_WORKER_STATUS
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}

    status = {
        key: data.get(key)
        for key in (
            "state",
            "timestamp",
            "mode",
            "worker_id",
            "checker_node",
            "poll_interval_seconds",
            "current_instance",
            "current_stage",
            "baseline_concurrency",
            "baseline_active_count",
            "current_instances",
            "active_stages",
            "reward_model",
            "reward_fallback_model",
            "reward_concurrency",
            "reward_active_count",
            "error",
        )
    }
    baseline_concurrency = _worker_count(data.get("baseline_concurrency"))
    baseline_active_count = _worker_count(data.get("baseline_active_count"))
    status["baseline_concurrency"] = baseline_concurrency if baseline_concurrency is not None else 1
    status["baseline_active_count"] = (
        baseline_active_count if baseline_active_count is not None else 0
    )

    current_instances: list[str] = []
    seen_instances: set[str] = set()
    raw_current_instances = data.get("current_instances")
    if isinstance(raw_current_instances, list):
        for value in raw_current_instances:
            instance = _string_value(value)
            if instance and instance not in seen_instances:
                seen_instances.add(instance)
                current_instances.append(instance)
    status["current_instances"] = current_instances

    active_stages: dict[str, str] = {}
    raw_active_stages = data.get("active_stages")
    if isinstance(raw_active_stages, dict):
        for raw_instance, raw_stage in raw_active_stages.items():
            instance = _string_value(raw_instance)
            stage = _string_value(raw_stage)
            if instance and stage:
                active_stages[instance] = stage
    status["active_stages"] = active_stages
    status["stale"] = False
    timestamp = _utc_timestamp(data.get("timestamp"))
    current_time = now or datetime.now(UTC)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=UTC)
    live_state = data.get("state") in {"running", "idle", "draining", "stopping"}
    if live_state and (
        timestamp is None
        or (current_time.astimezone(UTC) - timestamp).total_seconds() > stale_after_seconds
    ):
        status["state"] = "stale"
        status["baseline_active_count"] = 0
        status["current_instances"] = []
        status["active_stages"] = {}
        status["reward_active_count"] = 0
        status["stale"] = True
    return status


_NODE_BASELINE_WORKER_CACHE: dict[str, Any] = {"fetched_at": 0.0, "payload": None}


def _read_node_shard_status(host: str) -> list[dict[str, Any]]:
    """SSH to a Slurm node and read every shard's worker-status.json.

    The baseline verifiers run on the compute nodes (not the NFS-reachable
    controller), so the controller-side ``worker-status.json`` is stale.  We
    SSH in, glob the per-shard status files, and return one parsed record per
    shard.  Any failure returns an empty list so a single unreachable node
    never blanks the whole aggregate.
    """
    cmd = (
        "for f in "
        + NODE_BASELINE_WORKER_STATUS_GLOB
        + "; do [ -f \"$f\" ] && cat \"$f\" && echo; done"
    )
    try:
        proc = subprocess.run(
            [
                "ssh",
                "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "ConnectTimeout=5",
                host,
                cmd,
            ],
            capture_output=True,
            text=True,
            timeout=12,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    if proc.returncode != 0 or not proc.stdout:
        return []
    records: list[dict[str, Any]] = []
    for block in proc.stdout.split("\n}\n"):
        block = block.strip()
        if not block:
            continue
        if not block.endswith("}"):
            block += "}"
        try:
            record = json.loads(block)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            records.append(record)
    return records


def _read_local_shard_status() -> list[dict[str, Any]]:
    """Read the controller's own baseline worker shards directly (no SSH).

    The dashboard process runs on the controller, so when a baseline worker
    runs locally (e.g. ``swegen-baseline-validation.service`` as shard-count=1)
    its status files are on local disk.  This reads them without SSH so the
    local worker shows up live alongside (or instead of) the node shards.
    """
    import glob as _glob

    records: list[dict[str, Any]] = []
    for path in sorted(_glob.glob(LOCAL_BASELINE_WORKER_STATUS_GLOB)):
        try:
            with open(path, encoding="utf-8") as handle:
                record = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(record, dict):
            records.append(record)
    return records


def _normalize_shard_record(
    record: dict[str, Any],
    host: str,
    current_time: datetime,
) -> tuple[dict[str, Any], bool, int, int, str | None]:
    """Normalize one shard worker-status record for the aggregate.

    Returns ``(shard, is_live_running, active, concurrency, error)`` so the
    caller can accumulate totals without duplicating the field math.
    """
    state = _string_value(record.get("state")) or "unknown"
    active = _worker_count(record.get("baseline_active_count")) or 0
    concurrency = _worker_count(record.get("baseline_concurrency")) or 0
    error = _string_value(record.get("error"))
    timestamp = _utc_timestamp(record.get("timestamp"))
    stale = False
    if state in {"running", "idle", "draining", "stopping"} and (
        timestamp is None
        or (current_time - timestamp).total_seconds() > POSTCHECK_STATUS_STALE_SECONDS
    ):
        stale = True
    shard = {
        "host": host,
        "state": "stale" if stale else state,
        "baseline_active_count": active,
        "baseline_concurrency": concurrency,
        "error": error,
        "checker_node": _string_value(record.get("checker_node")),
        "current_instances": list(record.get("current_instances") or [])
        if isinstance(record.get("current_instances"), list)
        else [],
        "counts": record.get("counts") if isinstance(record.get("counts"), dict) else {},
    }
    return shard, (state == "running" and not stale), active, concurrency, error


def load_node_baseline_worker_status(
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Aggregate live baseline worker status across the controller + Slurm nodes.

    The controller's own shards are read from local disk (no SSH); the Slurm
    node shards are read over SSH.  SSH is expensive relative to a 2s dashboard
    tick, so the result is cached for ``NODE_BASELINE_WORKER_STATUS_TTL_SECONDS``.
    Falls back gracefully to a ``not_running`` aggregate when nothing is live.
    """
    import time as _time

    fetched_at = _time.monotonic()
    cached = _NODE_BASELINE_WORKER_CACHE
    if (
        cached["payload"] is not None
        and fetched_at - cached["fetched_at"] < NODE_BASELINE_WORKER_STATUS_TTL_SECONDS
    ):
        return cached["payload"]

    current_time = now or datetime.now(UTC)
    shards: list[dict[str, Any]] = []
    nodes: list[dict[str, Any]] = []
    active_total = 0
    concurrency_total = 0
    any_running = False
    any_error: str | None = None
    counts_total: dict[str, int] = {}

    def accumulate(record: dict[str, Any], host: str) -> dict[str, Any]:
        nonlocal active_total, concurrency_total, any_running, any_error
        shard, is_live, active, concurrency, error = _normalize_shard_record(
            record, host, current_time
        )
        if is_live:
            any_running = True
            active_total += active
        concurrency_total += concurrency
        if error and not any_error:
            any_error = error
        raw_counts = shard["counts"]
        for key, value in raw_counts.items():
            if isinstance(value, (int, float)):
                counts_total[str(key)] = counts_total.get(str(key), 0) + int(value)
        shards.append(shard)
        return shard

    # Controller-local shards (read directly, no SSH).
    local_records = _read_local_shard_status()
    local_shards = [accumulate(r, "controller") for r in local_records]
    if local_records:
        nodes.append({"host": "controller", "reachable": True, "shards": local_shards})

    for host in NODE_BASELINE_WORKER_HOSTS:
        node_records = _read_node_shard_status(host)
        if not node_records:
            nodes.append({"host": host, "reachable": False, "shards": []})
            continue
        node_shards = [accumulate(r, host) for r in node_records]
        nodes.append({"host": host, "reachable": True, "shards": node_shards})

    total_hosts = 1 + len(NODE_BASELINE_WORKER_HOSTS)
    payload = {
        "state": "running" if any_running else "not_running",
        "baseline_active_count": active_total,
        "baseline_concurrency": concurrency_total,
        "error": any_error,
        "shards": shards,
        "nodes": nodes,
        "counts": counts_total,
        "host_count": total_hosts,
        "reachable_count": sum(1 for n in nodes if n["reachable"]),
    }
    cached["fetched_at"] = fetched_at
    cached["payload"] = payload
    return payload


def load_latest_reward_backfills(run_dir: Path) -> dict[str, dict[str, Any]]:
    """Load independent tests-only reward-hack results, if available."""
    return LedgerRepo(run_dir / REWARD_BACKFILL_LEDGER).load_latest()


def _utc_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def build_stage_i_throughput(
    latest: dict[str, dict[str, Any]],
    *,
    now: datetime | None = None,
    bucket_minutes: int = STAGE_I_THROUGHPUT_BUCKET_MINUTES,
    bucket_count: int = STAGE_I_THROUGHPUT_BUCKET_COUNT,
) -> dict[str, Any]:
    """Bucket latest Stage-I statuses as success, failure, or unprocessed."""
    current_time = now or datetime.now(UTC)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=UTC)
    current_time = current_time.astimezone(UTC)
    current_bucket = current_time.replace(second=0, microsecond=0)
    current_bucket -= timedelta(minutes=current_bucket.minute % bucket_minutes)
    window_start = current_bucket - timedelta(minutes=bucket_minutes * (bucket_count - 1))
    window_end = current_bucket + timedelta(minutes=bucket_minutes)
    bucket_width = timedelta(minutes=bucket_minutes)
    buckets = []
    for index in range(bucket_count):
        start = window_start + bucket_width * index
        end = start + bucket_width
        buckets.append(
            {
                "start": start.isoformat(timespec="seconds"),
                "end": end.isoformat(timespec="seconds"),
                "processed": 0,
                "green": 0,
                "red": 0,
                "gray": 0,
            }
        )

    for record in latest.values():
        status = record.get("status")
        if status not in {"success", "failure", "unprocessed"}:
            continue
        timestamp_value = (
            record.get("source_timestamp")
            if status == "unprocessed" and record.get("source_timestamp")
            else record.get("timestamp")
        )
        timestamp = _utc_timestamp(timestamp_value)
        if timestamp is None or timestamp < window_start or timestamp >= window_end:
            continue
        index = int((timestamp - window_start) // bucket_width)
        bucket = buckets[index]
        bucket["processed"] += 1
        if status == "success":
            bucket["green"] += 1
        elif status == "failure":
            bucket["red"] += 1
        else:
            bucket["gray"] += 1

    return {
        "bucket_minutes": bucket_minutes,
        "bucket_count": bucket_count,
        "window_start": window_start.isoformat(timespec="seconds"),
        "window_end": window_end.isoformat(timespec="seconds"),
        "buckets": buckets,
    }


def load_reward_backfill_worker_status(
    run_dir: Path,
    *,
    now: datetime | None = None,
    stale_after_seconds: int = REWARD_BACKFILL_STATUS_STALE_SECONDS,
) -> dict[str, Any]:
    """Load bounded backfill-pool health without treating stale workers as active."""
    default: dict[str, Any] = {
        "state": "not_running",
        "timestamp": None,
        "reward_concurrency": REWARD_BACKFILL_DEFAULT_CONCURRENCY,
        "reward_active_count": 0,
        "reward_primary_model": None,
        "reward_fallback_model": None,
        "queue_policy": None,
        "poll_interval_seconds": None,
        "current_instances": [],
        "counts": {},
        "stale": False,
        "error": None,
    }
    path = run_dir / REWARD_BACKFILL_WORKER_STATUS
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default
    except (OSError, json.JSONDecodeError):
        return {**default, "state": "unavailable", "error": "invalid worker status"}
    if not isinstance(data, dict):
        return {**default, "state": "unavailable", "error": "invalid worker status"}

    status = {
        **default,
        **{
            key: data.get(key)
            for key in (
                "state",
                "timestamp",
                "worker_id",
                "checker_node",
                "reward_concurrency",
                "reward_active_count",
                "reward_primary_model",
                "reward_fallback_model",
                "queue_policy",
                "poll_interval_seconds",
                "current_instances",
                "counts",
                "error",
            )
            if key in data
        },
    }
    concurrency = _worker_count(data.get("reward_concurrency"))
    active = _worker_count(data.get("reward_active_count"))
    status["reward_concurrency"] = (
        concurrency
        if concurrency is not None and concurrency > 0
        else REWARD_BACKFILL_DEFAULT_CONCURRENCY
    )
    status["reward_active_count"] = active if active is not None else 0
    if not isinstance(status.get("current_instances"), list):
        status["current_instances"] = []
    if not isinstance(status.get("counts"), dict):
        status["counts"] = {}

    timestamp = _utc_timestamp(data.get("timestamp"))
    current_time = now or datetime.now(UTC)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=UTC)
    live_state = data.get("state") in {"running", "idle", "draining", "stopping"}
    if live_state and (
        timestamp is None
        or (current_time.astimezone(UTC) - timestamp).total_seconds() > stale_after_seconds
    ):
        status["state"] = "stale"
        status["reward_active_count"] = 0
        status["stale"] = True
    return status


def _reward_backfill_stage(record: dict[str, Any]) -> dict[str, Any]:
    stage = record.get("reward_hack")
    result = dict(stage) if isinstance(stage, dict) else {}
    state = result.get("state")
    if state not in {"pending", "running", "pass", "fail", "error"}:
        status = record.get("status")
        state = "pending" if status == "queued" else status
    if state not in {"pending", "running", "pass", "fail", "error"}:
        state = "pending"
    result["state"] = state
    if "error" not in result and record.get("error") is not None:
        result["error"] = record.get("error")
    result["source"] = "reward_backfill"
    return result


def merge_reward_backfill_evidence(
    postchecks: dict[str, dict[str, Any]],
    backfills: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Overlay the independent pool's reward stage onto sequential baselines.

    Terminal evidence wins over a pending/running stage. Between equivalent
    states, the newest timestamp wins. This prevents a late queue snapshot from
    hiding a completed check while still allowing a newer rerun to supersede it.
    """
    merged = {instance: dict(record) for instance, record in postchecks.items()}
    verdict_states = {"pass", "fail"}
    for instance, backfill in backfills.items():
        current = merged.get(instance, {"instance": instance})
        current_stage_value = current.get("reward_hack")
        current_stage = dict(current_stage_value) if isinstance(current_stage_value, dict) else {}
        backfill_stage = _reward_backfill_stage(backfill)
        current_state = current_stage.get("state", "pending")
        backfill_state = backfill_stage["state"]
        if current_state in verdict_states and backfill_state not in verdict_states:
            use_backfill = False
        elif backfill_state in verdict_states and current_state not in verdict_states:
            use_backfill = True
        elif current_state == "pending" and backfill_state == "running":
            use_backfill = True
        else:
            use_backfill = str(backfill.get("timestamp") or "") >= str(
                current.get("timestamp") or ""
            )
        if not use_backfill:
            merged[instance] = current
            continue

        result = dict(current)
        result["reward_hack"] = backfill_stage
        result["reward_backfill"] = {
            "status": backfill.get("status"),
            "attempt": backfill.get("attempt"),
            "source_node": backfill.get("source_node"),
            "timestamp": backfill.get("timestamp"),
        }
        result["timestamp"] = backfill.get("timestamp") or current.get("timestamp", "")
        result["attempt"] = backfill.get("attempt", current.get("attempt"))
        result["stage"] = "reward_hack"
        merged[instance] = result
    return merged


def apply_postcheck_evidence(
    latest: dict[str, dict[str, Any]],
    postchecks: dict[str, dict[str, Any]],
) -> None:
    """Overlay explicit post-check evidence without changing pipeline status."""
    for instance, postcheck in postchecks.items():
        generation = latest.get(instance)
        if generation is None or generation.get("status") != "success":
            continue
        nop = postcheck.get("nop") if isinstance(postcheck.get("nop"), dict) else {}
        oracle = postcheck.get("oracle") if isinstance(postcheck.get("oracle"), dict) else {}
        reward_hack = (
            postcheck.get("reward_hack") if isinstance(postcheck.get("reward_hack"), dict) else {}
        )
        if isinstance(nop.get("reward"), (int, float)) and not isinstance(nop.get("reward"), bool):
            generation["nop_reward"] = nop["reward"]
        if isinstance(oracle.get("reward"), (int, float)) and not isinstance(
            oracle.get("reward"), bool
        ):
            generation["oracle_reward"] = oracle["reward"]
        if nop.get("state") in {"pass", "fail"} and oracle.get("state") in {
            "pass",
            "fail",
        }:
            generation["validation_status"] = (
                "baseline_valid"
                if nop.get("state") == "pass"
                and _reward_matches(nop.get("reward"), 0)
                and oracle.get("state") == "pass"
                and _reward_matches(oracle.get("reward"), 1)
                else "validation_failed"
            )
        generation["postcheck_status"] = postcheck.get("status")
        generation["reward_hack_status"] = reward_hack.get("state")
        generation["reward_hack_is_hacking"] = reward_hack.get("is_hacking")
        generation["reward_hack_reason"] = reward_hack.get("reason")
        generation["reward_hack_error"] = reward_hack.get("error")


def collect_latest_statuses(
    run_dir: Path,
    *,
    include_unprocessed: bool = False,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Merge every live journal and reconcile it with successful creations."""
    latest, _order = load_latest_statuses(
        status_journal_paths(run_dir),
        run_dir=run_dir,
        include_unprocessed=include_unprocessed,
    )
    # Success ledger paths to merge. In Postgres mode all create.jsonl paths
    # resolve to one create_success table, so a single canonical path suffices
    # (and the files may not exist on disk at all). In jsonl mode, merge every
    # discovered path as before.
    canonical_success = run_dir / SUCCESS_LEDGER_NAME
    success_repo = LedgerRepo(canonical_success)
    success_paths = (
        [canonical_success]
        if success_repo.backend == "postgres"
        else success_ledger_paths(run_dir)
    )
    for ledger_path in success_paths:
        for instance, success_record in load_success_ledger(ledger_path, run_dir=run_dir).items():
            current = latest.get(instance)
            if current is None:
                latest[instance] = success_record
            elif current.get("status") == "unprocessed":
                if str(success_record.get("timestamp", "")) >= str(
                    current.get("timestamp", "")
                ):
                    latest[instance] = success_record
            elif current.get("status") != "success":
                latest[instance] = success_record
            elif str(success_record.get("timestamp", "")) >= str(current.get("timestamp", "")):
                _copy_node_identity(success_record, current)
                _copy_validation_evidence(success_record, current)
                latest[instance] = success_record

    order = sorted(
        latest,
        key=lambda instance: (
            str(latest[instance].get("timestamp", "")),
            instance,
        ),
        reverse=True,
    )
    return latest, order


def load_slurm_health(run_dir: Path) -> dict[str, Any]:
    path = run_dir / SLURM_HEALTH_NAME
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def count_jsonl_entries(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        with path.open(encoding="utf-8", errors="replace") as fh:
            return sum(1 for line in fh if line.strip())
    except OSError:
        return 0


def active_instances(run_dir: Path, proc_root: Path = Path("/proc")) -> list[str]:
    """Read procfs to find active ``swegen create`` children for this run.

    A relative ``--state-dir`` belongs to the process's working directory, not
    the dashboard's.  Resolving it against ``/proc/<pid>/cwd`` prevents workers
    in a separate workspace with the same run name from being counted as local
    controller workers.
    """
    instances: set[str] = set()
    resolved_run_dir = run_dir.resolve()
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            raw = (entry / "cmdline").read_bytes().decode(errors="replace")
        except OSError:
            continue
        if "swegen\x00create\x00" not in raw or "--state-dir\x00" not in raw:
            continue

        args = [value for value in raw.split("\x00") if value]
        try:
            state_dir_value = args[args.index("--state-dir") + 1]
        except (ValueError, IndexError):
            continue
        state_dir = Path(state_dir_value)
        try:
            if not state_dir.is_absolute():
                state_dir = (entry / "cwd").resolve() / state_dir
            resolved_state_dir = state_dir.resolve()
        except (OSError, RuntimeError):
            continue
        if resolved_state_dir != resolved_run_dir:
            continue
        match = CREATE_INSTANCE_RE.search(raw)
        if match:
            repo, pr = match.groups()
            instances.add(f"{repo.lower().replace('/', '__')}-{pr}")
    return sorted(instances)


def _worker_count(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _reward_matches(value: Any, expected: int) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float)) and value == expected


def _validation_value(record: dict[str, Any], field: str) -> Any:
    validation = record.get("validation")
    if isinstance(validation, dict) and field in validation:
        return validation[field]
    return record.get(field)


def validation_outcome(record: dict[str, Any]) -> str:
    """Classify only the validation guarantee explicitly represented in a record."""
    status = record.get("status")
    validation = record.get("validation")
    validation_status = (
        _string_value(validation.get("status")) if isinstance(validation, dict) else None
    )
    validation_status = validation_status or _string_value(record.get("validation_status"))
    normalized = validation_status.lower() if validation_status else ""

    if status == "failure":
        reason = _string_value(record.get("failure_reason")) or ""
        if normalized in VALIDATION_FAILED_STATUSES or any(
            marker in reason.lower() for marker in VALIDATION_FAILURE_MARKERS
        ):
            return "validation_failed"
        return "other_failure"

    if status != "success":
        return "unknown"

    nop_reward = _validation_value(record, "nop_reward")
    oracle_reward = _validation_value(record, "oracle_reward")
    nop_passed = _validation_value(record, "nop_passed")
    oracle_passed = _validation_value(record, "oracle_passed")
    if (_reward_matches(nop_reward, 0) and _reward_matches(oracle_reward, 1)) or (
        nop_passed is True and oracle_passed is True
    ):
        return "baseline_valid"
    if normalized in BASELINE_VALID_STATUSES:
        return "baseline_valid"
    if normalized in VALIDATION_FAILED_STATUSES:
        return "validation_failed"
    if normalized in UNVALIDATED_STATUSES:
        return "generated_unvalidated"
    return "generated_validation_unknown"


def build_validation_summary(latest: dict[str, dict[str, Any]]) -> dict[str, int]:
    summary = {
        "baseline_valid": 0,
        "validation_failed": 0,
        "generated_unvalidated": 0,
        "generated_validation_unknown": 0,
        "other_failures": 0,
    }
    for record in latest.values():
        outcome = validation_outcome(record)
        summary_key = "other_failures" if outcome == "other_failure" else outcome
        if summary_key in summary:
            summary[summary_key] += 1
    return summary


def build_slurm_node_stats(
    latest: dict[str, dict[str, Any]], slurm_health: dict[str, Any]
) -> list[dict[str, Any]]:
    """Combine outcomes and health for nodes in the current Slurm topology.

    Historical node journals remain part of the aggregate pipeline totals, but
    a cancelled node must not linger in the live node table after the collector
    removes it from ``slurm-health.json``.
    """
    stats: dict[str, dict[str, Any]] = {}
    current_nodes: set[str] | None = None

    def node_stats(node: str) -> dict[str, Any]:
        return stats.setdefault(
            node,
            {
                "node": node,
                "success": 0,
                "failure": 0,
                "active_workers": None,
                "expected_workers": None,
                "state": "",
            },
        )

    for record in latest.values():
        if record.get("node_scope") != "slurm":
            continue
        node = _string_value(record.get("node"))
        status = record.get("status")
        if node and status in {"success", "failure"}:
            node_stats(node)[status] += 1

    health_nodes = slurm_health.get("nodes")
    if isinstance(health_nodes, list):
        current_nodes = set()
        for record in health_nodes:
            if not isinstance(record, dict):
                continue
            node, _is_slurm = _node_from_record(record)
            if not node:
                continue
            current_nodes.add(node)
            item = node_stats(node)
            active = _worker_count(record.get("active_worker_processes"))
            if active is None:
                active = _worker_count(record.get("active_workers"))
            item["active_workers"] = active
            item["expected_workers"] = _worker_count(record.get("expected_workers"))
            item["state"] = _string_value(record.get("state")) or ""

    if current_nodes is not None:
        stats = {node: item for node, item in stats.items() if node in current_nodes}

    rows: list[dict[str, Any]] = []
    for node in sorted(stats):
        item = stats[node]
        processed = item["success"] + item["failure"]
        rows.append(
            {
                **item,
                "processed": processed,
                "yield_percent": round(
                    (item["success"] / processed * 100) if processed else 0.0,
                    2,
                ),
            }
        )
    return rows


def build_postcheck_summary(
    latest: dict[str, dict[str, Any]],
    postchecks: dict[str, dict[str, Any]],
    worker_status: dict[str, Any],
    reward_backfill_worker_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Summarize post-check coverage for every retained create success."""
    eligible = {
        instance for instance, record in latest.items() if record.get("status") == "success"
    }
    live_baseline_instances: set[str] = set()
    if worker_status.get("state") in {"running", "draining", "stopping"} and not worker_status.get(
        "stale"
    ):
        current_instances = worker_status.get("current_instances")
        if isinstance(current_instances, list):
            live_baseline_instances.update(
                instance
                for value in current_instances
                if (instance := _string_value(value)) is not None
            )
        current_instance = _string_value(worker_status.get("current_instance"))
        if current_instance:
            live_baseline_instances.add(current_instance)
    summary: dict[str, Any] = {
        "eligible": len(eligible),
        "not_queued": 0,
        "queued": 0,
        "running": 0,
        "accepted": 0,
        "rejected": 0,
        "errors": 0,
        "baseline_valid": 0,
        "baseline_rejected": 0,
        "baseline_errors": 0,
        "baseline_pending": 0,
        "baseline_running": 0,
        "baseline_blacklisted": 0,
        "reward_clean": 0,
        "reward_flagged": 0,
        "reward_errors_after_baseline": 0,
        "reward_pending_after_baseline": 0,
        "reward_running_after_baseline": 0,
        "nop": dict.fromkeys(("pass", "fail", "error", "pending", "running", "blacklisted"), 0),
        "oracle": dict.fromkeys(
            ("pass", "fail", "error", "pending", "running", "blacklisted"), 0
        ),
        "reward_hack": dict.fromkeys(("pass", "fail", "error", "pending", "running"), 0),
        "worker": {
            "state": worker_status.get("state", "not_running"),
            "mode": worker_status.get("mode"),
            "current_instance": worker_status.get("current_instance"),
            "current_stage": worker_status.get("current_stage"),
            "baseline_concurrency": worker_status.get("baseline_concurrency", 1),
            "baseline_active_count": worker_status.get("baseline_active_count", 0),
            "current_instances": worker_status.get("current_instances", []),
            "active_stages": worker_status.get("active_stages", {}),
            "timestamp": worker_status.get("timestamp"),
            "reward_model": worker_status.get("reward_model"),
            "reward_fallback_model": worker_status.get("reward_fallback_model"),
            "reward_concurrency": worker_status.get("reward_concurrency"),
            "reward_active_count": worker_status.get("reward_active_count"),
            "stale": worker_status.get("stale", False),
            "error": worker_status.get("error"),
        },
        "reward_backfill_worker": reward_backfill_worker_status
        or {
            "state": "not_running",
            "timestamp": None,
            "reward_concurrency": REWARD_BACKFILL_DEFAULT_CONCURRENCY,
            "reward_active_count": 0,
            "reward_primary_model": None,
            "reward_fallback_model": None,
            "current_instances": [],
            "counts": {},
            "stale": False,
            "error": None,
        },
        "by_node": [],
    }
    by_node: dict[str, dict[str, Any]] = {}
    for instance in eligible:
        generation = latest[instance]
        source_node = _string_value(generation.get("node")) or "pre-slurm/local"
        node_counts = by_node.setdefault(
            source_node,
            {
                "node": source_node,
                "eligible": 0,
                "baseline_valid": 0,
                "accepted": 0,
                "rejected": 0,
                "errors": 0,
                "pending": 0,
            },
        )
        node_counts["eligible"] += 1
        record = postchecks.get(instance)
        if record is None:
            summary["not_queued"] += 1
            summary["baseline_pending"] += 1
            node_counts["pending"] += 1
            for stage_name in ("nop", "oracle", "reward_hack"):
                summary[stage_name]["pending"] += 1
            continue

        stages: dict[str, dict[str, Any]] = {}
        for stage_name in ("nop", "oracle", "reward_hack"):
            stage = record.get(stage_name)
            stage = stage if isinstance(stage, dict) else {}
            state = stage.get("state")
            if (
                stage_name in {"nop", "oracle"}
                and state == "running"
                and instance not in live_baseline_instances
            ):
                # Append-only ledgers retain the last in-flight snapshot after
                # a crash. Only the instance named by a fresh worker heartbeat
                # is genuinely running; older snapshots are retryable pending.
                state = "pending"
            if (
                stage_name == "nop"
                and state == "pass"
                and not _reward_matches(stage.get("reward"), 0)
            ):
                state = "fail"
            elif (
                stage_name == "oracle"
                and state == "pass"
                and not _reward_matches(stage.get("reward"), 1)
            ):
                state = "fail"
            elif (
                stage_name == "reward_hack" and state == "pass" and stage.get("is_hacking") is True
            ):
                state = "fail"
            elif (
                stage_name == "reward_hack"
                and state == "pass"
                and stage.get("is_hacking") is not False
            ):
                state = "error"
            if state not in summary[stage_name]:
                state = "pending"
            summary[stage_name][state] += 1
            stages[stage_name] = {**stage, "state": state}

        nop_state = stages["nop"]["state"]
        oracle_state = stages["oracle"]["state"]
        baseline_is_valid = nop_state == "pass" and oracle_state == "pass"
        if baseline_is_valid:
            summary["baseline_valid"] += 1
            node_counts["baseline_valid"] += 1
            reward_state = stages["reward_hack"]["state"]
            if reward_state == "pass":
                summary["reward_clean"] += 1
            elif reward_state == "fail":
                summary["reward_flagged"] += 1
            elif reward_state == "error":
                summary["reward_errors_after_baseline"] += 1
            elif reward_state == "running":
                summary["reward_running_after_baseline"] += 1
            else:
                summary["reward_pending_after_baseline"] += 1
        elif "fail" in {nop_state, oracle_state}:
            summary["baseline_rejected"] += 1
        elif "blacklisted" in {nop_state, oracle_state}:
            summary["baseline_blacklisted"] += 1
        elif "error" in {nop_state, oracle_state}:
            summary["baseline_errors"] += 1
        elif "running" in {nop_state, oracle_state}:
            summary["baseline_running"] += 1
        else:
            summary["baseline_pending"] += 1

        stage_states = {stage["state"] for stage in stages.values()}
        if all(stage["state"] == "pass" for stage in stages.values()):
            status = "accepted"
        elif "fail" in stage_states:
            status = "rejected"
        elif "error" in stage_states:
            status = "error"
        elif "running" in stage_states:
            status = "running"
        else:
            status = "queued"
        status_key = "errors" if status == "error" else status
        summary[status_key] += 1
        if status in {"accepted", "rejected"}:
            node_counts[status] += 1
        elif status == "error":
            node_counts["errors"] += 1
        else:
            node_counts["pending"] += 1
    summary["by_node"] = sorted(by_node.values(), key=lambda row: row["node"])
    return summary


def _stage_i_outcome_instances(
    latest: dict[str, dict[str, Any]],
    historical_oracle_zero: set[str] | None = None,
) -> tuple[set[str], set[str], set[str], set[str]]:
    """Return Stage-I green/red evidence sets shared by the funnel and chart."""
    exact_green_instances = {
        instance
        for instance, record in latest.items()
        if _reward_matches(_validation_value(record, "nop_reward"), 0)
        and _reward_matches(_validation_value(record, "oracle_reward"), 1)
    }
    current_oracle_zero = {
        instance
        for instance, record in latest.items()
        if _reward_matches(_validation_value(record, "oracle_reward"), 0)
    } - exact_green_instances
    historical_oracle_zero = historical_oracle_zero or set()
    historical_recovered: set[str] = set()
    for instance in historical_oracle_zero - exact_green_instances - current_oracle_zero:
        record = latest.get(instance)
        if record is None or record.get("status") != "failure":
            continue
        reason = _string_value(record.get("failure_reason")) or ""
        if reason == "Transient network/API error":
            continue
        if validation_outcome(record) == "validation_failed":
            historical_recovered.add(instance)

    red_instances = current_oracle_zero | historical_recovered
    return (
        exact_green_instances,
        red_instances,
        current_oracle_zero,
        historical_recovered,
    )


def build_pipeline_summary(
    total: int,
    processed: int,
    latest: dict[str, dict[str, Any]],
    postcheck_summary: dict[str, Any],
    historical_oracle_zero: set[str] | None = None,
) -> dict[str, Any]:
    """Build the two mutually exclusive pipeline partitions shown in the UI.

    Stage I red is intentionally strict: only an explicit Oracle reward of 0
    qualifies. Historical ``Validation failed (NOP or Oracle)`` outcomes stay
    gray unless a preserved Harbor trial independently proves Oracle=0. A
    later exact NOP=0/Oracle=1 result always wins and keeps the instance green.
    """

    def partition(denominator: int, green: int, red: int) -> dict[str, int]:
        denominator = max(int(denominator), 0)
        green = min(max(int(green), 0), denominator)
        red = min(max(int(red), 0), denominator - green)
        return {
            "denominator": denominator,
            "green": green,
            "red": red,
            "gray": denominator - green - red,
        }

    (
        exact_green_instances,
        red_instances,
        current_oracle_zero,
        historical_recovered,
    ) = _stage_i_outcome_instances(latest, historical_oracle_zero)
    historical_oracle_zero = historical_oracle_zero or set()
    stage_i = partition(
        total,
        int(postcheck_summary.get("baseline_valid", 0)),
        len(red_instances),
    )
    transient_failures = sum(
        instance not in red_instances
        and record.get("status") == "failure"
        and _string_value(record.get("failure_reason")) == "Transient network/API error"
        for instance, record in latest.items()
    )
    validation_unresolved = sum(
        instance not in exact_green_instances
        and instance not in red_instances
        and validation_outcome(record)
        in {
            "validation_failed",
            "generated_unvalidated",
            "generated_validation_unknown",
        }
        for instance, record in latest.items()
    )
    unprocessed = min(max(int(total) - int(processed), 0), stage_i["gray"])
    remaining_gray = stage_i["gray"] - unprocessed
    transient_failures = min(transient_failures, remaining_gray)
    remaining_gray -= transient_failures
    validation_unresolved = min(validation_unresolved, remaining_gray)
    remaining_gray -= validation_unresolved
    stage_i["breakdown"] = {
        "unprocessed": unprocessed,
        "transient_api": transient_failures,
        "incomplete_or_other": remaining_gray,
        "validation_unresolved": validation_unresolved,
    }
    stage_i["red_breakdown"] = {
        "current_exact": len(current_oracle_zero),
        "historical_recovered": len(historical_recovered),
        "historical_evidence_instances": len(historical_oracle_zero),
    }

    stage_ii = partition(
        stage_i["green"],
        int(postcheck_summary.get("reward_clean", 0)),
        int(postcheck_summary.get("reward_flagged", 0)),
    )
    stage_ii["breakdown"] = {
        "pending": int(postcheck_summary.get("reward_pending_after_baseline", 0)),
        "running": int(postcheck_summary.get("reward_running_after_baseline", 0)),
        "errors": int(postcheck_summary.get("reward_errors_after_baseline", 0)),
    }
    return {"stage_i": stage_i, "stage_ii": stage_ii}


def build_stage_breakdown(
    total: int,
    processed: int,
    failure: int,
    postcheck_summary: dict[str, Any],
    node_baseline_worker_status: dict[str, Any],
    reward_backfill_worker_status: dict[str, Any],
) -> dict[str, Any]:
    """Build the three-stage breakdown shown in the dashboard.

    The three stages are explicit and reconcile to the shared postcheck ledger:

    1. SWEgen        — total = generated + unprocessed + errored;
                       progress = generated / total.
    2. NOP/ORACLE    — generated = oracle_passed + oracle_failed + errored
                       + unprocessed.
    3. Reward Hack   — oracle_passed = accepted + filtered + unprocessed
                       + errored.

    Each stage carries the live worker status for the component that owns it
    so the UI can show state/active/error inline.
    """

    def pct(numerator: int, denominator: int) -> float:
        return round((numerator / denominator * 100) if denominator else 0.0, 2)

    # ---- Stage 1: SWEgen (generation) -------------------------------------
    generated = processed  # create successes + failures that reached Stage I
    swegen_errored = failure
    unprocessed = max(int(total) - int(processed), 0)
    swegen_total = generated + unprocessed + swegen_errored
    # Guard against a stale ``total`` smaller than the processed ledger.
    swegen_total = max(swegen_total, generated + unprocessed)
    stage_swegen = {
        "name": "SWEgen",
        "total": swegen_total,
        "generated": generated,
        "unprocessed": unprocessed,
        "errored": swegen_errored,
        "progress_percent": pct(generated, swegen_total),
        "worker": _stage_worker_block(
            {
                "state": "running" if generated else "not_running",
                "active_count": None,
                "concurrency": None,
                "error": None,
                "stale": False,
            }
        ),
    }

    # ---- Stage 2: NOP/ORACLE (baseline validation) ------------------------
    nop = postcheck_summary.get("nop", {})
    oracle = postcheck_summary.get("oracle", {})
    oracle_passed = int(postcheck_summary.get("baseline_valid", 0))
    oracle_failed = (
        int(nop.get("fail", 0))
        + int(nop.get("blacklisted", 0))
        + int(oracle.get("fail", 0))
        + int(oracle.get("blacklisted", 0))
    )
    baseline_errored = (
        int(nop.get("error", 0)) + int(oracle.get("error", 0))
    )
    nop_oracle_unprocessed = (
        int(nop.get("pending", 0))
        + int(nop.get("running", 0))
        + int(oracle.get("pending", 0))
        + int(oracle.get("running", 0))
    )
    nop_oracle_generated = (
        oracle_passed + oracle_failed + baseline_errored + nop_oracle_unprocessed
    )
    stage_nop_oracle = {
        "name": "NOP/ORACLE",
        "total": nop_oracle_generated,
        "oracle_passed": oracle_passed,
        "oracle_failed": oracle_failed,
        "errored": baseline_errored,
        "unprocessed": nop_oracle_unprocessed,
        "nop": {
            "pass": int(nop.get("pass", 0)),
            "fail": int(nop.get("fail", 0)),
            "error": int(nop.get("error", 0)),
            "pending": int(nop.get("pending", 0)),
            "running": int(nop.get("running", 0)),
            "blacklisted": int(nop.get("blacklisted", 0)),
        },
        "oracle": {
            "pass": int(oracle.get("pass", 0)),
            "fail": int(oracle.get("fail", 0)),
            "error": int(oracle.get("error", 0)),
            "pending": int(oracle.get("pending", 0)),
            "running": int(oracle.get("running", 0)),
            "blacklisted": int(oracle.get("blacklisted", 0)),
        },
        "progress_percent": pct(oracle_passed, nop_oracle_generated),
        "worker": _stage_worker_block(
            {
                "state": node_baseline_worker_status.get("state", "not_running"),
                "active_count": node_baseline_worker_status.get("baseline_active_count", 0),
                "concurrency": node_baseline_worker_status.get("baseline_concurrency", 0),
                "error": node_baseline_worker_status.get("error"),
                "stale": False,
                "nodes": node_baseline_worker_status.get("nodes", []),
                "shard_count": len(node_baseline_worker_status.get("shards", [])),
                "reachable_count": node_baseline_worker_status.get("reachable_count", 0),
                "host_count": node_baseline_worker_status.get("host_count", 0),
            }
        ),
    }

    # ---- Stage 3: Reward Hack Filter --------------------------------------
    accepted = int(postcheck_summary.get("reward_clean", 0))
    filtered = int(postcheck_summary.get("reward_flagged", 0))
    reward_unprocessed = (
        int(postcheck_summary.get("reward_pending_after_baseline", 0))
        + int(postcheck_summary.get("reward_running_after_baseline", 0))
    )
    reward_errored = int(postcheck_summary.get("reward_errors_after_baseline", 0))
    reward_total = accepted + filtered + reward_unprocessed + reward_errored
    # Reward only runs on baseline_valid instances; clamp to oracle_passed.
    reward_total = min(reward_total, oracle_passed) if oracle_passed else reward_total
    stage_reward_hack = {
        "name": "Reward Hack Filter",
        "total": reward_total,
        "accepted": accepted,
        "filtered": filtered,
        "errored": reward_errored,
        "unprocessed": reward_unprocessed,
        "progress_percent": pct(accepted, reward_total),
        "worker": _stage_worker_block(
            {
                "state": reward_backfill_worker_status.get("state", "not_running")
                if reward_backfill_worker_status
                else "not_running",
                "active_count": (
                    reward_backfill_worker_status.get("reward_active_count", 0)
                    if reward_backfill_worker_status
                    else 0
                ),
                "concurrency": (
                    reward_backfill_worker_status.get("reward_concurrency", 0)
                    if reward_backfill_worker_status
                    else 0
                ),
                "error": (
                    reward_backfill_worker_status.get("error")
                    if reward_backfill_worker_status
                    else None
                ),
                "stale": (
                    reward_backfill_worker_status.get("stale", False)
                    if reward_backfill_worker_status
                    else False
                ),
                "model": (
                    reward_backfill_worker_status.get("reward_primary_model")
                    if reward_backfill_worker_status
                    else None
                ),
            }
        ),
    }

    return {
        "swegen": stage_swegen,
        "nop_oracle": stage_nop_oracle,
        "reward_hack": stage_reward_hack,
    }


def _stage_worker_block(spec: dict[str, Any]) -> dict[str, Any]:
    """Normalize a worker status spec for display under a stage."""
    return {
        "state": spec.get("state", "not_running"),
        "active_count": spec.get("active_count"),
        "concurrency": spec.get("concurrency"),
        "error": spec.get("error"),
        "stale": bool(spec.get("stale", False)),
        "model": spec.get("model"),
        "nodes": spec.get("nodes", []),
        "shard_count": spec.get("shard_count"),
        "reachable_count": spec.get("reachable_count"),
        "host_count": spec.get("host_count"),
    }


def calculate_status(
    run_dir: Path, input_jsonl: Path, total_entries: int | None = None
) -> dict[str, Any]:
    latest, order = collect_latest_statuses(run_dir)
    throughput_latest, _throughput_order = collect_latest_statuses(
        run_dir,
        include_unprocessed=True,
    )
    postchecks = load_authoritative_postchecks(run_dir)
    reward_backfills = load_latest_reward_backfills(run_dir)
    postchecks = merge_reward_backfill_evidence(postchecks, reward_backfills)
    apply_postcheck_evidence(latest, postchecks)
    postcheck_worker_status = load_postcheck_worker_status(run_dir)
    node_baseline_worker_status = load_node_baseline_worker_status()
    reward_backfill_worker_status = load_reward_backfill_worker_status(run_dir)
    success = sum(record.get("status") == "success" for record in latest.values())
    failure = sum(record.get("status") == "failure" for record in latest.values())
    processed = success + failure
    total = total_entries if total_entries is not None else count_jsonl_entries(input_jsonl)
    active = active_instances(run_dir)
    slurm_health = load_slurm_health(run_dir)
    slurm_active = slurm_health.get("active_workers", 0)
    slurm_expected = slurm_health.get("expected_workers", 0)
    if not isinstance(slurm_active, int):
        slurm_active = 0
    if not isinstance(slurm_expected, int):
        slurm_expected = 0

    slurm_nodes = build_slurm_node_stats(latest, slurm_health)
    validation_summary = build_validation_summary(latest)
    postcheck_summary = build_postcheck_summary(
        latest,
        postchecks,
        postcheck_worker_status,
        reward_backfill_worker_status,
    )
    historical_oracle_zero = load_historical_oracle_zero_instances(run_dir)
    generated_count = int(postcheck_summary["eligible"])
    baseline_count = int(postcheck_summary["baseline_valid"])
    accepted_count = int(postcheck_summary["accepted"])
    pipeline = build_pipeline_summary(
        total,
        processed,
        latest,
        postcheck_summary,
        historical_oracle_zero,
    )
    stages = build_stage_breakdown(
        total,
        processed,
        failure,
        postcheck_summary,
        node_baseline_worker_status,
        reward_backfill_worker_status,
    )
    ledger_path = shared_postcheck_ledger_path(run_dir)
    ledger = {"path": str(ledger_path) if ledger_path else None}
    if ledger_path is not None:
        try:
            stat = ledger_path.stat()
            ledger["size_bytes"] = stat.st_size
            ledger["mtime"] = datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(
                timespec="seconds"
            )
        except OSError:
            pass
    # In Postgres mode the ledger lives in the postcheck_status table; report
    # that plus a row count so the dashboard still shows ledger provenance.
    if not ledger_path:
        try:
            from swegen import db

            row = db.query_one("SELECT count(*) AS n FROM postcheck_status")
            if row is not None:
                ledger["backend"] = "postgres"
                ledger["table"] = "postcheck_status"
                ledger["rows"] = row["n"]
        except Exception:
            pass
    stage_i_throughput = build_stage_i_throughput(throughput_latest)

    def percentage(numerator: int, denominator: int) -> float:
        return round((numerator / denominator * 100) if denominator else 0.0, 2)

    funnel = {
        "generated": {
            "count": generated_count,
            "processed": processed,
            "denominator": processed,
            "failures": failure,
            "yield_percent": percentage(generated_count, processed),
        },
        "baseline_valid": {
            "count": baseline_count,
            "denominator": generated_count,
            "yield_percent": percentage(baseline_count, generated_count),
            "conversion_percent": percentage(baseline_count, generated_count),
            "pending": postcheck_summary["baseline_pending"],
            "running": postcheck_summary["baseline_running"],
            "rejected": postcheck_summary["baseline_rejected"],
            "errors": postcheck_summary["baseline_errors"],
        },
        "fully_accepted": {
            "count": accepted_count,
            "denominator": baseline_count,
            "yield_percent": percentage(accepted_count, baseline_count),
            "conversion_percent": percentage(accepted_count, baseline_count),
            "overall_percent": percentage(accepted_count, generated_count),
            "pending": postcheck_summary["reward_pending_after_baseline"],
            "running": postcheck_summary["reward_running_after_baseline"],
            "flagged": postcheck_summary["reward_flagged"],
            "errors": postcheck_summary["reward_errors_after_baseline"],
        },
    }

    health_timestamp = _utc_timestamp(slurm_health.get("timestamp"))
    health_now = datetime.now(UTC)
    collector_stale = (
        health_timestamp is None
        or (health_now - health_timestamp).total_seconds() > SLURM_HEALTH_STALE_SECONDS
    )
    health_nodes = slurm_health.get("nodes")
    health_nodes = health_nodes if isinstance(health_nodes, list) else []
    live_node_states = {"RUNNING", "COMPLETING", "CONFIGURING"}
    health = {
        "generation": {
            "state": "stale" if collector_stale else "live",
            "timestamp": slurm_health.get("timestamp"),
            "active_workers": slurm_active,
            "expected_workers": slurm_expected,
            "live_nodes": sum(
                isinstance(node, dict) and node.get("state") in live_node_states
                for node in health_nodes
            ),
            "total_nodes": len(health_nodes),
        },
        "baseline": postcheck_summary["worker"],
        "reward": postcheck_summary["reward_backfill_worker"],
    }

    validation_by_node = {row["node"]: row for row in postcheck_summary["by_node"]}
    pipeline_nodes = []
    pipeline_node_names: set[str] = set()
    for node in slurm_nodes:
        validation = validation_by_node.get(node["node"], {})
        pipeline_node_names.add(node["node"])
        pipeline_nodes.append(
            {
                "node": node["node"],
                "state": node["state"],
                "active_workers": node["active_workers"],
                "expected_workers": node["expected_workers"],
                "generated": node["success"],
                "baseline_valid": validation.get("baseline_valid", 0),
                "fully_accepted": validation.get("accepted", 0),
            }
        )
    for node_name, validation in sorted(validation_by_node.items()):
        if node_name in pipeline_node_names:
            continue
        pipeline_nodes.append(
            {
                "node": node_name,
                "state": "historical",
                "active_workers": 0,
                "expected_workers": 0,
                "generated": validation.get("eligible", 0),
                "baseline_valid": validation.get("baseline_valid", 0),
                "fully_accepted": validation.get("accepted", 0),
            }
        )
    success_by_node = {
        record["node"]: record["success"] for record in slurm_nodes if record["success"]
    }

    recent = []
    for instance in order[:25]:
        record = latest[instance]
        recent.append(
            {
                "instance": instance,
                "status": record.get("status"),
                "timestamp": record.get("timestamp", ""),
                "reason": record.get("failure_reason", ""),
                "worker_id": record.get("worker_id"),
                "node": record.get("node", "controller"),
                "validation_outcome": validation_outcome(record),
            }
        )

    return {
        "run": run_dir.name,
        "updated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "success": success,
        "failure": failure,
        "processed": processed,
        "total": total,
        "remaining": max(total - processed, 0),
        "active_workers": len(active) + slurm_active,
        "active_workers_local": len(active),
        "active_workers_slurm": slurm_active,
        "slurm_expected_workers": slurm_expected,
        "active_instances": active,
        "success_by_slurm_node": dict(sorted(success_by_node.items())),
        "slurm_nodes": slurm_nodes,
        "pipeline_nodes": pipeline_nodes,
        "validation_summary": validation_summary,
        "postcheck": postcheck_summary,
        "pipeline": pipeline,
        "stages": stages,
        "ledger": ledger,
        "stage_i_throughput": stage_i_throughput,
        "funnel": funnel,
        "health": health,
        "yield_percent": round((success / processed * 100) if processed else 0.0, 2),
        "dataset_yield_percent": round((success / total * 100) if total else 0.0, 4),
        "completion_percent": round((processed / total * 100) if total else 0.0, 2),
        "recent": recent,
    }


LEGACY_DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>SWE-gen Live Outcomes</title>
  <style>
    :root { color-scheme: dark; --bg:#0b1020; --panel:#141b2d; --muted:#8f9bb3; --good:#36d399; --bad:#fb7185; --warn:#fbbf24; --accent:#60a5fa; }
    * { box-sizing:border-box; }
    body { margin:0; background:radial-gradient(circle at top,#17213b 0,var(--bg) 45%); color:#edf2f7; font:15px/1.45 ui-sans-serif,system-ui,sans-serif; }
    main { width:min(1180px,calc(100% - 32px)); margin:32px auto; }
    header { display:flex; justify-content:space-between; align-items:end; gap:16px; margin-bottom:20px; }
    h1 { margin:0; font-size:28px; }
    .sub,.muted { color:var(--muted); }
    .live { color:var(--good); font-weight:700; }
    .grid { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:14px; }
    .card { background:rgba(20,27,45,.94); border:1px solid #26324d; border-radius:14px; padding:18px; box-shadow:0 12px 35px rgba(0,0,0,.22); }
    .label { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em; }
    .value { font-size:30px; font-weight:750; margin-top:5px; }
    .formula { color:var(--muted); font-size:13px; margin-top:3px; }
    .wide { grid-column:span 2; }
    .bar { height:10px; background:#202a42; border-radius:999px; overflow:hidden; margin-top:14px; }
    .fill { height:100%; width:0; background:linear-gradient(90deg,var(--accent),#a78bfa); transition:width .5s ease; }
    section { margin-top:18px; }
    table { width:100%; border-collapse:collapse; }
    th,td { text-align:left; padding:10px 8px; border-bottom:1px solid #26324d; vertical-align:top; }
    th { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.06em; }
    .success { color:var(--good); font-weight:700; }
    .failure { color:var(--bad); font-weight:700; }
    .unknown { color:var(--warn); font-weight:700; }
    .instances { word-break:break-word; }
    .evidence-grid { display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:10px; margin-top:14px; }
    .postcheck-grid { display:grid; grid-template-columns:repeat(7,minmax(0,1fr)); gap:10px; margin-top:14px; }
    .evidence-item { background:#101729; border:1px solid #26324d; border-radius:10px; padding:12px; }
    .evidence-value { font-size:24px; font-weight:750; margin-top:3px; }
    .node-summary { display:flex; justify-content:space-between; align-items:end; gap:16px; margin-bottom:10px; }
    .node-total { color:var(--muted); font-size:13px; text-align:right; }
    .validator-pools { display:grid; gap:5px; min-width:330px; }
    .validator-pool { color:var(--muted); font-size:13px; text-align:right; }
    .validator-pool-name { color:#cbd5e1; font-weight:700; }
    .node-table-wrap { max-height:300px; overflow:auto; border:1px solid #26324d; border-radius:10px; }
    .node-table { min-width:760px; }
    .node-table thead { position:sticky; top:0; z-index:1; background:#141b2d; }
    .node-table th,.node-table td { white-space:nowrap; }
    .node-table .node-name { min-width:310px; white-space:normal; overflow-wrap:anywhere; }
    .node-table tbody tr:last-child td { border-bottom:0; }
    .postcheck-table { min-width:1180px; }
    .postcheck-table-wrap { max-height:420px; overflow:auto; border:1px solid #26324d; border-radius:10px; margin-top:12px; }
    .postcheck-table-wrap thead { position:sticky; top:0; z-index:1; background:#141b2d; }
    .empty-row { color:var(--muted); text-align:center; }
    @media (max-width:850px) { .grid { grid-template-columns:repeat(2,1fr); } .evidence-grid,.postcheck-grid { grid-template-columns:repeat(2,1fr); } }
    @media (max-width:520px) { .grid,.evidence-grid,.postcheck-grid { grid-template-columns:1fr; } .wide { grid-column:span 1; } header { align-items:start; flex-direction:column; } }
  </style>
</head>
<body><main>
  <header><div><h1>SWE-gen Live Outcomes</h1><div class="sub">Run <span id="run">—</span></div></div><div id="connection" class="live">● LIVE</div></header>
  <div class="grid">
    <div class="card wide"><div class="label">Pipeline completion yield</div><div id="yield" class="value">—</div><div id="yieldFormula" class="formula">successful swegen create outcomes / processed</div><div class="bar"><div id="yieldBar" class="fill"></div></div></div>
    <div class="card"><div class="label">Failed</div><div id="failure" class="value failure">—</div><div class="formula">latest result per instance</div></div>
    <div class="card"><div class="label">Pipeline success / input</div><div id="datasetYield" class="value">—</div><div id="datasetYieldFormula" class="formula">successful create outcomes / total input</div></div>
    <div class="card wide"><div class="label">Dataset completion</div><div id="completion" class="value">—</div><div id="completionFormula" class="formula">processed / total input</div><div class="bar"><div id="completionBar" class="fill"></div></div></div>
    <div class="card wide"><div class="label">Active workers</div><div id="active" class="value">—</div><div id="activeScope" class="formula"></div><div id="activeInstances" class="formula instances"></div></div>
  </div>
  <section class="card">
    <div class="label">Validation evidence</div>
    <div class="formula">Only explicit NOP reward 0 plus Oracle reward 1 evidence is counted as baseline-valid. Legacy success records without evidence remain unknown.</div>
    <div class="evidence-grid">
      <div class="evidence-item"><div class="label">Baseline-valid</div><div id="baselineValid" class="evidence-value success">—</div><div class="formula">NOP=0 and Oracle=1 proven</div></div>
      <div class="evidence-item"><div class="label">Validation failed</div><div id="validationFailed" class="evidence-value failure">—</div><div class="formula">explicit baseline failure</div></div>
      <div class="evidence-item"><div class="label">Generated unvalidated</div><div id="generatedUnvalidated" class="evidence-value unknown">—</div><div class="formula">validation explicitly skipped</div></div>
      <div class="evidence-item"><div class="label">Validation unknown</div><div id="validationUnknown" class="evidence-value unknown">—</div><div class="formula">generated, but proof not recorded</div></div>
      <div class="evidence-item"><div class="label">Other failures</div><div id="otherFailures" class="evidence-value failure">—</div><div class="formula">generation/network/filter failures</div></div>
    </div>
  </section>
  <section class="card">
    <div class="node-summary"><div><div class="label">Standalone Harbor + reward-hack validation</div><div class="formula">NOP and Oracle stay ordered per task while the validator processes multiple tasks concurrently. A separate tests-only pool backfills reward-hack checks; infrastructure errors are retried and never counted as hacking.</div></div><div class="validator-pools"><div class="validator-pool"><span class="validator-pool-name">NOP/Oracle validator:</span> <span id="postcheckWorker">not running · NOP/Oracle 0/1 active</span></div><div class="validator-pool"><span class="validator-pool-name">Independent reward backfill:</span> <span id="rewardBackfillWorker">not running · 0/20 active</span></div></div></div>
    <div class="postcheck-grid">
      <div class="evidence-item"><div class="label">Eligible</div><div id="postcheckEligible" class="evidence-value">—</div><div class="formula">All retained create successes</div></div>
      <div class="evidence-item"><div class="label">Not queued</div><div id="postcheckNotQueued" class="evidence-value unknown">—</div></div>
      <div class="evidence-item"><div class="label">Queued</div><div id="postcheckQueued" class="evidence-value unknown">—</div></div>
      <div class="evidence-item"><div class="label">Running</div><div id="postcheckRunning" class="evidence-value">—</div></div>
      <div class="evidence-item"><div class="label">Fully accepted</div><div id="postcheckAccepted" class="evidence-value success">—</div><div class="formula">NOP=0, Oracle=1, clean</div></div>
      <div class="evidence-item"><div class="label">Rejected</div><div id="postcheckRejected" class="evidence-value failure">—</div><div class="formula">reward mismatch or flagged</div></div>
      <div class="evidence-item"><div class="label">Check errors</div><div id="postcheckErrors" class="evidence-value failure">—</div><div class="formula">retryable infrastructure</div></div>
    </div>
    <div class="postcheck-table-wrap"><table><thead><tr><th>Stage</th><th>Passed</th><th>Failed</th><th>Error</th><th>Pending</th><th>Running</th></tr></thead><tbody id="postcheckStageRows"></tbody></table></div>
    <div class="postcheck-table-wrap"><table class="postcheck-table"><thead><tr><th>Instance</th><th>Source node</th><th>Overall</th><th>NOP</th><th>Oracle</th><th>Reward-hack</th><th>Error / verdict</th><th>Updated</th></tr></thead><tbody id="postcheckRows"></tbody></table></div>
  </section>
  <section class="card">
    <div class="node-summary"><div><div class="label">Successful across all nodes</div><div class="formula">Pipeline outcomes by Slurm node; create success does not itself prove baseline validation or preserve NOP/Oracle evidence</div></div><div id="successSummary" class="node-total">—</div></div>
    <div class="node-table-wrap"><table class="node-table"><thead><tr><th>Node</th><th>State</th><th>Create success</th><th>Failure</th><th>Processed</th><th>Completion yield</th><th>Workers</th></tr></thead><tbody id="slurmNodeRows"></tbody></table></div>
  </section>
  <section class="card"><div class="label">Recent completed instances</div><table><thead><tr><th>Instance</th><th>Node</th><th>Pipeline status</th><th>Validation evidence</th><th>Reason</th><th>Updated</th></tr></thead><tbody id="recent"></tbody></table></section>
  <div class="muted" style="margin-top:12px">Updated <span id="updated">—</span> · refreshes every 2 seconds</div>
</main>
<script>
const fmt = n => Number(n).toLocaleString();
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const addTextCell = (row, value, className='') => {
  const cell=document.createElement('td'); cell.textContent=String(value ?? '');
  if (className) cell.className=className; row.appendChild(cell); return cell;
};
function renderSlurmNodes(nodes) {
  const fragment=document.createDocumentFragment();
  if (!nodes.length) {
    const row=document.createElement('tr'); const cell=addTextCell(row,'No Slurm nodes collected','empty-row');
    cell.colSpan=7; fragment.appendChild(row);
  } else {
    nodes.forEach(node=>{
      const row=document.createElement('tr');
      addTextCell(row,node.node,'node-name'); addTextCell(row,node.state||'—');
      addTextCell(row,fmt(node.success),'success'); addTextCell(row,fmt(node.failure),'failure');
      addTextCell(row,fmt(node.processed)); addTextCell(row,Number(node.yield_percent).toFixed(2)+'%');
      const active=node.active_workers == null ? '—' : fmt(node.active_workers);
      const expected=node.expected_workers == null ? '—' : fmt(node.expected_workers);
      addTextCell(row,`${active} / ${expected}`); fragment.appendChild(row);
    });
  }
  slurmNodeRows.replaceChildren(fragment);
}
const checkStateClass = state => state==='accepted'||state==='pass' ? 'success' : state==='rejected'||state==='fail'||state==='error' ? 'failure' : 'unknown';
const rewardStageText = (stage, expected) => {
  const state=stage?.state||'pending';
  if (state==='pass') return `✓ ${expected}`;
  if (state==='fail') return `✗ ${stage.reward ?? '—'}`;
  if (state==='error') return 'ERROR';
  return state.toUpperCase();
};
const hackStageText = stage => {
  const state=stage?.state||'pending';
  return state==='pass' ? 'clean' : state==='fail' ? 'FLAGGED' : state==='error' ? 'ERROR' : state.toUpperCase();
};
function renderPostcheck(postcheck) {
  const value=postcheck||{}; const worker=value.worker||{}; const backfill=value.reward_backfill_worker||{};
  postcheckEligible.textContent=fmt(value.eligible||0); postcheckNotQueued.textContent=fmt(value.not_queued||0);
  postcheckQueued.textContent=fmt(value.queued||0); postcheckRunning.textContent=fmt(value.running||0);
  postcheckAccepted.textContent=fmt(value.accepted||0); postcheckRejected.textContent=fmt(value.rejected||0);
  postcheckErrors.textContent=fmt(value.errors||0);
  const current=worker.current_instance ? ` · ${worker.current_instance} / ${worker.current_stage||'—'}` : '';
  const fallback=worker.reward_fallback_model ? ` → ${worker.reward_fallback_model}` : '';
  const model=worker.reward_model ? ` · ${worker.reward_model}${fallback}` : '';
  const baselineCapacity=worker.baseline_concurrency ?? 1;
  const baselineActive=worker.baseline_active_count ?? 0;
  const rewardLoad=worker.reward_concurrency ? ` · reward ${fmt(worker.reward_active_count||0)}/${fmt(worker.reward_concurrency)}` : '';
  postcheckWorker.textContent=`${worker.state||'not running'} · NOP/Oracle ${fmt(baselineActive)}/${fmt(baselineCapacity)} active${current}${model}${rewardLoad}`;
  const backfillCapacity=backfill.reward_concurrency ?? 20;
  const backfillActive=backfill.reward_active_count ?? 0;
  const backfillFallback=backfill.reward_fallback_model ? ` → ${backfill.reward_fallback_model}` : '';
  const backfillModel=backfill.reward_primary_model ? ` · ${backfill.reward_primary_model}${backfillFallback}` : '';
  const stale=backfill.stale ? ' · last heartbeat stale' : '';
  rewardBackfillWorker.textContent=`${backfill.state||'not running'} · ${fmt(backfillActive)}/${fmt(backfillCapacity)} active${backfillModel}${stale}`;

  const stages=[['NOP',value.nop||{}],['Oracle',value.oracle||{}],['Reward-hack (all pools)',value.reward_hack||{}]];
  const stageFragment=document.createDocumentFragment();
  stages.forEach(([label,stage])=>{ const row=document.createElement('tr'); addTextCell(row,label);
    addTextCell(row,fmt(stage.pass||0),'success'); addTextCell(row,fmt(stage.fail||0),'failure');
    addTextCell(row,fmt(stage.error||0),'failure'); addTextCell(row,fmt(stage.pending||0),'unknown');
    addTextCell(row,fmt(stage.running||0)); stageFragment.appendChild(row); });
  postcheckStageRows.replaceChildren(stageFragment);

  const rows=value.recent||[]; const fragment=document.createDocumentFragment();
  if (!rows.length) { const row=document.createElement('tr'); const cell=addTextCell(row,'No standalone validation results yet','empty-row'); cell.colSpan=8; fragment.appendChild(row); }
  rows.forEach(item=>{ const row=document.createElement('tr'); const nop=item.nop||{}; const oracle=item.oracle||{}; const hack=item.reward_hack||{};
    addTextCell(row,item.instance,'node-name'); addTextCell(row,item.source_node||'unknown');
    addTextCell(row,`${item.status||'queued'} / ${item.stage||'queue'}`,checkStateClass(item.status));
    addTextCell(row,rewardStageText(nop,0),checkStateClass(nop.state));
    addTextCell(row,rewardStageText(oracle,1),checkStateClass(oracle.state));
    addTextCell(row,hackStageText(hack),checkStateClass(hack.state));
    addTextCell(row,item.error||nop.error||oracle.error||hack.error||hack.reason||'');
    addTextCell(row,item.timestamp||''); fragment.appendChild(row); });
  postcheckRows.replaceChildren(fragment);
}
const validationLabel = value => ({
  baseline_valid:'Baseline-valid', validation_failed:'Failed',
  generated_unvalidated:'Unvalidated', generated_validation_unknown:'Unknown',
  other_failure:'—'
}[value]||'Unknown');
async function refresh() {
  try {
    const r = await fetch('/api/status', {cache:'no-store'}); if (!r.ok) throw new Error(r.status);
    const d = await r.json();
    run.textContent=d.run; yield.textContent=d.yield_percent.toFixed(2)+'%'; failure.textContent=fmt(d.failure);
    const validation=d.validation_summary||{};
    baselineValid.textContent=fmt(validation.baseline_valid||0);
    validationFailed.textContent=fmt(validation.validation_failed||0);
    generatedUnvalidated.textContent=fmt(validation.generated_unvalidated||0);
    validationUnknown.textContent=fmt(validation.generated_validation_unknown||0);
    otherFailures.textContent=fmt(validation.other_failures||0);
    const slurmNodes=d.slurm_nodes||[];
    const slurmSuccess=slurmNodes.reduce((total,node)=>total+Number(node.success||0),0);
    successSummary.textContent=`${fmt(d.success)} successful create outcomes · ${fmt(slurmSuccess)} Slurm-attributed`;
    renderSlurmNodes(slurmNodes);
    renderPostcheck(d.postcheck||{});
    yieldFormula.textContent=`${fmt(d.success)} successful create outcomes / ${fmt(d.processed)} processed`;
    yieldBar.style.width=Math.min(d.yield_percent,100)+'%'; completion.textContent=d.completion_percent.toFixed(2)+'%';
    completionFormula.textContent=`${fmt(d.processed)} processed / ${fmt(d.total)} total · ${fmt(d.remaining)} remaining`;
    completionBar.style.width=Math.min(d.completion_percent,100)+'%'; datasetYield.textContent=d.dataset_yield_percent.toFixed(4)+'%';
    datasetYieldFormula.textContent=`${fmt(d.success)} successful create outcomes / ${fmt(d.total)} total input`;
    active.textContent=fmt(d.active_workers); activeScope.textContent=`${fmt(d.active_workers_local)} local + ${fmt(d.active_workers_slurm)} Slurm / ${fmt(d.slurm_expected_workers)} expected Slurm`;
    activeInstances.textContent=d.active_instances.join(', ');
    updated.textContent=d.updated_at; connection.textContent='● LIVE'; connection.className='live';
    recent.innerHTML=d.recent.map(x=>`<tr><td>${esc(x.instance)}</td><td>${esc(x.node)}</td><td class="${x.status}">${esc(x.status)}</td><td>${esc(validationLabel(x.validation_outcome))}</td><td>${esc(x.reason)}</td><td>${esc(x.timestamp)}</td></tr>`).join('');
  } catch (e) { connection.textContent='● DISCONNECTED'; connection.className='failure'; }
}
refresh(); setInterval(refresh,2000);
</script></body></html>"""


DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>SWE-gen Pipeline</title>
  <style>
    :root { color-scheme:dark; --bg:#090e1a; --panel:#121a2a; --line:#26334d; --muted:#91a0b9; --text:#e8eef9; --green:#34d399; --red:#fb7185; --gray:#64748b; --amber:#fbbf24; }
    * { box-sizing:border-box; }
    body { margin:0; background:radial-gradient(circle at top,#16213a 0,#090e1a 40%); color:var(--text); font:15px/1.45 ui-sans-serif,system-ui,sans-serif; }
    main { width:min(1080px,calc(100% - 28px)); margin:0 auto; padding:28px 0 42px; }
    header { display:flex; justify-content:space-between; gap:20px; align-items:flex-start; margin-bottom:22px; }
    h1 { margin:0 0 5px; font-size:clamp(25px,4vw,38px); letter-spacing:-.03em; }
    .sub,.meta { color:var(--muted); }
    .live { color:var(--green); font-weight:800; white-space:nowrap; }
    .bad { color:var(--red); }
    .warn { color:var(--amber); }
    .panel { background:rgba(18,26,42,.96); border:1px solid var(--line); border-radius:16px; box-shadow:0 18px 60px rgba(0,0,0,.2); }
    .pipeline { display:grid; gap:18px; padding:22px; }
    .stage { padding:20px; border:1px solid var(--line); border-radius:14px; background:#0d1423; }
    .stage-header { display:flex; justify-content:space-between; gap:18px; align-items:flex-start; }
    .stage-number { color:#7dd3fc; font-size:12px; font-weight:900; letter-spacing:.12em; text-transform:uppercase; }
    .stage h2 { margin:5px 0 2px; font-size:clamp(20px,3vw,27px); }
    .stage-total { text-align:right; }
    .count { font-size:clamp(25px,4vw,40px); font-weight:900; line-height:1; letter-spacing:-.04em; white-space:nowrap; }
    .equation { margin-top:7px; color:var(--muted); font-size:13px; white-space:nowrap; }
    .stacked-bar { display:flex; width:100%; height:28px; margin-top:20px; overflow:hidden; border:1px solid #34425e; border-radius:8px; background:#1b2639; }
    .segment { width:0; height:100%; transition:width .35s ease; }
    .segment.green,.swatch.green { background:var(--green); }
    .segment.red,.swatch.red { background:var(--red); }
    .segment.gray,.swatch.gray { background:var(--gray); }
    .throughput { margin-top:12px; padding:10px 11px 8px; border:1px solid #253149; border-radius:10px; background:#0a1120; }
    .throughput-header { display:flex; justify-content:space-between; gap:12px; color:var(--muted); font-size:11px; }
    .throughput-bars { display:grid; grid-template-columns:repeat(24,minmax(2px,1fr)); gap:3px; align-items:end; height:70px; margin-top:7px; border-bottom:1px solid #34425e; }
    .throughput-column { display:flex; align-items:flex-end; height:100%; min-width:0; }
    .throughput-stack { display:flex; flex-direction:column-reverse; justify-content:flex-start; width:100%; height:100%; min-height:0; }
    .throughput-segment { width:100%; min-height:0; opacity:.88; transition:height .35s ease; }
    .throughput-segment.green { background:var(--green); }
    .throughput-segment.red { background:var(--red); }
    .throughput-segment.gray { background:var(--gray); }
    .throughput-axis { display:flex; justify-content:space-between; margin-top:4px; color:#71809a; font-size:10px; }
    .legend { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; margin-top:12px; }
    .legend-item { display:grid; grid-template-columns:12px minmax(0,1fr); gap:9px; align-items:start; min-width:0; padding:11px; border:1px solid #253149; border-radius:10px; background:#111a2b; }
    .swatch { width:12px; height:12px; margin-top:4px; border-radius:3px; }
    .legend-label { font-size:13px; font-weight:800; }
    .legend-value { margin-top:2px; font-size:18px; font-weight:900; }
    .legend-detail { margin-top:2px; color:var(--muted); font-size:12px; }
    .meta { margin-top:11px; font-size:13px; }
    .ledger-banner { display:flex; flex-wrap:wrap; gap:8px 18px; align-items:center; padding:13px 16px; border:1px solid #2a3a57; border-radius:10px; background:#0a1224; font-size:13px; }
    .ledger-banner .ledger-label { color:#7dd3fc; font-weight:900; letter-spacing:.08em; text-transform:uppercase; font-size:11px; }
    .ledger-banner code { color:#cdd9ee; font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px; word-break:break-all; }
    .ledger-banner .ledger-stat { color:var(--muted); }
    .stage-worker { display:flex; flex-wrap:wrap; gap:8px 14px; align-items:center; margin-top:14px; padding:11px 13px; border:1px solid #253149; border-radius:10px; background:#0a1120; font-size:12px; }
    .stage-worker .wlabel { color:#7dd3fc; font-weight:900; letter-spacing:.06em; text-transform:uppercase; font-size:10px; }
    .stage-worker .wval { color:#cdd9ee; font-weight:700; }
    .stage-worker .wstate { padding:2px 9px; border-radius:999px; font-weight:800; font-size:11px; }
    .stage-worker .wstate.live { background:rgba(54,211,153,.16); color:var(--green); }
    .stage-worker .wstate.bad { background:rgba(251,113,133,.16); color:var(--red); }
    .stage-worker .wstate.warn { background:rgba(251,191,36,.16); color:var(--warn); }
    .stage-worker .werr { color:var(--red); }
    .health { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:12px; margin-top:14px; }
    .health-card { padding:15px 17px; }
    .health-name { color:var(--muted); font-size:12px; font-weight:900; letter-spacing:.08em; text-transform:uppercase; }
    .health-value { margin-top:6px; font-weight:800; overflow-wrap:anywhere; }
    .controls { margin-bottom:18px; padding:18px 20px; }
    .controls-header { display:flex; justify-content:space-between; gap:16px; align-items:flex-start; }
    .controls-title { margin:3px 0 0; font-size:20px; }
    .control-state { padding:5px 10px; border:1px solid var(--line); border-radius:999px; font-size:12px; font-weight:900; text-transform:uppercase; }
    .control-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:14px; margin-top:16px; }
    .control-group { min-width:0; padding:14px; border:1px solid #253149; border-radius:11px; background:#0d1423; }
    .control-label { color:var(--muted); font-size:12px; font-weight:900; letter-spacing:.07em; text-transform:uppercase; }
    .control-value { margin-top:5px; font-weight:800; overflow-wrap:anywhere; }
    .button-row { display:flex; flex-wrap:wrap; gap:8px; margin-top:11px; }
    button { border:1px solid #40506e; border-radius:8px; padding:8px 12px; background:#1a2740; color:var(--text); font:inherit; font-weight:800; cursor:pointer; }
    button:hover { background:#233453; }
    button:disabled { cursor:not-allowed; opacity:.5; }
    button.danger { border-color:#9f4055; background:#442333; }
    button.primary { border-color:#277d67; background:#174638; }
    .route-inputs { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:8px; margin-top:10px; }
    .route-inputs label { display:grid; gap:4px; color:var(--muted); font-size:11px; font-weight:800; text-transform:uppercase; }
    input[type=number],select { width:100%; border:1px solid #40506e; border-radius:7px; padding:7px 8px; background:#101a2c; color:var(--text); font:inherit; }
    input[type=checkbox] { accent-color:var(--green); }
    .model-inputs { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:8px; margin-top:10px; }
    .model-inputs label { display:grid; gap:4px; min-width:0; color:var(--muted); font-size:11px; font-weight:800; text-transform:uppercase; }
    .model-endpoint { overflow-wrap:anywhere; color:var(--muted); font-size:10px; font-weight:500; text-transform:none; }
    .breaker-settings { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:8px; margin-top:10px; }
    .breaker-settings label { display:grid; gap:4px; color:var(--muted); font-size:10px; font-weight:800; text-transform:uppercase; }
    .control-message { min-height:20px; margin-top:10px; color:var(--muted); font-size:12px; }
    .token-row { display:flex; gap:8px; align-items:center; margin-top:11px; }
    .token-row label { color:var(--muted); font-size:12px; font-weight:800; white-space:nowrap; }
    .token-row input { min-width:180px; border:1px solid #40506e; border-radius:7px; padding:7px 8px; background:#101a2c; color:var(--text); font:inherit; }
    footer { margin-top:14px; color:var(--muted); font-size:13px; }
    @media (max-width:760px) {
      .stage-header { flex-direction:column; }
      .stage-total { text-align:left; }
      .legend,.health,.control-grid { grid-template-columns:1fr; }
      .model-inputs { grid-template-columns:1fr; }
      .controls-header { flex-direction:column; }
      .equation { white-space:normal; }
    }
    @media (prefers-reduced-motion:reduce) { .segment { transition:none; } }
  </style>
</head>
<body><main>
  <header>
    <div><h1>SWE-gen pipeline</h1><div class="sub">Run <span id="run">—</span> · three stages, synchronized to the shared ledger</div></div>
    <div style="text-align:right"><div id="connection" class="live" role="status" aria-live="polite">● LIVE</div><div class="button-row" style="justify-content:flex-end"><button id="downloadFilteredPackButton" class="primary" type="button">Download accepted task pack</button></div><div id="exportStatus" class="meta">Export mirror starting…</div></div>
  </header>

  <section class="panel controls" aria-labelledby="controlsTitle">
    <div class="controls-header">
      <div><div class="stage-number">Slurm control</div><h2 id="controlsTitle" class="controls-title">Desired cluster state</h2><div class="sub">Changes are written atomically and applied by the background reconciler.</div></div>
      <div><div id="controlState" class="control-state warn">Unavailable</div><div class="token-row"><label for="controlToken">Control token</label><input id="controlToken" type="password" autocomplete="off" spellcheck="false" placeholder="Required to save"></div></div>
    </div>
    <div class="control-grid">
      <div class="control-group">
        <div class="control-label">Pause / resume</div>
        <div id="controlRuntime" class="control-value">Controller status unavailable</div>
        <div class="button-row"><button id="resumeButton" class="primary" type="button">Resume Slurm</button><button id="pauseButton" class="danger" type="button">Pause Slurm</button></div>
      </div>
      <form id="routeForm" class="control-group">
        <div class="control-label">Per-node concurrency</div>
        <div class="route-inputs">
          <label>SG<input id="routeSg" name="sg" type="number" min="0" max="16" step="1" required></label>
          <label>HK<input id="routeHk" name="hk" type="number" min="0" max="16" step="1" required></label>
          <label>DE<input id="routeDe" name="de" type="number" min="0" max="16" step="1" required></label>
        </div>
        <div id="desiredWorkers" class="meta">— desired workers</div>
        <div class="button-row"><button id="applyRoutesButton" type="submit">Apply concurrency</button></div>
      </form>
      <form id="modelForm" class="control-group">
        <div class="control-label">Claude model roles</div>
        <div id="modelCatalogStatus" class="control-value">Loading model catalog</div>
        <div class="model-inputs">
          <label>Opus<select id="modelOpus" name="opus" required></select><span id="modelOpusEndpoint" class="model-endpoint">—</span></label>
          <label>Sonnet<select id="modelSonnet" name="sonnet" required></select><span id="modelSonnetEndpoint" class="model-endpoint">—</span></label>
        </div>
        <div class="button-row"><button id="applyModelsButton" type="submit">Apply models</button></div>
      </form>
      <form id="breakerForm" class="control-group">
        <div class="control-label">Global API circuit breaker</div>
        <div id="breakerRuntime" class="control-value">No controller evidence</div>
        <label class="meta"><input id="breakerEnabled" type="checkbox"> enabled</label>
        <div class="breaker-settings">
          <label>Failures<input id="breakerThreshold" type="number" min="1" max="1000000" step="1" required></label>
          <label>Window (s)<input id="breakerWindow" type="number" min="1" max="86400" step="1" required></label>
          <label>Cooldown (s)<input id="breakerCooldown" type="number" min="0" max="604800" step="1" required></label>
        </div>
        <div class="button-row"><button id="applyBreakerButton" type="submit">Apply breaker</button></div>
      </form>
    </div>
    <div class="meta">Run: <span id="controlRun">—</span> · active plan: <span id="controlPlan">—</span></div>
    <div id="controlMessage" class="control-message" role="status" aria-live="polite"></div>
  </section>

  <div class="ledger-banner">
    <span class="ledger-label">Shared ledger</span>
    <code id="ledgerPath">—</code>
    <span class="ledger-stat" id="ledgerStat">—</span>
  </div>

  <section class="panel pipeline">
    <article class="stage">
      <div class="stage-header">
        <div><div class="stage-number">Stage 1</div><h2>SWEgen</h2><div class="sub">Generation: created tasks (success + failure) plus unprocessed</div></div>
        <div class="stage-total"><div id="stageSwegenTotal" class="count">— / —</div><div id="stageSwegenEquation" class="equation">—</div></div>
      </div>
      <div id="stageSwegenBar" class="stacked-bar" role="img" aria-label="SWEgen counts loading">
        <div id="stageSwegenGreenBar" class="segment green"></div><div id="stageSwegenRedBar" class="segment red"></div><div id="stageSwegenGrayBar" class="segment gray"></div>
      </div>
      <div class="throughput">
        <div class="throughput-header"><span>Processed per 15 minutes · last 6 hours</span><span id="stageOneThroughputSummary">—</span></div>
        <div id="stageOneThroughput" class="throughput-bars" role="img" aria-label="Stage I throughput loading"></div>
        <div class="throughput-axis"><span id="stageOneThroughputStart">—</span><span>UTC</span><span id="stageOneThroughputEnd">—</span></div>
      </div>
      <div class="legend">
        <div class="legend-item"><span class="swatch green"></span><div><div class="legend-label">Generated</div><div id="stageSwegenGreenValue" class="legend-value">—</div><div class="legend-detail">Created (reached Stage I)</div></div></div>
        <div class="legend-item"><span class="swatch red"></span><div><div class="legend-label">Errored</div><div id="stageSwegenRedValue" class="legend-value">—</div><div class="legend-detail">Generation failures</div></div></div>
        <div class="legend-item"><span class="swatch gray"></span><div><div class="legend-label">Unprocessed</div><div id="stageSwegenGrayValue" class="legend-value">—</div><div class="legend-detail">Not yet generated</div></div></div>
      </div>
      <div id="stageSwegenMeta" class="meta">—</div>
      <div id="stageSwegenWorker" class="stage-worker">—</div>
    </article>

    <article class="stage">
      <div class="stage-header">
        <div><div class="stage-number">Stage 2</div><h2>NOP/ORACLE</h2><div class="sub">Baseline validation: NOP=0 and Oracle=1 must both hold</div></div>
        <div class="stage-total"><div id="stageNopOracleTotal" class="count">— / —</div><div id="stageNopOracleEquation" class="equation">—</div></div>
      </div>
      <div id="stageNopOracleBar" class="stacked-bar" role="img" aria-label="NOP/ORACLE counts loading">
        <div id="stageNopOracleGreenBar" class="segment green"></div><div id="stageNopOracleRedBar" class="segment red"></div><div id="stageNopOracleGrayBar" class="segment gray"></div>
      </div>
      <div class="legend">
        <div class="legend-item"><span class="swatch green"></span><div><div class="legend-label">Oracle passed</div><div id="stageNopOracleGreenValue" class="legend-value">—</div><div class="legend-detail">NOP=0 &amp;&amp; Oracle=1</div></div></div>
        <div class="legend-item"><span class="swatch red"></span><div><div class="legend-label">Oracle failed</div><div id="stageNopOracleRedValue" class="legend-value">—</div><div class="legend-detail">NOP/Oracle fail or blacklisted</div></div></div>
        <div class="legend-item"><span class="swatch gray"></span><div><div class="legend-label">Unprocessed + errored</div><div id="stageNopOracleGrayValue" class="legend-value">—</div><div class="legend-detail">Pending, running, or errored</div></div></div>
      </div>
      <div id="stageNopOracleMeta" class="meta">—</div>
      <div id="stageNopOracleWorker" class="stage-worker">—</div>
    </article>

    <article class="stage">
      <div class="stage-header">
        <div><div class="stage-number">Stage 3</div><h2>Reward Hack Filter</h2><div class="sub">Runs only on Stage 2 green (oracle_passed) tasks</div></div>
        <div class="stage-total"><div id="stageRewardTotal" class="count">— / —</div><div id="stageRewardEquation" class="equation">—</div></div>
      </div>
      <div id="stageRewardBar" class="stacked-bar" role="img" aria-label="Reward hack counts loading">
        <div id="stageRewardGreenBar" class="segment green"></div><div id="stageRewardRedBar" class="segment red"></div><div id="stageRewardGrayBar" class="segment gray"></div>
      </div>
      <div class="legend">
        <div class="legend-item"><span class="swatch green"></span><div><div class="legend-label">Accepted</div><div id="stageRewardGreenValue" class="legend-value">—</div><div class="legend-detail">Reward-hack clean (non-hacking)</div></div></div>
        <div class="legend-item"><span class="swatch red"></span><div><div class="legend-label">Filtered</div><div id="stageRewardRedValue" class="legend-value">—</div><div class="legend-detail">Reward hacking detected</div></div></div>
        <div class="legend-item"><span class="swatch gray"></span><div><div class="legend-label">Unprocessed + errored</div><div id="stageRewardGrayValue" class="legend-value">—</div><div class="legend-detail">Pending, running, or errored</div></div></div>
      </div>
      <div id="stageRewardMeta" class="meta">—</div>
      <div id="stageRewardWorker" class="stage-worker">—</div>
    </article>
  </section>

  <footer aria-live="polite">Updated <span id="updated">—</span></footer>
</main>
<script>
const number=value=>{ const parsed=Number(value); return Number.isFinite(parsed)&&parsed>=0?parsed:0; };
const fmt=value=>number(value).toLocaleString();
const pct=(count,total)=>total?`${(number(count)/number(total)*100).toFixed(2)}%`:'0.00%';
const text=(id,value)=>{ document.getElementById(id).textContent=value; };
const healthLabel=state=>({
  live:'Live', running:'Running', idle:'Idle', draining:'Draining', stopping:'Stopping',
  stopped:'Stopped', stale:'Heartbeat stale', error:'Worker error', unavailable:'Unavailable',
  not_running:'Not running',
})[state]||'Unavailable';
const stateClass=state=>state==='live'||state==='running'||state==='idle'||state==='draining'?'live':state==='error'||state==='stale'?'bad':'warn';
function renderStage(prefix,stage,labels) {
  const denominator=number(stage.denominator); const green=number(stage.green); const red=number(stage.red); const gray=number(stage.gray); const sum=green+red+gray;
  text(`${prefix}Total`,`${fmt(green)} / ${fmt(denominator)}`); text(`${prefix}Equation`,`${fmt(green)} + ${fmt(red)} + ${fmt(gray)} = ${fmt(denominator)}`);
  [['Green',green,labels.green],['Red',red,labels.red],['Gray',gray,labels.gray]].forEach(([name,count,label])=>{
    const segment=document.getElementById(`${prefix}${name}Bar`); const percent=denominator?count/denominator*100:0;
    segment.style.width=`${Math.max(0,Math.min(percent,100))}%`; segment.title=`${label}: ${fmt(count)} (${pct(count,denominator)})`;
    text(`${prefix}${name}Value`,`${fmt(count)} · ${pct(count,denominator)}`);
  });
  document.getElementById(`${prefix}Bar`).setAttribute('aria-label',`${labels.stage}: ${fmt(green)} ${labels.green}, ${fmt(red)} ${labels.red}, ${fmt(gray)} ${labels.gray}; ${fmt(sum)} of ${fmt(denominator)}`);
}
function renderStageBreakdown(prefix,stage,parts) {
  const total=number(stage.total); const green=number(parts.green); const red=number(parts.red); const gray=number(parts.gray); const sum=green+red+gray;
  text(`${prefix}Total`,`${fmt(green)} / ${fmt(total)}`); text(`${prefix}Equation`,parts.equation||`${fmt(green)} + ${fmt(red)} + ${fmt(gray)} = ${fmt(total)}`);
  [['Green',green,parts.greenLabel||''],['Red',red,parts.redLabel||''],['Gray',gray,parts.grayLabel||'']].forEach(([name,count,label])=>{
    const segment=document.getElementById(`${prefix}${name}Bar`); const percent=total?count/total*100:0;
    segment.style.width=`${Math.max(0,Math.min(percent,100))}%`; segment.title=`${label}: ${fmt(count)} (${pct(count,total)})`;
    text(`${prefix}${name}Value`,`${fmt(count)} · ${pct(count,total)}`);
  });
  text(`${prefix}Meta`,parts.meta||'');
  document.getElementById(`${prefix}Bar`).setAttribute('aria-label',`${stage.name||''}: ${fmt(green)} ${parts.greenLabel||''}, ${fmt(red)} ${parts.redLabel||''}, ${fmt(gray)} ${parts.grayLabel||''}; ${fmt(sum)} of ${fmt(total)}`);
}
function workerBadge(state) {
  const label=healthLabel(state||'not_running');
  return `<span class="wstate ${stateClass(state||'not_running')}">${label}</span>`;
}
function renderWorkerRow(elementId,worker,opts) {
  const target=document.getElementById(elementId); if (!target) return;
  opts=opts||{};
  const state=worker?.state||'not_running';
  const active=worker?.active_count; const concurrency=worker?.concurrency;
  const capText=(active!=null||concurrency!=null)?`<span class="wlabel">Active</span> <span class="wval">${fmt(active??0)}/${fmt(concurrency??0)}</span>`:'';
  const modelText=worker?.model?`<span class="wlabel">Model</span> <span class="wval">${worker.model}</span>`:'';
  const nodeText=(opts.nodes!=null)?`<span class="wlabel">Nodes</span> <span class="wval">${fmt(opts.reachable??0)}/${fmt(opts.hosts??0)} reachable · ${fmt(opts.shards??0)} shards</span>`:'';
  const errText=worker?.error?`<span class="werr">⚠ ${worker.error}</span>`:'';
  const staleText=worker?.stale?`<span class="werr">heartbeat stale</span>`:'';
  target.innerHTML=`<span class="wlabel">Worker</span> ${workerBadge(state)} ${capText} ${modelText} ${nodeText} ${staleText} ${errText}`;
}
function renderThroughput(series) {
  const buckets=Array.isArray(series?.buckets)?series.buckets:[]; const target=document.getElementById('stageOneThroughput');
  const maximum=Math.max(1,...buckets.map(bucket=>number(bucket.processed))); const total=buckets.reduce((sum,bucket)=>sum+number(bucket.processed),0);
  const totals={green:0,red:0,gray:0}; buckets.forEach(bucket=>{ Object.keys(totals).forEach(color=>{ totals[color]+=number(bucket[color]); }); });
  target.replaceChildren(...buckets.map(bucket=>{
    const column=document.createElement('div'); column.className='throughput-column';
    const stack=document.createElement('div'); stack.className='throughput-stack';
    ['green','red','gray'].forEach(color=>{ const segment=document.createElement('div'); segment.className=`throughput-segment ${color}`; segment.style.height=`${number(bucket[color])/maximum*100}%`; stack.appendChild(segment); });
    column.title=`${String(bucket.start||'').slice(11,16)}–${String(bucket.end||'').slice(11,16)} UTC: ${fmt(bucket.processed)} outcomes (${fmt(bucket.green)} successful, ${fmt(bucket.red)} failed, ${fmt(bucket.gray)} unprocessed)`;
    column.appendChild(stack); return column;
  }));
  const latest=buckets.at(-1)||{}; text('stageOneThroughputSummary',`${fmt(latest.processed)} latest · ${fmt(total)} total`);
  const first=buckets[0]||{}; text('stageOneThroughputStart',String(first.start||'—').replace('T',' ').slice(5,16)); text('stageOneThroughputEnd',String(latest.end||'—').replace('T',' ').slice(5,16));
  target.setAttribute('aria-label',`Stage I throughput: ${fmt(totals.green)} successful, ${fmt(totals.red)} failed, ${fmt(totals.gray)} unprocessed in ${fmt(buckets.length)} ${fmt(series?.bucket_minutes||15)}-minute windows`);
}
let routeDirty=false;
let modelDirty=false;
let breakerDirty=false;
let controlBusy=false;
let modelCatalog=[];
const controlElement=id=>document.getElementById(id);
const integerValue=id=>Number.parseInt(controlElement(id).value,10);
const controlTokenKey='swegen-control-token';
try { controlElement('controlToken').value=sessionStorage.getItem(controlTokenKey)||''; } catch (error) { controlElement('controlToken').value=''; }
controlElement('controlToken').addEventListener('input',event=>{ const value=event.currentTarget.value; try { if (value) sessionStorage.setItem(controlTokenKey,value); else sessionStorage.removeItem(controlTokenKey); } catch (error) {} });
function setControlDisabled(disabled) {
  ['resumeButton','pauseButton','applyRoutesButton','applyModelsButton','applyBreakerButton','routeSg','routeHk','routeDe','modelOpus','modelSonnet','breakerEnabled','breakerThreshold','breakerWindow','breakerCooldown'].forEach(id=>{ controlElement(id).disabled=disabled; });
}
function desiredWorkerTotal() {
  const nodes=number(controlElement('routeForm').dataset.nodeCount||0);
  const perNode=['routeSg','routeHk','routeDe'].reduce((total,id)=>total+number(integerValue(id)),0);
  text('desiredWorkers',`${fmt(nodes*perNode)} desired workers · ${fmt(nodes)} nodes × ${fmt(perNode)} per node`);
}
function modelEndpoint(modelName) {
  const item=modelCatalog.find(candidate=>candidate.model_name===modelName); const endpoints=Array.isArray(item?.api_bases)?[...item.api_bases]:[];
  if (endpoints.length===0&&item?.api_base) endpoints.push(item.api_base);
  return endpoints.length?endpoints.join(' · '):'Endpoint unavailable';
}
function renderModelEndpoints() {
  text('modelOpusEndpoint',modelEndpoint(controlElement('modelOpus').value)); text('modelSonnetEndpoint',modelEndpoint(controlElement('modelSonnet').value));
}
function replaceModelOptions(selectId,selected) {
  const select=controlElement(selectId); const choices=[...modelCatalog];
  if (selected&&!choices.some(item=>item.model_name===selected)) choices.push({model_name:selected,api_base:null,api_bases:[],endpoint_count:0});
  select.replaceChildren(...choices.map(item=>{ const option=document.createElement('option'); option.value=item.model_name; const count=number(item.endpoint_count??item.api_bases?.length??(item.api_base?1:0)); option.textContent=count?`${item.model_name} — ${fmt(count)} endpoint${count===1?'':'s'}`:item.model_name; return option; }));
  select.value=selected||choices[0]?.model_name||'';
}
function renderControl(snapshot) {
  const available=Boolean(snapshot&&snapshot.available); const config=available?snapshot.config||{}:{}; const observed=snapshot?.observed||{};
  setControlDisabled(!available||controlBusy);
  if (!available) {
    text('controlState','Unavailable'); controlElement('controlState').className='control-state bad';
    text('controlRuntime','Control config unavailable'); text('breakerRuntime','No controller evidence');
    text('controlMessage',snapshot?.error||'Control API unavailable'); return;
  }
  const desired=config.desired_state||'paused'; const slurm=config.slurm||{}; const routes=slurm.routes||{}; const models=config.models||{}; const breaker=config.circuit_breaker||{};
  const observedController=observed.controller||{}; const observedRun=observed.run||{}; const observedBreaker=observed.circuit_breaker||{}; const observedSlurm=observed.slurm||{};
  const controllerState=observedController.state||observed.state||'not_running';
  const appliedState=observedController.applied_state||observed.applied_state||observed.desired_state||'unknown';
  const tripped=observedBreaker.tripped===true||observedBreaker.state==='open'; const resetPending=snapshot?.breaker_reset_pending===true;
  text('controlState',resetPending?'Reset requested':tripped?'Circuit open':desired); controlElement('controlState').className=`control-state ${resetPending?'warn':tripped?'bad':desired==='running'?'live':'warn'}`;
  const activeWorkers=observedSlurm.active_workers??observed.active_workers;
  const activeText=activeWorkers==null?'':` · ${fmt(activeWorkers)} active workers`;
  text('controlRuntime',`Controller ${healthLabel(controllerState).toLowerCase()} · applied ${appliedState}${activeText}`);
  text('controlRun',`${config.run?.name||'—'} / ${config.run?.revision||'—'}`); text('controlPlan',observedRun.active_plan||observed.active_plan||config.run?.plan_path||'—');
  controlElement('routeForm').dataset.nodeCount=String(slurm.node_count||0);
  if (!routeDirty) {
    controlElement('routeSg').value=String(routes.sg??0); controlElement('routeHk').value=String(routes.hk??0); controlElement('routeDe').value=String(routes.de??0);
  }
  desiredWorkerTotal();
  modelCatalog=Array.isArray(snapshot.model_catalog)?snapshot.model_catalog.filter(item=>item&&typeof item.model_name==='string'):[];
  const endpointCount=modelCatalog.reduce((sum,item)=>sum+number(item.endpoint_count??item.api_bases?.length??(item.api_base?1:0)),0);
  text('modelCatalogStatus',snapshot.model_catalog_error||`${fmt(modelCatalog.length)} models · ${fmt(endpointCount)} model endpoints from ${config.run?.models_yaml||'models.yaml'}`);
  if (!modelDirty) { replaceModelOptions('modelOpus',models.opus||''); replaceModelOptions('modelSonnet',models.sonnet||''); }
  renderModelEndpoints();
  const failureCount=observedBreaker.failure_count??observedBreaker.failures_in_window??observedBreaker.api_failures??0;
  const breakerState=resetPending?'RESET REQUESTED':tripped?'OPEN':observedBreaker.state||'closed';
  text('breakerRuntime',`${breakerState} · ${fmt(failureCount)}/${fmt(breaker.failure_threshold)} failures in ${fmt(breaker.window_seconds)}s`);
  if (!breakerDirty) {
    controlElement('breakerEnabled').checked=Boolean(breaker.enabled);
    controlElement('breakerThreshold').value=String(breaker.failure_threshold??10);
    controlElement('breakerWindow').value=String(breaker.window_seconds??300);
    controlElement('breakerCooldown').value=String(breaker.cooldown_seconds??900);
  }
  if (snapshot.observed_error) text('controlMessage',snapshot.observed_error); else if (!controlBusy) text('controlMessage','');
}
async function refreshControl() {
  if (controlBusy) return;
  try {
    const response=await fetch('/api/control',{cache:'no-store'}); const body=await response.json();
    if (!response.ok) throw new Error(body.error||`HTTP ${response.status}`); renderControl(body);
  } catch (error) {
    setControlDisabled(true); text('controlState','Unavailable'); controlElement('controlState').className='control-state bad'; text('controlMessage',String(error));
  }
}
async function updateControl(patch,successMessage) {
  const token=controlElement('controlToken').value;
  if (!token) { text('controlMessage','Enter the control token before changing Slurm state.'); controlElement('controlToken').focus(); return; }
  controlBusy=true; setControlDisabled(true); text('controlMessage','Saving desired state…');
  try {
    const response=await fetch('/api/control',{method:'POST',headers:{'Content-Type':'application/json','X-SWEGEN-Control-Token':token},body:JSON.stringify(patch)});
    const body=await response.json(); if (!response.ok) throw new Error(body.error||`HTTP ${response.status}`);
    routeDirty=false; modelDirty=false; breakerDirty=false; controlBusy=false; renderControl(body); text('controlMessage',successMessage);
  } catch (error) {
    controlBusy=false; setControlDisabled(false); text('controlMessage',String(error));
  }
}
controlElement('resumeButton').addEventListener('click',()=>updateControl({desired_state:'running',reset_circuit_breaker:true},'Resume and circuit-breaker reset requested; the reconciler will converge the cluster.'));
controlElement('pauseButton').addEventListener('click',()=>updateControl({desired_state:'paused'},'Pause requested; the reconciler will suspend Slurm jobs.'));
['routeSg','routeHk','routeDe'].forEach(id=>controlElement(id).addEventListener('input',()=>{ routeDirty=true; desiredWorkerTotal(); }));
controlElement('routeForm').addEventListener('submit',event=>{ event.preventDefault(); if (!event.currentTarget.reportValidity()) return; const routes={sg:integerValue('routeSg'),hk:integerValue('routeHk'),de:integerValue('routeDe')}; if (routeDirty&&!window.confirm(`Apply SG/HK/DE ${routes.sg}/${routes.hk}/${routes.de}? This collects results, cancels the current Stage-I allocations, and relaunches all four nodes.`)) return; updateControl({routes},'Concurrency update saved.'); });
['modelOpus','modelSonnet'].forEach(id=>controlElement(id).addEventListener('change',()=>{ modelDirty=true; renderModelEndpoints(); }));
controlElement('modelForm').addEventListener('submit',event=>{ event.preventDefault(); if (!event.currentTarget.reportValidity()) return; const models={opus:controlElement('modelOpus').value,sonnet:controlElement('modelSonnet').value}; if (modelDirty&&!window.confirm(`Apply Opus ${models.opus} and Sonnet ${models.sonnet}? This collects results, cancels the current Stage-I allocations, and relaunches all four nodes.`)) return; updateControl({models},'Model role update saved.'); });
['breakerEnabled','breakerThreshold','breakerWindow','breakerCooldown'].forEach(id=>controlElement(id).addEventListener('input',()=>{ breakerDirty=true; }));
controlElement('breakerForm').addEventListener('submit',event=>{ event.preventDefault(); if (!event.currentTarget.reportValidity()) return; updateControl({circuit_breaker:{enabled:controlElement('breakerEnabled').checked,failure_threshold:integerValue('breakerThreshold'),window_seconds:integerValue('breakerWindow'),cooldown_seconds:integerValue('breakerCooldown')}},'Circuit-breaker settings saved.'); });
async function refresh() {
  try {
    const response=await fetch('/api/status',{cache:'no-store'}); if (!response.ok) throw new Error(response.status);
    const data=await response.json(); const stages=data.stages||{}; const health=data.health||{}; const ledger=data.ledger||{};
    text('run',data.run||'—');
    // Ledger banner — the single source of truth for Stage 2/3 counts.
    text('ledgerPath',ledger.path||'not found');
    const ledgerStatParts=[];
    if (ledger.size_bytes!=null) ledgerStatParts.push(`${(number(ledger.size_bytes)/1048576).toFixed(1)} MB`);
    if (ledger.mtime) ledgerStatParts.push(`modified ${String(ledger.mtime).replace('T',' ').slice(0,19)} UTC`);
    text('ledgerStat',ledgerStatParts.join(' · ')||'—');

    // Stage 1 — SWEgen: generated + unprocessed + errored = total.
    const swegen=stages.swegen||{};
    renderStageBreakdown('stageSwegen',swegen,{
      green:swegen.generated, red:swegen.errored, gray:swegen.unprocessed,
      greenLabel:'Generated', redLabel:'Errored', grayLabel:'Unprocessed',
      equation:`${fmt(swegen.generated)} generated + ${fmt(swegen.unprocessed)} unprocessed + ${fmt(swegen.errored)} errored = ${fmt(swegen.total)}`,
      meta:`${fmt(swegen.generated)} generated of ${fmt(swegen.total)} · ${pct(swegen.generated,swegen.total)} complete`,
    });
    renderThroughput(data.stage_i_throughput||{});
    renderWorkerRow('stageSwegenWorker',swegen.worker,{});

    // Stage 2 — NOP/ORACLE: oracle_passed + oracle_failed + errored + unprocessed = generated.
    const nopOracle=stages.nop_oracle||{}; const nop=nopOracle.nop||{}; const oracle=nopOracle.oracle||{};
    renderStageBreakdown('stageNopOracle',nopOracle,{
      green:nopOracle.oracle_passed, red:nopOracle.oracle_failed, gray:(number(nopOracle.errored)+number(nopOracle.unprocessed)),
      greenLabel:'Oracle passed', redLabel:'Oracle failed', grayLabel:'Errored + unprocessed',
      equation:`${fmt(nopOracle.oracle_passed)} oracle_passed + ${fmt(nopOracle.oracle_failed)} oracle_failed + ${fmt(nopOracle.errored)} errored + ${fmt(nopOracle.unprocessed)} unprocessed = ${fmt(nopOracle.total)}`,
      meta:`NOP: ${fmt(nop.pass)} pass · ${fmt(nop.fail)} fail · ${fmt(nop.blacklisted)} blacklisted · ${fmt(nop.pending)} pending | Oracle: ${fmt(oracle.pass)} pass · ${fmt(oracle.fail)} fail · ${fmt(oracle.blacklisted)} blacklisted · ${fmt(oracle.pending)} pending`,
    });
    renderWorkerRow('stageNopOracleWorker',nopOracle.worker,{nodes:true,reachable:nopOracle.worker?.reachable_count,hosts:nopOracle.worker?.host_count,shards:nopOracle.worker?.shard_count});

    // Stage 3 — Reward Hack Filter: accepted + filtered + unprocessed + errored = oracle_passed.
    const reward=stages.reward_hack||{};
    renderStageBreakdown('stageReward',reward,{
      green:reward.accepted, red:reward.filtered, gray:(number(reward.unprocessed)+number(reward.errored)),
      greenLabel:'Accepted', redLabel:'Filtered', grayLabel:'Unprocessed + errored',
      equation:`${fmt(reward.accepted)} accepted + ${fmt(reward.filtered)} filtered + ${fmt(reward.unprocessed)} unprocessed + ${fmt(reward.errored)} errored = ${fmt(reward.total)}`,
      meta:`${fmt(reward.accepted)} accepted of ${fmt(reward.total)} oracle_passed · ${pct(reward.accepted,reward.total)} clean`,
    });
    renderWorkerRow('stageRewardWorker',reward.worker,{});

    // Health cards — one per stage, sourced from the live worker blocks.
    const generation=health.generation||{}; const generationState=generation.state||'unavailable';
    text('stageOneHealth',`${healthLabel(generationState)} collector · ${fmt(generation.active_workers)} active generators · ${fmt(generation.live_nodes)}/${fmt(generation.total_nodes)} Slurm nodes live`);
    document.getElementById('stageOneHealth').className=`health-value ${stateClass(generationState)}`;
    const baselineWorker=nopOracle.worker||{}; const baselineState=baselineWorker.state||'not_running';
    text('stageTwoHealth',`${healthLabel(baselineState)} · ${fmt(baselineWorker.active_count)}/${fmt(baselineWorker.concurrency)} active · ${fmt(baselineWorker.reachable_count)}/${fmt(baselineWorker.host_count)} nodes · ${fmt(baselineWorker.shard_count)} shards`);
    document.getElementById('stageTwoHealth').className=`health-value ${stateClass(baselineState)}`;
    const rewardWorker=reward.worker||{}; const rewardState=rewardWorker.state||'not_running';
    text('stageRewardHealth',`${healthLabel(rewardState)} · ${fmt(rewardWorker.active_count)}/${fmt(rewardWorker.concurrency)} active${rewardWorker.model?` · ${rewardWorker.model}`:''}`);
    document.getElementById('stageRewardHealth').className=`health-value ${stateClass(rewardState)}`;
    text('updated',data.updated_at||'—'); text('connection','● LIVE'); document.getElementById('connection').className='live';
  } catch (error) { text('connection','● DISCONNECTED'); document.getElementById('connection').className='bad'; }
}
document.getElementById('downloadFilteredPackButton').addEventListener('click',()=>{ window.location.assign('/api/export/reward-hack-accepted.tar.gz'); });
async function refreshExportStatus() {
  try {
    const response=await fetch('/api/export/status',{cache:'no-store'}); const value=await response.json();
    if (!response.ok) throw new Error(value.error||`HTTP ${response.status}`);
    const issueCount=(Array.isArray(value.errors)?value.errors.length:0)+(Array.isArray(value.warnings)?value.warnings.length:0);
    const issues=issueCount?` · ${fmt(issueCount)} sync warning/error(s)`:'';
    text('exportStatus',`${value.state||'unknown'} · ${fmt(value.pack_count||0)}/5 packs retained${issues}`);
  } catch (error) { text('exportStatus','Export mirror unavailable'); }
}
refresh(); refreshControl(); refreshExportStatus(); setInterval(refresh,5000); setInterval(refreshControl,5000); setInterval(refreshExportStatus,5000);
</script></body></html>"""


class ControlConfigError(ValueError):
    """A desired Slurm control configuration is missing or unsafe."""


def _required_mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ControlConfigError(f"{field} must be a mapping")
    return value


def _required_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ControlConfigError(f"{field} must be a non-empty string")
    return value.strip()


def _bounded_integer(value: Any, field: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ControlConfigError(f"{field} must be an integer")
    if not minimum <= value <= maximum:
        raise ControlConfigError(f"{field} must be between {minimum} and {maximum}")
    return value


def _reject_unknown_fields(value: dict[str, Any], allowed: set[str], field: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ControlConfigError(f"{field} has unsupported fields: {', '.join(unknown)}")


def validate_control_config(value: Any) -> dict[str, Any]:
    """Validate and normalize the desired-state file consumed by the reconciler.

    The public dashboard deliberately accepts only this small schema. In
    particular, legacy proxy URLs and credentials can never be echoed through
    the API or copied into a rewritten configuration.
    """
    root = _required_mapping(value, "config")
    _reject_unknown_fields(
        root,
        {
            "version",
            "desired_state",
            "run",
            "models",
            "slurm",
            "controller",
            "circuit_breaker",
            "metadata",
        },
        "config",
    )
    version = _bounded_integer(root.get("version"), "version", 1, 1)
    desired_state = _required_string(root.get("desired_state"), "desired_state").lower()
    if desired_state not in {"running", "paused"}:
        raise ControlConfigError("desired_state must be 'running' or 'paused'")

    run = _required_mapping(root.get("run"), "run")
    _reject_unknown_fields(
        run,
        {"name", "dir", "input_jsonl", "models_yaml", "plan_path", "revision"},
        "run",
    )
    normalized_run = {
        "name": _required_string(run.get("name"), "run.name"),
        "dir": _required_string(run.get("dir"), "run.dir"),
        "input_jsonl": _required_string(run.get("input_jsonl"), "run.input_jsonl"),
        "models_yaml": _required_string(run.get("models_yaml"), "run.models_yaml"),
        "plan_path": _required_string(run.get("plan_path"), "run.plan_path"),
        "revision": _required_string(run.get("revision"), "run.revision"),
    }

    models = _required_mapping(root.get("models", DEFAULT_CONTROL_MODELS), "models")
    _reject_unknown_fields(models, set(CONTROL_MODEL_ROLES), "models")
    normalized_models = {
        role: _required_string(models.get(role), f"models.{role}")
        for role in CONTROL_MODEL_ROLES
    }

    slurm = _required_mapping(root.get("slurm"), "slurm")
    _reject_unknown_fields(slurm, {"node_count", "routes"}, "slurm")
    node_count = _bounded_integer(slurm.get("node_count"), "slurm.node_count", 1, 4)
    if node_count != 4:
        raise ControlConfigError("the current Slurm plan requires slurm.node_count to be 4")
    routes = _required_mapping(slurm.get("routes"), "slurm.routes")
    _reject_unknown_fields(routes, set(CONTROL_ROUTE_NAMES), "slurm.routes")
    normalized_routes = {
        route: _bounded_integer(
            routes.get(route),
            f"slurm.routes.{route}",
            0,
            CONTROL_ROUTE_MAX_CONCURRENCY,
        )
        for route in CONTROL_ROUTE_NAMES
    }
    if desired_state == "running" and not sum(normalized_routes.values()):
        raise ControlConfigError("a running topology must have at least one route worker")

    controller = _required_mapping(root.get("controller"), "controller")
    _reject_unknown_fields(
        controller,
        {"poll_interval_seconds", "status_path"},
        "controller",
    )
    normalized_controller = {
        "poll_interval_seconds": _bounded_integer(
            controller.get("poll_interval_seconds"),
            "controller.poll_interval_seconds",
            1,
            3600,
        ),
        "status_path": _required_string(controller.get("status_path"), "controller.status_path"),
    }

    breaker = _required_mapping(root.get("circuit_breaker"), "circuit_breaker")
    _reject_unknown_fields(
        breaker,
        {"enabled", "window_seconds", "failure_threshold", "cooldown_seconds"},
        "circuit_breaker",
    )
    enabled = breaker.get("enabled")
    if not isinstance(enabled, bool):
        raise ControlConfigError("circuit_breaker.enabled must be true or false")
    normalized_breaker = {
        "enabled": enabled,
        "window_seconds": _bounded_integer(
            breaker.get("window_seconds"),
            "circuit_breaker.window_seconds",
            1,
            86_400,
        ),
        "failure_threshold": _bounded_integer(
            breaker.get("failure_threshold"),
            "circuit_breaker.failure_threshold",
            1,
            1_000_000,
        ),
        "cooldown_seconds": _bounded_integer(
            breaker.get("cooldown_seconds"),
            "circuit_breaker.cooldown_seconds",
            0,
            604_800,
        ),
    }

    metadata_value = root.get("metadata", {})
    metadata = _required_mapping(metadata_value, "metadata")
    return {
        "version": version,
        "desired_state": desired_state,
        "run": normalized_run,
        "models": normalized_models,
        "slurm": {"node_count": node_count, "routes": normalized_routes},
        "controller": normalized_controller,
        "circuit_breaker": normalized_breaker,
        "metadata": copy.deepcopy(metadata),
    }


def default_control_config(run_dir: Path, input_jsonl: Path) -> dict[str, Any]:
    """Return safe defaults for a new dashboard/controller deployment."""
    resolved_run_dir = run_dir.resolve()
    return {
        "version": CONTROL_CONFIG_VERSION,
        "desired_state": "running",
        "run": {
            "name": resolved_run_dir.name,
            "dir": str(resolved_run_dir),
            "input_jsonl": str(input_jsonl.resolve()),
            "models_yaml": "/data/work/alex/SWE-gen/models.yaml",
            "plan_path": str(resolved_run_dir / "slurm-stage1-r9-4n-plan.json"),
            "revision": "r9",
        },
        "models": copy.deepcopy(DEFAULT_CONTROL_MODELS),
        "slurm": {
            "node_count": 4,
            "routes": dict.fromkeys(CONTROL_ROUTE_NAMES, 4),
        },
        "controller": {
            "poll_interval_seconds": 15,
            "status_path": str(resolved_run_dir / ".slurm-control" / "status.json"),
        },
        "circuit_breaker": {
            "enabled": True,
            "window_seconds": 300,
            "failure_threshold": 10,
            "cooldown_seconds": 900,
        },
        "metadata": {},
    }


def _public_api_base(value: Any) -> str | None:
    """Return a display-safe endpoint URL with userinfo/query data removed."""
    raw = _string_value(value)
    if raw is None:
        return None
    try:
        parsed = urlsplit(raw)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return None
    if parsed.scheme not in {"http", "https"} or hostname is None:
        return None
    host = f"[{hostname}]" if ":" in hostname else hostname
    authority = f"{host}:{port}" if port is not None else host
    return f"{parsed.scheme}://{authority}"


def load_model_catalog(models_yaml: Path) -> tuple[list[dict[str, Any]], str | None]:
    """Return exact model names with every credential-free endpoint alternative."""
    try:
        root = yaml.safe_load(models_yaml.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return [], "models YAML does not exist"
    except (OSError, yaml.YAMLError):
        return [], "models YAML could not be read"
    if not isinstance(root, dict) or not isinstance(root.get("model_list"), list):
        return [], "models YAML must contain a model_list"

    endpoints_by_name: dict[str, list[str]] = {}
    for entry in root["model_list"]:
        if not isinstance(entry, dict):
            continue
        model_name = _string_value(entry.get("model_name"))
        if model_name is None:
            continue
        litellm_params = entry.get("litellm_params")
        api_base = _public_api_base(
            litellm_params.get("api_base")
            if isinstance(litellm_params, dict)
            else None
        )
        endpoints = endpoints_by_name.setdefault(model_name, [])
        if api_base is not None and api_base not in endpoints:
            endpoints.append(api_base)
    return [
        {
            "model_name": name,
            "api_base": endpoints[0] if endpoints else None,
            "api_bases": endpoints,
            "endpoint_count": len(endpoints),
        }
        for name, endpoints in sorted(endpoints_by_name.items())
    ], None


def _atomic_write_yaml(path: Path, value: dict[str, Any]) -> None:
    """Durably replace a YAML file without exposing a partially written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    descriptor = -1
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            descriptor = -1
            yaml.safe_dump(value, stream, sort_keys=False, default_flow_style=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
        try:
            directory_descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        except OSError:
            return
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def ensure_control_token(path: Path) -> str:
    """Load or create a private bearer token for mutating control requests."""
    path.parent.mkdir(parents=True, exist_ok=True)
    generated = secrets.token_urlsafe(32)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        descriptor = -1
    if descriptor >= 0:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(generated + "\n")
            stream.flush()
            os.fsync(stream.fileno())
    try:
        token = path.read_text(encoding="utf-8").strip()
    except OSError as error:
        raise ControlConfigError("control token could not be read") from error
    if len(token) < 16:
        raise ControlConfigError("control token must contain at least 16 characters")
    try:
        path.chmod(0o600)
    except OSError as error:
        raise ControlConfigError("control token permissions could not be secured") from error
    return token


@contextmanager
def _exclusive_config_lock(config_path: Path):
    """Serialize read/modify/replace cycles with the external reconciler."""
    lock_path = config_path.with_name(config_path.name + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as stream:
        lock_path.chmod(0o600)
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


class ControlConfigStore:
    """Thread-safe, desired-state-only access for the dashboard control API."""

    def __init__(self, path: Path, initial_config: dict[str, Any] | None = None):
        self.path = path
        self.initial_config = (
            validate_control_config(initial_config) if initial_config is not None else None
        )
        self._lock = threading.Lock()

    def ensure_exists(self) -> None:
        """Create a new config, but never overwrite malformed or legacy data."""
        if self.path.exists() or self.initial_config is None:
            return
        with self._lock, _exclusive_config_lock(self.path):
            if not self.path.exists():
                _atomic_write_yaml(self.path, self.initial_config)

    def load(self) -> dict[str, Any]:
        try:
            raw = yaml.safe_load(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError as error:
            raise ControlConfigError(f"control config does not exist: {self.path}") from error
        except (OSError, yaml.YAMLError) as error:
            raise ControlConfigError("control config could not be read") from error
        return validate_control_config(raw)

    def _load_observed(self, config: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
        status_path = Path(config["controller"]["status_path"])
        if not status_path.is_absolute():
            status_path = self.path.parent / status_path
        try:
            value = json.loads(status_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}, f"controller status does not exist: {status_path}"
        except (OSError, json.JSONDecodeError):
            return {}, "controller status could not be read"
        if not isinstance(value, dict):
            return {}, "controller status must contain a JSON object"
        return value, None

    def snapshot(self) -> dict[str, Any]:
        try:
            config = self.load()
        except ControlConfigError as error:
            return {
                "available": False,
                "config_path": str(self.path),
                "error": str(error),
                "config": None,
                "observed": {},
                "observed_error": None,
                "model_catalog": [],
                "model_catalog_error": None,
                "desired_total_workers": 0,
                "breaker_reset_pending": False,
            }
        observed, observed_error = self._load_observed(config)
        observed_breaker = observed.get("circuit_breaker", {})
        if not isinstance(observed_breaker, dict):
            observed_breaker = {}
        reset_requested_at = _utc_timestamp(
            config.get("metadata", {}).get("circuit_breaker_reset_requested_at")
        )
        tripped_at = _utc_timestamp(observed_breaker.get("tripped_at"))
        observed_tripped = (
            observed_breaker.get("tripped") is True
            or observed_breaker.get("state") == "open"
        )
        breaker_reset_pending = bool(
            config["desired_state"] == "running"
            and observed_tripped
            and reset_requested_at is not None
            and (tripped_at is None or reset_requested_at >= tripped_at)
        )
        models_yaml = Path(config["run"]["models_yaml"])
        if not models_yaml.is_absolute():
            models_yaml = self.path.parent / models_yaml
        model_catalog, model_catalog_error = load_model_catalog(models_yaml)
        routes = config["slurm"]["routes"]
        desired_total = config["slurm"]["node_count"] * sum(routes.values())
        return {
            "available": True,
            "config_path": str(self.path),
            "error": None,
            "config": config,
            "observed": observed,
            "observed_error": observed_error,
            "model_catalog": model_catalog,
            "model_catalog_error": model_catalog_error,
            "desired_total_workers": desired_total,
            "breaker_reset_pending": breaker_reset_pending,
        }

    def update(self, patch: Any) -> dict[str, Any]:
        update = _required_mapping(patch, "request")
        _reject_unknown_fields(
            update,
            {
                "desired_state",
                "routes",
                "models",
                "circuit_breaker",
                "reset_circuit_breaker",
            },
            "request",
        )
        if not update:
            raise ControlConfigError("request must contain a control change")
        with self._lock, _exclusive_config_lock(self.path):
            current = self.load()
            candidate = copy.deepcopy(current)
            if "desired_state" in update:
                candidate["desired_state"] = update["desired_state"]
            if "routes" in update:
                route_patch = _required_mapping(update["routes"], "routes")
                _reject_unknown_fields(route_patch, set(CONTROL_ROUTE_NAMES), "routes")
                if not route_patch:
                    raise ControlConfigError("routes must contain at least one route")
                candidate["slurm"]["routes"].update(route_patch)
            if "models" in update:
                model_patch = _required_mapping(update["models"], "models")
                _reject_unknown_fields(model_patch, set(CONTROL_MODEL_ROLES), "models")
                if not model_patch:
                    raise ControlConfigError("models must contain at least one role")
                candidate["models"].update(model_patch)
            if "circuit_breaker" in update:
                breaker_patch = _required_mapping(update["circuit_breaker"], "circuit_breaker")
                _reject_unknown_fields(
                    breaker_patch,
                    {"enabled", "window_seconds", "failure_threshold", "cooldown_seconds"},
                    "circuit_breaker",
                )
                if not breaker_patch:
                    raise ControlConfigError("circuit_breaker must contain at least one setting")
                candidate["circuit_breaker"].update(breaker_patch)
            reset_circuit_breaker = update.get("reset_circuit_breaker", False)
            if "reset_circuit_breaker" in update and reset_circuit_breaker is not True:
                raise ControlConfigError("reset_circuit_breaker must be true when supplied")
            if reset_circuit_breaker and candidate["desired_state"] != "running":
                raise ControlConfigError(
                    "reset_circuit_breaker requires desired_state to be running"
                )
            metadata = candidate.setdefault("metadata", {})
            updated_at = datetime.now(UTC).isoformat(timespec="seconds")
            metadata["updated_at"] = updated_at
            metadata["updated_by"] = "dashboard"
            if reset_circuit_breaker:
                metadata["circuit_breaker_reset_requested_at"] = updated_at
            normalized = validate_control_config(candidate)
            _atomic_write_yaml(self.path, normalized)
            return normalized


class TaskExportError(RuntimeError):
    """The requested task pack could not be built completely."""


def _reward_hack_records(
    run_dir: Path, *, accepted: bool
) -> dict[str, dict[str, Any]]:
    """Return Stage-I tasks accepted or rejected by the reward-hack check."""
    latest, _order = collect_latest_statuses(run_dir)
    postchecks = load_authoritative_postchecks(run_dir)
    reward_backfills = load_latest_reward_backfills(run_dir)
    postchecks = merge_reward_backfill_evidence(postchecks, reward_backfills)

    selected: dict[str, dict[str, Any]] = {}
    for instance, generation in latest.items():
        if generation.get("status") != "success":
            continue
        record = postchecks.get(instance)
        if not isinstance(record, dict):
            continue
        nop = record.get("nop") if isinstance(record.get("nop"), dict) else {}
        oracle = record.get("oracle") if isinstance(record.get("oracle"), dict) else {}
        reward = (
            record.get("reward_hack")
            if isinstance(record.get("reward_hack"), dict)
            else {}
        )
        baseline_valid = (
            nop.get("state") == "pass"
            and _reward_matches(nop.get("reward"), 0)
            and oracle.get("state") == "pass"
            and _reward_matches(oracle.get("reward"), 1)
        )
        # Current workers persist state=fail for hacking. The state=pass form
        # is retained for compatibility with older ledgers that stored the
        # verdict separately as is_hacking=true.
        if accepted:
            reward_verdict = (
                reward.get("state") == "pass"
                and reward.get("is_hacking") is False
            )
        else:
            reward_verdict = (
                reward.get("is_hacking") is True
                and reward.get("state") in {"fail", "pass"}
            )
        if baseline_valid and reward_verdict:
            selected[instance] = {
                "instance": instance,
                "source_node": generation.get("node") or record.get("source_node"),
                "generation_timestamp": generation.get("timestamp"),
                "postcheck_timestamp": record.get("timestamp"),
                "reason": reward.get("reason"),
                "reward_hack": dict(reward),
            }
    return selected


def reward_hack_accepted_records(run_dir: Path) -> dict[str, dict[str, Any]]:
    """Return successful Stage-I tasks accepted by the reward-hack check."""
    return _reward_hack_records(run_dir, accepted=True)


def reward_hack_filtered_records(run_dir: Path) -> dict[str, dict[str, Any]]:
    """Return successful Stage-I tasks rejected by the reward-hack check."""
    return _reward_hack_records(run_dir, accepted=False)


def _safe_export_part(value: str, field: str) -> str:
    if not value or value in {".", ".."} or Path(value).name != value:
        raise TaskExportError(f"unsafe {field}: {value!r}")
    return value


def _load_export_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def export_plan_paths(run_dir: Path) -> list[Path]:
    """Return the active plan first, followed by recent fallback plans."""
    paths: list[Path] = []
    controller_status = _load_export_json(run_dir / ".slurm-control" / "status.json")
    if controller_status:
        run_status = controller_status.get("run")
        active_plan = run_status.get("active_plan") if isinstance(run_status, dict) else None
        if isinstance(active_plan, str):
            candidate = Path(active_plan).resolve()
            if candidate.is_file():
                paths.append(candidate)

    candidates = list(run_dir.glob("slurm-stage1-*-4n-plan.json"))
    candidates.extend(run_dir.glob("slurm-plan.json"))
    candidates = sorted(
        {path.resolve() for path in candidates if path.is_file()},
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return paths + [path for path in candidates if path not in paths]


def export_node_records(run_dir: Path) -> dict[str, dict[str, Any]]:
    """Resolve node IPs and remote run directories from the current plans."""
    records: dict[str, dict[str, Any]] = {}
    for plan_path in export_plan_paths(run_dir):
        plan = _load_export_json(plan_path)
        if not plan:
            continue
        run_name = str(plan.get("run_name") or run_dir.name)
        for value in plan.get("nodes", []):
            if not isinstance(value, dict):
                continue
            node = value.get("node")
            if not isinstance(node, str) or not node:
                continue
            if node in records:
                continue
            remote_run_dir = value.get("remote_run_dir")
            if not isinstance(remote_run_dir, str) or not remote_run_dir:
                remote_workspace = value.get("remote_workspace")
                if isinstance(remote_workspace, str) and remote_workspace:
                    remote_run_dir = f"{remote_workspace.rstrip('/')}/runs/{run_name}"
            if not isinstance(remote_run_dir, str) or not remote_run_dir.startswith("/"):
                continue
            node_ip = value.get("node_ip")
            records[node] = {
                "node": node,
                "node_ip": node_ip if isinstance(node_ip, str) and node_ip else node,
                "remote_run_dir": remote_run_dir,
            }
    return records


class TaskExportManager:
    """Mirror node-local tasks and build bounded reward-hack task packs."""

    def __init__(
        self,
        run_dir: Path,
        export_dir: Path | None = None,
        *,
        rsync_runner: Any | None = None,
    ) -> None:
        self.run_dir = run_dir.resolve()
        self.export_dir = (export_dir or self.run_dir / TASK_EXPORT_DIR_NAME).resolve()
        self.sources_dir = self.export_dir / TASK_EXPORT_SOURCE_DIR_NAME
        self.packs_dir = self.export_dir / TASK_EXPORT_PACK_DIR_NAME
        self._operation_lock = threading.Lock()
        self._status_lock = threading.Lock()
        self._rsync_runner = rsync_runner or subprocess.run
        self._status: dict[str, Any] = {
            "state": "not_started",
            "last_sync": None,
            "nodes": [],
            "errors": [],
            "warnings": [],
        }

    def status(self) -> dict[str, Any]:
        with self._status_lock:
            value = dict(self._status)
        value["pack_count"] = len(self.pack_paths())
        return value

    def pack_paths(self) -> list[Path]:
        if not self.packs_dir.is_dir():
            return []
        return sorted(
            self.packs_dir.glob(f"{TASK_EXPORT_PACK_PREFIX}*{TASK_EXPORT_PACK_SUFFIX}"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )

    def _set_status(self, **updates: Any) -> None:
        with self._status_lock:
            self._status.update(updates)

    def _rsync_component(self, node: dict[str, Any], component: str) -> dict[str, Any]:
        node_name = _safe_export_part(str(node["node"]), "node")
        remote_run_dir = str(node["remote_run_dir"])
        remote_path = f"{TASK_EXPORT_REMOTE_USER}@{node['node_ip']}:{remote_run_dir}/{component}/"
        destination = self.sources_dir / node_name / component
        destination.mkdir(parents=True, exist_ok=True)
        ssh_command = (
            "ssh -o BatchMode=yes -o ConnectTimeout=15 "
            "-o ServerAliveInterval=15 -o ServerAliveCountMax=4"
        )
        command = [
            "rsync",
            "--archive",
            "--delete",
            "--partial",
            "--compress",
            "--timeout=120",
            "-e",
            ssh_command,
            remote_path,
            str(destination) + os.sep,
        ]
        try:
            result = self._rsync_runner(
                command,
                capture_output=True,
                text=True,
                timeout=TASK_EXPORT_SYNC_TIMEOUT_SECONDS,
            )
            if result.returncode == 0:
                return {"node": node_name, "component": component, "state": "remote"}
            detail = (result.stderr or result.stdout or "rsync failed").strip()[-2000:]
        except (OSError, subprocess.SubprocessError) as error:
            detail = str(error)

        # The Slurm collector may already have a local per-node mirror. Keep
        # exports usable during SSH-key or host-key outages while remote rsync
        # continues to retry on the next cycle.
        local_source = self.run_dir / "slurm-nodes" / node_name / component
        if not local_source.is_dir():
            raise TaskExportError(f"{node_name}/{component}: {detail}")
        local_command = [
            "rsync",
            "--archive",
            "--delete",
            "--partial",
            str(local_source) + os.sep,
            str(destination) + os.sep,
        ]
        local_result = self._rsync_runner(
            local_command,
            capture_output=True,
            text=True,
            timeout=TASK_EXPORT_SYNC_TIMEOUT_SECONDS,
        )
        if local_result.returncode != 0:
            local_detail = (local_result.stderr or local_result.stdout or "rsync failed").strip()[-2000:]
            return {
                "node": node_name,
                "component": component,
                "state": "local-cache-partial",
                "warning": f"remote rsync unavailable: {detail}; local mirror partial: {local_detail}",
            }
        return {
            "node": node_name,
            "component": component,
            "state": "local-cache",
            "warning": f"remote rsync unavailable: {detail}",
        }

    def sync_once(self) -> dict[str, Any]:
        if not self._operation_lock.acquire(blocking=False):
            return self.status()
        try:
            nodes = list(export_node_records(self.run_dir).values())
            if not nodes:
                self._set_status(
                    state="unavailable",
                    last_sync=None,
                    nodes=[],
                    errors=["no Slurm plan nodes found"],
                )
                return self.status()
            results: list[dict[str, Any]] = []
            errors: list[str] = []
            with ThreadPoolExecutor(
                max_workers=min(4, len(nodes) * len(TASK_EXPORT_COMPONENTS))
            ) as executor:
                futures = {
                    executor.submit(self._rsync_component, node, component): (node, component)
                    for node in nodes
                    for component in TASK_EXPORT_COMPONENTS
                }
                for future in futures:
                    try:
                        results.append(future.result())
                    except (OSError, subprocess.SubprocessError, TaskExportError) as error:
                        errors.append(str(error))
            warnings = [str(item["warning"]) for item in results if item.get("warning")]
            self._set_status(
                state="error" if errors else "ready_with_warnings" if warnings else "ready",
                last_sync=datetime.now(UTC).isoformat(timespec="seconds"),
                nodes=results,
                errors=errors,
                warnings=warnings,
            )
            return self.status()
        finally:
            self._operation_lock.release()

    def _candidate_task_dirs(self, instance: str, source_node: str | None) -> list[Path]:
        instance = _safe_export_part(instance, "task instance")
        node_names: list[str] = []
        if source_node:
            node_names.append(_safe_export_part(source_node, "source node"))
        if self.sources_dir.is_dir():
            node_names.extend(
                path.name
                for path in sorted(self.sources_dir.iterdir())
                if path.is_dir() and path.name not in node_names
            )
        candidates: list[Path] = []
        for node_name in node_names:
            for component in ("tasks_voyager_postprocessed", "tasks"):
                candidates.append(self.sources_dir / node_name / component / instance)
                candidates.append(self.run_dir / "slurm-nodes" / node_name / component / instance)
        for component in ("tasks_voyager_postprocessed", "tasks"):
            candidates.append(self.run_dir / component / instance)
        return candidates

    def _find_task_dir(self, instance: str, source_node: str | None) -> Path | None:
        for candidate in self._candidate_task_dirs(instance, source_node):
            try:
                resolved = candidate.resolve()
            except OSError:
                continue
            if not resolved.is_dir() or not (resolved / "task.toml").is_file():
                continue
            if any(path.is_symlink() for path in resolved.rglob("*")):
                raise TaskExportError(f"task contains a symbolic link: {instance}")
            return resolved
        return None

    @staticmethod
    def _add_manifest(archive: tarfile.TarFile, manifest: dict[str, Any]) -> None:
        payload = json.dumps(manifest, indent=2, sort_keys=True).encode() + b"\n"
        info = tarfile.TarInfo("manifest.json")
        info.size = len(payload)
        info.mode = 0o644
        info.mtime = datetime.now(UTC).timestamp()
        archive.addfile(info, io.BytesIO(payload))

    def _prune_packs(self) -> None:
        packs = self.pack_paths()
        for path in packs[TASK_EXPORT_KEEP:]:
            path.unlink(missing_ok=True)

    def create_pack(self) -> Path:
        with self._operation_lock:
            records = reward_hack_accepted_records(self.run_dir)
            if not records:
                raise TaskExportError("no successful reward-hack-accepted tasks are available")
            self.packs_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
            target = self.packs_dir / f"{TASK_EXPORT_PACK_PREFIX}{timestamp}{TASK_EXPORT_PACK_SUFFIX}"
            temporary_handle = tempfile.NamedTemporaryFile(
                prefix=f".{target.name}.", dir=self.packs_dir, delete=False
            )
            temporary = Path(temporary_handle.name)
            temporary_handle.close()
            included: list[dict[str, Any]] = []
            missing: list[str] = []
            try:
                with tarfile.open(temporary, mode="w:gz") as archive:
                    for instance, record in sorted(records.items()):
                        task_dir = self._find_task_dir(instance, record.get("source_node"))
                        if task_dir is None:
                            missing.append(instance)
                            continue
                        archive.add(task_dir, arcname=instance, recursive=True)
                        source_component = (
                            "tasks_voyager_postprocessed"
                            if "tasks_voyager_postprocessed" in task_dir.parts
                            else "tasks"
                        )
                        included.append({**record, "source_component": source_component})
                    if missing:
                        raise TaskExportError(
                            f"{len(missing)} filtered task(s) are not mirrored locally; "
                            "wait for the next rsync cycle and retry"
                        )
                    self._add_manifest(
                        archive,
                        {
                            "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
                            "run": self.run_dir.name,
                            "filter": "Stage-I NOP=0, Oracle=1, reward-hack is_hacking=false",
                            "task_count": len(included),
                            "tasks": included,
                        },
                    )
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
            self._prune_packs()
            return target

    def run(
        self,
        stop_event: threading.Event,
        interval: int = TASK_EXPORT_SYNC_INTERVAL_SECONDS,
    ) -> None:
        self.sync_once()
        while not stop_event.wait(max(interval, 10)):
            try:
                self.sync_once()
            except Exception as error:  # pragma: no cover - defensive daemon boundary
                self._set_status(state="error", errors=[str(error)])


class StatusCache:
    """Refresh the expensive run scan in one background thread."""

    def __init__(self, run_dir: Path, input_jsonl: Path, total_entries: int):
        self.run_dir = run_dir
        self.input_jsonl = input_jsonl
        self.total_entries = total_entries
        self._lock = threading.Lock()
        self._body = b"{}"
        self.refresh()

    def refresh(self) -> None:
        body = json.dumps(
            calculate_status(self.run_dir, self.input_jsonl, self.total_entries),
            separators=(",", ":"),
        ).encode()
        with self._lock:
            self._body = body

    def body(self) -> bytes:
        with self._lock:
            return self._body

    def run(self, stop_event: threading.Event, interval: float = 2.0) -> None:
        while not stop_event.wait(interval):
            try:
                self.refresh()
            except Exception:
                # Keep serving the last valid snapshot; the next interval will
                # retry instead of making every HTTP request redo the scan.
                continue


def make_handler(
    status_cache: StatusCache,
    control_store: ControlConfigStore | None = None,
    control_token: str | None = None,
    export_manager: TaskExportManager | None = None,
):
    class DashboardHandler(BaseHTTPRequestHandler):
        def _send(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _send_json(self, status: HTTPStatus, value: Any) -> None:
            body = json.dumps(value, separators=(",", ":")).encode()
            self._send(status, body, "application/json")

        def _send_file(self, path: Path) -> None:
            try:
                size = path.stat().st_size
                stream = path.open("rb")
            except OSError:
                self._send(HTTPStatus.NOT_FOUND, b"export pack not found\n", "text/plain; charset=utf-8")
                return
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/gzip")
            self.send_header("Content-Length", str(size))
            self.send_header("Content-Disposition", f'attachment; filename="{path.name}"')
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            try:
                with stream:
                    while True:
                        chunk = stream.read(1024 * 1024)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
            except (BrokenPipeError, ConnectionResetError):
                return

        def do_GET(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path
            if path == "/":
                body = DASHBOARD_HTML.encode()
                content_type = "text/html; charset=utf-8"
                status = HTTPStatus.OK
            elif path == "/api/status":
                body = status_cache.body()
                content_type = "application/json"
                status = HTTPStatus.OK
            elif path == "/api/control":
                if control_store is None:
                    self._send_json(
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        {"error": "dashboard controls are not configured"},
                    )
                    return
                self._send_json(HTTPStatus.OK, control_store.snapshot())
                return
            elif path == "/api/export/status":
                if export_manager is None:
                    self._send_json(
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        {"error": "task export is not configured"},
                    )
                    return
                self._send_json(HTTPStatus.OK, export_manager.status())
                return
            elif path in {
                "/api/export/reward-hack-accepted.tar.gz",
                # Keep the old endpoint working for already-loaded pages.
                "/api/export/reward-hack-filtered.tar.gz",
            }:
                if export_manager is None:
                    self._send_json(
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        {"error": "task export is not configured"},
                    )
                    return
                try:
                    existing_packs = export_manager.pack_paths()
                    pack = existing_packs[0] if existing_packs else export_manager.create_pack()
                except (OSError, TaskExportError) as error:
                    self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": str(error)})
                    return
                self._send_file(pack)
                return
            elif path == "/healthz":
                body = b"ok\n"
                content_type = "text/plain; charset=utf-8"
                status = HTTPStatus.OK
            else:
                body = b"not found\n"
                content_type = "text/plain; charset=utf-8"
                status = HTTPStatus.NOT_FOUND

            self._send(status, body, content_type)

        def do_POST(self) -> None:  # noqa: N802
            if urlsplit(self.path).path != "/api/control":
                self._send(HTTPStatus.NOT_FOUND, b"not found\n", "text/plain; charset=utf-8")
                return
            if control_store is None:
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"error": "dashboard controls are not configured"},
                )
                return
            if control_token is None:
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"error": "dashboard control authentication is not configured"},
                )
                return
            supplied_token = self.headers.get("X-SWEGEN-Control-Token", "")
            if not hmac.compare_digest(supplied_token, control_token):
                self._send_json(
                    HTTPStatus.UNAUTHORIZED,
                    {"error": "invalid control token"},
                )
                return
            content_type = self.headers.get_content_type()
            if content_type != "application/json":
                self._send_json(
                    HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                    {"error": "Content-Type must be application/json"},
                )
                return
            try:
                content_length = int(self.headers.get("Content-Length", ""))
            except ValueError:
                content_length = -1
            if content_length < 0:
                self._send_json(HTTPStatus.LENGTH_REQUIRED, {"error": "Content-Length required"})
                return
            if content_length > CONTROL_REQUEST_MAX_BYTES:
                self._send_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": "request too large"})
                return
            try:
                request = json.loads(self.rfile.read(content_length))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid JSON request"})
                return
            try:
                control_store.update(request)
            except ControlConfigError as error:
                self._send_json(HTTPStatus.UNPROCESSABLE_ENTITY, {"error": str(error)})
                return
            except OSError:
                self._send_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "control config could not be written"},
                )
                return
            self._send_json(HTTPStatus.OK, control_store.snapshot())

        def log_message(self, format: str, *args: object) -> None:
            return

    return DashboardHandler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--input-jsonl", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--pid-file", type=Path)
    parser.add_argument(
        "--export-dir",
        type=Path,
        help="Local directory for rsynced task sources and generated task packs (default: <run-dir>/export).",
    )
    parser.add_argument(
        "--export-sync-interval",
        type=int,
        default=TASK_EXPORT_SYNC_INTERVAL_SECONDS,
        help=f"Seconds between task-source rsync cycles (default: {TASK_EXPORT_SYNC_INTERVAL_SECONDS}).",
    )
    parser.add_argument(
        "--control-config",
        type=Path,
        default=DEFAULT_CONTROL_CONFIG_PATH,
        help=f"Desired-state YAML consumed by the Slurm controller (default: {DEFAULT_CONTROL_CONFIG_PATH})",
    )
    parser.add_argument(
        "--control-token-file",
        type=Path,
        default=DEFAULT_CONTROL_TOKEN_PATH,
        help=f"Private token required by control POST requests (default: {DEFAULT_CONTROL_TOKEN_PATH})",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_dir = args.run_dir.resolve()
    input_jsonl = args.input_jsonl.resolve()
    total_entries = count_jsonl_entries(input_jsonl)
    status_cache = StatusCache(run_dir, input_jsonl, total_entries)
    control_store = ControlConfigStore(
        args.control_config.resolve(),
        default_control_config(run_dir, input_jsonl),
    )
    control_store.ensure_exists()
    control_token = ensure_control_token(args.control_token_file.resolve())
    export_dir = args.export_dir or run_dir / TASK_EXPORT_DIR_NAME
    if not export_dir.is_absolute():
        export_dir = run_dir / export_dir
    export_manager = TaskExportManager(run_dir, export_dir.resolve())
    stop_event = threading.Event()
    refresh_thread = threading.Thread(
        target=status_cache.run,
        args=(stop_event,),
        name="dashboard-status-refresh",
        daemon=True,
    )
    refresh_thread.start()
    export_thread = threading.Thread(
        target=export_manager.run,
        args=(stop_event, args.export_sync_interval),
        name="dashboard-task-export-sync",
        daemon=True,
    )
    export_thread.start()
    server = ThreadingHTTPServer(
        (args.host, args.port),
        make_handler(status_cache, control_store, control_token, export_manager),
    )

    if args.pid_file:
        args.pid_file.parent.mkdir(parents=True, exist_ok=True)
        args.pid_file.write_text(f"{os.getpid()}\n")

    def stop_server(_signum: int, _frame: object) -> None:
        stop_event.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop_server)
    signal.signal(signal.SIGINT, stop_server)
    try:
        print(f"SWE-gen dashboard listening on http://{args.host}:{args.port}", flush=True)
        server.serve_forever(poll_interval=0.5)
    finally:
        stop_event.set()
        refresh_thread.join(timeout=5)
        export_thread.join(timeout=5)
        server.server_close()
        if args.pid_file:
            args.pid_file.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
