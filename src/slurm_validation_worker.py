#!/usr/bin/env python3
"""Continuously post-check successful Slurm SWE-gen tasks.

In ``--baseline-only`` mode one coordinator can run a bounded pool of Harbor
NOP/Oracle validations while preserving NOP-before-Oracle ordering for each
task.  In the backward-compatible full mode it then either consumes the
independent reward pool's result or submits a focused tests-only
reward-hacking check to its own bounded thread pool.  Append-only ledgers make
every stage restart-safe and give the dashboard live evidence instead of
inferring validation from the SWE-gen subprocess exit code.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import logging
import os
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
from collections.abc import Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from dotenv import dotenv_values
from harbor.models.environment_type import EnvironmentType

from reward_hacking_detector.hacking import (
    HackCheckResult,
    LLMConfig,
    build_test_bundle,
    check_instance,
    check_instance_with_fallback,
    write_instance_log,
)
from run_dashboard import collect_latest_statuses
from slurm_collect import load_plan, redact
from swegen.tools.harbor_runner import parse_harbor_outcome, run_harbor_agent
from swegen.tools.validate_utils import validate_task_structure

logger = logging.getLogger("slurm_validation_worker")


def instance_shard(instance: str, shard_count: int) -> int:
    """Return the shard index that owns ``instance``.

    Uses a stable content hash (blake2b) rather than the built-in ``hash`` so
    the partition is identical across processes and nodes (Python's ``hash`` is
    salted per-process). Partitioning is disjoint and complete: every instance
    maps to exactly one shard in ``range(shard_count)``.
    """
    if shard_count <= 1:
        return 0
    digest = hashlib.blake2b(instance.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % shard_count


SAFE_INSTANCE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
SWEGEN_IMAGE_SUFFIX = "-swegenimage"
DEFAULT_SWR_REGISTRY = "swr-coder-data-trajectory-o84wch.swr-pro.myhuaweicloud.com"
DEFAULT_SWR_REPOSITORY = "aifm.coder.exp/swegen/generated"


def _docker_image_name(name: str) -> str:
    """Mirror Harbor's Docker image-name sanitization."""
    name = name.lower()
    if not re.match(r"^[a-z0-9]", name):
        name = "0" + name
    return re.sub(r"[^a-z0-9._-]", "-", name)


def local_image_tag(instance: str) -> str:
    """Return the local Docker image tag Harbor builds for an instance."""
    image_name = _docker_image_name(f"hb__{instance}")
    if not image_name.endswith(SWEGEN_IMAGE_SUFFIX):
        image_name = f"{image_name}{SWEGEN_IMAGE_SUFFIX}"
    return f"{image_name}:latest"


def swr_image_tag(instance: str, registry: str, repository: str) -> str:
    """Return the SWR registry image tag for an instance."""
    return f"{registry}/{repository}:{instance}"
# "blacklisted" is a terminal status for instances whose baseline (NOP/Oracle)
# repeatedly errors on retryable infrastructure failures (Docker build egress,
# Harbor timeouts). After --max-attempts tries the instance is retired so it
# stops consuming worker slots and proxy bandwidth on every retry cycle.
BLACKLISTED_STATUS = "blacklisted"
TERMINAL_STATUSES = {"accepted", "rejected", BLACKLISTED_STATUS}
BASELINE_TERMINAL_STATUSES = {"baseline_valid", "baseline_rejected", BLACKLISTED_STATUS}
TERMINAL_STAGE_STATES = {"pass", "fail"}
BACKFILL_BLOCKING_STATES = {"queued", "running"}
BACKFILL_TERMINAL_STATES = {"pass", "fail"}
STAGE_NAMES = ("nop", "oracle", "reward_hack")
PROXY_KEYS = {
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
}
NO_PROXY_KEYS = {"NO_PROXY", "no_proxy"}


def _no_proxy_host(endpoint: str) -> str:
    """Extract the bare host (no scheme/port) from an endpoint URL for no_proxy.

    ``no_proxy`` matches on host, so the port is dropped. Returns "" for a
    malformed/empty endpoint.
    """
    if not endpoint:
        return ""
    host = str(endpoint)
    if "://" in host:
        host = host.split("://", 1)[1]
    host = host.split("/", 1)[0]
    host = host.split(":", 1)[0]
    return host.strip()


RewardCheckValue = tuple[
    LLMConfig,
    HackCheckResult,
    list[tuple[LLMConfig, HackCheckResult]],
]
RewardJob = tuple[dict[str, Any], Path]


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def reward_matches(value: object, expected: int) -> bool:
    """Match numeric Harbor rewards without accepting bool-as-int values."""
    return not isinstance(value, bool) and isinstance(value, (int, float)) and value == expected


def private_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def append_private_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def load_latest_postchecks(path: Path) -> dict[str, dict[str, Any]]:
    """Load the latest complete snapshot for every instance in a ledger."""
    latest: dict[str, dict[str, Any]] = {}
    sequence: dict[str, tuple[int, str, int]] = {}
    if not path.is_file():
        return latest
    try:
        stream = path.open(encoding="utf-8", errors="replace")
    except OSError:
        return latest
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
            key = (attempt_number, timestamp if isinstance(timestamp, str) else "", line_index)
            if instance not in sequence or key >= sequence[instance]:
                latest[instance] = record
                sequence[instance] = key
    return latest


def backfill_reward_state(record: object) -> str | None:
    """Return the canonical state from one reward-backfill snapshot."""
    if not isinstance(record, dict):
        return None
    status = record.get("status")
    if status in BACKFILL_BLOCKING_STATES | BACKFILL_TERMINAL_STATES | {"error"}:
        return str(status)
    stage = record.get("reward_hack")
    if isinstance(stage, dict):
        state = stage.get("state")
        if state in BACKFILL_BLOCKING_STATES | BACKFILL_TERMINAL_STATES | {
            "pending",
            "error",
        }:
            return str(state)
    return None


def overlay_backfill_reward(
    record: dict[str, Any],
    backfill: dict[str, Any] | None,
) -> str | None:
    """Overlay authoritative backfill evidence onto a local postcheck record.

    In-flight states are recorded as provenance only, leaving the local stage
    pending. Terminal pass/fail evidence replaces the local reward stage and
    can then participate in finalization.
    """
    state = backfill_reward_state(backfill)
    if state is None or backfill is None:
        return state

    stage = record.get("reward_hack")
    if not isinstance(stage, dict):
        stage = queued_snapshot(
            str(record.get("instance") or "unknown"),
            {},
            str(record.get("worker_id") or "worker"),
        )["reward_hack"]
        record["reward_hack"] = stage

    stage.update(
        {
            "backfill_state": state,
            "backfill_timestamp": backfill.get("timestamp"),
            "backfill_attempt": backfill.get("attempt"),
        }
    )
    if state not in BACKFILL_TERMINAL_STATES:
        return state

    source_stage = backfill.get("reward_hack")
    if not isinstance(source_stage, dict):
        source_stage = {}

    def field(name: str, default: object = None) -> object:
        value = source_stage.get(name)
        return backfill.get(name, default) if value is None else value

    is_hacking = field("is_hacking")
    if not isinstance(is_hacking, bool):
        is_hacking = state == "fail"
    attempted_models = field("attempted_models", [])
    if not isinstance(attempted_models, list):
        attempted_models = []
    stage.update(
        {
            "state": state,
            "is_hacking": is_hacking,
            "model": field("model"),
            "attempted_models": attempted_models,
            "fallback_used": bool(field("fallback_used", False)),
            "fallback_reason": field("fallback_reason"),
            "test_framework": field("test_framework"),
            "reason": field("reason"),
            "error": field("error"),
            "source": "reward_backfill",
        }
    )
    return state


def pending_stage() -> dict[str, Any]:
    return {
        "state": "pending",
        "reward": None,
        "exit_code": None,
        "error": None,
        "job_result": None,
    }


def pending_reward_stage() -> dict[str, Any]:
    return {
        "state": "pending",
        "is_hacking": None,
        "model": None,
        "attempted_models": [],
        "fallback_used": False,
        "fallback_reason": None,
        "test_framework": None,
        "reason": None,
        "error": None,
    }


def queued_snapshot(
    instance: str,
    generation: dict[str, Any],
    worker_id: str,
) -> dict[str, Any]:
    timestamp = utc_now()
    return {
        "schema_version": 1,
        "event": "postcheck_status",
        "timestamp": timestamp,
        "instance": instance,
        "attempt": 1,
        "status": "queued",
        "stage": "queue",
        "worker_id": worker_id,
        "checker_node": socket.gethostname(),
        "source_node": generation.get("node"),
        "source_timestamp": generation.get("timestamp"),
        "queued_at": timestamp,
        "started_at": None,
        "finished_at": None,
        "next_retry_at": None,
        "nop": pending_stage(),
        "oracle": pending_stage(),
        "reward_hack": pending_reward_stage(),
        "error": None,
    }


def is_ready(
    record: dict[str, Any],
    now: datetime,
    *,
    baseline_only: bool = False,
) -> bool:
    status = record.get("status")
    if status in TERMINAL_STATUSES:
        return False
    if status == "baseline_valid":
        # A full-mode worker may continue an already completed baseline into
        # Stage 3. A baseline-only restart must leave it terminal.
        return not baseline_only
    if status == "baseline_rejected":
        return False
    if status in {"queued", "running"}:
        return True
    if status != "error":
        return False
    retry_at = parse_timestamp(record.get("next_retry_at"))
    return retry_at is None or retry_at <= now


def prepare_attempt(record: dict[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(record)
    if value.get("status") in {"error", "running"}:
        value["attempt"] = int(value.get("attempt") or 1) + 1
    for stage_name in STAGE_NAMES:
        stage = value.get(stage_name)
        if not isinstance(stage, dict):
            stage = pending_stage() if stage_name != "reward_hack" else pending_reward_stage()
            value[stage_name] = stage
        if stage.get("state") in {"running", "error"}:
            if stage_name == "reward_hack":
                stage.update(
                    {
                        "state": "pending",
                        "is_hacking": None,
                        "model": None,
                        "attempted_models": [],
                        "fallback_used": False,
                        "fallback_reason": None,
                        "test_framework": None,
                        "reason": None,
                        "error": None,
                    }
                )
            else:
                stage.update(
                    {
                        "state": "pending",
                        "reward": None,
                        "exit_code": None,
                        "error": None,
                        "job_result": None,
                    }
                )
    value.update(
        {
            "event": "postcheck_status",
            "timestamp": utc_now(),
            "status": "running",
            "stage": "fetch",
            "started_at": utc_now(),
            "finished_at": None,
            "next_retry_at": None,
            "error": None,
        }
    )
    return value


def stage_counts(records: dict[str, dict[str, Any]], eligible: set[str]) -> dict[str, Any]:
    counts: dict[str, Any] = {
        "eligible": len(eligible),
        "not_queued": len(eligible - records.keys()),
        "queued": 0,
        "running": 0,
        "accepted": 0,
        "rejected": 0,
        "baseline_valid": 0,
        "baseline_rejected": 0,
        "blacklisted": 0,
        "errors": 0,
        "nop": dict.fromkeys(("pass", "fail", "error", "pending", "running", "blacklisted"), 0),
        "oracle": dict.fromkeys(
            ("pass", "fail", "error", "pending", "running", "blacklisted"), 0
        ),
        "reward_hack": dict.fromkeys(("pass", "fail", "error", "pending", "running"), 0),
    }
    for instance in eligible:
        record = records.get(instance)
        if record is None:
            for stage_name in STAGE_NAMES:
                counts[stage_name]["pending"] += 1
            continue
        status = str(record.get("status") or "error")
        key = "errors" if status == "error" else status
        if key in counts and isinstance(counts[key], int):
            counts[key] += 1
        nop = record.get("nop") if isinstance(record.get("nop"), dict) else {}
        oracle = record.get("oracle") if isinstance(record.get("oracle"), dict) else {}
        baseline_valid = (
            nop.get("state") == "pass"
            and reward_matches(nop.get("reward"), 0)
            and oracle.get("state") == "pass"
            and reward_matches(oracle.get("reward"), 1)
        )
        if baseline_valid and status != "baseline_valid":
            counts["baseline_valid"] += 1
        for stage_name in STAGE_NAMES:
            stage = record.get(stage_name)
            state = stage.get("state") if isinstance(stage, dict) else "pending"
            if state not in counts[stage_name]:
                state = "pending"
            counts[stage_name][state] += 1
    return counts


class ValidationWorker:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.run_dir = args.run_dir.resolve()
        self.worker_dir = (args.worker_dir or self.run_dir / ".validation-worker").resolve()
        # The postcheck ledger can be shared across sharded nodes (each node
        # owns a disjoint set of instances, so the atomic O_APPEND writes never
        # conflict) while every node keeps its own scratch worker_dir. This lets
        # the single-ledger reward worker see baseline results from all shards.
        configured_ledger = getattr(args, "ledger_path", None)
        self.ledger_path = (
            configured_ledger.resolve()
            if configured_ledger
            else self.worker_dir / "postcheck-status.jsonl"
        )
        configured_backfill_ledger = getattr(args, "reward_backfill_ledger", None)
        self.reward_backfill_ledger_path = (
            configured_backfill_ledger or self.worker_dir / "reward-backfill-status.jsonl"
        ).resolve()
        self.status_path = self.worker_dir / "worker-status.json"
        self.pid_path = (args.pid_file or self.worker_dir / "worker.pid").resolve()
        self.tasks_dir = self.worker_dir / "tasks"
        # Optional shared (e.g. NFS) tree of pre-staged task dirs; when set,
        # fetch_task reads task inputs from here instead of pulling them over
        # Slurm from the generating node. None => original srun-based fetch.
        configured_task_source = getattr(args, "task_source_dir", None)
        self.task_source_dir = (
            configured_task_source.resolve() if configured_task_source else None
        )
        self.jobs_dir = self.worker_dir / "harbor-jobs"
        self.reward_logs_dir = self.worker_dir / "reward-logs"
        self.worker_id = args.worker_id
        # Queue sharding: each node processes a disjoint 1/shard_count of the
        # queue (owned instances are those whose stable hash maps to shard_index),
        # so N nodes can validate in parallel without duplicating work. Defaults
        # (0, 1) mean "single shard" == original single-node behavior.
        self.shard_index = int(getattr(args, "shard_index", 0) or 0)
        self.shard_count = max(1, int(getattr(args, "shard_count", 1) or 1))
        if not 0 <= self.shard_index < self.shard_count:
            raise RuntimeError(
                f"shard-index {self.shard_index} out of range for shard-count {self.shard_count}"
            )
        self.stop_requested = False
        self._coordinator_thread_ident = threading.get_ident()
        self._state_lock = threading.RLock()
        self._ledger_lock = threading.Lock()
        self._status_lock = threading.Lock()
        self.current_instance: str | None = None
        self.current_stage: str | None = None
        self.active_stages: dict[str, str] = {}
        self.generations: dict[str, dict[str, Any]] = {}
        self.generation_order: list[str] = []
        # When shards share one ledger, load only this shard's instances so a
        # worker never resumes/re-queues in-flight records owned by another
        # shard (which would double-process them). discover() already filters
        # newly-found work by _owns_shard; this applies the same rule at load.
        self.records = {
            instance: record
            for instance, record in load_latest_postchecks(self.ledger_path).items()
            if self._owns_shard(instance)
        }
        self.plan_paths = [path.resolve() for path in args.plan]
        self.llm_config: LLMConfig | None = None
        self.fallback_llm_config: LLMConfig | None = None
        self.baseline_executor: ThreadPoolExecutor | None = None
        self.baseline_futures: dict[Future[None], str] = {}
        self.reward_executor: ThreadPoolExecutor | None = None
        self.reward_futures: dict[Future[RewardCheckValue], RewardJob] = {}
        self._configure_proxy()
        if not self._is_baseline_only():
            self.llm_config, self.fallback_llm_config = self._configure_reward_llm()
        self._claim_pid()
        try:
            if self._is_parallel_baseline():
                self.baseline_executor = ThreadPoolExecutor(
                    max_workers=self._baseline_concurrency(),
                    thread_name_prefix="swegen-baseline",
                )
            elif not self._is_baseline_only():
                self.reward_executor = ThreadPoolExecutor(
                    max_workers=args.reward_concurrency,
                    thread_name_prefix="swegen-reward",
                )
        except Exception:
            self.close()
            raise

    def _is_baseline_only(self) -> bool:
        return bool(getattr(getattr(self, "args", None), "baseline_only", False))

    def _baseline_concurrency(self) -> int:
        return max(1, int(getattr(getattr(self, "args", None), "baseline_concurrency", 1)))

    def _is_parallel_baseline(self) -> bool:
        return self._is_baseline_only() and self._baseline_concurrency() > 1

    def _state_guard(self) -> threading.RLock:
        """Return the state lock, including for legacy object.__new__ tests."""
        lock = getattr(self, "_state_lock", None)
        if lock is None:
            lock = threading.RLock()
            self._state_lock = lock
        return lock

    def _set_active_stage(self, instance: str, stage: str) -> None:
        with self._state_guard():
            active_stages = getattr(self, "active_stages", None)
            if not isinstance(active_stages, dict):
                active_stages = {}
                self.active_stages = active_stages
            active_stages[instance] = stage
            first_instance = next(iter(active_stages), None)
            self.current_instance = first_instance
            self.current_stage = active_stages.get(first_instance) if first_instance else None

    def _clear_active_stage(self, instance: str) -> None:
        with self._state_guard():
            active_stages = getattr(self, "active_stages", None)
            if isinstance(active_stages, dict):
                active_stages.pop(instance, None)
            else:
                active_stages = {}
                self.active_stages = active_stages
            first_instance = next(iter(active_stages), None)
            self.current_instance = first_instance
            self.current_stage = active_stages.get(first_instance) if first_instance else None

    def active_baseline_instances(self) -> set[str]:
        with self._state_guard():
            active_stages = getattr(self, "active_stages", {})
            return set(active_stages) if isinstance(active_stages, dict) else set()

    def _worker_thread_may_publish_status(self) -> bool:
        if not self._is_parallel_baseline():
            return True
        coordinator = getattr(self, "_coordinator_thread_ident", threading.get_ident())
        return threading.get_ident() == coordinator

    def load_reward_backfill(self) -> dict[str, dict[str, Any]]:
        """Read the independent pool's ledger without ever mutating it."""
        if self._is_baseline_only():
            return {}
        path = getattr(self, "reward_backfill_ledger_path", None)
        if not isinstance(path, Path):
            return {}
        return load_latest_postchecks(path)

    def sync_backfill_reward(self, record: dict[str, Any]) -> str | None:
        if self._is_baseline_only():
            return None
        instance = str(record.get("instance") or "")
        backfill = self.load_reward_backfill().get(instance)
        return overlay_backfill_reward(record, backfill)

    def _configure_proxy(self) -> None:
        """Apply shared proxy/CA transport settings used by Harbor and Docker."""
        proxy_values = dotenv_values(self.args.proxy_env.resolve())
        for key in PROXY_KEYS | NO_PROXY_KEYS:
            value = proxy_values.get(key)
            if value:
                os.environ[key] = str(value)

        # Force Harbor's `docker compose build` onto the Docker daemon's built-in
        # BuildKit (the "default"/`docker` buildx driver) instead of the
        # `docker-container` driver, which spawns a separate buildkitd container
        # (~60-75 threads) per concurrent build. At Stage-2 concurrency, dozens
        # of those deadlock the shared host dockerd/containerd on futexes and
        # stall all builds (results computed but never harvested). One shared
        # daemon BuildKit removes the per-build multiplication.
        os.environ["DOCKER_BUILDKIT"] = "1"
        os.environ["BUILDX_BUILDER"] = "default"
        os.environ["COMPOSE_BAKE"] = "false"

        if self.args.ca_bundle:
            ca_bundle = self.args.ca_bundle.resolve()
            if not ca_bundle.is_file():
                raise RuntimeError(f"missing CA bundle: {ca_bundle}")
            for key in ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE"):
                os.environ[key] = str(ca_bundle)

    def _configure_reward_llm(self) -> tuple[LLMConfig, LLMConfig | None]:
        """Load reward-only credentials and model configuration."""
        # The reward endpoint (e.g. the glm LiteLLM proxy at 7.244.3.251:8088)
        # is reachable directly on the private network. Force it into no_proxy
        # so egress isn't routed through the corporate forward proxy, which
        # intermittently returns an HIS Proxy notification page (504) for this
        # host. Keep any existing no_proxy entries and append the reward host.
        reward_host = _no_proxy_host(self.args.reward_endpoint)
        existing = ",".join(
            v for v in (os.environ.get("NO_PROXY"), os.environ.get("no_proxy")) if v
        )
        no_proxy = ",".join(h for h in (*existing.split(","), reward_host) if h)
        os.environ["NO_PROXY"] = no_proxy
        os.environ["no_proxy"] = no_proxy

        credentials = dotenv_values(self.args.credentials_file.resolve())
        api_key = credentials.get("OPENAI_API_KEY") or credentials.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("credentials file has no OPENAI_API_KEY or ANTHROPIC_API_KEY")

        primary = LLMConfig(
            name=self.args.reward_checker_name,
            endpoint=self.args.reward_endpoint,
            model=self.args.reward_model,
            api_key=str(api_key),
        )
        fallback_model = str(self.args.reward_fallback_model or "").strip()
        fallback = (
            LLMConfig(
                name=self.args.reward_fallback_checker_name,
                endpoint=self.args.reward_endpoint,
                model=fallback_model,
                api_key=str(api_key),
            )
            if fallback_model
            else None
        )
        return primary, fallback

    def _claim_pid(self) -> None:
        try:
            existing = int(self.pid_path.read_text().strip())
        except (OSError, ValueError):
            existing = 0
        if existing:
            try:
                os.kill(existing, 0)
            except ProcessLookupError:
                pass
            else:
                raise RuntimeError(f"validation worker already running with pid {existing}")
        self.pid_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.pid_path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(f"{os.getpid()}\n")

    def close(self) -> None:
        try:
            if self.pid_path.read_text().strip() == str(os.getpid()):
                self.pid_path.unlink(missing_ok=True)
        except OSError:
            pass

    def append(self, record: dict[str, Any], *, publish: bool = True) -> None:
        record["timestamp"] = utc_now()
        ledger_lock = getattr(self, "_ledger_lock", None)
        if ledger_lock is None:
            ledger_lock = threading.Lock()
            self._ledger_lock = ledger_lock
        with ledger_lock:
            append_private_jsonl(self.ledger_path, record)
        with self._state_guard():
            self.records[str(record["instance"])] = copy.deepcopy(record)
        if publish and self._worker_thread_may_publish_status():
            self.publish_status("running" if self.has_active_work() else "idle")

    def active_reward_instances(self) -> set[str]:
        if self._is_baseline_only():
            return set()
        futures = getattr(self, "reward_futures", {})
        return {str(record.get("instance") or "") for record, _task_dir in futures.values()}

    def has_active_work(self) -> bool:
        return (
            bool(self.active_baseline_instances())
            or getattr(self, "current_instance", None) is not None
            or bool(getattr(self, "reward_futures", {}))
        )

    def publish_status(self, state: str, error: str | None = None) -> None:
        if not self._worker_thread_may_publish_status():
            return
        with self._state_guard():
            eligible = set(self.generations)
            records = copy.deepcopy(self.records)
            active_stages = dict(getattr(self, "active_stages", {}))
            legacy_instance = getattr(self, "current_instance", None)
            legacy_stage = getattr(self, "current_stage", None)
        if not active_stages and isinstance(legacy_instance, str) and legacy_instance:
            active_stages[legacy_instance] = str(legacy_stage or "unknown")
        counts = stage_counts(records, eligible)
        reward_futures = getattr(self, "reward_futures", {})
        display_instance = next(iter(active_stages), None)
        display_stage = active_stages.get(display_instance) if display_instance else None
        if display_instance is None and reward_futures:
            record, _task_dir = next(iter(reward_futures.values()))
            display_instance = str(record.get("instance") or "") or None
            display_stage = "reward_hack"
        recent = sorted(
            (record for instance, record in records.items() if instance in eligible),
            key=lambda record: str(record.get("timestamp") or ""),
            reverse=True,
        )[:200]
        llm_config = getattr(self, "llm_config", None)
        fallback_llm_config = getattr(self, "fallback_llm_config", None)
        baseline_only = self._is_baseline_only()
        status_lock = getattr(self, "_status_lock", None)
        if status_lock is None:
            status_lock = threading.Lock()
            self._status_lock = status_lock
        with status_lock:
            private_json(
                self.status_path,
                {
                    "schema_version": 1,
                    "event": "postcheck_worker_status",
                    "timestamp": utc_now(),
                    "state": state,
                    "mode": "baseline_only" if baseline_only else "full",
                    "worker_id": self.worker_id,
                    "checker_node": socket.gethostname(),
                    "poll_interval_seconds": self.args.poll_interval,
                    "current_instance": display_instance,
                    "current_stage": display_stage,
                    "current_instances": list(active_stages),
                    "active_stages": active_stages,
                    "baseline_concurrency": (self._baseline_concurrency() if baseline_only else 0),
                    "baseline_active_count": len(active_stages) if baseline_only else 0,
                    "reward_model": llm_config.model if llm_config else None,
                    "reward_primary_model": llm_config.model if llm_config else None,
                    "reward_fallback_model": (
                        fallback_llm_config.model if fallback_llm_config else None
                    ),
                    "reward_concurrency": 0 if baseline_only else self.args.reward_concurrency,
                    "reward_active_count": len(reward_futures),
                    "counts": counts,
                    "recent": recent,
                    "error": redact(error) if error else None,
                },
            )

    @contextmanager
    def status_heartbeat(self) -> Iterator[None]:
        """Keep worker-status.json fresh while the main thread is blocked."""
        if self._is_parallel_baseline():
            # The coordinator remains unblocked and is the sole status writer.
            # Per-Harbor heartbeat threads would otherwise race one another.
            yield
            return
        interval = float(getattr(self.args, "heartbeat_interval", 60.0))
        stopped = threading.Event()

        def publish_periodically() -> None:
            while not stopped.wait(interval):
                try:
                    self.publish_status("running")
                except Exception:
                    # The normal stage-completion write remains authoritative.
                    # A transient heartbeat write failure must not abandon a
                    # Harbor run that may already have spent minutes in Docker.
                    return

        thread = threading.Thread(
            target=publish_periodically,
            name="swegen-validation-heartbeat",
            daemon=True,
        )
        thread.start()
        try:
            yield
        finally:
            stopped.set()
            thread.join()

    def _owns_shard(self, instance: str) -> bool:
        """Whether this worker's shard owns ``instance`` (default: single shard)."""
        shard_count = max(1, int(getattr(self, "shard_count", 1)))
        shard_index = int(getattr(self, "shard_index", 0))
        return instance_shard(instance, shard_count) == shard_index

    def discover(self) -> None:
        latest, order = collect_latest_statuses(self.run_dir)
        generations = {
            instance: record
            for instance, record in latest.items()
            if record.get("status") == "success"
            and SAFE_INSTANCE_RE.fullmatch(instance)
            and (
                record.get("node_scope") == "slurm"
                or (self.run_dir / "tasks" / instance / "tests" / "test.sh").is_file()
            )
            and (self.args.instance is None or instance == self.args.instance)
            and self._owns_shard(instance)
        }
        # Stage 2 is a backfill queue. Oldest-first prevents continuous task
        # generation from starving successful instances from earlier runs.
        generation_order = list(
            reversed([instance for instance in order if instance in generations])
        )
        with self._state_guard():
            self.generations = generations
            self.generation_order = generation_order
            known_records = set(self.records)
        for instance in generation_order:
            if instance in known_records:
                continue
            if not SAFE_INSTANCE_RE.fullmatch(instance):
                continue
            record = queued_snapshot(instance, generations[instance], self.worker_id)
            self.append(record, publish=False)
        self.publish_status("running" if self.has_active_work() else "idle")

    def node_records(self) -> dict[str, dict[str, Any]]:
        records: dict[str, dict[str, Any]] = {}
        for plan_path in self.plan_paths:
            plan = load_plan(plan_path)
            history = plan.get("job_history")
            if isinstance(history, list):
                for record in sorted(
                    (item for item in history if isinstance(item, dict)),
                    key=lambda item: str(item.get("submitted_at") or ""),
                ):
                    node = record.get("node")
                    if isinstance(node, str) and node and record.get("remote_run_dir"):
                        records[node] = record
            for record in plan["nodes"]:
                node = record.get("node")
                if isinstance(node, str) and node:
                    records[node] = record
        return records

    def fetch_task(self, instance: str, record: dict[str, Any]) -> Path:
        destination = self.tasks_dir / instance
        if (destination / "tests" / "test.sh").is_file():
            return destination
        # Every task dir lives on the shared `/data` filesystem, so a task is
        # always reachable by reading it directly — never by submitting a Slurm
        # `srun tar` job. The old srun fallback flooded the shared `debug`
        # partition with thousands of pending `tar` jobs, jamming the scheduler
        # and starving Stage-1's own jobs (an ~18h Stage-1 outage). Resolve the
        # task from the shared locations in priority order, lazily, and copy
        # locally only when the match is not already a return-in-place path.
        #
        # 1. Pre-staged NFS `tasks/` tree (returned in place).
        staged_dir = getattr(self, "task_source_dir", None)
        if staged_dir is not None:
            staged_task = staged_dir / instance
            if (staged_task / "tests" / "test.sh").is_file():
                return staged_task
        # 2. This run dir's local task copy (returned in place).
        local_task = self.run_dir / "tasks" / instance
        if (local_task / "tests" / "test.sh").is_file():
            return local_task
        # 3. The generating node's run dir — also on the shared /data mount, so
        #    it is readable directly without any cross-node transport. Only now
        #    do we consult the plan for the source node.
        node = record.get("source_node")
        if isinstance(node, str) and node:
            node_record = self.node_records().get(node)
            if node_record is None:
                raise RuntimeError(f"source node is absent from active plans: {node}")
            remote_run_dir = node_record.get("remote_run_dir")
            if remote_run_dir:
                remote_task = Path(str(remote_run_dir)) / "tasks" / instance
                if (remote_task / "tests" / "test.sh").is_file():
                    return self._copy_shared_task(instance, remote_task, destination)
        raise RuntimeError(
            f"task {instance!r} not found in any shared location "
            "(NFS staging, local run dir, or source-node run dir)"
        )

    def _copy_shared_task(self, instance: str, source: Path, destination: Path) -> Path:
        self.tasks_dir.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=f".{instance}.fetch-", dir=self.tasks_dir))
        try:
            extracted = staging / instance
            shutil.copytree(source, extracted, symlinks=True)
            if not (extracted / "tests" / "test.sh").is_file():
                raise RuntimeError("copied task is missing tests/test.sh")
            if destination.exists():
                shutil.rmtree(destination)
            os.replace(extracted, destination)
        finally:
            shutil.rmtree(staging, ignore_errors=True)
        return destination

    def mark_infrastructure_error(
        self,
        record: dict[str, Any],
        stage: str,
        error: BaseException | str,
    ) -> None:
        message = redact(str(error))[-4000:]
        instance = str(record.get("instance") or "")
        attempt = int(record.get("attempt") or 1)
        max_attempts = int(getattr(self.args, "max_attempts", 3) or 0)
        if max_attempts > 0 and attempt >= max_attempts:
            # Exhausted the retry budget: retire the instance so it is no longer
            # re-queued. Persist it to the blacklist ledger for auditing.
            record.update(
                {
                    "status": BLACKLISTED_STATUS,
                    "stage": stage,
                    "finished_at": utc_now(),
                    "next_retry_at": None,
                    "error": message,
                }
            )
            # Move the offending baseline stage out of the "error" bucket so the
            # dashboard reflects it as retired rather than an active failure.
            stage_record = record.get(stage)
            if isinstance(stage_record, dict) and stage in ("nop", "oracle"):
                stage_record["state"] = BLACKLISTED_STATUS
            self._record_blacklist(instance, stage, attempt, message)
            self.append(record)
            return
        # Kick the instance back to "unresolved" with a retry delay so the
        # coordinator rotates to other ready instances instead of immediately
        # re-running the same failing one.
        record.update(
            {
                "status": "error",
                "stage": stage,
                "finished_at": utc_now(),
                "next_retry_at": (
                    datetime.now(UTC) + timedelta(seconds=self.args.retry_delay)
                ).isoformat(timespec="seconds"),
                "error": message,
            }
        )
        self.append(record)

    def _record_blacklist(
        self, instance: str, stage: str, attempts: int, error: str
    ) -> None:
        """Append a blacklisted instance to the persistent blacklist ledger.

        The ledger is an append-only JSONL alongside the postcheck ledger; the
        latest record per instance wins (mirrors the postcheck ledger format),
        so a re-run simply appends an updated row.
        """
        blacklist_path = self.worker_dir / "blacklist.jsonl"
        try:
            append_private_jsonl(
                blacklist_path,
                {
                    "instance": instance,
                    "stage": stage,
                    "attempts": attempts,
                    "blacklisted_at": utc_now(),
                    "error": error[-500:],
                },
            )
        except OSError as exc:
            logger.warning(f"[{instance}] could not write blacklist ledger: {exc}")

    def run_harbor_stage(
        self,
        record: dict[str, Any],
        task_dir: Path,
        agent: str,
    ) -> None:
        self.drain_reward_futures()
        expected = 0 if agent == "nop" else 1
        stage = record[agent]
        stage.update({"state": "running", "error": None})
        record.update({"status": "running", "stage": agent, "error": None})
        self._set_active_stage(str(record["instance"]), agent)
        self.append(record)

        with self.status_heartbeat():
            exit_code, result_path = run_harbor_agent(
                task_id=task_dir.name,
                dataset_path=task_dir.parent,
                jobs_dir=self.jobs_dir,
                agent=agent,
                timeout_multiplier=self.args.timeout_multiplier,
                capture_output=True,
                delete_after=False,
                environment=EnvironmentType.DOCKER,
                wall_timeout_seconds=getattr(self.args, "harbor_wall_timeout", None),
            )
        self.drain_reward_futures()
        outcome = parse_harbor_outcome(result_path)
        harbor_error = redact(outcome.error)[-4000:] if outcome.error else None
        stage.update(
            {
                "state": (
                    "error"
                    if outcome.reward is None
                    else "pass"
                    if outcome.reward == expected
                    else "fail"
                ),
                "reward": outcome.reward,
                "exit_code": exit_code,
                "error": harbor_error
                or ("Harbor produced no reward" if outcome.reward is None else None),
                "job_result": str(result_path) if result_path else None,
            }
        )
        self.append(record)

    def execute_reward_check(
        self, bundle: str, task_id: str, task_dir: Path | None = None
    ) -> RewardCheckValue:
        """Run one primary-to-fallback chain without mutating worker state."""
        if self._is_baseline_only() or self.llm_config is None:
            raise RuntimeError("reward checks are disabled in baseline-only mode")
        fallback_llm_config = getattr(self, "fallback_llm_config", None)
        if fallback_llm_config is not None:
            return asyncio.run(
                check_instance_with_fallback(
                    bundle,
                    self.llm_config,
                    fallback_llm_config,
                    task_id=task_id,
                    instance_dir=task_dir,
                )
            )

        # Compatibility for directly constructed workers in focused unit tests.
        pairs = asyncio.run(check_instance(bundle, [self.llm_config], task_id=task_id))
        return pairs[0][0], pairs[0][1], pairs

    def finish_reward_hack_stage(
        self,
        record: dict[str, Any],
        task_dir: Path,
        value: RewardCheckValue,
    ) -> None:
        selected_config, result, attempts = value
        self.reward_logs_dir.mkdir(parents=True, exist_ok=True)
        write_instance_log(
            self.reward_logs_dir / f"{task_dir.name}.log",
            task_dir.name,
            attempts,
        )
        stage = record["reward_hack"]
        stage.update(
            {
                "state": "error" if result.error else "fail" if result.is_hacking else "pass",
                "is_hacking": result.is_hacking if not result.error else None,
                "model": selected_config.model,
                "attempted_models": [config.model for config, _result in attempts],
                "fallback_used": len(attempts) > 1,
                "fallback_reason": (
                    redact(str(attempts[0][1].error))[-4000:] if len(attempts) > 1 else None
                ),
                "test_framework": result.test_framework or None,
                "reason": result.reason,
                "error": redact(result.error)[-4000:] if result.error else None,
            }
        )
        self.append(record)

    def drain_reward_futures(self, *, block: bool = False) -> int:
        """Apply completed reward results on the main thread.

        Worker threads perform only the HTTP request chain. Ledger, status,
        reward-log, and in-memory record mutations all happen here.
        """
        futures = getattr(self, "reward_futures", None)
        if not futures:
            return 0

        done = {future for future in futures if future.done()}
        if block and not done:
            done, _pending = wait(tuple(futures), return_when=FIRST_COMPLETED)

        completed = 0
        for future in done:
            record, task_dir = futures.pop(future)
            completed += 1
            try:
                value = future.result()
                self.finish_reward_hack_stage(record, task_dir, value)
                self.finalize(record)
            except Exception as error:
                stage = record.get("reward_hack")
                if isinstance(stage, dict):
                    stage.update(
                        {
                            "state": "error",
                            "is_hacking": None,
                            "error": redact(str(error))[-4000:],
                        }
                    )
                self.mark_infrastructure_error(record, "reward_hack", error)
        return completed

    def wait_for_reward_capacity(self) -> None:
        concurrency = int(getattr(self.args, "reward_concurrency", 1))
        self.drain_reward_futures()
        while len(getattr(self, "reward_futures", {})) >= concurrency:
            self.drain_reward_futures(block=True)

    def run_reward_hack_stage(self, record: dict[str, Any], task_dir: Path) -> bool:
        """Start a reward check and return whether it was scheduled asynchronously."""
        if self._is_baseline_only():
            raise RuntimeError("reward checks are disabled in baseline-only mode")
        stage = record["reward_hack"]
        stage.update(
            {
                "state": "running",
                "is_hacking": None,
                "model": self.llm_config.model,
                "attempted_models": [],
                "fallback_used": False,
                "fallback_reason": None,
                "test_framework": None,
                "reason": None,
                "error": None,
            }
        )
        record.update({"status": "running", "stage": "reward_hack", "error": None})
        self._set_active_stage(str(record["instance"]), "reward_hack")
        self.append(record)

        bundle = build_test_bundle(task_dir)
        if not bundle.strip():
            raise RuntimeError("reward-hacking checker found no readable tests")

        executor = getattr(self, "reward_executor", None)
        if executor is None:
            self.finish_reward_hack_stage(
                record,
                task_dir,
                self.execute_reward_check(bundle, task_dir.name, task_dir),
            )
            return False

        self.wait_for_reward_capacity()
        future = executor.submit(self.execute_reward_check, bundle, task_dir.name, task_dir)
        self.reward_futures[future] = (record, task_dir)
        self.publish_status("running")
        return True

    def push_verified_image(self, instance: str) -> dict[str, Any]:
        """Tag and push the verified Docker image to the SWR registry.

        Best-effort: failures are logged but do not change the accepted status.
        Returns a dict with push result metadata.
        """
        no_push = getattr(self.args, "no_push", False)
        if no_push:
            return {"pushed": False, "reason": "disabled by --no-push"}

        registry = getattr(self.args, "swr_registry", DEFAULT_SWR_REGISTRY)
        repository = getattr(self.args, "swr_repository", DEFAULT_SWR_REPOSITORY)
        local_tag = local_image_tag(instance)
        remote_tag = swr_image_tag(instance, registry, repository)

        result: dict[str, Any] = {
            "local_tag": local_tag,
            "remote_tag": remote_tag,
            "pushed": False,
        }

        # Check if the local image exists
        inspect = subprocess.run(
            ["docker", "image", "inspect", local_tag],
            capture_output=True,
            timeout=30,
        )
        if inspect.returncode != 0:
            result["error"] = "local image not found"
            return result

        # Tag for SWR
        tag_proc = subprocess.run(
            ["docker", "tag", local_tag, remote_tag],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if tag_proc.returncode != 0:
            result["error"] = f"docker tag failed: {tag_proc.stderr.strip()[-500:]}"
            return result

        # Push to SWR
        push_proc = subprocess.run(
            ["docker", "push", remote_tag],
            capture_output=True,
            text=True,
            timeout=1800,
        )
        if push_proc.returncode != 0:
            result["error"] = f"docker push failed: {push_proc.stderr.strip()[-500:]}"
            # Clean up the remote tag locally
            subprocess.run(["docker", "rmi", "-f", remote_tag], capture_output=True, timeout=30)
            return result

        result["pushed"] = True

        # Clean up: remove the remote tag locally (layers are shared)
        subprocess.run(["docker", "rmi", remote_tag], capture_output=True, timeout=30)

        # Clean up: remove the local swegenimage to reclaim disk
        subprocess.run(["docker", "rmi", local_tag], capture_output=True, timeout=30)

        return result

    def finalize(self, record: dict[str, Any]) -> None:
        errors = [
            str(record[name].get("error"))
            for name in STAGE_NAMES
            if isinstance(record.get(name), dict) and record[name].get("state") == "error"
        ]
        if errors:
            self.mark_infrastructure_error(
                record, str(record.get("stage") or "complete"), "; ".join(errors)
            )
            return
        nop = record.get("nop") if isinstance(record.get("nop"), dict) else {}
        oracle = record.get("oracle") if isinstance(record.get("oracle"), dict) else {}
        reward_hack = (
            record.get("reward_hack") if isinstance(record.get("reward_hack"), dict) else {}
        )
        accepted = (
            nop.get("state") == "pass"
            and reward_matches(nop.get("reward"), 0)
            and oracle.get("state") == "pass"
            and reward_matches(oracle.get("reward"), 1)
            and reward_hack.get("state") == "pass"
            and reward_hack.get("is_hacking") is False
        )
        record.update(
            {
                "status": "accepted" if accepted else "rejected",
                "stage": "complete",
                "finished_at": utc_now(),
                "next_retry_at": None,
                "error": None,
            }
        )
        if accepted:
            try:
                instance = str(record.get("instance") or "")
                push_result = self.push_verified_image(instance)
                record["image_push"] = push_result
            except Exception as exc:
                record["image_push"] = {"pushed": False, "error": str(exc)[-500:]}
        self.append(record)

    def finalize_baseline(self, record: dict[str, Any]) -> None:
        """Persist a restart-safe Stage 2 outcome without consuming Stage 3."""
        errors = [
            str(record[name].get("error"))
            for name in ("nop", "oracle")
            if isinstance(record.get(name), dict) and record[name].get("state") == "error"
        ]
        if errors:
            self.mark_infrastructure_error(
                record,
                str(record.get("stage") or "baseline_complete"),
                "; ".join(errors),
            )
            return
        nop = record.get("nop") if isinstance(record.get("nop"), dict) else {}
        oracle = record.get("oracle") if isinstance(record.get("oracle"), dict) else {}
        valid = (
            nop.get("state") == "pass"
            and reward_matches(nop.get("reward"), 0)
            and oracle.get("state") == "pass"
            and reward_matches(oracle.get("reward"), 1)
        )
        record["reward_hack"] = pending_reward_stage()
        record.update(
            {
                "status": "baseline_valid" if valid else "baseline_rejected",
                "stage": "baseline_complete",
                "finished_at": utc_now(),
                "next_retry_at": None,
                "error": None,
            }
        )
        self.append(record)

    def process(self, instance: str, *, coordinator_managed: bool = False) -> None:
        with self._state_guard():
            record = prepare_attempt(self.records[instance])
        baseline_only = self._is_baseline_only()
        if baseline_only:
            # Stage 2 owns only these fields. Any legacy local reward snapshot
            # must not prevent the independent Stage 3 worker from claiming it.
            record["reward_hack"] = pending_reward_stage()
        else:
            self.sync_backfill_reward(record)
        with self._state_guard():
            generation = copy.deepcopy(self.generations.get(instance, {}))
        if generation.get("node"):
            record["source_node"] = generation["node"]
        self._set_active_stage(instance, "fetch")
        self.append(record)
        try:
            with self.status_heartbeat():
                task_dir = self.fetch_task(instance, record)
            validate_task_structure(task_dir)
            self.drain_reward_futures()
        except Exception as error:
            self.mark_infrastructure_error(record, "fetch", error)
            if not coordinator_managed:
                self._clear_active_stage(instance)
            if not self._is_parallel_baseline():
                self.publish_status("running" if self.has_active_work() else "idle")
            return

        try:
            reward_scheduled = False
            if record["nop"].get("state") not in TERMINAL_STAGE_STATES:
                self.run_harbor_stage(record, task_dir, "nop")
            if record["nop"].get("state") != "pass":
                if baseline_only:
                    self.finalize_baseline(record)
                else:
                    self.finalize(record)
                return
            if record["oracle"].get("state") not in TERMINAL_STAGE_STATES:
                self.run_harbor_stage(record, task_dir, "oracle")
            if record["oracle"].get("state") != "pass":
                if baseline_only:
                    self.finalize_baseline(record)
                else:
                    self.finalize(record)
                return
            if baseline_only:
                self.finalize_baseline(record)
                return
            backfill_state = self.sync_backfill_reward(record)
            if backfill_state in BACKFILL_BLOCKING_STATES:
                record.update(
                    {
                        "status": "running",
                        "stage": "reward_hack",
                        "finished_at": None,
                        "next_retry_at": None,
                        "error": None,
                    }
                )
                self.append(record)
                return
            if record["reward_hack"].get("state") not in TERMINAL_STAGE_STATES:
                reward_scheduled = self.run_reward_hack_stage(record, task_dir)
            if not reward_scheduled:
                self.finalize(record)
        except Exception as error:
            stage = str(record.get("stage") or "unknown")
            stage_value = record.get(stage)
            if isinstance(stage_value, dict):
                stage_value.update({"state": "error", "error": redact(str(error))[-4000:]})
            self.mark_infrastructure_error(record, stage, error)
        finally:
            if not coordinator_managed:
                self._clear_active_stage(instance)
            if hasattr(self, "reward_futures") and not self._is_parallel_baseline():
                self.publish_status("running" if self.has_active_work() else "idle")

    def next_ready(self) -> str | None:
        now = datetime.now(UTC)
        if self._is_baseline_only():
            # Stage 2 follows generation order only. Reward pool state neither
            # prioritizes nor blocks NOP/Oracle work.
            with self._state_guard():
                active_stages = getattr(self, "active_stages", {})
                active_baselines = set(active_stages) if isinstance(active_stages, dict) else set()
                for instance in self.generation_order:
                    if instance in active_baselines:
                        continue
                    record = self.records.get(instance)
                    if record is not None and is_ready(record, now, baseline_only=True):
                        return instance
            return None
        active_rewards = self.active_reward_instances()
        backfill_records = self.load_reward_backfill()
        # Reward-clean tasks are the shortest path to a fully accepted task.
        # Scan them first so the independent backfill pool and sequential
        # NOP/Oracle verifier converge on the same instances instead of
        # consuming opposite ends of the generation queue.
        for clean_only in (True, False):
            for instance in self.generation_order:
                if instance in active_rewards:
                    continue
                record = self.records.get(instance)
                backfill_state = backfill_reward_state(backfill_records.get(instance))
                is_clean_backfill = backfill_state == "pass"
                if is_clean_backfill != clean_only:
                    continue
                baseline_terminal = bool(
                    record
                    and isinstance(record.get("nop"), dict)
                    and record["nop"].get("state") in TERMINAL_STAGE_STATES
                    and isinstance(record.get("oracle"), dict)
                    and record["oracle"].get("state") in TERMINAL_STAGE_STATES
                )
                if backfill_state in BACKFILL_BLOCKING_STATES and baseline_terminal:
                    continue
                if (
                    backfill_state in BACKFILL_TERMINAL_STATES
                    and baseline_terminal
                    and record is not None
                    and record.get("status") not in TERMINAL_STATUSES
                ):
                    return instance
                if record is not None and is_ready(record, now):
                    return instance
        return None

    def submit_baseline(self, instance: str) -> None:
        """Reserve and submit one Stage-2 task to the bounded Harbor pool."""
        executor = getattr(self, "baseline_executor", None)
        if executor is None:
            raise RuntimeError("baseline executor is not configured")
        self._set_active_stage(instance, "fetch")
        try:
            future = executor.submit(self.process, instance, coordinator_managed=True)
        except Exception:
            self._clear_active_stage(instance)
            raise
        self.baseline_futures[future] = instance

    def drain_baseline_futures(
        self,
        *,
        block: bool = False,
        timeout: float | None = None,
    ) -> int:
        """Reap Stage-2 tasks and release coordinator-owned reservations."""
        futures = getattr(self, "baseline_futures", None)
        if not futures:
            return 0

        done = {future for future in futures if future.done()}
        if block and not done:
            done, _pending = wait(
                tuple(futures),
                timeout=timeout,
                return_when=FIRST_COMPLETED,
            )

        completed = 0
        for future in done:
            instance = futures.pop(future)
            completed += 1
            try:
                future.result()
            except Exception as error:
                with self._state_guard():
                    current = copy.deepcopy(self.records.get(instance))
                    active_stages = getattr(self, "active_stages", {})
                    active_stage = (
                        active_stages.get(instance) if isinstance(active_stages, dict) else None
                    )
                if current is not None:
                    stage = str(active_stage or current.get("stage") or "unknown")
                    stage_value = current.get(stage)
                    if isinstance(stage_value, dict):
                        stage_value.update(
                            {
                                "state": "error",
                                "error": redact(str(error))[-4000:],
                            }
                        )
                    self.mark_infrastructure_error(current, stage, error)
            finally:
                self._clear_active_stage(instance)
        return completed

    def run_parallel_baseline(self) -> int:
        """Run one restart-safe coordinator with bounded Stage-2 concurrency."""
        submitted = 0
        last_discovery = 0.0
        heartbeat_interval = min(
            float(getattr(self.args, "heartbeat_interval", 60.0)),
            60.0,
        )
        try:
            while True:
                self.drain_baseline_futures()
                if self.stop_requested:
                    break

                now = time.monotonic()
                if not self.generations or now - last_discovery >= self.args.poll_interval:
                    self.discover()
                    last_discovery = now

                limit_reached = self.args.max_tasks > 0 and submitted >= self.args.max_tasks
                while (
                    not self.stop_requested
                    and not limit_reached
                    and len(self.baseline_futures) < self._baseline_concurrency()
                ):
                    instance = self.next_ready()
                    if instance is None:
                        break
                    self.submit_baseline(instance)
                    submitted += 1
                    limit_reached = self.args.max_tasks > 0 and submitted >= self.args.max_tasks

                self.publish_status("running" if self.baseline_futures else "idle")
                if self.stop_requested or limit_reached:
                    break

                if not self.baseline_futures:
                    if self.args.once:
                        break
                    deadline = time.monotonic() + min(self.args.poll_interval, 60)
                    while not self.stop_requested and time.monotonic() < deadline:
                        time.sleep(min(1.0, deadline - time.monotonic()))
                    continue

                until_discovery = max(
                    0.1,
                    self.args.poll_interval - (time.monotonic() - last_discovery),
                )
                self.drain_baseline_futures(
                    block=True,
                    timeout=min(heartbeat_interval, until_discovery),
                )
                self.publish_status("running" if self.baseline_futures else "idle")

            if self.baseline_futures:
                self.publish_status("stopping" if self.stop_requested else "draining")
            while self.baseline_futures:
                self.drain_baseline_futures(block=True, timeout=heartbeat_interval)
                if self.baseline_futures:
                    self.publish_status("stopping" if self.stop_requested else "draining")
            self.publish_status("stopped" if self.stop_requested else "idle")
            return 0
        except Exception as error:
            while self.baseline_futures:
                self.drain_baseline_futures(block=True, timeout=heartbeat_interval)
            self.publish_status("error", str(error))
            raise
        finally:
            executor = getattr(self, "baseline_executor", None)
            if executor is not None:
                executor.shutdown(wait=True, cancel_futures=False)
            self.close()

    def run(self) -> int:
        if self._is_parallel_baseline():
            return self.run_parallel_baseline()
        processed = 0
        last_discovery = 0.0
        try:
            while True:
                self.drain_reward_futures()
                if self.stop_requested:
                    break

                now = time.monotonic()
                if not self.generations or now - last_discovery >= self.args.poll_interval:
                    self.discover()
                    last_discovery = now

                if self.args.max_tasks > 0 and processed >= self.args.max_tasks:
                    break

                instance = self.next_ready()
                if instance is not None:
                    self.process(instance)
                    processed += 1
                    continue

                if self.args.once:
                    break

                if self.reward_futures:
                    self.publish_status("running")
                    done, _pending = wait(
                        tuple(self.reward_futures),
                        timeout=min(self.args.poll_interval, 60),
                        return_when=FIRST_COMPLETED,
                    )
                    if done:
                        self.drain_reward_futures()
                    continue

                self.publish_status("idle")
                deadline = time.monotonic() + min(self.args.poll_interval, 60)
                while not self.stop_requested and time.monotonic() < deadline:
                    time.sleep(min(1.0, deadline - time.monotonic()))

            if self.reward_futures:
                self.publish_status("stopping" if self.stop_requested else "draining")
            while self.reward_futures:
                self.drain_reward_futures(block=True)
            self.publish_status("stopped" if self.stop_requested else "idle")
            return 0
        except Exception as error:
            while self.reward_futures:
                self.drain_reward_futures(block=True)
            self.publish_status("error", str(error))
            raise
        finally:
            if self.baseline_executor is not None:
                self.baseline_executor.shutdown(wait=True, cancel_futures=False)
            if self.reward_executor is not None:
                self.reward_executor.shutdown(wait=True, cancel_futures=False)
            self.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--plan", required=True, action="append", type=Path)
    parser.add_argument("--worker-dir", type=Path)
    parser.add_argument(
        "--task-source-dir",
        type=Path,
        help=(
            "Directory of pre-staged task dirs (e.g. an NFS `tasks/` tree). When "
            "set, fetch_task reads task inputs from `<dir>/<instance>` before "
            "falling back to pulling them over Slurm from the generating node."
        ),
    )
    parser.add_argument(
        "--ledger-path",
        type=Path,
        help=(
            "Shared postcheck ledger path. Sharded nodes point here at one "
            "common ledger (disjoint instances → conflict-free atomic appends) "
            "while keeping separate --worker-dir scratch. Defaults to "
            "<worker-dir>/postcheck-status.jsonl."
        ),
    )
    parser.add_argument(
        "--shard-index",
        type=int,
        default=0,
        help=(
            "This worker's shard number in [0, shard-count). With shard-count>1 "
            "each node processes a disjoint hash-partition of the queue so N "
            "nodes validate in parallel without duplicating work."
        ),
    )
    parser.add_argument(
        "--shard-count",
        type=int,
        default=1,
        help="Total number of validation shards/nodes (default 1 = no sharding).",
    )
    parser.add_argument(
        "--reward-backfill-ledger",
        type=Path,
        help=("Read-only reward pool ledger (default: <worker-dir>/reward-backfill-status.jsonl)."),
    )
    parser.add_argument("--pid-file", type=Path)
    parser.add_argument("--worker-id", default="postcheck-0")
    parser.add_argument(
        "--instance",
        help="Restrict discovery to one successful instance (useful for a smoke check).",
    )
    parser.add_argument("--poll-interval", type=int, default=600)
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=60.0,
        help=(
            "Seconds between worker-status heartbeats during blocking fetch and "
            "Harbor operations (default: 60)."
        ),
    )
    parser.add_argument("--retry-delay", type=int, default=600)
    parser.add_argument("--fetch-timeout", type=int, default=600)
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help=(
            "Blacklist an instance after this many failed baseline attempts so it "
            "is no longer retried (0 disables the cap; default: 3)."
        ),
    )
    parser.add_argument(
        "--harbor-wall-timeout",
        type=float,
        default=1800.0,
        help=(
            "Maximum wall-clock seconds for one NOP or Oracle Harbor process "
            "before its complete client process group is terminated (default: 1800)."
        ),
    )
    parser.add_argument("--timeout-multiplier", type=float)
    parser.add_argument("--max-tasks", type=int, default=0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument(
        "--baseline-only",
        action="store_true",
        help=(
            "Run only Harbor NOP/Oracle validation and publish restart-safe "
            "baseline outcomes for the independent reward worker."
        ),
    )
    parser.add_argument(
        "--baseline-concurrency",
        type=int,
        default=1,
        help=(
            "Maximum concurrent NOP/Oracle task pipelines in --baseline-only "
            "mode (default: 1). Each task still runs NOP before Oracle."
        ),
    )
    parser.add_argument("--proxy-env", type=Path, default=Path(".env"))
    parser.add_argument(
        "--credentials-file",
        type=Path,
        default=Path(
            "slurm-runtime/20260716-sol-max-full-16w/workspace/.slurm-secrets/credentials.env"
        ),
    )
    parser.add_argument(
        "--ca-bundle",
        type=Path,
        default=Path(
            "slurm-runtime/20260716-sol-max-full-16w/workspace/.slurm-secrets/combined-ca.crt"
        ),
    )
    parser.add_argument("--reward-endpoint", default="https://arcyleung-ubuntu.tailb940e6.ts.net")
    parser.add_argument("--reward-model", default="gpt-5.3-codex-spark")
    parser.add_argument("--reward-fallback-model", default="gpt-5.6-terra")
    parser.add_argument("--reward-checker-name", default="spark-lightweight")
    parser.add_argument(
        "--reward-fallback-checker-name",
        default="terra-lightweight",
    )
    parser.add_argument(
        "--reward-concurrency",
        type=int,
        default=1,
        help="Maximum concurrent lightweight reward-hacking requests (default: 1).",
    )
    parser.add_argument(
        "--swr-registry",
        default=DEFAULT_SWR_REGISTRY,
        help=f"SWR Docker registry URL (default: {DEFAULT_SWR_REGISTRY}).",
    )
    parser.add_argument(
        "--swr-repository",
        default=DEFAULT_SWR_REPOSITORY,
        help=f"SWR repository path (default: {DEFAULT_SWR_REPOSITORY}).",
    )
    parser.add_argument(
        "--no-push",
        action="store_true",
        help="Disable pushing verified Docker images to the SWR registry.",
    )
    args = parser.parse_args(argv)
    if args.poll_interval < 1 or args.retry_delay < 1 or args.fetch_timeout < 1:
        parser.error("poll, retry, and fetch timeouts must be positive")
    if args.heartbeat_interval <= 0:
        parser.error("--heartbeat-interval must be positive")
    if args.harbor_wall_timeout <= 0:
        parser.error("--harbor-wall-timeout must be positive")
    if args.max_tasks < 0:
        parser.error("--max-tasks may not be negative")
    if args.baseline_concurrency < 1:
        parser.error("--baseline-concurrency must be positive")
    if args.reward_concurrency < 1:
        parser.error("--reward-concurrency must be positive")
    if args.instance is not None and not SAFE_INSTANCE_RE.fullmatch(args.instance):
        parser.error("--instance contains unsafe characters")
    if args.shard_count < 1:
        parser.error("--shard-count must be positive")
    if not 0 <= args.shard_index < args.shard_count:
        parser.error("--shard-index must be in [0, shard-count)")
    return args


def main(argv: list[str] | None = None) -> int:
    os.umask(0o077)
    args = parse_args(argv)
    worker = ValidationWorker(args)

    def request_stop(_signum: int, _frame: object) -> None:
        worker.stop_requested = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    return worker.run()


if __name__ == "__main__":
    raise SystemExit(main())
