#!/usr/bin/env python3
"""Run strict Stage 3 reward-hacking validation for baseline-valid tasks.

This service is deliberately independent from the sequential NOP/Oracle
validator.  It admits a task only after the sequential postcheck ledger records
exact NOP=0 and Oracle=1 evidence.  Its worker threads fetch only a task's
``tests/`` subtree and run the lightweight LLM inspector; they never invoke
Harbor or Docker.  Results are published to a separate append-only ledger so
the sequential validator and dashboard can consume them without concurrent
writes to the primary postcheck ledger.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
import re
import shutil
import signal
import socket
import tempfile
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from dotenv import dotenv_values

from reward_hacking_detector.hacking import (
    HackCheckResult,
    LLMConfig,
    build_test_bundle,
    check_instance_with_fallback,
    write_instance_log,
)
from run_dashboard import collect_latest_statuses
from slurm_collect import job_state, load_plan, redact, run_bytes, safe_extract, srun_base
from swegen.ledger_repo import LedgerRepo

SAFE_INSTANCE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
TERMINAL_STATUSES = {"pass", "fail"}
PROXY_KEYS = {
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
}


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


@dataclass(frozen=True)
class BackfillWorkResult:
    """Tests-only cache location and the completed primary/fallback chain."""

    task_dir: Path
    reward: RewardCheckValue


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


def reward_stage(state: str = "pending") -> dict[str, Any]:
    return {
        "state": state,
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
        "event": "reward_backfill_status",
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
        "reward_hack": reward_stage(),
        "error": None,
    }


def prepare_attempt(
    existing: dict[str, Any] | None,
    instance: str,
    generation: dict[str, Any],
    worker_id: str,
) -> dict[str, Any]:
    """Create a queued snapshot, preserving restart-safe attempt numbering."""
    if existing is None:
        return queued_snapshot(instance, generation, worker_id)

    value = copy.deepcopy(existing)
    status = value.get("status")
    if status in {"running", "error"}:
        value["attempt"] = max(int(value.get("attempt") or 1), 1) + 1
    else:
        value["attempt"] = max(int(value.get("attempt") or 1), 1)
    timestamp = utc_now()
    value.update(
        {
            "schema_version": 1,
            "event": "reward_backfill_status",
            "timestamp": timestamp,
            "instance": instance,
            "status": "queued",
            "stage": "queue",
            "worker_id": worker_id,
            "checker_node": socket.gethostname(),
            "source_node": generation.get("node") or value.get("source_node"),
            "source_timestamp": generation.get("timestamp") or value.get("source_timestamp"),
            "queued_at": timestamp,
            "started_at": None,
            "finished_at": None,
            "next_retry_at": None,
            "reward_hack": reward_stage(),
            "error": None,
        }
    )
    return value


def is_retry_ready(record: dict[str, Any], now: datetime) -> bool:
    status = record.get("status")
    if status in TERMINAL_STATUSES:
        return False
    if status in {"queued", "running"}:
        return True
    if status != "error":
        return False
    retry_at = parse_timestamp(record.get("next_retry_at"))
    return retry_at is None or retry_at <= now


def reward_state(record: dict[str, Any] | None) -> str | None:
    if not isinstance(record, dict):
        return None
    stage = record.get("reward_hack")
    if not isinstance(stage, dict):
        return None
    state = stage.get("state")
    return state if isinstance(state, str) else None


def reward_matches(value: object, expected: int) -> bool:
    """Match an exact numeric Harbor reward without treating bool as int."""
    return not isinstance(value, bool) and isinstance(value, (int, float)) and value == expected


def baseline_is_valid(record: object) -> bool:
    """Require authoritative Stage 2 evidence before admitting Stage 3 work."""
    if not isinstance(record, dict):
        return False
    nop = record.get("nop")
    oracle = record.get("oracle")
    return bool(
        isinstance(nop, dict)
        and nop.get("state") == "pass"
        and reward_matches(nop.get("reward"), 0)
        and isinstance(oracle, dict)
        and oracle.get("state") == "pass"
        and reward_matches(oracle.get("reward"), 1)
    )


class RewardBackfillWorker:
    """Bounded, restart-safe tests-only reward analysis pool."""

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.run_dir = args.run_dir.resolve()
        self.worker_dir = (args.worker_dir or self.run_dir / ".validation-worker").resolve()
        self.ledger_path = self.worker_dir / "reward-backfill-status.jsonl"
        # Ledger repository (Postgres by default; JSONL if
        # SWEGEN_LEDGER_BACKEND=jsonl). Path stem resolves the target table.
        self._ledger_repo = LedgerRepo(self.ledger_path)
        self.status_path = self.worker_dir / "reward-backfill-worker-status.json"
        self.pid_path = (args.pid_file or self.worker_dir / "reward-backfill-worker.pid").resolve()
        self.tasks_dir = self.worker_dir / "reward-backfill-tasks"
        self.logs_dir = self.worker_dir / "reward-backfill-logs"
        self.postcheck_ledger_path = (
            args.postcheck_ledger or self.worker_dir / "postcheck-status.jsonl"
        ).resolve()
        self.worker_id = args.worker_id
        self.plan_paths = [path.resolve() for path in args.plan]
        self.stop_requested = False
        self.generations: dict[str, dict[str, Any]] = {}
        self.generation_order: list[str] = []
        self.node_map: dict[str, dict[str, Any]] = {}
        self.records = self._ledger_repo.load_latest()
        self.sequential_records: dict[str, dict[str, Any]] = {}
        self._postcheck_signature: tuple[int, int] | None = None
        self.llm_config, self.fallback_llm_config = self._configure_network()
        self._claim_pid()
        try:
            self.executor = ThreadPoolExecutor(
                max_workers=args.reward_concurrency,
                thread_name_prefix="swegen-reward-backfill",
            )
        except Exception:
            self.close()
            raise
        self.futures: dict[Future[BackfillWorkResult], tuple[str, dict[str, Any]]] = {}

    def _configure_network(self) -> tuple[LLMConfig, LLMConfig | None]:
        proxy_path = self.args.proxy_env.resolve()
        if not proxy_path.is_file():
            raise RuntimeError(f"missing proxy environment file: {proxy_path}")
        proxy_values = dotenv_values(proxy_path)
        for key in PROXY_KEYS:
            value = proxy_values.get(key)
            if value:
                os.environ[key] = str(value)
        # The reward endpoint (the arcyleung-ubuntu tailnet LiteLLM proxy,
        # serving gpt-5.3-codex-spark / gpt-5.6-sol) is only reachable through
        # the corporate forward proxy; direct access from this host times out.
        # Clear any inherited no_proxy so the reward endpoint isn't
        # short-circuited.
        os.environ["NO_PROXY"] = ""
        os.environ["no_proxy"] = ""

        credentials_path = self.args.credentials_file.resolve()
        if not credentials_path.is_file():
            raise RuntimeError(f"missing credentials file: {credentials_path}")
        credentials = dotenv_values(credentials_path)
        api_key = credentials.get("OPENAI_API_KEY") or credentials.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("credentials file has no OPENAI_API_KEY or ANTHROPIC_API_KEY")

        if self.args.ca_bundle:
            ca_bundle = self.args.ca_bundle.resolve()
            if not ca_bundle.is_file():
                raise RuntimeError(f"missing CA bundle: {ca_bundle}")
            for key in ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE"):
                os.environ[key] = str(ca_bundle)

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
        self.pid_path.parent.mkdir(parents=True, exist_ok=True)
        for _attempt in range(2):
            try:
                descriptor = os.open(
                    self.pid_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o600,
                )
            except FileExistsError:
                try:
                    existing = int(self.pid_path.read_text(encoding="utf-8").strip())
                except (OSError, ValueError):
                    existing = 0
                if existing:
                    try:
                        os.kill(existing, 0)
                    except ProcessLookupError:
                        pass
                    except PermissionError as error:
                        raise RuntimeError(
                            f"reward backfill worker pid {existing} cannot be inspected"
                        ) from error
                    else:
                        raise RuntimeError(
                            f"reward backfill worker already running with pid {existing}"
                        )
                try:
                    self.pid_path.unlink()
                except FileNotFoundError:
                    pass
                continue
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(f"{os.getpid()}\n")
            return
        raise RuntimeError(f"unable to claim reward backfill pid file: {self.pid_path}")

    def close(self) -> None:
        try:
            if self.pid_path.read_text(encoding="utf-8").strip() == str(os.getpid()):
                self.pid_path.unlink(missing_ok=True)
        except OSError:
            pass

    def append(self, record: dict[str, Any], *, publish: bool = True) -> None:
        record["timestamp"] = utc_now()
        self._ledger_repo.append(record)
        self.records[str(record["instance"])] = copy.deepcopy(record)
        if publish:
            self.publish_status("running" if self.futures else "idle")

    def _refresh_sequential_records(self, *, force: bool = False) -> None:
        # In jsonl mode the file mtime/size acts as a cheap "unchanged?"
        # cache; in postgres mode there is no file to stat, so always reload
        # (the query is the source of truth and cheap).
        postcheck_repo = LedgerRepo(self.postcheck_ledger_path)
        if postcheck_repo.backend == "jsonl":
            try:
                stat = self.postcheck_ledger_path.stat()
                signature = (stat.st_mtime_ns, stat.st_size)
            except OSError:
                signature = (-1, -1)
            if not force and signature == self._postcheck_signature:
                return
        self.sequential_records = postcheck_repo.load_latest()
        if postcheck_repo.backend == "jsonl":
            self._postcheck_signature = signature

    def node_records(self) -> dict[str, dict[str, Any]]:
        """Resolve each node to the newest usable record across both plans."""
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
                if isinstance(node, str) and node and record.get("remote_run_dir"):
                    records[node] = record
        return records

    def discover(self) -> None:
        latest, newest_first = collect_latest_statuses(self.run_dir)
        self.generations = {
            instance: record
            for instance, record in latest.items()
            if record.get("status") == "success"
            and (
                record.get("node_scope") == "slurm"
                or (self.run_dir / "tasks" / instance / "tests" / "test.sh").is_file()
            )
            and (self.args.instance is None or instance == self.args.instance)
            and SAFE_INSTANCE_RE.fullmatch(instance)
        }
        # The sequential validator consumes newest-first.  Backfilling from the
        # opposite end of the run sharply reduces duplicate claim races.
        self.generation_order = list(
            reversed([instance for instance in newest_first if instance in self.generations])
        )
        self.node_map = self.node_records()
        self._refresh_sequential_records(force=True)
        self.publish_status("running" if self.futures else "idle")

    def _sequential_blocks(self, instance: str) -> bool:
        state = reward_state(self.sequential_records.get(instance))
        return state in {"running", "pass", "fail"}

    def _baseline_is_valid(self, instance: str) -> bool:
        return baseline_is_valid(self.sequential_records.get(instance))

    def next_ready(self) -> str | None:
        self._refresh_sequential_records()
        active = {instance for instance, _record in self.futures.values()}
        now = datetime.now(UTC)
        for instance in self.generation_order:
            if (
                not self._baseline_is_valid(instance)
                or instance in active
                or self._sequential_blocks(instance)
            ):
                continue
            record = self.records.get(instance)
            if record is None or is_retry_ready(record, now):
                return instance
        return None

    def fetch_task(self, instance: str, record: dict[str, Any]) -> Path:
        """Fetch only ``tests/`` for one task into the dedicated data cache."""
        destination = self.tasks_dir / instance
        if (destination / "tests" / "test.sh").is_file():
            return destination

        node = record.get("source_node")
        if not isinstance(node, str) or not node:
            local_task = self.run_dir / "tasks" / instance
            if (local_task / "tests" / "test.sh").is_file():
                return local_task
            raise RuntimeError("successful record has neither a source node nor local tests")
        node_record = self.node_map.get(node)
        if node_record is None:
            raise RuntimeError(f"source node is absent from active plans: {node}")
        job_id = str(node_record["job_id"]) if node_record.get("job_id") else None
        state = job_state(job_id)
        remote_run_dir = str(node_record["remote_run_dir"])
        command = srun_base(node, job_id, state) + [
            "--chdir=/data/work/slurm-swegen",
            "tar",
            "-C",
            f"{remote_run_dir}/tasks",
            "-czf",
            "-",
            "--",
            f"{instance}/tests",
        ]
        completed = run_bytes(command, timeout=self.args.fetch_timeout)
        if not completed.stdout:
            raise RuntimeError(f"empty tests archive from {node}")

        self.tasks_dir.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=f".{instance}.fetch-", dir=self.tasks_dir))
        try:
            safe_extract(completed.stdout, staging)
            extracted = staging / instance
            if not (extracted / "tests" / "test.sh").is_file():
                raise RuntimeError("fetched task is missing tests/test.sh")
            unexpected = [path.name for path in extracted.iterdir() if path.name != "tests"]
            if unexpected:
                raise RuntimeError(
                    "tests-only archive contained unexpected task entries: "
                    + ", ".join(sorted(unexpected))
                )
            if any(path.is_symlink() for path in (extracted / "tests").rglob("*")):
                raise RuntimeError("tests-only archive contains a symbolic link")
            if destination.exists():
                shutil.rmtree(destination)
            os.replace(extracted, destination)
        finally:
            shutil.rmtree(staging, ignore_errors=True)
        return destination

    def execute(self, instance: str, record: dict[str, Any]) -> BackfillWorkResult:
        """Fetch tests and perform one Spark-to-Terra request chain."""
        task_dir = self.fetch_task(instance, record)
        bundle = build_test_bundle(task_dir)
        if not bundle.strip():
            raise RuntimeError("reward-hacking checker found no readable tests")
        value = asyncio.run(
            check_instance_with_fallback(
                bundle,
                self.llm_config,
                self.fallback_llm_config,
                task_id=instance,
                instance_dir=task_dir,
            )
        )
        return BackfillWorkResult(task_dir=task_dir, reward=value)

    def submit(self, instance: str) -> bool:
        """Submit one baseline-valid task, rechecking admission at claim time."""
        self._refresh_sequential_records()
        if not self._baseline_is_valid(instance):
            return False
        generation = self.generations[instance]
        record = prepare_attempt(
            self.records.get(instance),
            instance,
            generation,
            self.worker_id,
        )
        self.append(record, publish=False)
        stage = record["reward_hack"]
        stage.update({"state": "running", "model": self.llm_config.model})
        record.update(
            {
                "status": "running",
                "stage": "reward_hack",
                "started_at": utc_now(),
                "finished_at": None,
                "next_retry_at": None,
                "error": None,
            }
        )
        self.append(record, publish=False)
        try:
            future = self.executor.submit(self.execute, instance, copy.deepcopy(record))
        except Exception as error:
            self.mark_error(record, error)
            return False
        self.futures[future] = (instance, record)
        self.publish_status("running")
        return True

    def mark_error(self, record: dict[str, Any], error: BaseException | str) -> None:
        message = redact(str(error))[-4000:]
        stage = record.get("reward_hack")
        if not isinstance(stage, dict):
            stage = reward_stage()
            record["reward_hack"] = stage
        stage.update({"state": "error", "is_hacking": None, "error": message})
        record.update(
            {
                "status": "error",
                "stage": "reward_hack",
                "finished_at": utc_now(),
                "next_retry_at": (
                    datetime.now(UTC) + timedelta(seconds=self.args.retry_delay)
                ).isoformat(timespec="seconds"),
                "error": message,
            }
        )
        self.append(record)

    def finish(self, record: dict[str, Any], result: BackfillWorkResult) -> None:
        selected_config, verdict, attempts = result.reward
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        write_instance_log(
            self.logs_dir / f"{result.task_dir.name}.log",
            result.task_dir.name,
            attempts,
        )
        stage = record["reward_hack"]
        stage.update(
            {
                "state": "error" if verdict.error else "fail" if verdict.is_hacking else "pass",
                "is_hacking": verdict.is_hacking if not verdict.error else None,
                "model": selected_config.model,
                "attempted_models": [config.model for config, _value in attempts],
                "fallback_used": len(attempts) > 1,
                "fallback_reason": (
                    redact(str(attempts[0][1].error))[-4000:] if len(attempts) > 1 else None
                ),
                "test_framework": verdict.test_framework or None,
                "reason": verdict.reason,
                "error": redact(verdict.error)[-4000:] if verdict.error else None,
            }
        )
        if verdict.error:
            self.mark_error(record, verdict.error)
            return
        record.update(
            {
                "status": "fail" if verdict.is_hacking else "pass",
                "stage": "reward_hack",
                "finished_at": utc_now(),
                "next_retry_at": None,
                "error": None,
            }
        )
        self.append(record)

    def drain(self, *, block: bool = False) -> int:
        if not self.futures:
            return 0
        done = {future for future in self.futures if future.done()}
        if block and not done:
            done, _pending = wait(tuple(self.futures), return_when=FIRST_COMPLETED)
        for future in done:
            _instance, record = self.futures.pop(future)
            try:
                self.finish(record, future.result())
            except Exception as error:
                self.mark_error(record, error)
        return len(done)

    def counts(self) -> dict[str, int]:
        counts = {
            "generated_success": len(self.generations),
            "eligible": 0,
            "waiting_for_baseline": 0,
            "retained_before_baseline": 0,
            "not_queued": 0,
            "queued": 0,
            "running": 0,
            "pass": 0,
            "fail": 0,
            "errors": 0,
            "sequential_running": 0,
            "sequential_terminal": 0,
        }
        active = {instance for instance, _record in self.futures.values()}
        for instance in self.generations:
            record = self.records.get(instance)
            status = record.get("status") if isinstance(record, dict) else None
            if not self._baseline_is_valid(instance):
                counts["waiting_for_baseline"] += 1
                if status in TERMINAL_STATUSES:
                    # Keep older restart-safe results on disk, but do not count
                    # them as Stage 3 output until Stage 2 admits the instance.
                    counts["retained_before_baseline"] += 1
                continue
            counts["eligible"] += 1
            sequential_state = reward_state(self.sequential_records.get(instance))
            if status in TERMINAL_STATUSES:
                counts[str(status)] += 1
            elif instance in active or status == "running":
                counts["running"] += 1
            elif sequential_state in TERMINAL_STATUSES:
                counts["sequential_terminal"] += 1
            elif sequential_state == "running":
                counts["sequential_running"] += 1
            elif status == "queued":
                counts["queued"] += 1
            elif status == "error":
                counts["errors"] += 1
            else:
                counts["not_queued"] += 1
        return counts

    def publish_status(self, state: str, error: str | None = None) -> None:
        self._refresh_sequential_records()
        current_instances = sorted(instance for instance, _record in self.futures.values())
        recent = sorted(
            (record for instance, record in self.records.items() if instance in self.generations),
            key=lambda record: str(record.get("timestamp") or ""),
            reverse=True,
        )[:200]
        private_json(
            self.status_path,
            {
                "schema_version": 1,
                "event": "reward_backfill_worker_status",
                "timestamp": utc_now(),
                "state": state,
                "worker_id": self.worker_id,
                "checker_node": socket.gethostname(),
                "poll_interval_seconds": self.args.poll_interval,
                "queue_order": "oldest_first",
                "queue_policy": "exact_nop_0_oracle_1",
                "current_instance": current_instances[0] if current_instances else None,
                "current_instances": current_instances,
                "reward_model": self.llm_config.model,
                "reward_primary_model": self.llm_config.model,
                "reward_fallback_model": (
                    self.fallback_llm_config.model if self.fallback_llm_config else None
                ),
                "reward_concurrency": self.args.reward_concurrency,
                "reward_active_count": len(self.futures),
                "counts": self.counts(),
                "recent": recent,
                "error": redact(error) if error else None,
            },
        )

    def run(self) -> int:
        submitted = 0
        last_discovery = 0.0
        try:
            while True:
                self.drain()
                if self.stop_requested:
                    break

                now = time.monotonic()
                if not self.generations or now - last_discovery >= self.args.poll_interval:
                    self.discover()
                    last_discovery = now

                while len(self.futures) < self.args.reward_concurrency:
                    if self.args.max_tasks > 0 and submitted >= self.args.max_tasks:
                        break
                    instance = self.next_ready()
                    if instance is None:
                        break
                    if self.submit(instance):
                        submitted += 1

                max_reached = self.args.max_tasks > 0 and submitted >= self.args.max_tasks
                if self.futures:
                    self.publish_status("draining" if max_reached else "running")
                    done, _pending = wait(
                        tuple(self.futures),
                        timeout=min(self.args.poll_interval, 60),
                        return_when=FIRST_COMPLETED,
                    )
                    if done:
                        self.drain()
                    continue

                if max_reached or self.args.once:
                    break

                self.publish_status("idle")
                deadline = time.monotonic() + min(self.args.poll_interval, 60)
                while not self.stop_requested and time.monotonic() < deadline:
                    time.sleep(min(1.0, deadline - time.monotonic()))

            if self.futures:
                self.publish_status("stopping" if self.stop_requested else "draining")
            while self.futures:
                self.drain(block=True)
            self.publish_status("stopped" if self.stop_requested else "idle")
            return 0
        except Exception as error:
            while self.futures:
                self.drain(block=True)
            self.publish_status("error", str(error))
            raise
        finally:
            self.executor.shutdown(wait=True, cancel_futures=False)
            self.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--plan", required=True, action="append", type=Path)
    parser.add_argument("--worker-dir", type=Path)
    parser.add_argument("--pid-file", type=Path)
    parser.add_argument("--postcheck-ledger", type=Path)
    parser.add_argument("--worker-id", default="reward-backfill-0")
    parser.add_argument("--instance")
    parser.add_argument("--poll-interval", type=int, default=600)
    parser.add_argument("--retry-delay", type=int, default=600)
    parser.add_argument("--fetch-timeout", type=int, default=600)
    parser.add_argument("--max-tasks", type=int, default=0)
    parser.add_argument("--once", action="store_true")
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
    parser.add_argument("--reward-fallback-checker-name", default="terra-lightweight")
    parser.add_argument(
        "--reward-concurrency",
        type=int,
        default=20,
        help="Maximum concurrent tests-only reward checks (default: 20).",
    )
    args = parser.parse_args(argv)
    if args.poll_interval < 1 or args.retry_delay < 1 or args.fetch_timeout < 1:
        parser.error("poll, retry, and fetch timeouts must be positive")
    if args.max_tasks < 0:
        parser.error("--max-tasks may not be negative")
    if args.reward_concurrency < 1:
        parser.error("--reward-concurrency must be positive")
    if args.instance is not None and not SAFE_INSTANCE_RE.fullmatch(args.instance):
        parser.error("--instance contains unsafe characters")
    return args


def main(argv: list[str] | None = None) -> int:
    os.umask(0o077)
    args = parse_args(argv)
    worker = RewardBackfillWorker(args)

    def request_stop(_signum: int, _frame: object) -> None:
        worker.stop_requested = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    return worker.run()


if __name__ == "__main__":
    raise SystemExit(main())
