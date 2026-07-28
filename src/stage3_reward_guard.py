#!/usr/bin/env python3
"""Guard Stage 3 reward checks and journal 15-minute funnel progress.

The guard reads only newly appended reward-backfill ledger records.  Exact
rate-limit/authentication failures stop the verified Stage 3 worker
immediately.  Retry-exhausted connection and proxy failures are tolerated
unless they become a substantial share of a rolling window.  No Stage 1,
Stage 2, collector, dashboard, Slurm job, or Docker process is targeted.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import signal
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

OUTCOME_STATUSES = {"pass", "fail", "error"}
IMMEDIATE_ERROR_RE = re.compile(
    r"(?i)(?:"
    r"\bHTTP(?:/\d(?:\.\d)?)?\s+(?:401|403|429)\b|"
    r"\b(?:401\s+Unauthorized|403\s+Forbidden|429\s+Too\s+Many\s+Requests)\b|"
    r"\brate\s+limit\s+exceeded\b|"
    r"\ball\s+credentials\b[^\n]{0,160}\bcooling\s+down\b"
    r")"
)
TRANSIENT_ERROR_RE = re.compile(
    r"(?i)(?:"
    r"ConnectError|ConnectTimeout|ReadTimeout|PoolTimeout|RemoteProtocolError|"
    r"ProxyError|ECONNRESET|ETIMEDOUT|connection\s+reset|"
    r"(?:HTTP\s+)?(?:502|503|504)\b"
    r")"
)


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def parse_timestamp(value: object) -> float | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def private_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def append_private_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def error_text(record: dict[str, Any]) -> str:
    stage = record.get("reward_hack")
    if not isinstance(stage, dict):
        stage = {}
    values = (
        record.get("error"),
        stage.get("error"),
        stage.get("fallback_reason"),
    )
    return "\n".join(str(value) for value in values if value)


@dataclass(frozen=True)
class Outcome:
    timestamp: float
    instance: str
    status: str
    category: str
    detail: str = ""

    def as_state(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "instance": self.instance,
            "status": self.status,
            "category": self.category,
        }


def classify_record(record: object) -> Outcome | None:
    if not isinstance(record, dict):
        return None
    status = record.get("status")
    if status not in OUTCOME_STATUSES:
        return None
    timestamp = parse_timestamp(record.get("timestamp"))
    if timestamp is None:
        return None
    instance = str(record.get("instance") or "unknown")
    detail = error_text(record)
    if IMMEDIATE_ERROR_RE.search(detail):
        category = "immediate_api_error"
    elif status == "error" and TRANSIENT_ERROR_RE.search(detail):
        category = "transient_api_error"
    elif status == "error":
        category = "other_error"
    else:
        category = "completed"
    return Outcome(timestamp, instance, str(status), category, detail[-1000:])


def load_recent_outcomes(value: object) -> list[Outcome]:
    if not isinstance(value, list):
        return []
    outcomes: list[Outcome] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        try:
            outcomes.append(
                Outcome(
                    timestamp=float(item["timestamp"]),
                    instance=str(item.get("instance") or "unknown"),
                    status=str(item.get("status") or "unknown"),
                    category=str(item.get("category") or "other_error"),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return outcomes


def transient_trip(
    outcomes: list[Outcome],
    *,
    concurrency: int,
    minimum: int,
    rate: float,
) -> tuple[bool, dict[str, Any]]:
    transient = sum(item.category == "transient_api_error" for item in outcomes)
    denominator = len(outcomes)
    observed_rate = transient / denominator if denominator else 0.0
    effective_minimum = max(minimum, math.ceil(concurrency / 2))
    details = {
        "outcomes": denominator,
        "transient_errors": transient,
        "transient_error_rate": round(observed_rate, 4),
        "required_errors": effective_minimum,
        "required_rate": rate,
    }
    return transient >= effective_minimum and observed_rate >= rate, details


def read_appended_jsonl(
    path: Path,
    *,
    inode: int | None,
    offset: int,
) -> tuple[list[dict[str, Any]], int | None, int]:
    try:
        stat = path.stat()
    except OSError:
        return [], inode, offset
    if inode != stat.st_ino or stat.st_size < offset:
        inode = stat.st_ino
        offset = 0
    try:
        with path.open("rb") as stream:
            stream.seek(offset)
            data = stream.read()
    except OSError:
        return [], inode, offset
    newline = data.rfind(b"\n")
    if newline < 0:
        return [], inode, offset
    complete = data[: newline + 1]
    offset += newline + 1
    records: list[dict[str, Any]] = []
    for raw_line in complete.splitlines():
        try:
            value = json.loads(raw_line)
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(value, dict):
            records.append(value)
    return records, inode, offset


def fetch_dashboard(url: str, timeout: float) -> dict[str, Any]:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with opener.open(request, timeout=timeout) as response:
            value = json.load(response)
    except (OSError, ValueError, urllib.error.URLError) as error:
        return {"error": f"{type(error).__name__}: {error}"[-1000:]}
    if not isinstance(value, dict):
        return {"error": "dashboard returned a non-object response"}
    funnel = value.get("funnel")
    health = value.get("health")
    if not isinstance(funnel, dict):
        funnel = {}
    if not isinstance(health, dict):
        health = {}
    counts: dict[str, int] = {}
    for key in ("generated", "baseline_valid", "fully_accepted"):
        stage = funnel.get(key)
        count = stage.get("count") if isinstance(stage, dict) else None
        if isinstance(count, int) and not isinstance(count, bool):
            counts[key] = count
    reward = health.get("reward")
    baseline = health.get("baseline")
    return {
        "counts": counts,
        "baseline_state": baseline.get("state") if isinstance(baseline, dict) else None,
        "reward_state": reward.get("state") if isinstance(reward, dict) else None,
        "reward_active_count": (
            reward.get("reward_active_count") if isinstance(reward, dict) else None
        ),
    }


def funnel_event(snapshot: dict[str, Any], previous: object) -> dict[str, Any]:
    counts = snapshot.get("counts")
    if not isinstance(counts, dict):
        counts = {}
    old_counts = previous if isinstance(previous, dict) else {}
    delta: dict[str, int | None] = {}
    for key, current in counts.items():
        old = old_counts.get(key)
        delta[key] = current - old if isinstance(old, int) else None
    stage2_delta = delta.get("baseline_valid")
    stage3_delta = delta.get("fully_accepted")
    return {
        "event": "funnel_check",
        "timestamp": utc_now(),
        "funnel": counts,
        "delta": delta,
        "stage2_increasing": (
            None if "baseline_valid" not in counts or stage2_delta is None else stage2_delta > 0
        ),
        "stage3_increasing": (
            None if "fully_accepted" not in counts or stage3_delta is None else stage3_delta > 0
        ),
        "baseline_state": snapshot.get("baseline_state"),
        "reward_state": snapshot.get("reward_state"),
        "reward_active_count": snapshot.get("reward_active_count"),
        "error": snapshot.get("error"),
    }


def read_worker_pid(path: Path, run_dir: Path) -> tuple[int | None, str | None]:
    try:
        pid = int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None, "worker pid file is absent or invalid"
    proc = Path("/proc") / str(pid)
    try:
        argv = [
            part.decode(errors="replace")
            for part in (proc / "cmdline").read_bytes().split(b"\0")
            if part
        ]
        cwd = (proc / "cwd").resolve()
    except OSError:
        return None, f"worker pid {pid} is not running"
    if not any(part.endswith("slurm_reward_backfill_worker.py") for part in argv):
        return None, f"pid {pid} is not the Stage 3 reward worker"
    try:
        index = argv.index("--run-dir")
        configured = Path(argv[index + 1])
    except (ValueError, IndexError):
        return None, f"Stage 3 pid {pid} has no --run-dir argument"
    configured = configured if configured.is_absolute() else cwd / configured
    if configured.resolve() != run_dir.resolve():
        return None, f"Stage 3 pid {pid} belongs to a different run directory"
    return pid, None


def stop_stage3(pid_file: Path, run_dir: Path, dry_run: bool) -> dict[str, Any]:
    pid, error = read_worker_pid(pid_file, run_dir)
    if pid is None:
        return {"action": "worker_absent", "pid": None, "error": error}
    if dry_run:
        return {"action": "would_signal_stage3", "pid": pid, "signal": "SIGTERM"}
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError as stop_error:
        return {
            "action": "signal_failed",
            "pid": pid,
            "signal": "SIGTERM",
            "error": f"{type(stop_error).__name__}: {stop_error}",
        }
    return {"action": "signaled_stage3", "pid": pid, "signal": "SIGTERM"}


class Stage3Guard:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.run_dir = args.run_dir.resolve()
        self.worker_dir = (args.worker_dir or self.run_dir / ".validation-worker").resolve()
        self.ledger_path = self.worker_dir / "reward-backfill-status.jsonl"
        self.worker_pid_path = (
            args.worker_pid_file or self.worker_dir / "reward-backfill-worker.pid"
        ).resolve()
        self.state_path = (
            args.state_file or self.worker_dir / "stage3-reward-guard-state.json"
        ).resolve()
        self.output_path = (
            args.output_jsonl or self.worker_dir / "stage3-reward-guard.jsonl"
        ).resolve()
        self.status_path = (
            args.status_file or self.worker_dir / "stage3-reward-guard-status.json"
        ).resolve()
        self.pid_path = (args.pid_file or self.worker_dir / "stage3-reward-guard.pid").resolve()
        self.started_epoch = time.time()
        self.state = load_json(self.state_path)
        self.state["triggered"] = False
        self.recent = load_recent_outcomes(self.state.get("recent_outcomes"))
        self.stop_requested = threading.Event()
        self._claim_pid()

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
                        raise RuntimeError(f"guard pid {existing} cannot be inspected") from error
                    else:
                        raise RuntimeError(f"Stage 3 guard already running with pid {existing}")
                self.pid_path.unlink(missing_ok=True)
                continue
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(f"{os.getpid()}\n")
            return
        raise RuntimeError(f"unable to claim Stage 3 guard pid file: {self.pid_path}")

    def close(self) -> None:
        try:
            if self.pid_path.read_text(encoding="utf-8").strip() == str(os.getpid()):
                self.pid_path.unlink(missing_ok=True)
        except OSError:
            pass

    def save(self) -> None:
        self.state["recent_outcomes"] = [item.as_state() for item in self.recent]
        private_json(self.state_path, self.state)

    def emit(self, value: dict[str, Any]) -> None:
        append_private_jsonl(self.output_path, value)
        print(json.dumps(value, sort_keys=True), flush=True)

    def publish_status(self, state: str, **fields: Any) -> None:
        private_json(
            self.status_path,
            {
                "schema_version": 1,
                "event": "stage3_reward_guard_status",
                "timestamp": utc_now(),
                "state": state,
                "scan_interval_seconds": self.args.scan_interval,
                "funnel_interval_seconds": self.args.funnel_interval,
                "transient_window_seconds": self.args.transient_window,
                "transient_minimum": max(
                    self.args.transient_minimum,
                    math.ceil(self.args.reward_concurrency / 2),
                ),
                "transient_rate": self.args.transient_rate,
                "dry_run": self.args.dry_run,
                **fields,
            },
        )

    def scan(self) -> dict[str, Any] | None:
        inode_value = self.state.get("ledger_inode")
        inode = inode_value if isinstance(inode_value, int) else None
        offset_value = self.state.get("ledger_offset")
        offset = offset_value if isinstance(offset_value, int) else 0
        resuming = inode is not None
        records, inode, offset = read_appended_jsonl(
            self.ledger_path,
            inode=inode,
            offset=offset,
        )
        self.state["ledger_inode"] = inode
        self.state["ledger_offset"] = offset

        cutoff = time.time() - self.args.transient_window
        initial_cutoff = self.started_epoch - self.args.lookback_seconds
        new_outcomes: list[Outcome] = []
        for record in records:
            outcome = classify_record(record)
            if outcome is None or (not resuming and outcome.timestamp < initial_cutoff):
                continue
            new_outcomes.append(outcome)
            self.recent.append(outcome)
        self.recent = [item for item in self.recent if item.timestamp >= cutoff]

        immediate = next(
            (item for item in new_outcomes if item.category == "immediate_api_error"),
            None,
        )
        tripped, transient = transient_trip(
            self.recent,
            concurrency=self.args.reward_concurrency,
            minimum=self.args.transient_minimum,
            rate=self.args.transient_rate,
        )
        if immediate is not None:
            return {
                "reason": "immediate_api_error",
                "instance": immediate.instance,
                "status": immediate.status,
                "detail": immediate.detail,
                "window": transient,
            }
        if tripped:
            return {"reason": "major_transient_api_error_rate", "window": transient}
        if new_outcomes:
            counts: dict[str, int] = {}
            for item in new_outcomes:
                counts[item.category] = counts.get(item.category, 0) + 1
            self.emit(
                {
                    "event": "api_check",
                    "timestamp": utc_now(),
                    "new_outcomes": len(new_outcomes),
                    "categories": counts,
                    "window": transient,
                }
            )
        return None

    def record_funnel(self) -> None:
        snapshot = fetch_dashboard(self.args.dashboard_url, self.args.dashboard_timeout)
        event = funnel_event(snapshot, self.state.get("last_funnel"))
        self.emit(event)
        counts = snapshot.get("counts")
        if isinstance(counts, dict) and counts:
            self.state["last_funnel"] = counts
            self.state["last_funnel_at"] = event["timestamp"]

    def run(self) -> int:
        next_funnel = 0.0
        try:
            while not self.stop_requested.is_set():
                now = time.monotonic()
                if now >= next_funnel:
                    self.record_funnel()
                    next_funnel = now + self.args.funnel_interval

                trigger = self.scan()
                self.save()
                if trigger is not None:
                    stop = stop_stage3(
                        self.worker_pid_path,
                        self.run_dir,
                        self.args.dry_run,
                    )
                    event = {
                        "event": "stage3_stop",
                        "timestamp": utc_now(),
                        "trigger": trigger,
                        "stop": stop,
                    }
                    self.state.update(
                        {
                            "triggered": True,
                            "triggered_at": event["timestamp"],
                            "trigger": trigger,
                            "stop": stop,
                        }
                    )
                    self.save()
                    self.emit(event)
                    self.publish_status("triggered", trigger=trigger, stop=stop)
                    return 0

                self.publish_status("monitoring")
                if self.args.once:
                    return 0
                self.stop_requested.wait(self.args.scan_interval)
            self.publish_status("stopped")
            return 0
        finally:
            self.save()
            self.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--worker-dir", type=Path)
    parser.add_argument("--worker-pid-file", type=Path)
    parser.add_argument("--state-file", type=Path)
    parser.add_argument("--output-jsonl", type=Path)
    parser.add_argument("--status-file", type=Path)
    parser.add_argument("--pid-file", type=Path)
    parser.add_argument("--dashboard-url", default="http://127.0.0.1:8765/api/status")
    parser.add_argument("--dashboard-timeout", type=float, default=10.0)
    parser.add_argument("--scan-interval", type=float, default=15.0)
    parser.add_argument("--funnel-interval", type=float, default=900.0)
    parser.add_argument("--transient-window", type=float, default=300.0)
    parser.add_argument("--transient-minimum", type=int, default=10)
    parser.add_argument("--transient-rate", type=float, default=0.25)
    parser.add_argument("--reward-concurrency", type=int, default=20)
    parser.add_argument("--lookback-seconds", type=float, default=30.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    positive = {
        "dashboard timeout": args.dashboard_timeout,
        "scan interval": args.scan_interval,
        "funnel interval": args.funnel_interval,
        "transient window": args.transient_window,
        "reward concurrency": args.reward_concurrency,
    }
    for name, value in positive.items():
        if value <= 0:
            parser.error(f"--{name.replace(' ', '-')} must be positive")
    if args.transient_minimum < 1:
        parser.error("--transient-minimum must be positive")
    if not 0 < args.transient_rate <= 1:
        parser.error("--transient-rate must be in (0, 1]")
    if args.lookback_seconds < 0:
        parser.error("--lookback-seconds may not be negative")
    return args


def main(argv: list[str] | None = None) -> int:
    os.umask(0o077)
    args = parse_args(argv)
    guard = Stage3Guard(args)

    def request_stop(_signum: int, _frame: object) -> None:
        guard.stop_requested.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    return guard.run()


if __name__ == "__main__":
    raise SystemExit(main())
