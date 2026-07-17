#!/usr/bin/env python3
"""Launch the r2 SG-extra batch after sustained multi-proxy health.

This gate observes only the authoritative r2 health snapshot.  Every observed
snapshot must report ``overall_healthy=true`` and have a fresh UTC timestamp
for one continuous stability window.  A missing, malformed, stale, future, or
unhealthy snapshot immediately resets that window.

Once the window is satisfied, the gate starts one four-worker SG-extra tmux
session through ``run_orchestrator.sh`` with ``.env`` and maintains a dedicated
tmux health-monitor session for that worker group.  Commands contain only fixed
paths and non-secret settings; proxy URLs and credentials remain exclusively
inside the launcher's inherited environment and are never reported here.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import pwd
import shlex
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REQUIRED_USER = "alex"
RUN_NAME = "20260716-sol-max-full-16w"
WORKER_COUNT = 4
REQUIRED_GROUP_NAMES = frozenset({"SG", "HK-A", "HK-B", "WG-A", "WG-B"})

HEALTH_RELATIVE_PATH = Path("runs") / RUN_NAME / "multi-proxy-health-r2.json"
GATE_STATUS_RELATIVE_PATH = Path("runs") / RUN_NAME / "sg-extra-stability-gate-r2.json"
GATE_PID_RELATIVE_PATH = Path("runs") / RUN_NAME / "sg-extra-stability-gate-r2.pid"

INPUT_RELATIVE_PATH = Path("data_cache/orchestrator_shards/sg-extra-doubletons.jsonl")
WORKER_LOG_RELATIVE_PATH = Path("runs") / RUN_NAME / "orchestrator-logs-sg-extra-r2-4w"
WORKER_LAUNCH_LOG_RELATIVE_PATH = Path("runs") / RUN_NAME / "orchestrator-sg-extra-r2-4w-launch.log"
WORKER_PROGRESS_RELATIVE_PATH = Path("runs") / RUN_NAME / "orchestrator-progress-sg-extra-r2.jsonl"
WORKER_INSTANCE_STATUS_RELATIVE_PATH = (
    Path("runs") / RUN_NAME / "orchestrator-instance-status-sg-extra-r2.jsonl"
)

WORKER_SESSION = "swegen-sg-extra-r2"
MONITOR_SESSION = "swegen-sg-extra-monitor-r2"
MONITOR_STATUS_RELATIVE_PATH = Path("runs") / RUN_NAME / "multi-proxy-health-sg-extra-r2.json"
MONITOR_PID_RELATIVE_PATH = Path("runs") / RUN_NAME / "multi-proxy-health-sg-extra-r2.pid"
MONITOR_LOG_RELATIVE_PATH = Path("runs") / RUN_NAME / "multi-proxy-health-sg-extra-r2-monitor.log"


def utc_timestamp(epoch: float | None = None) -> str:
    value = datetime.now(UTC) if epoch is None else datetime.fromtimestamp(epoch, UTC)
    return value.isoformat(timespec="seconds")


def parse_utc_timestamp(value: object) -> float | None:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.timestamp()


@dataclass(frozen=True)
class HealthEvaluation:
    source_available: bool
    source_overall_healthy: bool
    source_fresh: bool
    acceptable: bool
    reason: str
    source_timestamp: str | None = None
    source_age_seconds: float | None = None
    cumulative_error_counts: tuple[tuple[str, int], ...] = ()


def evaluate_health_record(
    record: object,
    *,
    now_epoch: float,
    max_health_age_seconds: float,
    max_future_skew_seconds: float,
) -> HealthEvaluation:
    """Validate one health-monitor record without copying its group details."""
    if not isinstance(record, dict):
        return HealthEvaluation(False, False, False, False, "source_not_object")
    if record.get("event") != "multi_proxy_health":
        return HealthEvaluation(True, False, False, False, "source_event_invalid")

    groups = record.get("groups")
    if not isinstance(groups, list):
        return HealthEvaluation(True, False, False, False, "source_groups_invalid")
    named_groups = {
        group.get("name"): group
        for group in groups
        if isinstance(group, dict) and isinstance(group.get("name"), str)
    }
    if not REQUIRED_GROUP_NAMES.issubset(named_groups):
        return HealthEvaluation(True, False, False, False, "source_groups_missing")
    if record.get("expected_group_count") != len(REQUIRED_GROUP_NAMES):
        return HealthEvaluation(True, False, False, False, "source_group_count_invalid")

    required_groups_healthy = True
    cumulative_errors: dict[str, int] = {}
    for name in REQUIRED_GROUP_NAMES:
        group = named_groups[name]
        new_errors = group.get("new_error_counts")
        error_counts = group.get("error_counts")
        valid_new_errors = isinstance(new_errors, dict) and all(
            isinstance(value, int) and value >= 0 for value in new_errors.values()
        )
        zero_new_errors = valid_new_errors and not any(new_errors.values())
        valid_error_counts = isinstance(error_counts, dict) and all(
            isinstance(value, int) and value >= 0 for value in error_counts.values()
        )
        if valid_error_counts:
            for key, value in error_counts.items():
                cumulative_errors[str(key)] = cumulative_errors.get(str(key), 0) + value
        if (
            group.get("enabled") is not True
            or group.get("required") is not True
            or group.get("healthy") is not True
            or not zero_new_errors
            or not valid_error_counts
        ):
            required_groups_healthy = False
    cumulative_error_counts = tuple(sorted(cumulative_errors.items()))

    raw_timestamp = record.get("timestamp")
    source_epoch = parse_utc_timestamp(raw_timestamp)
    if source_epoch is None:
        return HealthEvaluation(
            True,
            record.get("overall_healthy") is True,
            False,
            False,
            "source_timestamp_invalid",
        )

    source_timestamp = str(raw_timestamp)
    age_seconds = now_epoch - source_epoch
    rounded_age = round(age_seconds, 3)
    overall_healthy = (
        record.get("overall_healthy") is True
        and record.get("healthy_expected_group_count") == len(REQUIRED_GROUP_NAMES)
        and required_groups_healthy
    )
    if age_seconds < -max_future_skew_seconds:
        return HealthEvaluation(
            True,
            overall_healthy,
            False,
            False,
            "source_timestamp_in_future",
            source_timestamp,
            rounded_age,
            cumulative_error_counts,
        )
    if age_seconds > max_health_age_seconds:
        return HealthEvaluation(
            True,
            overall_healthy,
            False,
            False,
            "source_stale",
            source_timestamp,
            rounded_age,
            cumulative_error_counts,
        )
    if not overall_healthy:
        return HealthEvaluation(
            True,
            False,
            True,
            False,
            "source_unhealthy",
            source_timestamp,
            rounded_age,
            cumulative_error_counts,
        )
    return HealthEvaluation(
        True,
        True,
        True,
        True,
        "healthy",
        source_timestamp,
        rounded_age,
        cumulative_error_counts,
    )


@dataclass
class ErrorCountTracker:
    """Catch error snapshots missed between polls via cumulative counter deltas."""

    previous_counts: tuple[tuple[str, int], ...] | None = None

    def observe(self, health: HealthEvaluation) -> HealthEvaluation:
        counts = health.cumulative_error_counts
        if not counts:
            self.previous_counts = None
            return health
        previous = self.previous_counts
        self.previous_counts = counts
        if previous is None or previous == counts:
            return health
        return replace(
            health,
            acceptable=False,
            reason="source_cumulative_errors_changed",
        )


def read_health_status(
    path: Path,
    *,
    now_epoch: float,
    max_health_age_seconds: float,
    max_future_skew_seconds: float,
) -> HealthEvaluation:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return HealthEvaluation(False, False, False, False, "source_missing")
    except OSError:
        return HealthEvaluation(False, False, False, False, "source_unreadable")
    try:
        record = json.loads(raw)
    except json.JSONDecodeError:
        return HealthEvaluation(True, False, False, False, "source_invalid_json")
    return evaluate_health_record(
        record,
        now_epoch=now_epoch,
        max_health_age_seconds=max_health_age_seconds,
        max_future_skew_seconds=max_future_skew_seconds,
    )


@dataclass(frozen=True)
class StabilityObservation:
    stable_since_epoch: float | None
    stable_elapsed_seconds: float
    stable_window_satisfied: bool
    reset_this_poll: bool


@dataclass
class StabilityTracker:
    required_seconds: float
    stable_since_epoch: float | None = None
    stable_since_monotonic: float | None = None

    def observe(
        self,
        acceptable: bool,
        *,
        now_epoch: float,
        now_monotonic: float,
    ) -> StabilityObservation:
        if not acceptable:
            reset = self.stable_since_monotonic is not None
            self.stable_since_epoch = None
            self.stable_since_monotonic = None
            return StabilityObservation(None, 0.0, False, reset)

        if self.stable_since_monotonic is None:
            self.stable_since_epoch = now_epoch
            self.stable_since_monotonic = now_monotonic
        elapsed = max(0.0, now_monotonic - self.stable_since_monotonic)
        return StabilityObservation(
            self.stable_since_epoch,
            round(elapsed, 3),
            elapsed >= self.required_seconds,
            False,
        )


@dataclass(frozen=True)
class SessionCommand:
    session_name: str
    argv: tuple[str, ...]


@dataclass(frozen=True)
class LaunchCommands:
    worker: SessionCommand
    monitor: SessionCommand


def _redirected_shell_command(argv: list[str], output_path: Path) -> str:
    # tmux accepts one shell-command string.  Every interpolated field is a
    # fixed path and is quoted individually; no generic user command enters it.
    return f"exec {shlex.join(argv)} >> {shlex.quote(str(output_path))} 2>&1"


def build_launch_commands(workspace: Path) -> LaunchCommands:
    workspace = workspace.resolve()
    run_dir = workspace / "runs" / RUN_NAME
    input_path = workspace / INPUT_RELATIVE_PATH
    worker_log_dir = workspace / WORKER_LOG_RELATIVE_PATH
    launch_log = workspace / WORKER_LAUNCH_LOG_RELATIVE_PATH
    progress_log = workspace / WORKER_PROGRESS_RELATIVE_PATH
    instance_status_log = workspace / WORKER_INSTANCE_STATUS_RELATIVE_PATH

    worker_shell = _redirected_shell_command(
        [str(workspace / "run_orchestrator.sh")],
        launch_log,
    )
    worker_argv = (
        "tmux",
        "new-session",
        "-d",
        "-s",
        WORKER_SESSION,
        "-c",
        str(workspace),
        "-e",
        "SWEGEN_PROXY_ENV_FILE=.env",
        "-e",
        f"SWEGEN_WORKERS={WORKER_COUNT}",
        "-e",
        f"SWEGEN_RUN_NAME={RUN_NAME}",
        "-e",
        f"SWEGEN_INPUT_JSONL={input_path}",
        "-e",
        f"SWEGEN_ORCHESTRATOR_LOG_DIR={worker_log_dir}",
        "-e",
        f"SWEGEN_PROGRESS_JSONL={progress_log}",
        "-e",
        f"SWEGEN_INSTANCE_STATUS_JSONL={instance_status_log}",
        worker_shell,
    )

    group = json.dumps(
        {
            "name": "SG-extra",
            "session": WORKER_SESSION,
            "input": INPUT_RELATIVE_PATH.name,
            "logs": str(worker_log_dir),
            "workers": WORKER_COUNT,
            "launch": str(launch_log),
            "progress": str(progress_log),
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    monitor_status = workspace / MONITOR_STATUS_RELATIVE_PATH
    monitor_pid = workspace / MONITOR_PID_RELATIVE_PATH
    monitor_log = workspace / MONITOR_LOG_RELATIVE_PATH
    monitor_shell = _redirected_shell_command(
        [
            str(workspace / ".venv/bin/python"),
            str(workspace / "src/multi_proxy_health_monitor.py"),
            "--run-dir",
            str(run_dir),
            "--interval",
            "30",
            "--max-log-age",
            "1800",
            "--status-file",
            str(monitor_status),
            "--pid-file",
            str(monitor_pid),
            "--group",
            group,
        ],
        monitor_log,
    )
    monitor_argv = (
        "tmux",
        "new-session",
        "-d",
        "-s",
        MONITOR_SESSION,
        "-c",
        str(workspace),
        monitor_shell,
    )
    return LaunchCommands(
        worker=SessionCommand(WORKER_SESSION, worker_argv),
        monitor=SessionCommand(MONITOR_SESSION, monitor_argv),
    )


def tmux_session_active(session_name: str) -> bool:
    try:
        completed = subprocess.run(
            ["tmux", "has-session", "-t", f"={session_name}"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return completed.returncode == 0


@dataclass(frozen=True)
class SessionResult:
    active: bool
    action: str


def ensure_tmux_session(command: SessionCommand) -> SessionResult:
    if tmux_session_active(command.session_name):
        return SessionResult(True, "already_active")
    try:
        completed = subprocess.run(
            command.argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        # A timed-out client can race with successful server-side creation.
        active = tmux_session_active(command.session_name)
        return SessionResult(active, "active_after_uncertain_launch" if active else "launch_failed")
    if completed.returncode == 0:
        return SessionResult(True, "launched")
    active = tmux_session_active(command.session_name)
    return SessionResult(active, "already_active" if active else "launch_failed")


def atomic_write_json(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


@dataclass
class GateRuntime:
    scale_triggered: bool = False
    worker_session_started: bool = False


def restore_runtime(path: Path) -> GateRuntime:
    """Restore only durable launch milestones, never an unfinished timer."""
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return GateRuntime()
    if not isinstance(record, dict) or record.get("event") != "sg_extra_stability_gate":
        return GateRuntime()
    launch = record.get("launch")
    if not isinstance(launch, dict):
        launch = {}
    return GateRuntime(
        scale_triggered=record.get("scale_triggered") is True,
        worker_session_started=launch.get("worker_session_started") is True,
    )


class PidFileLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._file: Any | None = None

    def __enter__(self) -> PidFileLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        file = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            file.close()
            raise RuntimeError("another SG-extra stability gate is already running") from None
        file.seek(0)
        file.truncate()
        file.write(f"{os.getpid()}\n")
        file.flush()
        os.fsync(file.fileno())
        self._file = file
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        if self._file is None:
            return
        try:
            self._file.seek(0)
            if self._file.read().strip() == str(os.getpid()):
                self.path.unlink(missing_ok=True)
        except OSError:
            pass
        finally:
            self._file.close()
            self._file = None


def build_status_record(
    *,
    now_epoch: float,
    health: HealthEvaluation,
    stability: StabilityObservation,
    stable_seconds: float,
    runtime: GateRuntime,
    worker_result: SessionResult,
    monitor_result: SessionResult,
    stop_requested: bool = False,
) -> dict[str, Any]:
    if stop_requested:
        state = "stopping"
    elif runtime.scale_triggered and runtime.worker_session_started:
        state = "scaled"
    elif runtime.scale_triggered:
        state = "launch_pending"
    elif health.acceptable:
        state = "waiting_for_stability"
    else:
        state = "waiting_for_health"
    return {
        "timestamp": utc_timestamp(now_epoch),
        "event": "sg_extra_stability_gate",
        "topology": "r2",
        "state": state,
        "source_health_file": str(HEALTH_RELATIVE_PATH),
        "source_health": {
            "available": health.source_available,
            "overall_healthy": health.source_overall_healthy,
            "fresh": health.source_fresh,
            "acceptable": health.acceptable,
            "reason": health.reason,
            "timestamp": health.source_timestamp,
            "age_seconds": health.source_age_seconds,
        },
        "stable_seconds_required": stable_seconds,
        "stable_since": (
            utc_timestamp(stability.stable_since_epoch)
            if stability.stable_since_epoch is not None
            else None
        ),
        "stable_elapsed_seconds": stability.stable_elapsed_seconds,
        "stable_window_satisfied": stability.stable_window_satisfied,
        "stable_window_reset_this_poll": stability.reset_this_poll,
        "scale_triggered": runtime.scale_triggered,
        "launch": {
            "worker_session": WORKER_SESSION,
            "worker_count": WORKER_COUNT,
            "worker_session_started": runtime.worker_session_started,
            "worker_session_active": worker_result.active,
            "worker_action": worker_result.action,
            "monitor_session": MONITOR_SESSION,
            "monitor_session_active": monitor_result.active,
            "monitor_action": monitor_result.action,
        },
    }


def emit_status(path: Path, record: dict[str, Any]) -> None:
    atomic_write_json(path, record)
    print(json.dumps(record, sort_keys=True), flush=True)


def require_run_user() -> None:
    try:
        expected_uid = pwd.getpwnam(REQUIRED_USER).pw_uid
    except KeyError:
        raise SystemExit(f"required account {REQUIRED_USER!r} does not exist") from None
    if os.geteuid() != expected_uid:
        raise SystemExit(
            f"run this gate as {REQUIRED_USER} (for example with sudo -u {REQUIRED_USER})"
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stable-seconds", type=float, default=1800.0)
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument(
        "--max-health-age",
        type=float,
        default=90.0,
        help="Maximum source snapshot age in seconds (default: 90)",
    )
    parser.add_argument(
        "--max-future-skew",
        type=float,
        default=5.0,
        help="Allowed source clock lead in seconds (default: 5)",
    )
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    if args.stable_seconds <= 0:
        parser.error("--stable-seconds must be greater than zero")
    if args.interval <= 0:
        parser.error("--interval must be greater than zero")
    if args.max_health_age <= 0:
        parser.error("--max-health-age must be greater than zero")
    if args.max_future_skew < 0:
        parser.error("--max-future-skew cannot be negative")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    require_run_user()
    workspace = Path(__file__).resolve().parents[1]
    health_path = workspace / HEALTH_RELATIVE_PATH
    status_path = workspace / GATE_STATUS_RELATIVE_PATH
    pid_path = workspace / GATE_PID_RELATIVE_PATH
    commands = build_launch_commands(workspace)
    tracker = StabilityTracker(args.stable_seconds)
    error_tracker = ErrorCountTracker()
    runtime = restore_runtime(status_path)
    stop_event = threading.Event()
    last_record: dict[str, Any] | None = None

    def request_stop(_signum: int, _frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    try:
        with PidFileLock(pid_path):
            while not stop_event.is_set():
                now_epoch = time.time()
                health = error_tracker.observe(
                    read_health_status(
                        health_path,
                        now_epoch=now_epoch,
                        max_health_age_seconds=args.max_health_age,
                        max_future_skew_seconds=args.max_future_skew,
                    )
                )
                stability = tracker.observe(
                    health.acceptable,
                    now_epoch=now_epoch,
                    now_monotonic=time.monotonic(),
                )
                if stability.stable_window_satisfied:
                    runtime.scale_triggered = True

                worker_result = SessionResult(
                    tmux_session_active(WORKER_SESSION),
                    "not_requested",
                )
                monitor_result = SessionResult(
                    tmux_session_active(MONITOR_SESSION),
                    "not_requested",
                )
                if runtime.scale_triggered and not stop_event.is_set():
                    if not runtime.worker_session_started:
                        worker_result = ensure_tmux_session(commands.worker)
                        if worker_result.active:
                            runtime.worker_session_started = True
                    else:
                        worker_result = SessionResult(
                            tmux_session_active(WORKER_SESSION),
                            "previously_started",
                        )
                    # The worker itself is intentionally never relaunched after
                    # a successful start.  Its monitor is kept continuously
                    # present and can safely be recreated by its fixed name.
                    if runtime.worker_session_started:
                        monitor_result = ensure_tmux_session(commands.monitor)

                last_record = build_status_record(
                    now_epoch=now_epoch,
                    health=health,
                    stability=stability,
                    stable_seconds=args.stable_seconds,
                    runtime=runtime,
                    worker_result=worker_result,
                    monitor_result=monitor_result,
                )
                emit_status(status_path, last_record)
                if args.once:
                    break
                stop_event.wait(args.interval)

            if stop_event.is_set() and last_record is not None:
                stopping_record = {
                    **last_record,
                    "timestamp": utc_timestamp(),
                    "state": "stopping",
                }
                emit_status(status_path, stopping_record)
    except RuntimeError as error:
        raise SystemExit(str(error)) from None
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
