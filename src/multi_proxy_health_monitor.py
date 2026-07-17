#!/usr/bin/env python3
"""Continuously report health for independently proxied SWE-gen worker groups.

Each ``--group`` identifies one orchestrator by its tmux session, input-file
marker, worker log directory, and expected worker count.  Group specifications
may be JSON objects or comma-separated ``key=value`` fields, for example::

    --group 'name=SG-main,session=swegen-sg,input=prs.jsonl,logs=orchestrator-logs-sg,workers=4'
    --group '{"name":"HK-A","session":"swegen-hk-a","input":"hk-a.jsonl",\
              "logs":"orchestrator-logs-hk-a-4w","workers":4}'

Relative log paths are resolved beneath ``--run-dir``.  ``enabled=false`` is
useful for groups that are configured but not launched yet; ``required=false``
keeps an enabled observational group from affecting overall health.

The monitor deliberately never emits process command lines or environment
values.  Process arguments are read only to attribute descendants to the
correct orchestrator and are discarded before the JSON record is built.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import signal
import subprocess
import threading
import time
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ERROR_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "api_retry",
        re.compile(r"(?i)(?:\[\s*api[_ -]?retry\s*\]|\bapi[_ -]?retry\b)"),
    ),
    ("econnreset", re.compile(r"(?i)\bECONNRESET\b")),
    (
        "request_timeout",
        re.compile(r"(?i)\b(?:request[_ -]?timeout|request\s+(?:timed\s+out|timeout)|ETIMEDOUT)\b"),
    ),
    (
        "socket_error",
        re.compile(
            r"(?i)\b(?:socket[_ -]?error|socket\s+hang\s+up|"
            r"socket\s+connection\s+was\s+closed\s+unexpectedly)\b"
        ),
    ),
    (
        "http_429",
        re.compile(
            r"(?i)(?:\bHTTP(?:/\d(?:\.\d)?)?\s+429\b|\b429\s+Too\s+Many\s+Requests\b|"
            r"\b(?:error[_ -]?status|status(?:\s+code)?|response|API\s+Error)"
            r"[\"']?\s*[:=]?\s*429\b)"
        ),
    ),
    (
        "http_502",
        re.compile(
            r"(?i)(?:\bHTTP(?:/\d(?:\.\d)?)?\s+502\b|\b502\s+Bad\s+Gateway\b|"
            r"\b(?:error[_ -]?status|status(?:\s+code)?|response|API\s+Error)"
            r"[\"']?\s*[:=]?\s*502\b)"
        ),
    ),
    (
        "http_503",
        re.compile(
            r"(?i)(?:\bHTTP(?:/\d(?:\.\d)?)?\s+503\b|\b503\s+Service\s+Unavailable\b|"
            r"\b(?:error[_ -]?status|status(?:\s+code)?|response|API\s+Error)"
            r"[\"']?\s*[:=]?\s*503\b)"
        ),
    ),
    (
        "http_504",
        re.compile(
            r"(?i)(?:\bHTTP(?:/\d(?:\.\d)?)?\s+504\b|\b504\s+Gateway\s+Timeout\b|"
            r"\b(?:error[_ -]?status|status(?:\s+code)?|response|API\s+Error)"
            r"[\"']?\s*[:=]?\s*504\b)"
        ),
    ),
)
ERROR_KEYS = tuple(name for name, _pattern in ERROR_PATTERNS)

LAUNCH_STARTED_RE = re.compile(r"(?m)^\[worker \d+\].*\bstarting(?:\s|\[|\()")
LAUNCH_OK_RE = re.compile(r"(?m)^\[worker \d+\].*\bOK\b")
LAUNCH_FAILED_RE = re.compile(r"(?im)^\[worker \d+\].*\bfailed\b")
PROGRESS_PATH_RE = re.compile(r"(?m)^Per-task progress JSONL\s*->\s*(.+?)\s*$")

GROUP_ALIASES = {
    "tmux": "session",
    "tmux_session": "session",
    "input_file": "input",
    "input_marker": "input",
    "log_dir": "logs",
    "expected_workers": "workers",
    "expected_worker_count": "workers",
    "launch_log": "launch",
    "progress_log": "progress",
}
GROUP_KEYS = {
    "name",
    "session",
    "input",
    "logs",
    "workers",
    "enabled",
    "required",
    "launch",
    "progress",
}


@dataclass(frozen=True)
class GroupSpec:
    name: str
    tmux_session: str
    input_marker: str
    log_dir: Path
    expected_workers: int
    enabled: bool = True
    required: bool = True
    launch_log: Path | None = None
    progress_log: Path | None = None


@dataclass(frozen=True)
class ProcessInfo:
    pid: int
    ppid: int
    argv: tuple[str, ...]


@dataclass
class FilePatternState:
    inode: int
    offset: int = 0
    pending: str = ""
    counts: Counter[str] = field(default_factory=Counter)


@dataclass
class PatternAccumulator:
    files: dict[Path, FilePatternState] = field(default_factory=dict)

    def scan(self, paths: Iterable[Path]) -> tuple[dict[str, int], dict[str, int]]:
        """Scan appended complete lines and return cumulative and new counts."""
        new_counts: Counter[str] = Counter()
        for path in paths:
            try:
                stat = path.stat()
            except OSError:
                continue
            if not path.is_file():
                continue

            state = self.files.get(path)
            if state is None or state.inode != stat.st_ino or stat.st_size < state.offset:
                state = FilePatternState(inode=stat.st_ino)
                self.files[path] = state

            try:
                with path.open("rb") as fh:
                    fh.seek(state.offset)
                    data = fh.read()
                    state.offset = fh.tell()
            except OSError:
                continue
            if not data:
                continue

            text = state.pending + data.decode("utf-8", errors="replace")
            newline = text.rfind("\n")
            if newline < 0:
                state.pending = text
                continue
            complete = text[: newline + 1]
            state.pending = text[newline + 1 :]
            increments = count_error_patterns(complete)
            state.counts.update(increments)
            new_counts.update(increments)

        cumulative: Counter[str] = Counter()
        for state in self.files.values():
            cumulative.update(state.counts)
        return zero_filled_counts(cumulative), zero_filled_counts(new_counts)


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def parse_bool(value: object, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{field_name} must be true or false")


def _group_mapping(raw: str) -> dict[str, object]:
    raw = raw.strip()
    if not raw:
        raise ValueError("group specification cannot be empty")
    if raw.startswith("{"):
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as error:
            raise ValueError("group JSON is invalid") from error
        if not isinstance(value, dict):
            raise ValueError("group JSON must be an object")
        return {str(key): item for key, item in value.items()}

    mapping: dict[str, object] = {}
    try:
        fields = next(csv.reader([raw], skipinitialspace=True))
    except csv.Error as error:
        raise ValueError("group key=value fields are invalid") from error
    for field_value in fields:
        key, separator, value = field_value.partition("=")
        if not separator:
            raise ValueError(f"group field {field_value!r} must use key=value")
        key = key.strip()
        if not key:
            raise ValueError("group field name cannot be empty")
        if key in mapping:
            raise ValueError(f"duplicate group field: {key}")
        mapping[key] = value.strip()
    return mapping


def parse_group_spec(raw: str) -> GroupSpec:
    mapping = _group_mapping(raw)
    normalized: dict[str, object] = {}
    for original_key, value in mapping.items():
        key = GROUP_ALIASES.get(original_key, original_key)
        if key not in GROUP_KEYS:
            raise ValueError(f"unknown group field: {original_key}")
        if key in normalized:
            raise ValueError(f"duplicate group field: {key}")
        normalized[key] = value

    missing = [
        key for key in ("name", "session", "input", "logs", "workers") if key not in normalized
    ]
    if missing:
        raise ValueError(f"group is missing required fields: {', '.join(missing)}")

    strings = {key: str(normalized[key]).strip() for key in ("name", "session", "input", "logs")}
    empty = [key for key, value in strings.items() if not value]
    if empty:
        raise ValueError(f"group fields cannot be empty: {', '.join(empty)}")
    try:
        workers = int(str(normalized["workers"]))
    except ValueError as error:
        raise ValueError("group workers must be an integer") from error
    if workers < 1:
        raise ValueError("group workers must be at least 1")

    launch = normalized.get("launch")
    progress = normalized.get("progress")
    return GroupSpec(
        name=strings["name"],
        tmux_session=strings["session"],
        input_marker=strings["input"],
        log_dir=Path(strings["logs"]),
        expected_workers=workers,
        enabled=parse_bool(normalized.get("enabled", True), "enabled"),
        required=parse_bool(normalized.get("required", True), "required"),
        launch_log=(
            Path(str(launch).strip()) if launch is not None and str(launch).strip() else None
        ),
        progress_log=(
            Path(str(progress).strip()) if progress is not None and str(progress).strip() else None
        ),
    )


def resolve_group_paths(spec: GroupSpec, run_dir: Path) -> GroupSpec:
    def beneath_run(path: Path | None) -> Path | None:
        if path is None or path.is_absolute():
            return path
        return run_dir / path

    log_dir = spec.log_dir if spec.log_dir.is_absolute() else run_dir / spec.log_dir
    return replace(
        spec,
        log_dir=log_dir,
        launch_log=beneath_run(spec.launch_log),
        progress_log=beneath_run(spec.progress_log),
    )


def zero_filled_counts(counts: Mapping[str, int]) -> dict[str, int]:
    return {key: int(counts.get(key, 0)) for key in ERROR_KEYS}


def count_error_patterns(text: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for line in text.splitlines():
        for name, pattern in ERROR_PATTERNS:
            if pattern.search(line):
                counts[name] += 1
    return zero_filled_counts(counts)


def snapshot_processes() -> dict[int, ProcessInfo]:
    """Read same-user processes without returning or logging command text."""
    processes: dict[int, ProcessInfo] = {}
    current_uid = os.geteuid()
    for proc_path in Path("/proc").iterdir():
        if not proc_path.name.isdigit():
            continue
        try:
            if proc_path.stat().st_uid != current_uid:
                continue
            pid = int(proc_path.name)
            status = (proc_path / "status").read_text(errors="replace")
            ppid_match = re.search(r"(?m)^PPid:\s+(\d+)\s*$", status)
            if ppid_match is None:
                continue
            raw_argv = (proc_path / "cmdline").read_bytes().split(b"\0")
            argv = tuple(value.decode("utf-8", errors="replace") for value in raw_argv if value)
        except (OSError, ValueError):
            continue
        if argv:
            processes[pid] = ProcessInfo(pid, int(ppid_match.group(1)), argv)
    return processes


def _is_orchestrator(process: ProcessInfo, input_marker: str) -> bool:
    has_entrypoint = any(
        argument == "src/orchestrator.py" or argument.endswith("/src/orchestrator.py")
        for argument in process.argv
    )
    return has_entrypoint and any(input_marker in argument for argument in process.argv)


def _descendant_pids(roots: Iterable[int], processes: Mapping[int, ProcessInfo]) -> set[int]:
    children: dict[int, list[int]] = defaultdict(list)
    for process in processes.values():
        children[process.ppid].append(process.pid)
    descendants: set[int] = set()
    stack = list(roots)
    while stack:
        parent = stack.pop()
        for child in children.get(parent, []):
            if child in descendants:
                continue
            descendants.add(child)
            stack.append(child)
    return descendants


def _is_swegen_worker(process: ProcessInfo) -> bool:
    has_swegen = any(Path(argument).name == "swegen" for argument in process.argv)
    return has_swegen and "create" in process.argv


def _is_claude_sdk(process: ProcessInfo) -> bool:
    executable_is_claude = bool(process.argv) and Path(process.argv[0]).name == "claude"
    return executable_is_claude or any(
        "claude_agent_sdk/_bundled/claude" in argument for argument in process.argv
    )


def process_counts(spec: GroupSpec, processes: Mapping[int, ProcessInfo]) -> tuple[int, int, int]:
    orchestrators = {
        process.pid
        for process in processes.values()
        if _is_orchestrator(process, spec.input_marker)
    }
    descendants = _descendant_pids(orchestrators, processes)
    workers = sum(
        1 for pid in descendants if pid in processes and _is_swegen_worker(processes[pid])
    )
    claude = sum(1 for pid in descendants if pid in processes and _is_claude_sdk(processes[pid]))
    return len(orchestrators), workers, claude


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


def _read_text(path: Path | None) -> str:
    if path is None:
        return ""
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def find_launch_log(spec: GroupSpec, run_dir: Path) -> Path | None:
    if spec.launch_log is not None:
        return spec.launch_log
    name = spec.log_dir.name
    if name == "orchestrator-logs":
        candidate = run_dir / "orchestrator-launch.log"
    elif name.startswith("orchestrator-logs-"):
        candidate = run_dir / f"orchestrator-{name.removeprefix('orchestrator-logs-')}-launch.log"
    else:
        candidate = run_dir / f"{name}-launch.log"
    if candidate.exists():
        return candidate

    matches: list[Path] = []
    for path in run_dir.glob("*launch.log"):
        text = _read_text(path)
        if spec.log_dir.name in text or str(spec.log_dir) in text:
            matches.append(path)
    if not matches:
        return None
    return max(matches, key=lambda path: path.stat().st_mtime)


def _resolve_reported_path(value: str, run_dir: Path) -> Path:
    path = Path(value.strip())
    if path.is_absolute():
        return path
    candidates = (Path.cwd() / path, run_dir / path, run_dir / path.name)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[-1]


def find_progress_log(spec: GroupSpec, run_dir: Path, launch_log: Path | None) -> Path | None:
    if spec.progress_log is not None:
        return spec.progress_log
    launch_text = _read_text(launch_log)
    matches = PROGRESS_PATH_RE.findall(launch_text)
    if matches:
        return _resolve_reported_path(matches[-1], run_dir)
    return None


def count_task_events(launch_log: Path | None, progress_log: Path | None) -> dict[str, int]:
    launch_text = _read_text(launch_log)
    launch_started = len(LAUNCH_STARTED_RE.findall(launch_text))
    launch_ok = len(LAUNCH_OK_RE.findall(launch_text))
    launch_failed = len(LAUNCH_FAILED_RE.findall(launch_text))

    progress_ok = 0
    progress_failed = 0
    progress_records = 0
    for line in _read_text(progress_log).splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict) or record.get("event") != "task_finished":
            continue
        status = record.get("status")
        if status == "success":
            progress_ok += 1
            progress_records += 1
        elif status == "failure":
            progress_failed += 1
            progress_records += 1

    if progress_records:
        ok = progress_ok
        failed = progress_failed
    else:
        ok = launch_ok
        failed = launch_failed
    return {
        "started": max(launch_started, ok + failed),
        "ok": ok,
        "failed": failed,
    }


def recent_log_activity(
    worker_logs: Sequence[Path],
    launch_log: Path | None,
    progress_log: Path | None,
    now_epoch: float,
) -> float | None:
    paths = [*worker_logs]
    paths.extend(path for path in (launch_log, progress_log) if path is not None)
    mtimes: list[float] = []
    for path in paths:
        try:
            mtimes.append(path.stat().st_mtime)
        except OSError:
            continue
    if not mtimes:
        return None
    return round(max(0.0, now_epoch - max(mtimes)), 3)


def evaluate_group_health(
    *,
    enabled: bool,
    tmux_active: bool,
    orchestrator_process_count: int,
    worker_process_count: int,
    expected_worker_count: int,
    worker_log_file_count: int,
    recent_log_activity_age_seconds: float | None,
    max_log_age_seconds: float,
    new_error_counts: Mapping[str, int],
) -> bool:
    return bool(
        enabled
        and tmux_active
        and orchestrator_process_count == 1
        # A worker thread has no `swegen create` child while it post-processes a
        # completed task or dequeues the next package.  Requiring an exact live
        # child count therefore creates false alarms at every task boundary.
        and worker_process_count <= expected_worker_count
        and worker_log_file_count >= expected_worker_count
        and recent_log_activity_age_seconds is not None
        and recent_log_activity_age_seconds <= max_log_age_seconds
        and not any(new_error_counts.values())
    )


def inspect_group(
    spec: GroupSpec,
    run_dir: Path,
    processes: Mapping[int, ProcessInfo],
    accumulator: PatternAccumulator,
    max_log_age_seconds: float,
    now_epoch: float,
) -> dict[str, Any]:
    launch_log = find_launch_log(spec, run_dir)
    progress_log = find_progress_log(spec, run_dir, launch_log)
    worker_logs = sorted(spec.log_dir.glob("*.log")) if spec.log_dir.is_dir() else []
    error_paths = [*worker_logs]
    if launch_log is not None:
        error_paths.append(launch_log)
    cumulative_errors, new_errors = accumulator.scan(error_paths)
    orchestrator_count, worker_count, claude_count = process_counts(spec, processes)
    tmux_active = tmux_session_active(spec.tmux_session)
    log_age = recent_log_activity(worker_logs, launch_log, progress_log, now_epoch)
    tasks = count_task_events(launch_log, progress_log)
    healthy = evaluate_group_health(
        enabled=spec.enabled,
        tmux_active=tmux_active,
        orchestrator_process_count=orchestrator_count,
        worker_process_count=worker_count,
        expected_worker_count=spec.expected_workers,
        worker_log_file_count=len(worker_logs),
        recent_log_activity_age_seconds=log_age,
        max_log_age_seconds=max_log_age_seconds,
        new_error_counts=new_errors,
    )
    return {
        "name": spec.name,
        "enabled": spec.enabled,
        "required": spec.required,
        "tmux_session": spec.tmux_session,
        "tmux_active": tmux_active,
        "orchestrator_process_count": orchestrator_count,
        "expected_worker_count": spec.expected_workers,
        "worker_process_count": worker_count,
        "claude_sdk_descendants_live": claude_count > 0,
        "claude_sdk_process_count": claude_count,
        "worker_log_file_count": len(worker_logs),
        "recent_log_activity_age_seconds": log_age,
        "max_log_age_seconds": max_log_age_seconds,
        "error_counts": cumulative_errors,
        "new_error_counts": new_errors,
        "tasks": tasks,
        "healthy": healthy,
    }


def build_health_record(
    specs: Sequence[GroupSpec],
    run_dir: Path,
    accumulators: dict[str, PatternAccumulator],
    max_log_age_seconds: float,
    *,
    now_epoch: float | None = None,
    processes: Mapping[int, ProcessInfo] | None = None,
) -> dict[str, Any]:
    current_epoch = time.time() if now_epoch is None else now_epoch
    snapshot = snapshot_processes() if processes is None else processes
    groups = [
        inspect_group(
            spec,
            run_dir,
            snapshot,
            accumulators.setdefault(spec.name, PatternAccumulator()),
            max_log_age_seconds,
            current_epoch,
        )
        for spec in specs
    ]
    expected = [group for group in groups if group["enabled"] and group["required"]]
    overall_healthy = bool(expected) and all(group["healthy"] for group in expected)
    return {
        "timestamp": utc_now(),
        "event": "multi_proxy_health",
        "overall_healthy": overall_healthy,
        "expected_group_count": len(expected),
        "healthy_expected_group_count": sum(1 for group in expected if group["healthy"]),
        "groups": groups,
    }


def atomic_write_json(path: Path, record: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def _write_pid_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(f"{os.getpid()}\n")
    os.replace(temporary, path)


def _remove_own_pid_file(path: Path) -> None:
    try:
        if path.read_text().strip() == str(os.getpid()):
            path.unlink(missing_ok=True)
    except OSError:
        pass


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--status-file", type=Path, required=True)
    parser.add_argument("--pid-file", type=Path)
    parser.add_argument(
        "--group",
        action="append",
        type=parse_group_spec,
        required=True,
        help=(
            "Repeatable JSON or key=value group specification with name, session, input, "
            "logs, workers, and optional enabled/required/launch/progress fields"
        ),
    )
    parser.add_argument(
        "--max-log-age",
        type=float,
        default=300.0,
        help="Maximum seconds since activity in group logs for a healthy group (default: 300)",
    )
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    if args.interval <= 0:
        parser.error("--interval must be greater than zero")
    if args.max_log_age <= 0:
        parser.error("--max-log-age must be greater than zero")
    names = [spec.name for spec in args.group]
    duplicates = sorted(name for name, count in Counter(names).items() if count > 1)
    if duplicates:
        parser.error(f"group names must be unique: {', '.join(duplicates)}")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    run_dir = args.run_dir.resolve()
    specs = [resolve_group_paths(spec, run_dir) for spec in args.group]
    status_file = args.status_file.resolve()
    pid_file = args.pid_file.resolve() if args.pid_file is not None else None
    accumulators: dict[str, PatternAccumulator] = {}
    stop_event = threading.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    if pid_file is not None:
        _write_pid_file(pid_file)

    try:
        while not stop_event.is_set():
            record = build_health_record(
                specs,
                run_dir,
                accumulators,
                args.max_log_age,
            )
            atomic_write_json(status_file, record)
            print(json.dumps(record, sort_keys=True), flush=True)
            if args.once:
                break
            stop_event.wait(args.interval)
    finally:
        if pid_file is not None:
            _remove_own_pid_file(pid_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
