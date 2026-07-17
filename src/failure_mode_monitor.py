#!/usr/bin/env python3
"""Continuously detect new SWE-gen failure modes at a fixed interval."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


FAILURE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("claude_connection_error", re.compile(r"API Error:\s*Connection error", re.I)),
    (
        "tls_or_certificate_error",
        re.compile(r"certificate verification failed|SSL_ERROR|UNEXPECTED_EOF", re.I),
    ),
    (
        "network_timeout",
        re.compile(r"timed out after|TimeoutError|ETIMEDOUT|ReadTimeout", re.I),
    ),
    (
        "model_auth_or_access_error",
        re.compile(r"key_model_access_denied|401 Unauthorized|authentication_error", re.I),
    ),
    (
        "docker_build_or_compose_failure",
        re.compile(r"Docker compose command failed|failed to solve", re.I),
    ),
    ("missing_test_command", re.compile(r"Test command not filled", re.I)),
    (
        "patch_apply_failure",
        re.compile(
            r"can't find file to patch|malformed patch|patch[^\n]*(?:FAILED|failed)|fix\.patch[^\n]*failed",
            re.I,
        ),
    ),
    (
        "nop_unexpected_pass",
        re.compile(r"Harbor nop:[^\n]*actual reward=1", re.I),
    ),
    (
        "oracle_validation_failure",
        re.compile(r"Harbor oracle:[^\n]*actual reward=0", re.I),
    ),
    (
        "harbor_unknown_reward",
        re.compile(r"Harbor (?:nop|oracle):[^\n]*actual reward=unknown", re.I),
    ),
    (
        "llm_tool_concurrency_error",
        re.compile(r"tool use concurrency issues", re.I),
    ),
    (
        "claude_subagent_task_missing",
        re.compile(r"No task found with ID", re.I),
    ),
    (
        "dependency_install_failure",
        re.compile(r"npm ERR!|ERR_PNPM|No matching distribution found", re.I),
    ),
    ("disk_space_exhausted", re.compile(r"No space left on device", re.I)),
    (
        "docker_daemon_unavailable",
        re.compile(r"Cannot connect to the Docker daemon", re.I),
    ),
)

ARTIFACT_NAMES = {
    "exception.txt",
    "job.log",
    "trial.log",
    "test-stdout.txt",
    "test-stderr.txt",
    "verifier_stdout.txt",
    "verifier_stderr.txt",
}


def utc_now() -> datetime:
    return datetime.now(UTC)


def slugify_reason(reason: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", reason.lower()).strip("_")
    return f"status_{slug or 'unknown_failure'}"


def detect_failure_modes(text: str) -> set[str]:
    return {name for name, pattern in FAILURE_PATTERNS if pattern.search(text)}


def read_appended(path: Path, offset: int) -> tuple[str, int]:
    try:
        size = path.stat().st_size
        if size < offset:
            offset = 0
        with path.open(encoding="utf-8", errors="replace") as fh:
            fh.seek(offset)
            text = fh.read()
            return text, fh.tell()
    except OSError:
        return "", offset


def read_sample(path: Path, limit: int = 1_000_000) -> str:
    """Read a bounded first+last sample from a potentially large artifact."""
    try:
        size = path.stat().st_size
        with path.open("rb") as fh:
            if size <= limit * 2:
                data = fh.read()
            else:
                data = fh.read(limit)
                fh.seek(-limit, os.SEEK_END)
                data += b"\n...<middle omitted>...\n" + fh.read(limit)
        return data.decode(errors="replace")
    except OSError:
        return ""


def load_state(path: Path) -> dict[str, Any]:
    try:
        state = json.loads(path.read_text())
        return state if isinstance(state, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def monitor_once(run_dir: Path, state_path: Path, interval: int) -> dict[str, Any]:
    state_exists = state_path.exists()
    state = load_state(state_path)
    known_modes = set(state.get("known_modes", []))
    log_offsets = {
        str(path): int(offset)
        for path, offset in state.get("log_offsets", {}).items()
        if isinstance(path, str) and isinstance(offset, int)
    }
    status_offset = int(state.get("status_offset", 0))
    last_scan_epoch = float(state.get("last_scan_epoch", 0.0))
    modes_seen: set[str] = set()
    new_failure_events = 0

    status_path = run_dir / "orchestrator-instance-status.jsonl"
    status_text, status_offset = read_appended(status_path, status_offset)
    for line in status_text.splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict) or record.get("status") != "failure":
            continue
        new_failure_events += 1
        reason = record.get("failure_reason")
        if isinstance(reason, str) and reason.strip():
            modes_seen.add(slugify_reason(reason))

    current_log_paths = sorted(run_dir.glob("orchestrator-logs*/*.log"))
    current_offsets: dict[str, int] = {}
    for log_path in current_log_paths:
        key = str(log_path)
        text, offset = read_appended(log_path, log_offsets.get(key, 0))
        current_offsets[key] = offset
        modes_seen.update(detect_failure_modes(text))

    harbor_root = run_dir / "harbor-jobs"
    scan_after = 0.0 if not state_exists else max(last_scan_epoch - 2.0, 0.0)
    if harbor_root.exists():
        for artifact in harbor_root.rglob("*"):
            if not artifact.is_file() or artifact.name not in ARTIFACT_NAMES:
                continue
            try:
                if artifact.stat().st_mtime < scan_after:
                    continue
            except OSError:
                continue
            modes_seen.update(detect_failure_modes(read_sample(artifact)))

    baseline = not state_exists
    new_modes = sorted(modes_seen - known_modes) if not baseline else []
    known_modes.update(modes_seen)
    checked_at = utc_now()
    next_check = checked_at + timedelta(seconds=interval)
    new_state = {
        "known_modes": sorted(known_modes),
        "status_offset": status_offset,
        "log_offsets": current_offsets,
        "last_scan_epoch": time.time(),
        "last_checked_at": checked_at.isoformat(timespec="seconds"),
    }
    save_state(state_path, new_state)

    if baseline:
        message = "baseline recorded; waiting for new failure modes"
    elif new_modes:
        message = "new failure modes detected"
    else:
        message = "no new failure modes; waiting for next interval"
    return {
        "timestamp": checked_at.isoformat(timespec="seconds"),
        "event": "baseline" if baseline else "failure_mode_check",
        "message": message,
        "new_failure_events": new_failure_events,
        "new_modes": new_modes,
        "known_modes": sorted(known_modes),
        "next_check_at": next_check.isoformat(timespec="seconds"),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--interval", type=int, default=900)
    parser.add_argument("--state-file", type=Path, required=True)
    parser.add_argument("--pid-file", type=Path)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.interval < 1:
        raise SystemExit("--interval must be at least 1 second")
    run_dir = args.run_dir.resolve()
    state_file = args.state_file.resolve()
    stop_event = threading.Event()

    if args.pid_file:
        args.pid_file.parent.mkdir(parents=True, exist_ok=True)
        args.pid_file.write_text(f"{os.getpid()}\n")

    def stop(_signum: int, _frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        while not stop_event.is_set():
            result = monitor_once(run_dir, state_file, args.interval)
            print(json.dumps(result, sort_keys=True), flush=True)
            if args.once:
                break
            stop_event.wait(args.interval)
    finally:
        if args.pid_file:
            args.pid_file.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
