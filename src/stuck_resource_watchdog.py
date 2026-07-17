#!/usr/bin/env python3
"""Terminate stale SWE-Gen task process groups and Harbor containers.

The watchdog is deliberately conservative: it only acts on resources with
strong evidence that they belong to a SWE-Gen task under this workspace. It
never targets the orchestrator, dashboard, monitors, itself, or Codex/tooling.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

WORKSPACE = Path(__file__).resolve().parents[1]
RUNS_ROOT = WORKSPACE / "runs"
TASK_MARKERS = (
    "swegen create",
    "harbor run",
    "claude_agent_sdk/_bundled/claude",
    "docker build",
    "docker compose",
    "docker-buildx",
    "buildx build",
    "buildx bake",
)
EXCLUDED_MARKERS = (
    "src/orchestrator.py",
    "run_dashboard.py",
    "failure_mode_monitor.py",
    "proxy_stability_monitor.py",
    "stuck_resource_watchdog.py",
    "/codex",
    "codex exec",
)


def emit(event: str, **fields: Any) -> None:
    record = {
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
        "event": event,
        **fields,
    }
    print(json.dumps(record, sort_keys=True), flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", type=int, default=900)
    parser.add_argument("--max-age", type=int, default=10800)
    parser.add_argument("--grace", type=int, default=10)
    parser.add_argument("--pid-file", type=Path)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def proc_snapshot() -> dict[int, dict[str, Any]]:
    """Return PID metadata read directly from /proc."""
    snapshot: dict[int, dict[str, Any]] = {}
    try:
        uptime = float(Path("/proc/uptime").read_text().split()[0])
    except (OSError, ValueError, IndexError):
        return snapshot
    ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])

    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            stat_text = (entry / "stat").read_text()
            after_comm = stat_text.rsplit(")", 1)[1].split()
            ppid = int(after_comm[1])
            pgrp = int(after_comm[2])
            started_ticks = int(after_comm[19])
            cmdline = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode(
                errors="replace"
            ).strip()
            if not cmdline:
                cmdline = (entry / "comm").read_text(errors="replace").strip()
        except (OSError, ValueError, IndexError):
            continue
        snapshot[pid] = {
            "pid": pid,
            "ppid": ppid,
            "pgrp": pgrp,
            "age": max(0.0, uptime - (started_ticks / ticks)),
            "cmd": cmdline,
        }
    return snapshot


def protected_groups(snapshot: dict[int, dict[str, Any]]) -> set[int]:
    """Protect this watchdog's group and all ancestor process groups."""
    protected = {os.getpgrp()}
    pid = os.getpid()
    seen: set[int] = set()
    while pid and pid not in seen:
        seen.add(pid)
        process = snapshot.get(pid)
        if process is None:
            break
        protected.add(int(process["pgrp"]))
        pid = int(process["ppid"])
    return protected


def stale_task_groups(
    snapshot: dict[int, dict[str, Any]], max_age: int
) -> list[dict[str, Any]]:
    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for process in snapshot.values():
        groups[int(process["pgrp"])].append(process)

    protected = protected_groups(snapshot)
    stale: list[dict[str, Any]] = []
    workspace = str(WORKSPACE)
    runs_root = str(RUNS_ROOT)
    for pgrp, members in groups.items():
        if pgrp <= 1 or pgrp in protected:
            continue
        commands = [str(member["cmd"]) for member in members]
        combined = "\n".join(commands)
        if any(marker in combined for marker in EXCLUDED_MARKERS):
            continue
        if workspace not in combined and runs_root not in combined and "runs/" not in combined:
            continue
        if not any(marker in combined for marker in TASK_MARKERS):
            continue

        leader = next((item for item in members if item["pid"] == pgrp), None)
        age = float(leader["age"] if leader else max(item["age"] for item in members))
        if age < max_age:
            continue
        stale.append(
            {
                "pgrp": pgrp,
                "age_seconds": int(age),
                "pids": sorted(int(item["pid"]) for item in members),
                "leader": str((leader or members[0])["cmd"])[:1000],
            }
        )
    return sorted(stale, key=lambda item: item["age_seconds"], reverse=True)


def group_exists(pgrp: int) -> bool:
    return any(item["pgrp"] == pgrp for item in proc_snapshot().values())


def terminate_groups(groups: list[dict[str, Any]], grace: int, dry_run: bool) -> None:
    signaled: list[int] = []
    for item in groups:
        pgrp = int(item["pgrp"])
        emit("stale_process_group", action="would_terminate" if dry_run else "terminate", **item)
        if dry_run:
            continue
        try:
            os.killpg(pgrp, signal.SIGTERM)
            signaled.append(pgrp)
        except ProcessLookupError:
            continue
        except PermissionError as error:
            emit("process_group_error", pgrp=pgrp, error=str(error))

    if not signaled:
        return
    time.sleep(max(0, grace))
    for pgrp in signaled:
        if not group_exists(pgrp):
            emit("process_group_stopped", pgrp=pgrp, signal="TERM")
            continue
        try:
            os.killpg(pgrp, signal.SIGKILL)
            emit("process_group_stopped", pgrp=pgrp, signal="KILL")
        except ProcessLookupError:
            emit("process_group_stopped", pgrp=pgrp, signal="TERM")
        except PermissionError as error:
            emit("process_group_error", pgrp=pgrp, error=str(error))


def docker_json(*args: str) -> Any:
    result = subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=60, check=False
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"docker {' '.join(args)} failed")
    return json.loads(result.stdout)


def stale_containers(max_age: int) -> list[dict[str, Any]]:
    try:
        result = subprocess.run(
            ["docker", "ps", "-q"], capture_output=True, text=True, timeout=30, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        emit("docker_error", error=str(error))
        return []
    if result.returncode != 0:
        emit("docker_error", error=result.stderr.strip())
        return []

    now = datetime.now(UTC)
    stale: list[dict[str, Any]] = []
    for container_id in result.stdout.split():
        try:
            details = docker_json("inspect", container_id)[0]
            started = datetime.fromisoformat(details["State"]["StartedAt"].replace("Z", "+00:00"))
        except (OSError, RuntimeError, KeyError, IndexError, TypeError, ValueError) as error:
            emit("docker_inspect_error", container=container_id, error=str(error))
            continue
        age = int((now - started).total_seconds())
        if age < max_age:
            continue
        mounts = details.get("Mounts") or []
        sources = [str(item.get("Source", "")) for item in mounts if isinstance(item, dict)]
        if not any(source.startswith(str(RUNS_ROOT)) for source in sources):
            continue
        stale.append(
            {
                "container": container_id,
                "name": str(details.get("Name", "")).lstrip("/"),
                "age_seconds": age,
                "mount_sources": sources,
            }
        )
    return sorted(stale, key=lambda item: item["age_seconds"], reverse=True)


def terminate_containers(
    containers: list[dict[str, Any]], grace: int, dry_run: bool
) -> None:
    for item in containers:
        container_id = str(item["container"])
        emit("stale_container", action="would_stop" if dry_run else "stop", **item)
        if dry_run:
            continue
        stopped = subprocess.run(
            ["docker", "stop", "--time", str(max(0, grace)), container_id],
            capture_output=True,
            text=True,
            timeout=max(30, grace + 20),
            check=False,
        )
        if stopped.returncode == 0:
            emit("container_stopped", container=container_id, signal="TERM")
            continue
        killed = subprocess.run(
            ["docker", "kill", container_id],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        emit(
            "container_stopped" if killed.returncode == 0 else "container_error",
            container=container_id,
            signal="KILL",
            error="" if killed.returncode == 0 else killed.stderr.strip(),
        )


def run_cycle(args: argparse.Namespace) -> None:
    snapshot = proc_snapshot()
    groups = stale_task_groups(snapshot, args.max_age)
    containers = stale_containers(args.max_age)
    emit(
        "scan",
        dry_run=args.dry_run,
        max_age_seconds=args.max_age,
        stale_process_groups=len(groups),
        stale_containers=len(containers),
    )
    terminate_groups(groups, args.grace, args.dry_run)
    terminate_containers(containers, args.grace, args.dry_run)


def main() -> int:
    args = parse_args()
    if args.max_age <= 0 or args.interval <= 0:
        raise SystemExit("--max-age and --interval must be positive")
    if args.pid_file:
        args.pid_file.parent.mkdir(parents=True, exist_ok=True)
        args.pid_file.write_text(f"{os.getpid()}\n")

    stopping = False

    def stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    emit(
        "started",
        pid=os.getpid(),
        interval_seconds=args.interval,
        max_age_seconds=args.max_age,
        dry_run=args.dry_run,
    )
    try:
        while not stopping:
            try:
                run_cycle(args)
            except Exception as error:  # Keep the long-lived watchdog alive.
                emit("cycle_error", error=f"{type(error).__name__}: {error}")
            if args.once:
                break
            deadline = time.monotonic() + args.interval
            while not stopping and time.monotonic() < deadline:
                time.sleep(min(1.0, deadline - time.monotonic()))
    finally:
        if args.pid_file:
            args.pid_file.unlink(missing_ok=True)
        emit("stopped", pid=os.getpid())
    return 0


if __name__ == "__main__":
    sys.exit(main())
