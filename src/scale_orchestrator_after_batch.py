#!/usr/bin/env python3
"""Drain one orchestrator's current task batch, then resume it at higher concurrency.

The running orchestrator immediately starts another ``swegen create`` whenever a
worker finishes.  To get a real batch boundary without interrupting in-flight
tasks, this supervisor atomically replaces the ``swegen`` entrypoint with a
temporary parent-specific gate.  Calls from every other process still execute
normally; only replacement children of the target orchestrator wait.  After no
original create process remains, the old orchestrator is stopped and the same
run is resumed with the requested worker count.
"""

from __future__ import annotations

import argparse
import json
import os
import pwd
import shutil
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-pid", type=int, required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--run-name", required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--repo-cache-dir", type=Path, required=True)
    parser.add_argument("--cc-timeout", type=int, required=True)
    parser.add_argument("--run-user", default="alex")
    parser.add_argument("--pid-file", type=Path)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--settle-seconds", type=float, default=20.0)
    parser.add_argument("--launch-timeout", type=float, default=300.0)
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def proc_info(pid: int) -> dict[str, Any] | None:
    proc = Path("/proc") / str(pid)
    try:
        stat_fields = (proc / "stat").read_text().rsplit(")", 1)[1].split()
        cmd = (proc / "cmdline").read_bytes().replace(b"\0", b" ").decode(
            errors="replace"
        ).strip()
        exe = os.readlink(proc / "exe")
    except (FileNotFoundError, OSError, IndexError, ValueError):
        return None
    return {
        "pid": pid,
        "ppid": int(stat_fields[1]),
        "pgrp": int(stat_fields[2]),
        "start_ticks": int(stat_fields[19]),
        "cmd": cmd,
        "exe": exe,
    }


def direct_children(parent_pid: int) -> list[dict[str, Any]]:
    children: list[dict[str, Any]] = []
    for item in Path("/proc").iterdir():
        if not item.name.isdigit():
            continue
        info = proc_info(int(item.name))
        if info is not None and info["ppid"] == parent_pid:
            children.append(info)
    return children


def same_process(pid: int, start_ticks: int) -> bool:
    info = proc_info(pid)
    return info is not None and info["start_ticks"] == start_ticks


def classify_create_children(parent_pid: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    active: list[dict[str, Any]] = []
    parked: list[dict[str, Any]] = []
    for child in direct_children(parent_pid):
        if "swegen create" not in child["cmd"]:
            continue
        if Path(child["exe"]).name.startswith("python"):
            active.append(child)
        else:
            parked.append(child)
    return active, parked


def write_json(path: Path, **fields: Any) -> None:
    payload = {"timestamp": utc_now(), **fields}
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(temp, path)


def emit(status_path: Path, stage: str, **fields: Any) -> None:
    record = {"stage": stage, **fields}
    write_json(status_path, **record)
    print(json.dumps({"timestamp": utc_now(), **record}, sort_keys=True), flush=True)


def gate_script(target_pid: int, marker: Path, real_entrypoint: Path) -> str:
    return f"""#!/usr/bin/env bash
set -euo pipefail

target_pid={target_pid}
marker={str(marker)!r}
real_entrypoint={str(real_entrypoint)!r}

if [[ "$PPID" == "$target_pid" && -e "$marker" ]]; then
  echo "[scale-drain] worker slot parked until the current batch is drained"
  trap 'exit 0' TERM INT HUP
  while [[ -e "$marker" ]] && kill -0 "$target_pid" 2>/dev/null; do
    sleep 1
  done
  exit 75
fi

exec "$real_entrypoint" "$@"
"""


def install_gate(
    swegen_bin: Path, backup: Path, marker: Path, target_pid: int
) -> None:
    if backup.exists():
        raise RuntimeError(f"gate backup already exists: {backup}")
    original = swegen_bin.stat()
    shutil.copy2(swegen_bin, backup)
    os.chown(backup, original.st_uid, original.st_gid)
    marker.write_text(f"{target_pid}\n")

    temp = swegen_bin.with_name(f".{swegen_bin.name}.scale-gate.{os.getpid()}")
    temp.write_text(gate_script(target_pid, marker, backup))
    os.chmod(temp, original.st_mode & 0o7777)
    os.chown(temp, original.st_uid, original.st_gid)
    os.replace(temp, swegen_bin)


def restore_entrypoint(swegen_bin: Path, backup: Path, marker: Path) -> None:
    if backup.exists():
        original = backup.stat()
        temp = swegen_bin.with_name(f".{swegen_bin.name}.restore.{os.getpid()}")
        shutil.copy2(backup, temp)
        os.chown(temp, original.st_uid, original.st_gid)
        os.replace(temp, swegen_bin)
    marker.unlink(missing_ok=True)


def terminate_process(pid: int, start_ticks: int, grace: float = 15.0) -> None:
    if not same_process(pid, start_ticks):
        return
    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        if not same_process(pid, start_ticks):
            return
        time.sleep(0.25)
    if same_process(pid, start_ticks):
        os.kill(pid, signal.SIGKILL)


def terminate_parked(children: list[dict[str, Any]]) -> None:
    for child in children:
        try:
            os.killpg(int(child["pgrp"]), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass


def launch_resumed(args: argparse.Namespace, workspace: Path, run_dir: Path) -> subprocess.Popen:
    account = pwd.getpwnam(args.run_user)
    log_dir = run_dir / f"orchestrator-logs-{args.workers}w"
    log_dir.mkdir(parents=True, exist_ok=True)
    os.chown(log_dir, account.pw_uid, account.pw_gid)

    launch_log = run_dir / f"orchestrator-{args.workers}w-launch.log"
    launch_log.touch(exist_ok=True)
    os.chown(launch_log, account.pw_uid, account.pw_gid)
    stream = launch_log.open("a")

    child_env = os.environ.copy()
    child_env.update(
        {
            "HOME": account.pw_dir,
            "USER": args.run_user,
            "LOGNAME": args.run_user,
            "CLAUDE_CONFIG_DIR": f"/tmp/swegen-claude-{account.pw_uid}",
        }
    )
    command = [
        "/usr/bin/setpriv",
        f"--reuid={account.pw_uid}",
        f"--regid={account.pw_gid}",
        "--init-groups",
        "--",
        str(workspace / ".venv/bin/python"),
        str(workspace / "src/orchestrator.py"),
        str(args.input),
        "--workers",
        str(args.workers),
        "--run-name",
        args.run_name,
        "--repo-cache-dir",
        str(args.repo_cache_dir),
        "--cc-timeout",
        str(args.cc_timeout),
        "--log-dir",
        str(log_dir),
    ]
    try:
        return subprocess.Popen(
            command,
            cwd=workspace,
            env=child_env,
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    finally:
        stream.close()


def replace_pid_file(path: Path, pid: int, user: str) -> None:
    account = pwd.getpwnam(user)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(f"{pid}\n")
    os.chown(temp, account.pw_uid, account.pw_gid)
    os.replace(temp, path)


def main() -> int:
    args = parse_args()
    if args.workers <= 0 or args.poll_seconds <= 0 or args.settle_seconds < 0:
        raise SystemExit("workers and poll-seconds must be positive; settle-seconds cannot be negative")

    workspace = Path(__file__).resolve().parents[1]
    run_dir = workspace / "runs" / args.run_name
    if args.pid_file is not None:
        args.pid_file.parent.mkdir(parents=True, exist_ok=True)
        args.pid_file.write_text(f"{os.getpid()}\n")
    status_path = run_dir / f"scale-to-{args.workers}-status.json"
    marker = run_dir / f".scale-to-{args.workers}.drain"
    swegen_bin = workspace / ".venv/bin/swegen"
    backup = swegen_bin.with_name(
        f"{swegen_bin.name}.pre-scale-{args.workers}.{args.target_pid}"
    )

    target = proc_info(args.target_pid)
    if target is None or "src/orchestrator.py" not in target["cmd"]:
        emit(status_path, "error", error="target orchestrator is not running")
        return 1
    target_start = int(target["start_ticks"])

    def interrupted(signum: int, _frame: object) -> None:
        raise InterruptedError(f"received signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)

    gate_installed = False
    old_stopped = False
    try:
        install_gate(swegen_bin, backup, marker, args.target_pid)
        gate_installed = True
        active, parked = classify_create_children(args.target_pid)
        emit(
            status_path,
            "draining",
            target_pid=args.target_pid,
            active_current_tasks=len(active),
            parked_replacements=len(parked),
            requested_workers=args.workers,
        )

        last_report = 0.0
        drained_since: float | None = None
        final_parked: list[dict[str, Any]] = []
        while True:
            if not same_process(args.target_pid, target_start):
                raise RuntimeError("target orchestrator exited before the controlled drain")
            active, parked = classify_create_children(args.target_pid)
            now = time.monotonic()
            if active:
                drained_since = None
            elif drained_since is None:
                drained_since = now
            final_parked = parked

            if now - last_report >= 30:
                emit(
                    status_path,
                    "draining",
                    target_pid=args.target_pid,
                    active_current_tasks=len(active),
                    parked_replacements=len(parked),
                    requested_workers=args.workers,
                )
                last_report = now
            if drained_since is not None and now - drained_since >= args.settle_seconds:
                break
            time.sleep(args.poll_seconds)

        active, final_parked = classify_create_children(args.target_pid)
        if active:
            raise RuntimeError("target exited while current tasks were still active")
        emit(
            status_path,
            "batch_drained",
            target_pid=args.target_pid,
            parked_replacements=len(final_parked),
        )

        terminate_process(args.target_pid, target_start)
        old_stopped = True
        terminate_parked(final_parked)
        restore_entrypoint(swegen_bin, backup, marker)
        gate_installed = False

        emit(status_path, "launching", requested_workers=args.workers)
        resumed = launch_resumed(args, workspace, run_dir)
        replace_pid_file(run_dir / "orchestrator.pid", resumed.pid, args.run_user)

        deadline = time.monotonic() + args.launch_timeout
        observed_children = 0
        while time.monotonic() < deadline:
            if resumed.poll() is not None:
                raise RuntimeError(
                    f"resumed orchestrator exited with return code {resumed.returncode}"
                )
            active, parked = classify_create_children(resumed.pid)
            observed_children = len(active) + len(parked)
            if observed_children >= args.workers:
                break
            time.sleep(1)

        emit(
            status_path,
            "running",
            orchestrator_pid=resumed.pid,
            requested_workers=args.workers,
            observed_create_children=observed_children,
            launch_verified=observed_children >= args.workers,
        )
        return 0
    except BaseException as error:
        emit(
            status_path,
            "error",
            error=f"{type(error).__name__}: {error}",
            old_orchestrator_stopped=old_stopped,
        )
        return 1
    finally:
        if gate_installed:
            restore_entrypoint(swegen_bin, backup, marker)


if __name__ == "__main__":
    raise SystemExit(main())
