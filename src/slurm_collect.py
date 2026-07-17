#!/usr/bin/env python3
"""Collect node-local SWE-gen Slurm state into the controller run directory."""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import shlex
import signal
import subprocess
import tarfile
import time
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

ACTIVE_STATES = {"RUNNING", "COMPLETING", "CONFIGURING"}
PENDING_STATES = {"PENDING", "SUSPENDED"}
SECRET_RE = re.compile(
    r"(?i)(://)[^/@\s]+@|\b(?:sk|ghp)_[A-Za-z0-9_-]{10,}|\bsk-[A-Za-z0-9_-]{10,}"
)
REMOTE_HEALTH_CODE = r"""
import json, pathlib, re, sys, time
root = pathlib.Path(sys.argv[1])
cutoff = float(sys.argv[2])
patterns = {
  'api_retry': re.compile(r"subtype=['\"]api_retry|\bapi_retry\b", re.I),
  'tls_eof': re.compile(r"UNEXPECTED_EOF_WHILE_READING|EOF occurred in violation", re.I),
  'econnreset': re.compile(r"ECONNRESET|Connection reset by peer", re.I),
  'request_timeout': re.compile(r"ETIMEDOUT|ReadTimeout|request[_ ]timeout|timed out", re.I),
  'http_500': re.compile(r"error_status['\"]?:\s*500|\b500 Internal Server Error", re.I),
  'http_504': re.compile(r"error_status['\"]?:\s*504|\b504 Gateway Timeout", re.I),
}
counts = {key: 0 for key in patterns}
files = list(root.glob('orchestrator-logs*/worker-*.log')) + list(root.glob('orchestrator-*-launch.log'))
latest = 0.0
for path in files:
  try:
    modified = path.stat().st_mtime
    if modified < cutoff: continue
    latest = max(latest, modified)
    text = path.read_text(encoding='utf-8', errors='replace')
  except OSError:
    continue
  for key, pattern in patterns.items(): counts[key] += len(pattern.findall(text))
workspace = str(root.parent.parent)
active_workers = 0
orchestrators = 0
for command_path in pathlib.Path('/proc').glob('[0-9]*/cmdline'):
  try: command = command_path.read_bytes().decode(errors='replace')
  except OSError: continue
  if workspace not in command: continue
  if 'swegen\x00create\x00' in command: active_workers += 1
  if 'src/orchestrator.py\x00' in command: orchestrators += 1
print(json.dumps({'error_counts': counts, 'worker_log_files': len(files), 'latest_log_age_seconds': round(max(time.time()-latest, 0), 3) if latest else None, 'active_worker_processes': active_workers, 'orchestrator_processes': orchestrators}))
""".strip()


def redact(text: str) -> str:
    return SECRET_RE.sub(
        lambda match: f"{match.group(1)}<REDACTED>@" if match.group(1) else "<REDACTED>", text
    )


def command_prefix() -> list[str]:
    return ["sudo", "-u", "alex", "-H"] if os.geteuid() == 0 else []


def run_bytes(argv: list[str], timeout: int = 300) -> subprocess.CompletedProcess[bytes]:
    proc = subprocess.run(argv, capture_output=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(
            f"command failed ({proc.returncode}): {shlex.join(argv)}: "
            f"{redact(proc.stderr.decode(errors='replace'))[-2000:]}"
        )
    return proc


def load_plan(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("nodes"), list):
        raise ValueError(f"invalid Slurm plan: {path}")
    return data


def job_state(job_id: str | None) -> str:
    if not job_id:
        return "NOT_SUBMITTED"
    prefix = command_prefix()
    queued = subprocess.run(
        prefix + ["squeue", "-h", "-j", job_id, "-o", "%T"],
        capture_output=True,
        text=True,
    )
    state = queued.stdout.strip().splitlines()
    if queued.returncode == 0 and state:
        return state[0].strip().upper()
    accounted = subprocess.run(
        prefix + ["sacct", "-n", "-X", "-j", job_id, "--format=State", "--parsable2"],
        capture_output=True,
        text=True,
    )
    for line in accounted.stdout.splitlines():
        value = line.strip().split("|", 1)[0].split("+", 1)[0].upper()
        if value:
            return value
    return "UNKNOWN"


def srun_base(node: str, job_id: str | None, state: str) -> list[str]:
    args = command_prefix() + ["srun", "--quiet"]
    if job_id and state in ACTIVE_STATES:
        args.extend([f"--jobid={job_id}", "--overlap"])
    args.extend(["--nodes=1", "--ntasks=1", f"--nodelist={node}"])
    return args


def safe_extract(payload: bytes, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
        for member in archive.getmembers():
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError(f"unsafe archive member: {member.name}")
        archive.extractall(destination, filter="data")


def collect_archive(
    node: str,
    job_id: str | None,
    state: str,
    remote_run_dir: str,
    destination: Path,
    include_tasks: bool,
) -> None:
    if state in PENDING_STATES or state == "NOT_SUBMITTED":
        return
    task_find = ""
    if include_tasks:
        task_find = " -o -path './tasks/*' -o -path './tasks_voyager_postprocessed/*'"
    script = (
        "set -euo pipefail; "
        f"cd {shlex.quote(remote_run_dir)}; "
        "find . -type f \\( "
        "-name 'create.jsonl' -o -name 'orchestrator-instance-status*.jsonl' "
        "-o -name 'orchestrator-progress*.jsonl' -o -name 'task_references.json' "
        "-o -name 'slurm-*.out'"
        f"{task_find} \\) -print0 | tar --null -czf - --files-from -"
    )
    proc = run_bytes(srun_base(node, job_id, state) + ["bash", "-lc", script], timeout=600)
    if proc.stdout:
        safe_extract(proc.stdout, destination)


def collect_health(
    node: str,
    job_id: str | None,
    state: str,
    remote_run_dir: str,
    submitted_at: str | None,
) -> dict[str, Any]:
    if state in PENDING_STATES or state == "NOT_SUBMITTED":
        return {"error_counts": {}, "worker_log_files": 0, "latest_log_age_seconds": None}
    cutoff = 0.0
    if submitted_at:
        try:
            cutoff = datetime.fromisoformat(submitted_at).timestamp()
        except ValueError:
            cutoff = 0.0
    argv = srun_base(node, job_id, state) + [
        "python3",
        "-c",
        REMOTE_HEALTH_CODE,
        remote_run_dir,
        str(cutoff),
    ]
    proc = run_bytes(argv, timeout=300)
    return json.loads(proc.stdout.decode(errors="replace"))


def atomic_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def collect_once(plan_path: Path, include_tasks: bool = False) -> dict[str, Any]:
    plan = load_plan(plan_path)
    run_dir = Path(plan.get("run_dir") or plan_path.parent).resolve()
    nodes_health: list[dict[str, Any]] = []
    for record in plan["nodes"]:
        node = str(record["node"])
        job_id = str(record["job_id"]) if record.get("job_id") else None
        state = job_state(job_id)
        destination = run_dir / "slurm-nodes" / node
        node_health: dict[str, Any] = {
            "node": node,
            "node_ip": record.get("node_ip"),
            "job_id": job_id,
            "state": state,
            "expected_workers": int(record.get("expected_workers", 0)),
            "collected": False,
        }
        try:
            collect_archive(
                node,
                job_id,
                state,
                str(record["remote_run_dir"]),
                destination,
                include_tasks,
            )
            node_health.update(
                collect_health(
                    node,
                    job_id,
                    state,
                    str(record["remote_run_dir"]),
                    record.get("submitted_at"),
                )
            )
            node_health["collected"] = state not in PENDING_STATES and state != "NOT_SUBMITTED"
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
            node_health["collection_error"] = redact(str(exc))
        nodes_health.append(node_health)

    active_workers = sum(int(item.get("active_worker_processes", 0)) for item in nodes_health)
    health = {
        "event": "slurm_health",
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
        "run_name": plan.get("run_name", run_dir.name),
        "expected_workers": int(plan.get("expected_workers", 0)),
        "active_workers": active_workers,
        "nodes": nodes_health,
    }
    atomic_json(run_dir / "slurm-health.json", health)
    return health


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=int, default=30)
    parser.add_argument("--include-tasks", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stop = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    while True:
        health = collect_once(args.plan.resolve(), args.include_tasks)
        states = ", ".join(f"{node['node']}={node['state']}" for node in health["nodes"])
        print(
            f"{health['timestamp']} active={health['active_workers']}/"
            f"{health['expected_workers']} {states}",
            flush=True,
        )
        if not args.watch or stop:
            break
        time.sleep(max(args.interval, 5))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
