#!/usr/bin/env python3
"""Collect node-local SWE-gen Slurm state into the controller run directory."""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import shlex
import shutil
import signal
import socket
import subprocess
import tarfile
import tempfile
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

ACTIVE_STATES = {"RUNNING", "COMPLETING", "CONFIGURING"}
PENDING_STATES = {"PENDING", "SUSPENDED"}
SECRET_RE = re.compile(
    r"(?i)(://)[^/@\s]+@|\b(?:sk|ghp)_[A-Za-z0-9_-]{10,}|\bsk-[A-Za-z0-9_-]{10,}"
)
REMOTE_HEALTH_CODE = r"""
import datetime, json, pathlib, re, sys, time
root = pathlib.Path(sys.argv[1])
cutoff = float(sys.argv[2])
health_window_seconds = float(sys.argv[3]) if len(sys.argv) > 3 else 300.0
now = time.time()
recent_cutoff = now - max(health_window_seconds, 0.0)
timestamp = re.compile(r"^(20\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d))\s")
api_retry = re.compile(r"SystemMessage\(subtype=['\"]api_retry['\"]", re.I)
diagnostic_marker = re.compile(r"\[(?:System|SDK|Network|HTTP|API)\]", re.I)
assistant_api_error = re.compile(r"\[Assistant\].*\bAPI Error:", re.I)
patterns = {
  'tls_eof': re.compile(r"UNEXPECTED_EOF_WHILE_READING|EOF occurred in violation", re.I),
  'tls_error': re.compile(
    r"\b(?:TLS|SSL)(?:V\d+(?:\.\d+)?)?\b.{0,80}"
    r"(?:error|fail(?:ed|ure)?|handshake|certificate|alert)",
    re.I,
  ),
  'econnreset': re.compile(
    r"\bECONNRESET\b|Connection reset(?: by peer)?|ConnectionResetError|"
    r"server disconnected|peer closed connection",
    re.I,
  ),
  'request_timeout': re.compile(
    r"\b(?:ETIMEDOUT|ReadTimeout|ConnectTimeout|RequestTimeout|TimeoutError|"
    r"APITimeoutError|request[_ ]timeout|request\s+timed\s+out|"
    r"upstream\s+timed\s+out|socket\s+timeout)\b",
    re.I,
  ),
  'http_401': re.compile(
    r"(?:\bHTTP(?:/\d(?:\.\d)?)?\s+401\b|\b401\s+Unauthorized\b|"
    r"\b(?:error_status|status(?:\s+code)?|response|API\s+Error)"
    r"[\"']?\s*[:=]?\s*401\b)",
    re.I,
  ),
  'http_403': re.compile(
    r"(?:\bHTTP(?:/\d(?:\.\d)?)?\s+403\b|\b403\s+Forbidden\b|"
    r"\b(?:error_status|status(?:\s+code)?|response|API\s+Error)"
    r"[\"']?\s*[:=]?\s*403\b)",
    re.I,
  ),
  'http_429': re.compile(
    r"(?:\bHTTP(?:/\d(?:\.\d)?)?\s+429\b|\b429\s+Too\s+Many\s+Requests\b|"
    r"\b(?:error_status|status(?:\s+code)?|response|API\s+Error)"
    r"[\"']?\s*[:=]?\s*429\b)",
    re.I,
  ),
  'http_500': re.compile(
    r"(?:\bHTTP(?:/\d(?:\.\d)?)?\s+500\b|\b500\s+Internal\s+Server\s+Error\b|"
    r"\b(?:error_status|status(?:\s+code)?|response|API\s+Error)"
    r"[\"']?\s*[:=]?\s*500\b)",
    re.I,
  ),
  'http_502': re.compile(
    r"(?:\bHTTP(?:/\d(?:\.\d)?)?\s+502\b|\b502\s+Bad\s+Gateway\b|"
    r"\b(?:error_status|status(?:\s+code)?|response|API\s+Error)"
    r"[\"']?\s*[:=]?\s*502\b)",
    re.I,
  ),
  'http_503': re.compile(
    r"(?:\bHTTP(?:/\d(?:\.\d)?)?\s+503\b|\b503\s+Service\s+Unavailable\b|"
    r"\b(?:error_status|status(?:\s+code)?|response|API\s+Error)"
    r"[\"']?\s*[:=]?\s*503\b)",
    re.I,
  ),
  'http_504': re.compile(
    r"(?:\bHTTP(?:/\d(?:\.\d)?)?\s+504\b|\b504\s+Gateway\s+Timeout\b|"
    r"\b(?:error_status|status(?:\s+code)?|response|API\s+Error)"
    r"[\"']?\s*[:=]?\s*504\b)",
    re.I,
  ),
  'authentication': re.compile(
    r"\b(?:AuthenticationError|authentication[_\s-]+(?:error|failed|failure|required)|"
    r"auth(?:entication)?\s+failed|unauthorized|invalid[_\s-]+(?:api[_\s-]+)?key|"
    r"API\s+key\s+(?:is\s+)?invalid)\b",
    re.I,
  ),
  'credential_exhaustion': re.compile(
    r"\b(?:(?:all\s+)?(?:API\s+)?credentials?(?:\s+(?:are|were))?\s+"
    r"(?:exhausted|depleted|unavailable)|no\s+(?:valid|available|usable)\s+"
    r"(?:API\s+)?credentials?|credential(?:s|\s+pool)?[_\s-]+"
    r"exhaust(?:ed|ion))\b",
    re.I,
  ),
  'quota_exhaustion': re.compile(
    r"\b(?:insufficient[_\s-]+quota|resource[_\s-]+exhausted|"
    r"quota(?:\s+(?:is|has\s+been))?\s+(?:exceeded|exhausted)|"
    r"exceeded\s+(?:your\s+)?(?:current\s+)?quota|usage\s+(?:limit\s+)?exhausted|"
    r"credit\s+balance\s+is\s+too\s+low|billing\s+hard\s+limit)\b",
    re.I,
  ),
}
counts = {'api_retry': 0, **{key: 0 for key in patterns}}
recent_counts = {'api_retry': 0, **{key: 0 for key in patterns}}
recent_api_failure_events = 0
files = list(root.glob('orchestrator-logs*/worker-*.log')) + list(root.glob('orchestrator-*-launch.log'))
latest = 0.0
for path in files:
  try:
    modified = path.stat().st_mtime
    if modified < cutoff: continue
    latest = max(latest, modified)
    lines = path.read_text(encoding='utf-8', errors='replace').splitlines()
  except OSError:
    continue
  for line in lines:
    # Claude's structured retry message repeats ``api_retry`` in its data
    # payload. Count the SystemMessage itself once instead of every token.
    retry = bool(api_retry.search(line))
    if retry: counts['api_retry'] += 1
    timestamp_match = timestamp.match(line)
    if not timestamp_match: continue
    try:
      event_time = datetime.datetime.fromisoformat(
        timestamp_match.group(1).replace('Z', '+00:00')
      ).timestamp()
    except ValueError:
      continue
    is_diagnostic = bool(
      retry or diagnostic_marker.search(line) or assistant_api_error.search(line)
    )
    if not is_diagnostic: continue
    is_recent = event_time >= recent_cutoff
    matched_failure = retry
    if retry and is_recent: recent_counts['api_retry'] += 1
    for key, pattern in patterns.items():
      if pattern.search(line):
        counts[key] += 1
        matched_failure = True
        if is_recent: recent_counts[key] += 1
    # A retry diagnostic may also contain a status code and transport error.
    # Count categories independently, but count the source line as one event.
    if is_recent and matched_failure: recent_api_failure_events += 1
workspace = str(root.parent.parent)
active_workers = 0
orchestrators = 0
for command_path in pathlib.Path('/proc').glob('[0-9]*/cmdline'):
  try: command = command_path.read_bytes().decode(errors='replace')
  except OSError: continue
  if workspace not in command: continue
  if 'swegen\x00create\x00' in command: active_workers += 1
  if 'src/orchestrator.py\x00' in command: orchestrators += 1
print(json.dumps({'error_counts': counts, 'recent_error_counts': recent_counts, 'recent_api_failure_events': recent_api_failure_events, 'health_window_seconds': health_window_seconds, 'worker_log_files': len(files), 'latest_log_age_seconds': round(max(time.time()-latest, 0), 3) if latest else None, 'active_worker_processes': active_workers, 'orchestrator_processes': orchestrators}))
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


def merge_plans(plan_paths: Sequence[Path]) -> tuple[dict[str, Any], Path]:
    """Merge compatible plans while retaining the first plan's run identity.

    ``load_plan`` deliberately remains the single-file API used by the
    bottleneck monitor.  The collector CLI uses this helper when ``--plan`` is
    repeated.
    """
    paths = tuple(Path(path).resolve() for path in plan_paths)
    if not paths:
        raise ValueError("at least one Slurm plan is required")

    primary_path = paths[0]
    primary = load_plan(primary_path)
    primary_run_dir = Path(primary.get("run_dir") or primary_path.parent).resolve()
    primary_run_name = str(primary.get("run_name") or primary_run_dir.name)
    merged_nodes: list[dict[str, Any]] = []
    seen_nodes: dict[str, Path] = {}
    expected_workers = 0

    for path in paths:
        plan = load_plan(path)
        run_name = plan.get("run_name")
        if run_name is not None and str(run_name) != primary_run_name:
            raise ValueError(
                f"incompatible Slurm plan run name {run_name!r} in {path}; "
                f"expected {primary_run_name!r}"
            )
        plan_expected = plan.get("expected_workers")
        if plan_expected is None:
            plan_expected = sum(int(node.get("expected_workers", 0)) for node in plan["nodes"])
        expected_workers += int(plan_expected)
        for record in plan["nodes"]:
            node = str(record.get("node") or "")
            if not node:
                raise ValueError(f"invalid Slurm node record in {path}: missing node")
            previous = seen_nodes.get(node)
            if previous is not None:
                raise ValueError(
                    f"duplicate Slurm node record {node!r} in {path}; already present in {previous}"
                )
            seen_nodes[node] = path
            merged_nodes.append(record)

    merged = {
        **primary,
        "run_name": primary_run_name,
        "run_dir": str(primary_run_dir),
        "expected_workers": expected_workers,
        "nodes": merged_nodes,
    }
    return merged, primary_path


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


def remote_shell_argv(
    node: str,
    node_ip: str | None,
    job_id: str | None,
    state: str,
    command: str,
    transport: str,
) -> list[str]:
    if transport == "srun":
        return srun_base(node, job_id, state) + ["bash", "-lc", command]
    if transport != "ssh":
        raise ValueError("collection transport must be srun or ssh")
    if node == socket.gethostname():
        return command_prefix() + ["bash", "-lc", command]
    if not node_ip:
        raise ValueError(f"SSH collection requires node_ip for {node}")
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=15",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=4",
        f"alex@{node_ip}",
        f"bash -lc {shlex.quote(command)}",
    ]


def collection_unavailable(state: str, transport: str) -> bool:
    if state == "NOT_SUBMITTED" or state == "PENDING":
        return True
    return transport == "srun" and state == "SUSPENDED"


def safe_extract(payload: bytes, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
        for member in archive.getmembers():
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError(f"unsafe archive member: {member.name}")
        archive.extractall(destination, filter="data")


# Journal/log files worth collecting off a node before its jobs are cancelled.
# The bulky ``tasks/`` tree is deliberately excluded: on this cluster every node
# shares one ``/data`` filesystem, so task dirs are already readable by the
# controller and re-archiving ~180k files over SSH is both redundant and slow
# enough to wedge the reconcile loop.
_ARCHIVE_NAME_PATTERNS = (
    "create.jsonl",
    "orchestrator-instance-status*.jsonl",
    "orchestrator-progress*.jsonl",
    "task_references.json",
    "slurm-*.out",
)


def _shared_fs_archive(remote_run_dir: str, destination: Path) -> bool:
    """Fast path when ``remote_run_dir`` is already visible on shared storage.

    Copies only the small journal/log set locally (no tar-over-SSH, no task
    tree). Returns True when the fast path was taken, False to fall back to the
    remote transport.
    """
    source = Path(remote_run_dir)
    if not source.is_dir():
        return False
    destination.mkdir(parents=True, exist_ok=True)
    for pattern in _ARCHIVE_NAME_PATTERNS:
        for match in source.rglob(pattern):
            if not match.is_file():
                continue
            relative = match.relative_to(source)
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(match, target)
    return True


def collect_archive(
    node: str,
    job_id: str | None,
    state: str,
    remote_run_dir: str,
    destination: Path,
    include_tasks: bool,
    *,
    node_ip: str | None = None,
    transport: str = "srun",
) -> None:
    if collection_unavailable(state, transport):
        return
    # ``include_tasks`` is retained for API compatibility but no longer pulls the
    # tasks tree: those files already live on shared ``/data`` (see
    # _ARCHIVE_NAME_PATTERNS). When the run dir is locally visible, copy the
    # journal set directly and skip the SSH transport entirely.
    if _shared_fs_archive(remote_run_dir, destination):
        return
    name_find = " -o ".join(f"-name {shlex.quote(pattern)}" for pattern in _ARCHIVE_NAME_PATTERNS)
    script = (
        "set -euo pipefail; "
        f"cd {shlex.quote(remote_run_dir)}; "
        f"find . -type f \\( {name_find} \\) -print0 | tar --null -czf - --files-from -"
    )
    proc = run_bytes(
        remote_shell_argv(node, node_ip, job_id, state, script, transport),
        timeout=600,
    )
    if proc.stdout:
        safe_extract(proc.stdout, destination)


def collect_health(
    node: str,
    job_id: str | None,
    state: str,
    remote_run_dir: str,
    submitted_at: str | None,
    health_window_seconds: int = 300,
    *,
    node_ip: str | None = None,
    transport: str = "srun",
) -> dict[str, Any]:
    if collection_unavailable(state, transport):
        return {
            "error_counts": {},
            "recent_error_counts": {},
            "recent_api_failure_events": 0,
            "health_window_seconds": health_window_seconds,
            "worker_log_files": 0,
            "latest_log_age_seconds": None,
        }
    cutoff = 0.0
    if submitted_at:
        try:
            cutoff = datetime.fromisoformat(submitted_at).timestamp()
        except ValueError:
            cutoff = 0.0
    health_argv = [
        "python3",
        "-c",
        REMOTE_HEALTH_CODE,
        remote_run_dir,
        str(cutoff),
        str(health_window_seconds),
    ]
    if transport == "srun":
        argv = srun_base(node, job_id, state) + health_argv
    else:
        argv = remote_shell_argv(
            node,
            node_ip,
            job_id,
            state,
            shlex.join(health_argv),
            transport,
        )
    proc = run_bytes(argv, timeout=300)
    return json.loads(proc.stdout.decode(errors="replace"))


def atomic_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        target_mode = path.stat().st_mode & 0o777
    except FileNotFoundError:
        target_mode = 0o644
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            json.dump(data, handle, indent=2, sort_keys=True)
            handle.write("\n")
        temporary.chmod(target_mode)
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def collect_merged(
    plan_paths: Sequence[Path],
    include_tasks: bool = False,
    health_window_seconds: int = 300,
    transport: str = "srun",
) -> dict[str, Any]:
    plan, primary_path = merge_plans(plan_paths)
    plan_path = primary_path
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
                node_ip=(str(record["node_ip"]) if record.get("node_ip") else None),
                transport=transport,
            )
            node_health.update(
                collect_health(
                    node,
                    job_id,
                    state,
                    str(record["remote_run_dir"]),
                    record.get("submitted_at"),
                    health_window_seconds,
                    node_ip=(str(record["node_ip"]) if record.get("node_ip") else None),
                    transport=transport,
                )
            )
            node_health["collected"] = not collection_unavailable(state, transport)
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
            node_health["collection_error"] = redact(str(exc))
        nodes_health.append(node_health)

    active_workers = sum(int(item.get("active_worker_processes", 0)) for item in nodes_health)
    recent_error_counts: dict[str, int] = {}
    for item in nodes_health:
        for key, value in item.get("recent_error_counts", {}).items():
            recent_error_counts[key] = recent_error_counts.get(key, 0) + int(value)
    health = {
        "event": "slurm_health",
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
        "run_name": plan.get("run_name", run_dir.name),
        "expected_workers": int(plan.get("expected_workers", 0)),
        "active_workers": active_workers,
        "health_window_seconds": health_window_seconds,
        "recent_error_counts": recent_error_counts,
        "recent_api_failure_events": sum(
            int(item.get("recent_api_failure_events", 0)) for item in nodes_health
        ),
        "nodes": nodes_health,
    }
    atomic_json(run_dir / "slurm-health.json", health)
    return health


def collect_once(
    plan_path: Path,
    include_tasks: bool = False,
    health_window_seconds: int = 300,
    transport: str = "srun",
) -> dict[str, Any]:
    """Collect one plan, preserving the original public API."""
    return collect_merged([plan_path], include_tasks, health_window_seconds, transport)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True, action="append")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=int, default=30)
    parser.add_argument("--include-tasks", action="store_true")
    parser.add_argument("--health-window-seconds", type=int, default=300)
    parser.add_argument("--transport", choices=("srun", "ssh"), default="srun")
    return parser.parse_args(argv)


def main() -> int:
    args = parse_args()
    stop = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    while True:
        health = collect_merged(
            [path.resolve() for path in args.plan],
            args.include_tasks,
            args.health_window_seconds,
            args.transport,
        )
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
