#!/usr/bin/env python3
"""Guard a SWE-Gen run against proxy HTTP 400/502 responses.

The monitor scans worker and bridge logs frequently while probing both upstream
SOCKS endpoints and both local HTTP bridges on a slower, independent cadence.
On the first HTTP 400/502 it stops the active orchestrator, captures concise
diagnostics, and resumes the same run once at 4+4 concurrency. If a reduced run
sees another 400/502, it is stopped and left stopped for investigation.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

WORKSPACE = Path(__file__).resolve().parents[1]
DEFAULT_ENDPOINT = "https://arcyleung-ubuntu.tailb940e6.ts.net/v1/models"
PROXY_ENV_KEYS = {
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
}
PROXY_LOG_ERROR_RE = re.compile(
    r"(?i)(?:"
    r"\b(?:400\s+bad\s+request|502\s+bad\s+gateway)\b.{0,160}"
    r"(?:proxy|connect|tunnel)|"
    r"(?:proxy|connect|tunnel).{0,160}"
    r"\b(?:400\s+bad\s+request|502\s+bad\s+gateway)\b|"
    r"unexpected (?:server )?response.{0,40}\b(?:400|502)\b|"
    r"connect tunnel failed,\s*response\s+(?:400|502)\b|"
    r"proxy[^\n]{0,120}\bstatus(?: code)?[=: ]+(?:400|502)\b"
    r")"
)
CURL_PROXY_STATUS_RE = re.compile(
    r"(?i)\bconnect tunnel failed,\s*response\s+(400|502)\b"
)


@dataclass(frozen=True)
class Route:
    label: str
    kind: str
    url: str


@dataclass(frozen=True)
class ProbeResult:
    label: str
    kind: str
    proxy: str
    returncode: int
    http_code: int
    total_seconds: float
    error: str


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def emit(event: str, **fields: Any) -> None:
    print(
        json.dumps({"timestamp": utc_now(), "event": event, **fields}, sort_keys=True),
        flush=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--worker-log-dir", type=Path, required=True)
    parser.add_argument("--run-script", type=Path, default=WORKSPACE / "run_orchestrator.sh")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Worker/proxy log scan interval in seconds (default: 5)",
    )
    parser.add_argument(
        "--probe-interval",
        type=float,
        default=60.0,
        help="Route probe batch interval in seconds (default: 60)",
    )
    parser.add_argument("--connect-timeout", type=float, default=8.0)
    parser.add_argument("--max-time", type=float, default=15.0)
    parser.add_argument("--grace", type=float, default=10.0)
    parser.add_argument("--debug-workers", type=int, default=8)
    parser.add_argument("--debug-workers-per-endpoint", type=int, default=4)
    parser.add_argument("--pid-file", type=Path)
    parser.add_argument("--state-file", type=Path)
    parser.add_argument(
        "--initial-mode",
        choices=("normal", "debug_4x4", "debug_stopped"),
        default="normal",
        help="Initial fallback state, used when attaching to an existing debug run",
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--trigger-status",
        type=int,
        action="append",
        default=None,
        help="HTTP status that triggers fallback; repeatable (default: 400, 502)",
    )
    return parser.parse_args()


def default_routes() -> list[Route]:
    return [
        Route("socks87", "socks", "10.218.163.87:1080"),
        Route("bridge87", "http", "http://127.0.0.1:18087"),
        Route("socks108", "socks", "10.218.163.108:1080"),
        Route("bridge108", "http", "http://127.0.0.1:18108"),
    ]


def probe_bucket(result: ProbeResult, expected_status: int = 401) -> str:
    if result.returncode == 0 and result.http_code == expected_status:
        return "expected_http"
    if result.http_code == 400:
        return "http_400"
    if result.http_code == 502:
        return "http_502"
    if result.http_code == 0:
        return "transport_failure"
    return "other_http"


def clean_probe_env() -> dict[str, str]:
    return {key: value for key, value in os.environ.items() if key not in PROXY_ENV_KEYS}


def curl_proxy_http_status(error: str) -> int | None:
    """Extract a proxy CONNECT status curl reports only in stderr."""
    match = CURL_PROXY_STATUS_RE.search(error)
    return int(match.group(1)) if match else None


def probe(route: Route, args: argparse.Namespace) -> ProbeResult:
    command = [
        "curl",
        "--noproxy",
        "",
        "--silent",
        "--show-error",
        "--connect-timeout",
        str(args.connect_timeout),
        "--max-time",
        str(args.max_time),
        "--output",
        "/dev/null",
        "--write-out",
        "%{http_code}\t%{time_total}",
    ]
    if route.kind == "socks":
        command.extend(["--socks5-hostname", route.url])
    else:
        command.extend(["--proxy", route.url])
    command.append(args.endpoint)
    try:
        completed = subprocess.run(
            command,
            env=clean_probe_env(),
            capture_output=True,
            text=True,
            timeout=max(args.max_time + 5.0, 10.0),
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return ProbeResult(route.label, route.kind, route.url, -1, 0, 0.0, str(error))

    code_text, _, time_text = completed.stdout.strip().partition("\t")
    try:
        http_code = int(code_text or "0")
    except ValueError:
        http_code = 0
    try:
        total_seconds = float(time_text or "0")
    except ValueError:
        total_seconds = 0.0
    error = completed.stderr.strip().splitlines()[-1] if completed.stderr.strip() else ""
    if http_code == 0:
        proxy_status = curl_proxy_http_status(error)
        if proxy_status is not None:
            http_code = proxy_status
    return ProbeResult(
        route.label,
        route.kind,
        route.url,
        completed.returncode,
        http_code,
        total_seconds,
        error,
    )


ProbeFuture = tuple[Route, Future[ProbeResult]]


def submit_probe_batch(
    executor: ThreadPoolExecutor,
    routes: list[Route],
    args: argparse.Namespace,
) -> list[ProbeFuture]:
    """Start one non-blocking probe per route."""
    return [(route, executor.submit(probe, route, args)) for route in routes]


def take_completed_probe_batch(
    pending: list[ProbeFuture] | None,
) -> list[ProbeResult] | None:
    """Return a completed batch without ever waiting for unfinished probes."""
    if pending is None or not all(future.done() for _, future in pending):
        return None
    results: list[ProbeResult] = []
    for route, future in pending:
        try:
            results.append(future.result())
        except Exception as error:  # Defensive: a probe must not kill the monitor.
            results.append(
                ProbeResult(
                    route.label,
                    route.kind,
                    route.url,
                    -1,
                    0,
                    0.0,
                    f"{type(error).__name__}: {error}",
                )
            )
    return results


def record_probe_batch(state: dict[str, Any], probes: list[ProbeResult]) -> None:
    """Update persistent state after one complete, current-mode probe batch."""
    cycle_stable = all(probe_bucket(result) == "expected_http" for result in probes)
    state["stable"] = cycle_stable
    state["last_probe_checked_at"] = utc_now()
    state["last_probes"] = [asdict(result) for result in probes]
    state["probe_cycles"] = int(state.get("probe_cycles", 0)) + 1
    if cycle_stable:
        state["consecutive_unstable_cycles"] = 0
    else:
        state["consecutive_unstable_cycles"] = int(
            state.get("consecutive_unstable_cycles", 0)
        ) + 1
    counts = state["probe_counts"]
    for result in probes:
        counts[result.label][probe_bucket(result)] += 1


def proc_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            raw = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            cmd = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode(
                errors="replace"
            )
        except (OSError, IndexError, ValueError):
            continue
        rows.append(
            {
                "pid": int(entry.name),
                "ppid": int(raw[1]),
                "pgrp": int(raw[2]),
                "cmd": cmd,
            }
        )
    return rows


def orchestrator_pid(run_name: str, run_dir: Path) -> int | None:
    pid_file = run_dir / "orchestrator.pid"
    try:
        candidate = int(pid_file.read_text().strip())
    except (OSError, ValueError):
        candidate = 0
    for row in proc_rows():
        if row["pid"] == candidate and "src/orchestrator.py" in row["cmd"]:
            return candidate
    marker = f"--run-name {run_name}"
    for row in proc_rows():
        if "src/orchestrator.py" in row["cmd"] and marker in row["cmd"]:
            return int(row["pid"])
    return None


def group_exists(pgrp: int) -> bool:
    return any(int(row["pgrp"]) == pgrp for row in proc_rows())


def stop_run(run_dir: Path, grace: float, dry_run: bool) -> dict[str, Any]:
    pid = orchestrator_pid(run_dir.name, run_dir)
    if pid is None:
        return {"orchestrator_pid": None, "worker_groups": [], "already_stopped": True}
    rows = proc_rows()
    worker_groups = sorted(
        {
            int(row["pgrp"])
            for row in rows
            if int(row["ppid"]) == pid and "swegen create" in str(row["cmd"])
        }
    )
    details = {
        "orchestrator_pid": pid,
        "worker_groups": worker_groups,
        "already_stopped": False,
    }
    if dry_run:
        return details

    try:
        os.kill(pid, signal.SIGSTOP)
    except ProcessLookupError:
        return {**details, "already_stopped": True}
    for pgrp in worker_groups:
        try:
            os.killpg(pgrp, signal.SIGTERM)
        except ProcessLookupError:
            pass
    try:
        os.kill(pid, signal.SIGTERM)
        os.kill(pid, signal.SIGCONT)
    except ProcessLookupError:
        pass

    deadline = time.monotonic() + max(0.0, grace)
    while time.monotonic() < deadline:
        if orchestrator_pid(run_dir.name, run_dir) is None and not any(
            group_exists(group) for group in worker_groups
        ):
            break
        time.sleep(0.25)

    for pgrp in worker_groups:
        if group_exists(pgrp):
            try:
                os.killpg(pgrp, signal.SIGKILL)
            except ProcessLookupError:
                pass
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    pid_file = run_dir / "orchestrator.pid"
    pid_file.unlink(missing_ok=True)
    return details


def stop_run_containers(run_dir: Path, dry_run: bool) -> list[str]:
    try:
        listed = subprocess.run(
            ["docker", "ps", "-q"], capture_output=True, text=True, timeout=20, check=False
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    stopped: list[str] = []
    for container_id in listed.stdout.split():
        inspected = subprocess.run(
            ["docker", "inspect", container_id],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        try:
            details = json.loads(inspected.stdout)[0]
        except (json.JSONDecodeError, IndexError, TypeError):
            continue
        mounts = details.get("Mounts") or []
        if not any(
            str(item.get("Source", "")).startswith(str(run_dir.resolve()))
            for item in mounts
            if isinstance(item, dict)
        ):
            continue
        stopped.append(container_id)
        if not dry_run:
            subprocess.run(
                ["docker", "rm", "-f", container_id],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
    return stopped


def tail_text(path: Path, max_bytes: int = 32768) -> str:
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - max_bytes))
            return handle.read().decode(errors="replace")
    except OSError:
        return ""


def capture_diagnostics(
    run_dir: Path,
    worker_log_dir: Path,
    reason: dict[str, Any],
    probes: list[ProbeResult],
) -> Path:
    output_dir = run_dir / "proxy-diagnostics"
    output_dir.mkdir(parents=True, exist_ok=True)
    output = output_dir / f"proxy-trigger-{stamp()}.log"
    sections = [
        json.dumps(
            {
                "timestamp": utc_now(),
                "reason": reason,
                "probes": [asdict(item) for item in probes],
            },
            indent=2,
            sort_keys=True,
        )
    ]
    for path in sorted(worker_log_dir.glob("worker-*.log")):
        text = tail_text(path)
        if text:
            sections.append(f"\n===== {path.name} =====\n{text}")
    for path in sorted((WORKSPACE / ".swegen" / "proxy-bridges").glob("*.log")):
        text = tail_text(path, 16384)
        if text:
            sections.append(f"\n===== bridge:{path.name} =====\n{text}")
    output.write_text("\n".join(sections), errors="replace")
    return output


def initialize_offsets(paths: list[Path]) -> dict[str, int]:
    offsets: dict[str, int] = {}
    for path in paths:
        try:
            offsets[str(path)] = path.stat().st_size
        except OSError:
            offsets[str(path)] = 0
    return offsets


def monitored_logs(worker_log_dir: Path) -> list[Path]:
    return sorted(worker_log_dir.glob("worker-*.log")) + sorted(
        (WORKSPACE / ".swegen" / "proxy-bridges").glob("*.log")
    )


def scan_new_log_errors(paths: list[Path], offsets: dict[str, int]) -> list[dict[str, str]]:
    matches: list[dict[str, str]] = []
    for path in paths:
        key = str(path)
        start = offsets.get(key, 0)
        try:
            with path.open("rb") as handle:
                handle.seek(0, os.SEEK_END)
                end = handle.tell()
                if end < start:
                    start = 0
                handle.seek(start)
                new_text = handle.read().decode(errors="replace")
                offsets[key] = end
        except OSError:
            continue
        for line in new_text.splitlines():
            if PROXY_LOG_ERROR_RE.search(line):
                matches.append({"file": key, "line": line[-1000:]})
    return matches


def write_state(path: Path | None, state: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def launch_debug_run(args: argparse.Namespace, trigger_stamp: str) -> dict[str, Any]:
    debug_log_dir = args.run_dir / f"orchestrator-logs-debug-4x4-{trigger_stamp}"
    launch_log = args.run_dir / f"orchestrator-debug-4x4-{trigger_stamp}-launch.log"
    if args.dry_run:
        return {
            "launcher_pid": None,
            "orchestrator_pid": None,
            "worker_log_dir": str(debug_log_dir),
            "launch_log": str(launch_log),
            "dry_run": True,
        }
    debug_log_dir.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment.update(
        {
            "SWEGEN_WORKERS": str(args.debug_workers),
            "SWEGEN_PROXY_WORKERS_PER_ENDPOINT": str(args.debug_workers_per_endpoint),
            "SWEGEN_RUN_NAME": args.run_dir.name,
            "SWEGEN_ORCHESTRATOR_LOG_DIR": str(debug_log_dir),
        }
    )
    launch_handle = launch_log.open("a")
    process = subprocess.Popen(
        [str(args.run_script.resolve())],
        cwd=WORKSPACE,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=launch_handle,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    launch_handle.close()
    debug_orchestrator_pid: int | None = None
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        debug_orchestrator_pid = orchestrator_pid(args.run_dir.name, args.run_dir)
        if debug_orchestrator_pid is not None:
            pid_file = args.run_dir / "orchestrator.pid"
            pid_file.write_text(f"{debug_orchestrator_pid}\n")
            break
        if process.poll() is not None:
            break
        time.sleep(0.25)
    return {
        "launcher_pid": process.pid,
        "orchestrator_pid": debug_orchestrator_pid,
        "worker_log_dir": str(debug_log_dir),
        "launch_log": str(launch_log),
        "dry_run": False,
    }


def claim_pid_file(pid_file: Path | None) -> None:
    if pid_file is None:
        return
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing = int(pid_file.read_text().strip())
    except (OSError, ValueError):
        existing = 0
    if existing:
        try:
            os.kill(existing, 0)
        except ProcessLookupError:
            pass
        else:
            raise SystemExit(f"monitor already running with pid {existing}")
    pid_file.write_text(f"{os.getpid()}\n")


def main() -> int:
    args = parse_args()
    args.run_dir = args.run_dir.resolve()
    args.worker_log_dir = args.worker_log_dir.resolve()
    args.run_script = args.run_script.resolve()
    trigger_statuses = set(args.trigger_status or [400, 502])
    if (
        args.interval <= 0
        or args.probe_interval <= 0
        or args.max_time <= 0
        or args.debug_workers <= 0
    ):
        raise SystemExit(
            "interval, probe-interval, max-time, and debug-workers must be positive"
        )
    if args.debug_workers != args.debug_workers_per_endpoint * 2:
        raise SystemExit("debug-workers must equal two endpoints x workers-per-endpoint")

    claim_pid_file(args.pid_file)
    stopping = False

    def stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    mode = args.initial_mode
    worker_log_dir = args.worker_log_dir
    log_paths = monitored_logs(worker_log_dir)
    offsets = initialize_offsets(log_paths)
    state: dict[str, Any] = {
        "pid": os.getpid(),
        "mode": mode,
        "started_at": utc_now(),
        "worker_log_dir": str(worker_log_dir),
        "trigger_statuses": sorted(trigger_statuses),
        "scan_interval_seconds": args.interval,
        "probe_interval_seconds": args.probe_interval,
        "stable": False,
        "probe_in_flight": False,
        "log_scan_cycles": 0,
        "probe_cycles": 0,
        "consecutive_unstable_cycles": 0,
        "probe_counts": {
            route.label: {
                "expected_http": 0,
                "http_400": 0,
                "http_502": 0,
                "transport_failure": 0,
                "other_http": 0,
            }
            for route in default_routes()
        },
    }
    write_state(args.state_file, state)
    emit(
        "started",
        pid=os.getpid(),
        interval_seconds=args.interval,
        scan_interval_seconds=args.interval,
        probe_interval_seconds=args.probe_interval,
        mode=mode,
        routes=[asdict(route) for route in default_routes()],
        trigger_statuses=sorted(trigger_statuses),
        dry_run=args.dry_run,
    )

    routes = default_routes()
    pending_probes: list[ProbeFuture] | None = None
    pending_probe_mode: str | None = None
    last_probes: list[ProbeResult] = []
    next_probe_at = time.monotonic()

    try:
        with ThreadPoolExecutor(max_workers=len(routes)) as executor:
            while not stopping:
                # Log-trigger detection stays on the short interval and never waits for
                # curl. This is the primary immediate-response path for live workers.
                current_paths = monitored_logs(worker_log_dir)
                for path in current_paths:
                    offsets.setdefault(str(path), 0)
                log_errors = scan_new_log_errors(current_paths, offsets)
                state["last_checked_at"] = utc_now()
                state["last_log_scan_at"] = state["last_checked_at"]
                state["log_scan_cycles"] = int(state.get("log_scan_cycles", 0)) + 1

                completed_probes = take_completed_probe_batch(pending_probes)
                triggering_probes: list[ProbeResult] = []
                if completed_probes is not None:
                    started_mode = pending_probe_mode
                    pending_probes = None
                    pending_probe_mode = None
                    state["probe_in_flight"] = False
                    state["last_probe_completed_at"] = utc_now()
                    if started_mode != mode:
                        emit(
                            "probe_batch_discarded",
                            started_mode=started_mode,
                            current_mode=mode,
                            reason="run mode changed while probes were in flight",
                        )
                    else:
                        last_probes = completed_probes
                        for result in completed_probes:
                            emit("probe", **asdict(result), mode=mode)
                        record_probe_batch(state, completed_probes)
                        triggering_probes = [
                            result
                            for result in completed_probes
                            if result.http_code in trigger_statuses
                        ]

                reason: dict[str, Any] | None = None
                if triggering_probes:
                    reason = {
                        "type": "probe_http_status",
                        "routes": [asdict(item) for item in triggering_probes],
                    }
                elif log_errors:
                    reason = {"type": "proxy_log_status", "matches": log_errors[:20]}

                if reason is not None and mode != "debug_stopped":
                    trigger_stamp = stamp()
                    diagnostics = capture_diagnostics(
                        args.run_dir, worker_log_dir, reason, last_probes
                    )
                    stopped = stop_run(args.run_dir, args.grace, args.dry_run)
                    containers = stop_run_containers(args.run_dir, args.dry_run)
                    emit(
                        "triggered",
                        mode=mode,
                        reason=reason,
                        diagnostics=str(diagnostics),
                        stop=stopped,
                        containers=containers,
                    )
                    if mode == "normal":
                        launched = launch_debug_run(args, trigger_stamp)
                        mode = "debug_4x4"
                        worker_log_dir = Path(launched["worker_log_dir"])
                        offsets = initialize_offsets(monitored_logs(worker_log_dir))
                        state.update(
                            {
                                "mode": mode,
                                "worker_log_dir": str(worker_log_dir),
                                "fallback_started_at": utc_now(),
                                "fallback": launched,
                                "last_trigger": reason,
                                "diagnostics": str(diagnostics),
                            }
                        )
                        next_probe_at = time.monotonic()
                        emit("fallback_started", **launched)
                    else:
                        mode = "debug_stopped"
                        state.update(
                            {
                                "mode": mode,
                                "debug_stopped_at": utc_now(),
                                "last_trigger": reason,
                                "diagnostics": str(diagnostics),
                            }
                        )
                        emit("debug_stopped", reason=reason, diagnostics=str(diagnostics))
                elif reason is not None:
                    emit("post_stop_proxy_error", reason=reason)

                once_done = args.once and int(state.get("probe_cycles", 0)) >= 1
                now = time.monotonic()
                if not once_done and pending_probes is None and now >= next_probe_at:
                    pending_probes = submit_probe_batch(executor, routes, args)
                    pending_probe_mode = mode
                    next_probe_at = now + args.probe_interval
                    state.update(
                        {
                            "probe_in_flight": True,
                            "last_probe_started_at": utc_now(),
                        }
                    )
                    emit(
                        "probe_batch_started",
                        mode=mode,
                        routes=[route.label for route in routes],
                    )

                state["mode"] = mode
                write_state(args.state_file, state)

                if once_done:
                    break
                deadline = time.monotonic() + args.interval
                while not stopping and time.monotonic() < deadline:
                    time.sleep(min(0.5, deadline - time.monotonic()))
    finally:
        if args.pid_file:
            args.pid_file.unlink(missing_ok=True)
        state.update({"stopped_at": utc_now(), "mode": mode})
        write_state(args.state_file, state)
        emit("stopped", pid=os.getpid(), mode=mode)
    return 0


if __name__ == "__main__":
    sys.exit(main())
