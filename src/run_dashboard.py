#!/usr/bin/env python3
"""Live WebUI for an orchestrator run's validation yield."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import threading
from collections.abc import Iterable
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

CREATE_INSTANCE_RE = re.compile(r"--repo\x00([^\x00]+)\x00--pr\x00([^\x00]+)")
STATUS_JOURNAL_GLOB = "orchestrator-instance-status*.jsonl"
BACKUP_MARKER = ".before-"
SUCCESS_LEDGER_NAME = "create.jsonl"
SLURM_PLAN_NAME = "slurm-plan.json"
SLURM_HEALTH_NAME = "slurm-health.json"
SLURM_NODE_DIR_NAMES = {"slurm-nodes", "slurm_nodes"}
DISCOVERY_PRUNE_DIRS = {
    ".cache",
    ".git",
    ".swegen",
    "__pycache__",
    "cache",
    "caches",
    "data_cache",
    "harbor-jobs",
    "node_modules",
    "postprocessed-output",
    "repo-cache",
    "repo_cache",
    "repos",
    "tasks",
}
NODE_FIELDS = (
    "slurm_node",
    "slurm_node_name",
    "slurmd_nodename",
    "node",
    "node_name",
    "hostname",
    "host",
)


def _is_backup_component(name: str) -> bool:
    lower = name.lower()
    return (
        BACKUP_MARKER in lower
        or lower in {"backup", "backups", "snapshot", "snapshots"}
        or lower.startswith(("backup-", "snapshot-"))
        or lower.endswith(("-backup", "-snapshot"))
    )


def _discover_run_files(run_dir: Path, filename_matches: Any) -> list[Path]:
    """Recursively find live run artifacts without entering output/cache trees."""
    paths: list[Path] = []
    if not run_dir.is_dir():
        return paths

    for root, dirnames, filenames in os.walk(run_dir):
        dirnames[:] = sorted(
            dirname
            for dirname in dirnames
            if dirname.lower() not in DISCOVERY_PRUNE_DIRS
            and not _is_backup_component(dirname)
        )
        root_path = Path(root)
        for filename in filenames:
            if _is_backup_component(filename) or not filename_matches(filename):
                continue
            paths.append(root_path / filename)
    return sorted(paths)


def status_journal_paths(run_dir: Path) -> list[Path]:
    """Return live local and Slurm status journals under the run directory."""
    return _discover_run_files(
        run_dir,
        lambda name: name.startswith("orchestrator-instance-status")
        and name.endswith(".jsonl"),
    )


def success_ledger_paths(run_dir: Path) -> list[Path]:
    """Return authoritative success ledgers from local and Slurm node state."""
    return _discover_run_files(run_dir, lambda name: name == SUCCESS_LEDGER_NAME)


def _string_value(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _node_from_record(record: dict[str, Any]) -> tuple[str | None, bool]:
    for field in NODE_FIELDS:
        node = _string_value(record.get(field))
        if node:
            return node, field.startswith("slurm")

    slurm = record.get("slurm")
    if isinstance(slurm, dict):
        for field in NODE_FIELDS:
            node = _string_value(slurm.get(field))
            if node:
                return node, True
    return None, False


def _node_from_path(path: Path, run_dir: Path | None) -> tuple[str | None, bool]:
    if run_dir is None:
        return None, False
    try:
        parts = path.relative_to(run_dir).parts
    except ValueError:
        return None, False

    for index, part in enumerate(parts[:-1]):
        if part.lower() in SLURM_NODE_DIR_NAMES and index + 1 < len(parts) - 1:
            return parts[index + 1], True
    # The legacy Slurm launcher stored one directory per node beneath this
    # exact folder. Do not interpret similarly named flat proxy log folders as
    # machines.
    for index, part in enumerate(parts[:-1]):
        if part == "orchestrator-logs" and index + 1 < len(parts) - 1:
            return parts[index + 1], True
    return None, False


def _enrich_node_identity(
    record: dict[str, Any], source_path: Path, run_dir: Path | None
) -> dict[str, Any]:
    enriched = dict(record)
    node, is_slurm = _node_from_record(enriched)
    if node is None:
        node, is_slurm = _node_from_path(source_path, run_dir)
    if node is not None:
        enriched["node"] = node
        if is_slurm:
            enriched["node_scope"] = "slurm"
    return enriched


def _copy_node_identity(target: dict[str, Any], source: dict[str, Any]) -> None:
    if not _string_value(target.get("node")) and _string_value(source.get("node")):
        target["node"] = source["node"]
    if target.get("node_scope") != "slurm" and source.get("node_scope") == "slurm":
        target["node_scope"] = "slurm"


def load_latest_statuses(
    status_paths: Path | Iterable[Path],
    *,
    run_dir: Path | None = None,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Return the latest status per instance across one or more journals."""
    latest: dict[str, dict[str, Any]] = {}
    sequence: dict[str, tuple[str, int, int]] = {}
    paths = [status_paths] if isinstance(status_paths, Path) else list(status_paths)

    for path_index, status_path in enumerate(paths):
        if not status_path.exists():
            continue
        try:
            fh = status_path.open(encoding="utf-8", errors="replace")
        except OSError:
            continue
        with fh:
            for index, line in enumerate(fh):
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(record, dict):
                    continue
                instance = record.get("instance")
                status = record.get("status")
                if not isinstance(instance, str) or status not in {"success", "failure"}:
                    continue
                timestamp = record.get("timestamp")
                key = (
                    timestamp if isinstance(timestamp, str) else "",
                    path_index,
                    index,
                )
                if instance not in sequence or key >= sequence[instance]:
                    latest[instance] = _enrich_node_identity(record, status_path, run_dir)
                    sequence[instance] = key

    order = sorted(latest, key=sequence.__getitem__, reverse=True)
    return latest, order


def load_success_ledger(
    create_path: Path, *, run_dir: Path | None = None
) -> dict[str, dict[str, Any]]:
    """Load the run's authoritative successful-instance ledger."""
    successes: dict[str, dict[str, Any]] = {}
    sequence: dict[str, tuple[str, int]] = {}
    if not create_path.exists():
        return successes

    try:
        fh = create_path.open(encoding="utf-8", errors="replace")
    except OSError:
        return successes
    with fh:
        for index, line in enumerate(fh):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            instance = record.get("task_id")
            if not isinstance(instance, str) or not instance:
                harbor = record.get("harbor")
                instance = Path(harbor).name if isinstance(harbor, str) else ""
            if not instance:
                continue
            timestamp = record.get("ts")
            if not isinstance(timestamp, str):
                timestamp = ""
            key = (timestamp, index)
            if instance not in sequence or key >= sequence[instance]:
                success_record = {
                    "instance": instance,
                    "status": "success",
                    "timestamp": timestamp,
                    "worker_id": None,
                }
                successes[instance] = _enrich_node_identity(
                    success_record, create_path, run_dir
                )
                sequence[instance] = key
    return successes


def collect_latest_statuses(run_dir: Path) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Merge every live journal and reconcile it with successful creations."""
    latest, _order = load_latest_statuses(
        status_journal_paths(run_dir), run_dir=run_dir
    )
    for ledger_path in success_ledger_paths(run_dir):
        for instance, success_record in load_success_ledger(
            ledger_path, run_dir=run_dir
        ).items():
            current = latest.get(instance)
            if current is None or current.get("status") != "success":
                latest[instance] = success_record
            elif str(success_record.get("timestamp", "")) >= str(
                current.get("timestamp", "")
            ):
                _copy_node_identity(success_record, current)
                latest[instance] = success_record

    order = sorted(
        latest,
        key=lambda instance: (
            str(latest[instance].get("timestamp", "")),
            instance,
        ),
        reverse=True,
    )
    return latest, order


def load_slurm_health(run_dir: Path) -> dict[str, Any]:
    path = run_dir / SLURM_HEALTH_NAME
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def count_jsonl_entries(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        with path.open(encoding="utf-8", errors="replace") as fh:
            return sum(1 for line in fh if line.strip())
    except OSError:
        return 0


def active_instances(run_dir: Path) -> list[str]:
    """Read /proc to find active `swegen create` children for this run."""
    instances: set[str] = set()
    run_name = run_dir.name
    proc = Path("/proc")
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            raw = (entry / "cmdline").read_bytes().decode(errors="replace")
        except OSError:
            continue
        if "swegen\x00create\x00" not in raw or "--state-dir\x00" not in raw:
            continue
        if (
            f"--state-dir\x00{run_dir}\x00" not in raw
            and f"--state-dir\x00runs/{run_name}\x00" not in raw
        ):
            continue
        match = CREATE_INSTANCE_RE.search(raw)
        if match:
            repo, pr = match.groups()
            instances.add(f"{repo.lower().replace('/', '__')}-{pr}")
    return sorted(instances)


def calculate_status(
    run_dir: Path, input_jsonl: Path, total_entries: int | None = None
) -> dict[str, Any]:
    latest, order = collect_latest_statuses(run_dir)
    success = sum(record.get("status") == "success" for record in latest.values())
    failure = sum(record.get("status") == "failure" for record in latest.values())
    processed = success + failure
    total = total_entries if total_entries is not None else count_jsonl_entries(input_jsonl)
    active = active_instances(run_dir)
    slurm_health = load_slurm_health(run_dir)
    slurm_active = slurm_health.get("active_workers", 0)
    slurm_expected = slurm_health.get("expected_workers", 0)
    if not isinstance(slurm_active, int):
        slurm_active = 0
    if not isinstance(slurm_expected, int):
        slurm_expected = 0

    success_by_node: dict[str, int] = {}
    for record in latest.values():
        if record.get("status") != "success":
            continue
        node = _string_value(record.get("node"))
        if node and record.get("node_scope") == "slurm":
            success_by_node[node] = success_by_node.get(node, 0) + 1

    recent = []
    for instance in order[:25]:
        record = latest[instance]
        recent.append(
            {
                "instance": instance,
                "status": record.get("status"),
                "timestamp": record.get("timestamp", ""),
                "reason": record.get("failure_reason", ""),
                "worker_id": record.get("worker_id"),
                "node": record.get("node", "controller"),
            }
        )

    return {
        "run": run_dir.name,
        "updated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "success": success,
        "failure": failure,
        "processed": processed,
        "total": total,
        "remaining": max(total - processed, 0),
        "active_workers": len(active) + slurm_active,
        "active_workers_local": len(active),
        "active_workers_slurm": slurm_active,
        "slurm_expected_workers": slurm_expected,
        "active_instances": active,
        "success_by_slurm_node": dict(sorted(success_by_node.items())),
        "yield_percent": round((success / processed * 100) if processed else 0.0, 2),
        "dataset_yield_percent": round((success / total * 100) if total else 0.0, 4),
        "completion_percent": round((processed / total * 100) if total else 0.0, 2),
        "recent": recent,
    }


DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>SWE-gen Live Yield</title>
  <style>
    :root { color-scheme: dark; --bg:#0b1020; --panel:#141b2d; --muted:#8f9bb3; --good:#36d399; --bad:#fb7185; --accent:#60a5fa; }
    * { box-sizing:border-box; }
    body { margin:0; background:radial-gradient(circle at top,#17213b 0,var(--bg) 45%); color:#edf2f7; font:15px/1.45 ui-sans-serif,system-ui,sans-serif; }
    main { width:min(1180px,calc(100% - 32px)); margin:32px auto; }
    header { display:flex; justify-content:space-between; align-items:end; gap:16px; margin-bottom:20px; }
    h1 { margin:0; font-size:28px; }
    .sub,.muted { color:var(--muted); }
    .live { color:var(--good); font-weight:700; }
    .grid { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:14px; }
    .card { background:rgba(20,27,45,.94); border:1px solid #26324d; border-radius:14px; padding:18px; box-shadow:0 12px 35px rgba(0,0,0,.22); }
    .label { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em; }
    .value { font-size:30px; font-weight:750; margin-top:5px; }
    .formula { color:var(--muted); font-size:13px; margin-top:3px; }
    .wide { grid-column:span 2; }
    .bar { height:10px; background:#202a42; border-radius:999px; overflow:hidden; margin-top:14px; }
    .fill { height:100%; width:0; background:linear-gradient(90deg,var(--accent),#a78bfa); transition:width .5s ease; }
    section { margin-top:18px; }
    table { width:100%; border-collapse:collapse; }
    th,td { text-align:left; padding:10px 8px; border-bottom:1px solid #26324d; vertical-align:top; }
    th { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.06em; }
    .success { color:var(--good); font-weight:700; }
    .failure { color:var(--bad); font-weight:700; }
    .instances { word-break:break-word; }
    @media (max-width:850px) { .grid { grid-template-columns:repeat(2,1fr); } }
    @media (max-width:520px) { .grid { grid-template-columns:1fr; } .wide { grid-column:span 1; } header { align-items:start; flex-direction:column; } }
  </style>
</head>
<body><main>
  <header><div><h1>SWE-gen Live Yield</h1><div class="sub">Run <span id="run">—</span></div></div><div id="connection" class="live">● LIVE</div></header>
  <div class="grid">
    <div class="card wide"><div class="label">Validation yield</div><div id="yield" class="value">—</div><div id="yieldFormula" class="formula">successful NOP+Oracle / processed</div><div class="bar"><div id="yieldBar" class="fill"></div></div></div>
    <div class="card"><div class="label">Successful across all nodes</div><div id="success" class="value success">—</div><div id="successByNode" class="formula">NOP=0 and Oracle=1</div></div>
    <div class="card"><div class="label">Failed</div><div id="failure" class="value failure">—</div><div class="formula">latest result per instance</div></div>
    <div class="card wide"><div class="label">Dataset completion</div><div id="completion" class="value">—</div><div id="completionFormula" class="formula">processed / total input</div><div class="bar"><div id="completionBar" class="fill"></div></div></div>
    <div class="card"><div class="label">Dataset yield</div><div id="datasetYield" class="value">—</div><div id="datasetYieldFormula" class="formula">successful / total input</div></div>
    <div class="card"><div class="label">Active workers</div><div id="active" class="value">—</div><div id="activeScope" class="formula"></div><div id="activeInstances" class="formula instances"></div></div>
  </div>
  <section class="card"><div class="label">Recent completed instances</div><table><thead><tr><th>Instance</th><th>Node</th><th>Status</th><th>Reason</th><th>Updated</th></tr></thead><tbody id="recent"></tbody></table></section>
  <div class="muted" style="margin-top:12px">Updated <span id="updated">—</span> · refreshes every 2 seconds</div>
</main>
<script>
const fmt = n => Number(n).toLocaleString();
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function refresh() {
  try {
    const r = await fetch('/api/status', {cache:'no-store'}); if (!r.ok) throw new Error(r.status);
    const d = await r.json();
    run.textContent=d.run; yield.textContent=d.yield_percent.toFixed(2)+'%'; success.textContent=fmt(d.success); failure.textContent=fmt(d.failure);
    const nodeParts=Object.entries(d.success_by_slurm_node||{}).map(([node,count])=>`${node}: ${fmt(count)}`);
    successByNode.textContent=nodeParts.length ? `Slurm · ${nodeParts.join(' · ')}` : 'NOP=0 and Oracle=1';
    yieldFormula.textContent=`${fmt(d.success)} successful / ${fmt(d.processed)} processed`;
    yieldBar.style.width=Math.min(d.yield_percent,100)+'%'; completion.textContent=d.completion_percent.toFixed(2)+'%';
    completionFormula.textContent=`${fmt(d.processed)} processed / ${fmt(d.total)} total · ${fmt(d.remaining)} remaining`;
    completionBar.style.width=Math.min(d.completion_percent,100)+'%'; datasetYield.textContent=d.dataset_yield_percent.toFixed(4)+'%';
    datasetYieldFormula.textContent=`${fmt(d.success)} successful / ${fmt(d.total)} total input`;
    active.textContent=fmt(d.active_workers); activeScope.textContent=`${fmt(d.active_workers_local)} local + ${fmt(d.active_workers_slurm)} Slurm / ${fmt(d.slurm_expected_workers)} expected Slurm`;
    activeInstances.textContent=d.active_instances.join(', ');
    updated.textContent=d.updated_at; connection.textContent='● LIVE'; connection.className='live';
    recent.innerHTML=d.recent.map(x=>`<tr><td>${esc(x.instance)}</td><td>${esc(x.node)}</td><td class="${x.status}">${esc(x.status)}</td><td>${esc(x.reason)}</td><td>${esc(x.timestamp)}</td></tr>`).join('');
  } catch (e) { connection.textContent='● DISCONNECTED'; connection.className='failure'; }
}
refresh(); setInterval(refresh,2000);
</script></body></html>"""


def make_handler(run_dir: Path, input_jsonl: Path, total_entries: int):
    class DashboardHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/" or self.path.startswith("/?"):
                body = DASHBOARD_HTML.encode()
                content_type = "text/html; charset=utf-8"
                status = HTTPStatus.OK
            elif self.path == "/api/status":
                body = json.dumps(
                    calculate_status(run_dir, input_jsonl, total_entries),
                    separators=(",", ":"),
                ).encode()
                content_type = "application/json"
                status = HTTPStatus.OK
            elif self.path == "/healthz":
                body = b"ok\n"
                content_type = "text/plain; charset=utf-8"
                status = HTTPStatus.OK
            else:
                body = b"not found\n"
                content_type = "text/plain; charset=utf-8"
                status = HTTPStatus.NOT_FOUND

            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    return DashboardHandler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--input-jsonl", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--pid-file", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_dir = args.run_dir.resolve()
    input_jsonl = args.input_jsonl.resolve()
    total_entries = count_jsonl_entries(input_jsonl)
    server = ThreadingHTTPServer(
        (args.host, args.port),
        make_handler(run_dir, input_jsonl, total_entries),
    )

    if args.pid_file:
        args.pid_file.parent.mkdir(parents=True, exist_ok=True)
        args.pid_file.write_text(f"{os.getpid()}\n")

    def stop_server(_signum: int, _frame: object) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop_server)
    signal.signal(signal.SIGINT, stop_server)
    try:
        print(f"SWE-gen dashboard listening on http://{args.host}:{args.port}", flush=True)
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()
        if args.pid_file:
            args.pid_file.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
