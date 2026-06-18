#!/usr/bin/env python3
"""Tiny local web page: successful vs total task counts.

Serves a small auto-refreshing page (and a /api/counts JSON endpoint) showing
the line counts of the successful and total JSONLs. Standard library only.

Usage:
    python serve_counts.py                      # http://127.0.0.1:8077
    python serve_counts.py --port 9000
    python serve_counts.py --host 0.0.0.0       # reachable from other hosts
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

SUCCESSFUL = Path("data_cache/swe-gen-oss-successful.jsonl")
TOTAL = Path("data_cache/swe-gen-oss-total.jsonl")
HACKING_RESULTS = Path(
    "data_cache/successful_20260612_200042_bz_codex_hacking_out/"
    "hacking_results.dedup.jsonl"
)
YIELD_PNG = Path("data_cache/yield_over_time.png")
AGE_PNG = Path("data_cache/pr_age_distribution.png")

# extract_successful.py is re-run on a background thread roughly hourly; the
# download button always serves the newest zip it has produced. It needs the
# project venv (for the `harbor` package) and scans the task folders.
SWE_GEN_ROOT = Path("/shared_workspace_mfs/alex/swe-gen-mod/SWE-gen")
EXTRACT_SCRIPT = SWE_GEN_ROOT / "extract_successful.py"
EXTRACT_PYTHON = SWE_GEN_ROOT / ".venv" / "bin" / "python"
TASKS_DIR = SWE_GEN_ROOT / "tasks"
OUTPUT_ZIP_DIR = SWE_GEN_ROOT / "output_zip"
EXTRACT_INTERVAL_SEC = 3600  # re-extract about once an hour

# Only one extraction runs at a time (the scan + zip is slow and heavy).
_extract_lock = threading.Lock()


def run_extract() -> Path:
    """Run extract_successful.py once and return the path to the zip it wrote.

    Raises RuntimeError with the captured stderr/stdout on failure.
    """
    OUTPUT_ZIP_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_zip = OUTPUT_ZIP_DIR / f"successful_{timestamp}.zip"
    proc = subprocess.run(
        [
            str(EXTRACT_PYTHON), str(EXTRACT_SCRIPT),
            "--dir", str(TASKS_DIR),
            "--out", str(out_zip),
        ],
        cwd=str(SWE_GEN_ROOT),
        capture_output=True,
        text=True,
        timeout=EXTRACT_INTERVAL_SEC,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"extract_successful.py exited {proc.returncode}\n"
            f"{proc.stderr.strip() or proc.stdout.strip()}"
        )
    if not out_zip.is_file():
        # No successful tasks -> the script prints "Nothing to archive" and
        # writes no zip. Surface that rather than a confusing missing-file error.
        raise RuntimeError(
            "no zip produced (no successful tasks?).\n"
            f"{proc.stdout.strip()}"
        )
    return out_zip


def latest_zip() -> Path | None:
    """The most recent zip in OUTPUT_ZIP_DIR (by mtime), or None if none exist."""
    try:
        zips = [p for p in OUTPUT_ZIP_DIR.glob("*.zip") if p.is_file()]
    except OSError:
        return None
    return max(zips, key=lambda p: p.stat().st_mtime) if zips else None


def extract_loop() -> None:
    """Background worker: rebuild the successful-tasks zip about once an hour.

    Runs immediately on start, then every EXTRACT_INTERVAL_SEC. Errors are
    logged and the loop keeps going so a single failure doesn't kill the timer.
    """
    while True:
        with _extract_lock:
            try:
                zp = run_extract()
                print(f"[extract] wrote {zp}", flush=True)
            except Exception as exc:
                print(f"[extract] failed: {exc}", flush=True)
        time.sleep(EXTRACT_INTERVAL_SEC)


def count_lines(path: Path) -> int:
    """Number of non-empty lines in a JSONL file (0 if missing)."""
    try:
        with path.open("r", errors="replace") as fh:
            return sum(1 for line in fh if line.strip())
    except OSError:
        return 0


def count_non_hacking(path: Path) -> int:
    """Number of instances in the hacking-results JSONL with no hacking flagged.

    An instance counts as non-hacking when none of its per-LLM results have
    ``is_hacking`` set. Malformed lines are skipped; a missing file yields 0.
    """
    n = 0
    try:
        with path.open("r", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not any(
                    r.get("is_hacking") for r in rec.get("llm_results", [])
                ):
                    n += 1
    except OSError:
        return 0
    return n


def counts() -> dict:
    s = count_lines(SUCCESSFUL)
    t = count_lines(TOTAL)
    nh = count_non_hacking(HACKING_RESULTS)
    out = {
        "successful": s,
        "total": t,
        "pct": round(100 * s / t, 1) if t else 0.0,
        "non_hacking": nh,
        "clean_pct": round(100 * nh / t, 1) if t else 0.0,
        "zip_available": False,
        "zip_name": None,
        "zip_time": None,
    }
    zp = latest_zip()
    if zp is not None:
        out["zip_available"] = True
        out["zip_name"] = zp.name
        out["zip_time"] = datetime.fromtimestamp(
            zp.stat().st_mtime
        ).strftime("%Y-%m-%d %H:%M:%S")
    return out


PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>Task yield</title>
<style>
  body { font-family: system-ui, sans-serif; background:#0f1115; color:#e6e6e6;
         margin:0; padding:2rem 1rem; }
  .wrap { max-width:1100px; margin:0 auto; display:flex; flex-direction:column;
          align-items:center; gap:2rem; }
  .card { background:#1b1f27; padding:2rem 3rem; border-radius:16px;
          box-shadow:0 8px 30px rgba(0,0,0,.4); text-align:center; min-width:320px; }
  h1 { font-size:1rem; font-weight:600; color:#9aa4b2; margin:0 0 1rem; letter-spacing:.04em; }
  .big { font-size:3.5rem; font-weight:700; }
  .frac { font-size:1.6rem; color:#9aa4b2; margin-top:.3rem; }
  .bar { height:14px; background:#2a2f3a; border-radius:8px; overflow:hidden; margin:1.4rem 0 .4rem; }
  .fill { height:100%; background:linear-gradient(90deg,#54A24B,#7bd06f); width:0; transition:width .4s; }
  .sub { margin-top:1.2rem; }
  .sub-label { font-size:.78rem; color:#9aa4b2; letter-spacing:.03em; }
  .sub-label b { color:#cbd5e1; font-weight:600; }
  .bar.sub-bar { height:8px; margin:.5rem 0 .3rem; }
  .fill.sub-fill { background:linear-gradient(90deg,#4c78a8,#7aa6d6); }
  .ts { font-size:.75rem; color:#6b7280; margin-top:1rem; }
  button.extract { margin-top:1.4rem; font:inherit; font-weight:600; cursor:pointer;
                   color:#fff; background:#54A24B; border:none; border-radius:10px;
                   padding:.7rem 1.4rem; transition:background .2s, opacity .2s; }
  button.extract:hover:not(:disabled) { background:#62b557; }
  button.extract:disabled { opacity:.55; cursor:progress; }
  .extract-status { font-size:.8rem; color:#9aa4b2; margin-top:.7rem; min-height:1em; }
  .extract-note { font-size:.72rem; color:#f0b86b; line-height:1.45; margin-top:.9rem;
                  max-width:380px; }
  section { width:100%; text-align:center; }
  section h2 { font-size:1.15rem; font-weight:600; color:#cbd5e1; margin:0 0 .8rem; }
  section img { max-width:100%; height:auto; border-radius:12px; background:#fff;
                box-shadow:0 6px 24px rgba(0,0,0,.4); }
</style></head>
<body><div class="wrap">
  <div class="card">
    <h1>SWE-GEN-OSS &mdash; NON-HACKING / TOTAL</h1>
    <div class="big"><span id="cleanPct">&mdash;</span>%</div>
    <div class="frac"><span id="nonHack">&mdash;</span> / <span id="tot">&mdash;</span></div>
    <div class="bar"><div class="fill" id="cleanFill"></div></div>
    <div class="sub">
      <div class="sub-label">Successful / total:
        <b><span id="succ">&mdash;</span> / <span id="totSub">&mdash;</span></b>
        (<span id="pct">&mdash;</span>%)</div>
      <div class="bar sub-bar"><div class="fill sub-fill" id="fill"></div></div>
    </div>
    <div class="ts">updated <span id="ts">&mdash;</span> &middot; refreshes every 5s</div>
    <button class="extract" id="extractBtn" onclick="downloadZip()" disabled>Download latest successful zip</button>
    <div class="extract-status" id="extractStatus">Checking for latest archive&hellip;</div>
    <div class="extract-note">The archive is rebuilt by a background job about once an hour; the counts above and the graphs below are refreshed by separate jobs on their own hourly schedules. Because they run at different times, the set of tasks in the downloaded zip may not exactly match the numbers shown here. Additionally, included tasks have not yet undergone a second round of verification.</div>
  </div>
  <section>
    <h2>Currently Processed Instances</h2>
    <img id="yieldImg" alt="yield over time" onerror="this.style.display='none'">
  </section>
  <section>
    <h2>Total Instances</h2>
    <img id="ageImg" alt="PR age distribution" onerror="this.style.display='none'">
  </section>
</div>
<script>
async function tick() {
  try {
    const r = await fetch('/api/counts'); const d = await r.json();
    document.getElementById('cleanPct').textContent = d.clean_pct;
    document.getElementById('nonHack').textContent = d.non_hacking.toLocaleString();
    document.getElementById('tot').textContent = d.total.toLocaleString();
    document.getElementById('cleanFill').style.width = d.clean_pct + '%';
    document.getElementById('pct').textContent = d.pct;
    document.getElementById('succ').textContent = d.successful.toLocaleString();
    document.getElementById('totSub').textContent = d.total.toLocaleString();
    document.getElementById('fill').style.width = d.pct + '%';
    document.getElementById('ts').textContent = new Date().toLocaleTimeString();
    const btn = document.getElementById('extractBtn');
    const st = document.getElementById('extractStatus');
    if (d.zip_available) {
      btn.disabled = false;
      st.textContent = 'Latest archive: ' + d.zip_name + ' (built ' + d.zip_time + ')';
    } else {
      btn.disabled = true;
      st.textContent = 'No archive yet — the first one is being generated…';
    }
  } catch (e) {}
}
function refreshImages() {
  const t = Date.now();
  const y = document.getElementById('yieldImg');
  const a = document.getElementById('ageImg');
  y.style.display = ''; y.src = '/yield.png?t=' + t;
  a.style.display = ''; a.src = '/age.png?t=' + t;
}
function downloadZip() {
  // The latest zip is pre-built by the background job; just fetch the file.
  window.location.href = '/download';
}
tick(); setInterval(tick, 5000);
refreshImages(); setInterval(refreshImages, 30000);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, body: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: Path, ctype: str) -> None:
        try:
            data = path.read_bytes()
        except OSError:
            self._send(404, b"not generated yet", "text/plain")
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _send_zip(self, path: Path) -> None:
        """Stream a zip file to the client as an attachment (chunked, no buffering)."""
        try:
            size = path.stat().st_size
            fh = path.open("rb")
        except OSError:
            self._send(404, b"archive unavailable", "text/plain")
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/zip")
        self.send_header("Content-Disposition", f'attachment; filename="{path.name}"')
        self.send_header("Content-Length", str(size))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        with fh:
            shutil.copyfileobj(fh, self.wfile)

    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/api/counts"):
            self._send(200, json.dumps(counts()).encode(), "application/json")
        elif self.path.startswith("/yield.png"):
            self._send_file(YIELD_PNG, "image/png")
        elif self.path.startswith("/age.png"):
            self._send_file(AGE_PNG, "image/png")
        elif self.path.startswith("/download"):
            zp = latest_zip()
            if zp is None:
                self._send(404, b"No archive has been generated yet. "
                           b"Please check back shortly.", "text/plain")
            else:
                self._send_zip(zp)
        elif self.path in ("/", "/index.html"):
            self._send(200, PAGE.encode(), "text/html; charset=utf-8")
        else:
            self._send(404, b"not found", "text/plain")

    def log_message(self, *args) -> None:  # quiet
        pass


def main() -> int:
    ap = argparse.ArgumentParser(description="Serve successful/total task counts.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8077)
    args = ap.parse_args()
    print(f"Serving on http://{args.host}:{args.port}  (Ctrl-C to stop)", flush=True)
    print(f"  successful: {SUCCESSFUL}\n  total:      {TOTAL}", flush=True)
    print(f"  rebuilding {OUTPUT_ZIP_DIR}/successful_*.zip every "
          f"~{EXTRACT_INTERVAL_SEC // 60} min", flush=True)
    threading.Thread(target=extract_loop, daemon=True).start()
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
