#!/usr/bin/env python3
"""make_graph.py — task yield over time.

Pipeline:
  1. Scan the task folders and classify each as successful/unsuccessful using the
     SAME logic as extract_successful.py (latest NOP run reward 0 AND latest
     Oracle run reward 1), reusing swegen's ``parse_harbor_outcome``.
  2. For every task record repo name, instance_id, pull_number and the
     successful flag, and write them to a JSONL.
  3. Fetch the PR's creation date from the GitHub API and append it as a field
     (cached in the JSONL so re-runs only fetch what's missing).
  4. Plot total vs successful PRs bucketed by PR age (0-3, 4-6, 7-12, ... months)
     as a line graph (total line shaded below), with the overall yield
     (passing / total, plus the raw counts) shown beside the graph.

Usage:
    python make_graph.py
    python make_graph.py --dir <tasks> --out-jsonl pr_yield.jsonl --out-graph yield.png
    python make_graph.py --limit 50 --jobs 8           # quick test
"""

from __future__ import annotations

import argparse
import glob as _glob
import json
import re
import sys
import threading
import time
import tomllib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import requests  # noqa: E402

# --- Repo / data locations ---------------------------------------------------
REPO_ROOT = Path("/shared_workspace_mfs/alex/swe-gen-mod/SWE-gen")
DEFAULT_TASKS_DIR = REPO_ROOT / "tasks"
DEFAULT_OUT_JSONL = REPO_ROOT / "pr_yield.jsonl"
DEFAULT_OUT_GRAPH = REPO_ROOT / "yield_over_time.png"
SWEGEN_TOML = REPO_ROOT / "swegen.toml"

# Make swegen importable for the (shared) reward parser.
_SRC = REPO_ROOT / "src"
if _SRC.is_dir():
    sys.path.insert(0, str(_SRC))
try:
    from swegen.tools.harbor_runner import parse_harbor_outcome
except Exception as exc:  # pragma: no cover
    sys.exit(f"error: could not import swegen.tools.harbor_runner ({exc}).")

# Age buckets (in completed months). Last bucket is open-ended.
AGE_BUCKETS: list[tuple[str, int, int | None]] = [
    ("0-3", 0, 3),
    ("4-6", 4, 6),
    ("7-12", 7, 12),
    ("13-24", 13, 24),
    ("25-36", 25, 36),
    ("37+", 37, None),
]


# --- Success detection (mirrors extract_successful.py) -----------------------
def is_task_dir(path: Path) -> bool:
    return (path / "task.toml").is_file() and (path / "environment").is_dir()


def _latest_result(jobs_dir: Path, task_id: str, agent: str) -> Path | None:
    pattern = f"{_glob.escape(task_id)}-{agent}-*"
    best_path: Path | None = None
    best_mtime = -1.0
    for job_dir in jobs_dir.glob(pattern):
        if not job_dir.is_dir():
            continue
        for result_file in job_dir.rglob("result.json"):
            try:
                mtime = result_file.stat().st_mtime
            except OSError:
                continue
            if mtime > best_mtime:
                best_mtime = mtime
                best_path = result_file
    return best_path


def task_succeeded(jobs_dir: Path, task_id: str) -> bool:
    nop_path = _latest_result(jobs_dir, task_id, "nop")
    oracle_path = _latest_result(jobs_dir, task_id, "oracle")
    nop_reward = parse_harbor_outcome(nop_path).reward if nop_path else None
    oracle_reward = parse_harbor_outcome(oracle_path).reward if oracle_path else None
    return nop_reward == 0 and oracle_reward == 1


def resolve_jobs_dir(out_dir: Path, override: Path | None) -> Path:
    if override is not None:
        return override
    sibling = out_dir.parent / ".swegen" / "harbor-jobs"
    if sibling.is_dir():
        return sibling
    nested = out_dir / ".swegen" / "harbor-jobs"
    if nested.is_dir():
        return nested
    return sibling


# --- instance_id -> repo / pr ------------------------------------------------
def parse_instance_id(instance_id: str) -> tuple[str | None, str | None]:
    """``owner__repo-<pr>`` -> ("owner/repo", "<pr>"). (None, None) if unparseable."""
    m = re.match(r"^(?P<slug>.+)-(?P<pr>\d+)$", instance_id)
    if not m:
        return None, None
    slug, pr = m.group("slug"), m.group("pr")
    if "__" not in slug:
        return None, pr
    owner, repo = slug.split("__", 1)
    return f"{owner}/{repo}", pr


# --- GitHub PR date fetching with token rotation -----------------------------
class TokenPool:
    """Round-robin GitHub tokens across threads."""

    def __init__(self, tokens: list[str]):
        self._tokens = tokens or [None]  # [None] => unauthenticated
        self._i = 0
        self._lock = threading.Lock()

    def next(self) -> str | None:
        with self._lock:
            tok = self._tokens[self._i % len(self._tokens)]
            self._i += 1
            return tok


def load_tokens(cli_tokens: list[str] | None) -> list[str]:
    if cli_tokens:
        return cli_tokens
    try:
        with SWEGEN_TOML.open("rb") as fh:
            data = tomllib.load(fh)
        gh = data.get("github", {})
        toks = [t for t in (gh.get("gh_tokens") or []) if isinstance(t, str) and t.strip()]
        if not toks and isinstance(gh.get("token"), str):
            toks = [gh["token"]]
        return toks
    except (OSError, tomllib.TOMLDecodeError):
        return []


def fetch_pr_date(repo: str, pr: str, pool: TokenPool, max_retries: int = 5) -> str | None:
    """Return the PR's ``created_at`` ISO timestamp, or None if unavailable."""
    url = f"https://api.github.com/repos/{repo}/pulls/{pr}"
    for attempt in range(max_retries):
        token = pool.next()
        headers = {"Accept": "application/vnd.github+json"}
        if token:
            headers["Authorization"] = f"token {token}"
        try:
            resp = requests.get(url, headers=headers, timeout=30)
        except requests.RequestException:
            time.sleep(min(30, 2 ** attempt))
            continue
        if resp.status_code == 200:
            return resp.json().get("created_at")
        if resp.status_code == 404:
            return None
        if resp.status_code in (403, 429):
            # Rate limited / forbidden: back off and rotate to the next token.
            time.sleep(min(60, 2 ** attempt + 1))
            continue
        # Transient server error etc.
        time.sleep(min(30, 2 ** attempt))
    return None


# --- Age bucketing -----------------------------------------------------------
def months_old(pr_date_iso: str, now: datetime) -> int | None:
    try:
        d = datetime.fromisoformat(pr_date_iso.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
    months = (now.year - d.year) * 12 + (now.month - d.month)
    if now.day < d.day:
        months -= 1
    return max(0, months)


def bucket_for(months: int) -> str | None:
    for label, lo, hi in AGE_BUCKETS:
        if months >= lo and (hi is None or months <= hi):
            return label
    return None


# --- Main --------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Classify tasks, fetch PR dates, and plot yield over time.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--dir", type=Path, default=DEFAULT_TASKS_DIR,
                        help="Directory of Harbor-format task folders.")
    parser.add_argument("--jobs-dir", type=Path, default=None,
                        help="Harbor jobs dir (default: <dir>/../.swegen/harbor-jobs).")
    parser.add_argument("--out-jsonl", type=Path, default=DEFAULT_OUT_JSONL,
                        help="Output JSONL (also used as the PR-date cache).")
    parser.add_argument("--out-graph", type=Path, default=DEFAULT_OUT_GRAPH,
                        help="Output graph PNG.")
    parser.add_argument("--github-token", action="append", default=None,
                        help="GitHub token (repeatable). Default: swegen.toml gh_tokens.")
    parser.add_argument("--jobs", type=int, default=8,
                        help="Concurrent GitHub API fetches.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only process the first N task folders (testing).")
    parser.add_argument("--no-fetch", action="store_true",
                        help="Skip GitHub fetching; use only cached dates.")
    args = parser.parse_args(argv)

    tasks_dir: Path = args.dir
    if not tasks_dir.is_dir():
        print(f"error: --dir not found: {tasks_dir}", file=sys.stderr)
        return 2
    jobs_dir = resolve_jobs_dir(tasks_dir, args.jobs_dir)
    if not jobs_dir.is_dir():
        print(f"error: jobs dir not found: {jobs_dir} (pass --jobs-dir)", file=sys.stderr)
        return 2

    # 1+2. Classify every task folder.
    task_dirs = sorted(p for p in tasks_dir.iterdir() if p.is_dir() and is_task_dir(p))
    if args.limit is not None:
        task_dirs = task_dirs[: args.limit]
    print(f"Scanning {len(task_dirs)} task folder(s) in {tasks_dir} ...", flush=True)

    # Load cached PR dates from a prior run of this script.
    cache: dict[str, str | None] = {}
    if args.out_jsonl.is_file():
        for line in args.out_jsonl.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "instance_id" in obj and obj.get("pr_date") is not None:
                cache[obj["instance_id"]] = obj["pr_date"]

    records: list[dict] = []
    for td in task_dirs:
        instance_id = td.name
        repo, pr = parse_instance_id(instance_id)
        records.append(
            {
                "repo": repo,
                "instance_id": instance_id,
                "pull_number": pr,
                "successful": task_succeeded(jobs_dir, instance_id),
                "pr_date": cache.get(instance_id),
            }
        )
    n_success = sum(1 for r in records if r["successful"])
    print(f"  successful: {n_success} / {len(records)} "
          f"({100 * n_success / max(1, len(records)):.1f}%)", flush=True)

    # 3. Fetch missing PR dates.
    to_fetch = [
        r for r in records
        if r["pr_date"] is None and r["repo"] and r["pull_number"]
    ]
    if to_fetch and not args.no_fetch:
        pool = TokenPool(load_tokens(args.github_token))
        n_tokens = len(pool._tokens)
        print(f"Fetching {len(to_fetch)} PR date(s) via GitHub API "
              f"({n_tokens} token(s), {args.jobs} workers) ...", flush=True)
        done = 0
        with ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool_ex:
            futs = {
                pool_ex.submit(fetch_pr_date, r["repo"], r["pull_number"], pool): r
                for r in to_fetch
            }
            for fut in as_completed(futs):
                r = futs[fut]
                try:
                    r["pr_date"] = fut.result()
                except Exception:
                    r["pr_date"] = None
                done += 1
                if done % 100 == 0 or done == len(to_fetch):
                    print(f"  fetched {done}/{len(to_fetch)}", flush=True)

    # 2/3. Write the JSONL.
    args.out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with args.out_jsonl.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")
    n_dates = sum(1 for r in records if r["pr_date"])
    print(f"Wrote {len(records)} record(s) to {args.out_jsonl} "
          f"({n_dates} with PR dates).", flush=True)

    # 4. Bucket by age and plot.
    now = datetime.now(timezone.utc)
    labels = [b[0] for b in AGE_BUCKETS]
    totals = {lbl: 0 for lbl in labels}
    succ = {lbl: 0 for lbl in labels}
    undated = 0
    for r in records:
        if not r["pr_date"]:
            undated += 1
            continue
        m = months_old(r["pr_date"], now)
        if m is None:
            undated += 1
            continue
        lbl = bucket_for(m)
        if lbl is None:
            continue
        totals[lbl] += 1
        if r["successful"]:
            succ[lbl] += 1

    total_all = len(records)
    succ_all = n_success
    yield_pct = 100 * succ_all / total_all if total_all else 0.0

    x = list(range(len(labels)))
    total_y = [totals[lbl] for lbl in labels]
    succ_y = [succ[lbl] for lbl in labels]

    fig = plt.figure(figsize=(13, 6))
    gs = fig.add_gridspec(1, 2, width_ratios=[3.2, 1])
    ax = fig.add_subplot(gs[0])
    ax.fill_between(x, total_y, color="#4C78A8", alpha=0.25, zorder=1)
    ax.plot(x, total_y, color="#4C78A8", marker="o", linewidth=2,
            label="Total PRs", zorder=3)
    ax.plot(x, succ_y, color="#54A24B", marker="o", linewidth=2,
            label="Successful PRs", zorder=4)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_xlabel("PR age (months)")
    ax.set_ylabel("PR count")
    ax.set_title("Task yield over time")
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    ax.legend(loc="upper left")
    ax.margins(x=0.02)

    # Stats panel beside the graph.
    ax2 = fig.add_subplot(gs[1])
    ax2.axis("off")
    stats = (
        f"Total yield\n"
        f"{yield_pct:.1f}%\n"
        f"({succ_all} / {total_all})\n\n"
        f"Successful PRs:\n{succ_all}\n\n"
        f"Total PRs:\n{total_all}"
    )
    ax2.text(
        0.5, 0.5, stats, ha="center", va="center", fontsize=14,
        transform=ax2.transAxes,
        bbox=dict(boxstyle="round,pad=0.8", facecolor="#F5F5F5", edgecolor="#888"),
    )
    if undated:
        ax.annotate(f"(+{undated} PRs without a usable date, excluded from buckets)",
                    xy=(0.5, -0.16), xycoords="axes fraction", ha="center",
                    fontsize=8, color="gray")

    fig.tight_layout()
    args.out_graph.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out_graph, dpi=150, bbox_inches="tight")
    print(f"Wrote graph to {args.out_graph}", flush=True)
    print(f"Overall yield: {succ_all}/{total_all} = {yield_pct:.1f}%", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
