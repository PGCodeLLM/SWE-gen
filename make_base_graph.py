#!/usr/bin/env python3
"""make_base_graph.py — total PR age distribution.

A trimmed-down sibling of make_graph.py. Instead of classifying tasks by
success, it reads a flat list of repo/PR pairs (``19576_repo_pr_pairs.jsonl``),
fetches each PR's creation date from the GitHub API and writes it back as a
``pr_date`` field, then plots the total PR count bucketed by PR age (0-3, 4-6,
7-12, ... months). No yield / successful-PR logic.

Requests are sent UNAUTHENTICATED (no GitHub tokens) with exponential backoff,
and each date is flushed to the output JSONL as soon as it arrives — so a
stopped run loses nothing and a re-run resumes from where it left off.

Usage:
    python make_base_graph.py
    python make_base_graph.py --input 19576_repo_pr_pairs.jsonl --limit 50
    python make_base_graph.py --out-graph pr_age_distribution.png
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import requests  # noqa: E402

REPO_ROOT = Path("/shared_workspace_mfs/alex/swe-gen-mod/SWE-gen")
DEFAULT_INPUT = REPO_ROOT / "19576_repo_pr_pairs.jsonl"
DEFAULT_OUT_JSONL = REPO_ROOT / "repo_pr_pairs_dated.jsonl"
DEFAULT_OUT_GRAPH = REPO_ROOT / "pr_age_distribution.png"

# Age buckets (completed months). Last bucket is open-ended.
AGE_BUCKETS: list[tuple[str, int, int | None]] = [
    ("0-3", 0, 3),
    ("4-6", 4, 6),
    ("7-12", 7, 12),
    ("13-24", 13, 24),
    ("25-36", 25, 36),
    ("37+", 37, None),
]


# --- Unauthenticated PR-date fetch with exponential backoff ------------------
# No GitHub tokens are used. The unauthenticated REST limit is ~60 requests/hour
# per IP, so 403/429 responses are expected; we back off exponentially and let a
# re-run (resuming from the cache) pick up whatever is still missing.
BASE_BACKOFF = 5.0   # seconds
MAX_BACKOFF = 1800.0  # cap a single sleep at 30 minutes


def _backoff(attempt: int) -> float:
    return min(MAX_BACKOFF, BASE_BACKOFF * (2 ** attempt))


def fetch_pr_date(
    repo: str, pr: str, token: str | None = None, max_retries: int = 12
) -> str | None:
    """Return the PR's ``created_at`` ISO timestamp, or None if unavailable.

    Sends the request authenticated when ``token`` is given (raising the rate
    limit to ~5000/hr) or unauthenticated otherwise (~60/hr), backing off
    exponentially on rate limits or transient errors and announcing each backoff
    and its duration. Returns None after ``max_retries`` (the caller can fill it
    on a later run via the cache).
    """
    url = f"https://api.github.com/repos/{repo}/pulls/{pr}"
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"token {token}"
    rate_note = "authenticated ~5000/hr" if token else "unauthenticated ~60/hr"
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
        except requests.RequestException as exc:
            wait = _backoff(attempt)
            print(f"  [backoff] {repo}#{pr}: request error ({type(exc).__name__}); "
                  f"retrying in {wait:.0f}s (attempt {attempt + 1}/{max_retries})",
                  flush=True)
            time.sleep(wait)
            continue
        if resp.status_code == 200:
            return resp.json().get("created_at")
        if resp.status_code == 404:
            return None
        if resp.status_code in (403, 429):
            wait = _backoff(attempt)
            # Surface how long GitHub says the limit lasts, when it tells us.
            detail = ""
            retry_after = resp.headers.get("Retry-After")
            reset = resp.headers.get("X-RateLimit-Reset")
            if retry_after:
                detail = f", Retry-After={retry_after}s"
            elif reset:
                try:
                    secs = int(float(reset)) - int(time.time())
                    detail = f", limit resets in ~{max(0, secs)}s"
                except ValueError:
                    pass
            print(f"  [rate-limit] {repo}#{pr}: HTTP {resp.status_code} "
                  f"({rate_note}){detail}; backing off {wait:.0f}s "
                  f"(attempt {attempt + 1}/{max_retries})", flush=True)
            time.sleep(wait)
            continue
        # Other server errors (5xx): exponential backoff and retry.
        wait = _backoff(attempt)
        print(f"  [backoff] {repo}#{pr}: HTTP {resp.status_code}; "
              f"retrying in {wait:.0f}s (attempt {attempt + 1}/{max_retries})",
              flush=True)
        time.sleep(wait)
    print(f"  [give-up] {repo}#{pr}: no date after {max_retries} attempt(s)", flush=True)
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Plot the total PR age distribution from a repo/PR pairs JSONL.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT,
                        help="JSONL of {repo, pull_number, instance_id} pairs.")
    parser.add_argument("--out-jsonl", type=Path, default=DEFAULT_OUT_JSONL,
                        help="Output JSONL (also used as the PR-date cache).")
    parser.add_argument("--out-graph", type=Path, default=DEFAULT_OUT_GRAPH,
                        help="Output graph PNG.")
    parser.add_argument("--github-token", default=None,
                        help="Optional GitHub token. If given, requests are "
                             "authenticated (~5000/hr); otherwise unauthenticated "
                             "(~60/hr). No token is used unless passed here.")
    parser.add_argument("--jobs", type=int, default=1,
                        help="Concurrent GitHub fetches. Keep low when "
                             "unauthenticated (~60 req/hr per IP); a token allows "
                             "more.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only process the first N pairs (testing).")
    parser.add_argument("--no-fetch", action="store_true",
                        help="Skip GitHub fetching; use only cached dates.")
    args = parser.parse_args(argv)

    if not args.input.is_file():
        print(f"error: input not found: {args.input}", file=sys.stderr)
        return 2

    records: list[dict] = []
    for lineno, line in enumerate(args.input.read_text().splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as exc:
            print(f"warning: skipping {args.input}:{lineno}: {exc}", file=sys.stderr)
    if args.limit is not None:
        records = records[: args.limit]
    if not records:
        print("No records to process.")
        return 1
    print(f"Loaded {len(records)} repo/PR pair(s) from {args.input}", flush=True)

    # Cache key: prefer instance_id, else repo#pull_number.
    def key(r: dict) -> str:
        return str(r.get("instance_id") or f"{r.get('repo')}#{r.get('pull_number')}")

    cache: dict[str, str] = {}
    if args.out_jsonl.is_file():
        for line in args.out_jsonl.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("pr_date"):
                cache[key(obj)] = obj["pr_date"]
    # Preserve any pr_date already on a record (from the input file or a prior
    # run's output); fill from the cache only when one is missing. Existing dates
    # are NEVER overwritten.
    for r in records:
        if not r.get("pr_date"):
            r["pr_date"] = cache.get(key(r))  # cached date, or None
    n_known = sum(1 for r in records if r.get("pr_date"))
    if n_known:
        print(f"  {n_known} record(s) already have a PR date "
              f"(kept as-is, not refetched).", flush=True)

    # Atomically rewrite the dated JSONL (called incrementally as dates arrive,
    # so a stopped run never loses progress).
    args.out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    flush_lock = threading.Lock()

    def flush_jsonl() -> None:
        tmp = args.out_jsonl.with_name(args.out_jsonl.name + ".tmp")
        with tmp.open("w") as fh:
            for r in records:
                fh.write(json.dumps(r) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, args.out_jsonl)

    # Seed the output file (carries cached dates + nulls for the rest).
    flush_jsonl()

    # Fetch missing PR dates (exponential backoff; token optional).
    to_fetch = [
        r for r in records
        if not r.get("pr_date") and r.get("repo") and r.get("pull_number")
    ]
    if to_fetch and not args.no_fetch:
        auth = "authenticated" if args.github_token else "unauthenticated"
        print(f"Fetching {len(to_fetch)} PR date(s) from the GitHub API "
              f"({auth}, {args.jobs} worker(s)) ...", flush=True)
        done = 0
        with ThreadPoolExecutor(max_workers=max(1, args.jobs)) as ex:
            futs = {
                ex.submit(fetch_pr_date, str(r["repo"]), str(r["pull_number"]),
                          args.github_token): r
                for r in to_fetch
            }
            for fut in as_completed(futs):
                r = futs[fut]
                try:
                    date = fut.result()
                except Exception:
                    date = None
                # Only write a real date; never overwrite an existing one with None.
                if date:
                    r["pr_date"] = date
                done += 1
                # Save to the JSONL as each one comes in.
                with flush_lock:
                    flush_jsonl()
                if done % 50 == 0 or done == len(to_fetch):
                    print(f"  fetched {done}/{len(to_fetch)}", flush=True)

    flush_jsonl()
    n_dates = sum(1 for r in records if r.get("pr_date"))
    print(f"Wrote {len(records)} record(s) to {args.out_jsonl} "
          f"({n_dates} with PR dates).", flush=True)

    # Bucket by age and plot the total distribution.
    now = datetime.now(timezone.utc)
    labels = [b[0] for b in AGE_BUCKETS]
    totals = {lbl: 0 for lbl in labels}
    undated = 0
    for r in records:
        if not r.get("pr_date"):
            undated += 1
            continue
        m = months_old(r["pr_date"], now)
        if m is None:
            undated += 1
            continue
        lbl = bucket_for(m)
        if lbl is not None:
            totals[lbl] += 1

    total_all = len(records)
    x = list(range(len(labels)))
    total_y = [totals[lbl] for lbl in labels]

    fig = plt.figure(figsize=(13, 6))
    gs = fig.add_gridspec(1, 2, width_ratios=[3.2, 1])
    ax = fig.add_subplot(gs[0])
    ax.fill_between(x, total_y, color="#4C78A8", alpha=0.25, zorder=1)
    ax.plot(x, total_y, color="#4C78A8", marker="o", linewidth=2,
            label="Total PRs", zorder=3)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_xlabel("PR age (months)")
    ax.set_ylabel("PR count")
    ax.set_title("PR age distribution")
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    ax.legend(loc="upper left")
    ax.margins(x=0.02)

    ax2 = fig.add_subplot(gs[1])
    ax2.axis("off")
    stats = f"Total PRs:\n{total_all}"
    if undated:
        stats += f"\n\n({n_dates} dated,\n{undated} undated)"
    ax2.text(
        0.5, 0.5, stats, ha="center", va="center", fontsize=14,
        transform=ax2.transAxes,
        bbox=dict(boxstyle="round,pad=0.8", facecolor="#F5F5F5", edgecolor="#888"),
    )

    fig.tight_layout()
    args.out_graph.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out_graph, dpi=150, bbox_inches="tight")
    print(f"Wrote graph to {args.out_graph}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
