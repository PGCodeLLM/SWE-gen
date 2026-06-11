#!/usr/bin/env python3
"""Run ``swegen analyze`` over every task folder in a directory.

``swegen analyze`` works on a single task directory at a time. This wrapper
points it at a directory of Harbor-format task folders (each ``<task_id>/`` with
a ``task.toml`` and an ``environment/`` dir), runs ``swegen analyze`` on each,
and collects everything under an output directory (default: ``analyze_out``):

    analyze_out/
      jobs/                  # --jobs-dir for swegen analyze (all task artifacts)
      logs/<task_id>.log     # captured stdout+stderr of each analyze run
      summary.jsonl          # {"task_id", "returncode", "log"} per task

If the path you pass is itself a single task folder (it has ``task.toml`` +
``environment/``), only that one task is analyzed.

Any options this script does not recognise are forwarded verbatim to
``swegen analyze`` (so you can pass ``-k``, ``--env``, ``--model``, etc.):

    python src/analyse_all.py tasks
    python src/analyse_all.py tasks --out analyze_out
    python src/analyse_all.py tasks --max-parallel 2 -- -k 5 --env docker -v
    python src/analyse_all.py tasks/jni-rs__jni-rs-398   # one task

The trailing ``--`` is optional but makes the forwarded args unambiguous.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

# This script lives in <repo>/src, so the repo root is its parent's parent.
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SWEGEN = REPO_ROOT / ".venv" / "bin" / "swegen"


def is_task_dir(path: Path) -> bool:
    """Whether ``path`` looks like a Harbor-format task folder.

    Mirrors Harbor's own minimum: a task config plus an environment directory.
    """
    return (path / "task.toml").is_file() and (path / "environment").is_dir()


def find_task_dirs(path: Path) -> list[Path]:
    """Task folders to analyze: ``path`` itself if it is one, else its children."""
    if is_task_dir(path):
        return [path]
    return sorted(p for p in path.iterdir() if p.is_dir() and is_task_dir(p))


def analyze_one(
    swegen: str, task_dir: Path, jobs_dir: Path, logs_dir: Path, extra: list[str]
) -> dict:
    """Run ``swegen analyze`` for one task, capturing output to a per-task log."""
    task_id = task_dir.name
    log_path = logs_dir / f"{task_id}.log"
    cmd = [
        swegen, "analyze", str(task_dir.resolve()),
        "--jobs-dir", str(jobs_dir.resolve()),
        *extra,
    ]
    # Force unbuffered child stdout: swegen uses a Rich console, which switches
    # to block-buffering when stdout is a file (not a TTY). Without this the log
    # only updates when the buffer fills or the process exits, so a long step
    # (e.g. baseline validation) looks like it "hung" mid-line.
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    with log_path.open("w") as log:
        log.write(f"$ {' '.join(cmd)}\n\n")
        log.flush()
        proc = subprocess.run(
            cmd, cwd=str(REPO_ROOT), stdout=log, stderr=subprocess.STDOUT, env=env
        )
    return {"task_id": task_id, "returncode": proc.returncode, "log": str(log_path)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run `swegen analyze` on every task folder in a directory; collect "
            "artifacts and logs under an output directory."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog="Unrecognised options are forwarded to `swegen analyze`.",
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Directory of Harbor-format task folders (or a single task folder).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("analyze_out"),
        help="Output directory for jobs/, logs/ and summary.jsonl.",
    )
    parser.add_argument(
        "--swegen",
        default=None,
        help="Path to the swegen executable "
        "(default: <repo>/.venv/bin/swegen, else `swegen` on PATH).",
    )
    parser.add_argument(
        "--max-parallel",
        type=int,
        default=1,
        help="How many tasks to analyze concurrently. Each analyze already runs "
        "several trials, so keep this low to avoid overloading Docker.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only analyze the first N task folders (testing).",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Keep going if a task's analyze fails (default: stop on first "
        "failure when running sequentially).",
    )
    args, extra = parser.parse_known_args(argv)
    # Drop a leading bare "--" separator if argparse handed it to us.
    if extra and extra[0] == "--":
        extra = extra[1:]

    path: Path = args.path
    if not path.is_dir():
        print(f"error: path not found or not a directory: {path}", file=sys.stderr)
        return 2

    swegen = args.swegen or (
        str(DEFAULT_SWEGEN) if DEFAULT_SWEGEN.exists() else "swegen"
    )

    task_dirs = find_task_dirs(path)
    if args.limit is not None:
        task_dirs = task_dirs[: args.limit]
    if not task_dirs:
        print(f"No Harbor-format task folders found under {path}.")
        return 1

    out_dir: Path = args.out
    jobs_dir = out_dir / "jobs"
    logs_dir = out_dir / "logs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    print(
        f"Analyzing {len(task_dirs)} task(s) from {path} with "
        f"max-parallel={args.max_parallel} -> {out_dir}",
        flush=True,
    )

    results: list[dict] = []
    failed = 0

    def record(res: dict) -> None:
        nonlocal failed
        results.append(res)
        ok = res["returncode"] == 0
        if not ok:
            failed += 1
        mark = "ok" if ok else f"FAIL(rc={res['returncode']})"
        print(f"  [{len(results)}/{len(task_dirs)}] {res['task_id']}: {mark} "
              f"-> {res['log']}", flush=True)

    if args.max_parallel <= 1:
        for task_dir in task_dirs:
            res = analyze_one(swegen, task_dir, jobs_dir, logs_dir, extra)
            record(res)
            if res["returncode"] != 0 and not args.continue_on_error:
                print(f"Stopping: {res['task_id']} failed "
                      "(pass --continue-on-error to keep going).", file=sys.stderr)
                break
    else:
        with ThreadPoolExecutor(max_workers=args.max_parallel) as pool:
            futs = {
                pool.submit(analyze_one, swegen, td, jobs_dir, logs_dir, extra): td
                for td in task_dirs
            }
            for fut in as_completed(futs):
                record(fut.result())

    # Write the run summary atomically.
    summary_path = out_dir / "summary.jsonl"
    tmp = summary_path.with_suffix(".jsonl.tmp")
    with tmp.open("w") as fh:
        for res in results:
            fh.write(json.dumps(res) + "\n")
    tmp.replace(summary_path)

    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(
        f"[{stamp}] analyzed {len(results)}/{len(task_dirs)} task(s); "
        f"{failed} failed. Summary: {summary_path}",
        flush=True,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
