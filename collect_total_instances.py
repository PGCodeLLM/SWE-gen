#!/usr/bin/env python3
"""Write a JSONL of every Harbor task, for periodic (hourly) runs.

Scans a directory of Harbor-format task folders (default: ``tasks``) and records
*every* task into a JSONL file, skipping only those with a run currently in
progress. Each line is:

    {"instance_id": "<task folder name>", "path": "<absolute path to the folder>"}

This is the counterpart to ``collect_successful_instances.py``: that script keeps
only the successes (NOP reward=0 AND Oracle reward=1); this one keeps *all*
instances — successes, failures, and never-run/broken ones alike — while skipping
any task that still has a run in progress.

A run is considered "in progress" when its Harbor job directory
(``<task_id>-<agent>-N``) has been created (it has a ``lock.json``) but has not
yet produced a ``result.json``. If any of a task's NOP/Oracle job directories is
in that state, the task is skipped this cycle and will be picked up on a later
run once the run completes.

The output file is written atomically (temp file + rename), so a reader never
sees a partially-written file even while this runs on a schedule.

Usage:
    python collect_total_instances.py
    python collect_total_instances.py --dir tasks --out /path/to/out.jsonl
    python collect_total_instances.py --jobs-dir .swegen/harbor-jobs -v

Run hourly via cron, e.g.:
    9 * * * * cd /shared_workspace_mfs/alex/swe-gen-mod/SWE-gen && \
        .venv/bin/python collect_total_instances.py >> /tmp/collect_total.log 2>&1
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# Default destination for the JSONL of all finished instances.
DEFAULT_OUT = Path("/shared_workspace_mfs/alex/swe-gen-oss-total.jsonl")

# Job directories are named "<task_id>-<agent>-<N>" (e.g. "foo__bar-12-nop-3").
_JOB_DIR_RE = re.compile(r"^(?P<task>.+)-(?P<agent>nop|oracle)-\d+$")

# Make the in-repo `swegen` package importable when run from the repo root.
_SRC = Path(__file__).resolve().parent / "src"
if _SRC.is_dir():
    sys.path.insert(0, str(_SRC))

try:
    from swegen.tools.harbor_runner import parse_harbor_outcome  # noqa: F401
except Exception as exc:  # pragma: no cover - environment/setup issue
    sys.exit(
        "error: could not import swegen.tools.harbor_runner "
        f"({exc}).\nRun this from the repository root with the project's "
        "virtualenv active (it needs the `harbor` package)."
    )


def is_task_dir(path: Path) -> bool:
    """Whether ``path`` looks like a Harbor-format task folder.

    Mirrors Harbor's own minimum (``harbor.models.task.Task.is_valid_dir``):
    a task config plus an environment directory.
    """
    return (path / "task.toml").is_file() and (path / "environment").is_dir()


def index_job_dirs(jobs_dir: Path) -> dict[str, dict[str, list[Path]]]:
    """Index ``<task_id>-<agent>-N`` job dirs in a single scan of ``jobs_dir``.

    Returns ``{task_id: {agent: [job_dir, ...]}}``. Globbing per task over a
    directory holding tens of thousands of jobs is far too slow, so we read the
    directory once and bucket entries by the parsed (task_id, agent).
    """
    index: dict[str, dict[str, list[Path]]] = defaultdict(lambda: defaultdict(list))
    with os.scandir(jobs_dir) as it:
        for entry in it:
            if not entry.is_dir():
                continue
            m = _JOB_DIR_RE.match(entry.name)
            if not m:
                continue
            index[m.group("task")][m.group("agent")].append(Path(entry.path))
    return index


def _job_has_result(job_dir: Path) -> bool:
    """Whether ``job_dir`` contains any ``result.json`` (i.e. the run finished)."""
    return next(job_dir.rglob("result.json"), None) is not None


def task_in_progress(agent_jobs: dict[str, list[Path]]) -> bool:
    """True when any NOP/Oracle job directory has started but not finished.

    A Harbor job writes ``result.json`` only once the run completes, so a job
    directory with no ``result.json`` anywhere underneath it is still running.
    """
    for agent in ("nop", "oracle"):
        for job_dir in agent_jobs.get(agent, ()):
            if not _job_has_result(job_dir):
                return True
    return False


def resolve_jobs_dir(out_dir: Path, override: Path | None) -> Path:
    """Pick the Harbor jobs directory, mirroring how swegen lays it out."""
    if override is not None:
        return override
    sibling = out_dir.parent / ".swegen" / "harbor-jobs"
    if sibling.is_dir():
        return sibling
    nested = out_dir / ".swegen" / "harbor-jobs"
    if nested.is_dir():
        return nested
    return sibling  # report the swegen-default path in the error below


def write_jsonl_atomic(out_path: Path, rows: list[dict]) -> None:
    """Write ``rows`` to ``out_path`` as JSONL atomically (temp file + rename)."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(f"{out_path.name}.tmp.{os.getpid()}")
    with tmp_path.open("w") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, out_path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Collect every Harbor task (success, failure, or broken/never-run; "
            "skipping only in-progress) into a JSONL of {instance_id, path}."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--dir",
        type=Path,
        default=Path("tasks"),
        help="Directory containing Harbor-format task folders.",
    )
    parser.add_argument(
        "--jobs-dir",
        type=Path,
        default=None,
        help="Harbor jobs directory holding run results "
        "(default: <dir>/../.swegen/harbor-jobs).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help="Output JSONL path.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print each instance as it is recorded or skipped.",
    )
    args = parser.parse_args(argv)

    out_dir: Path = args.dir
    if not out_dir.is_dir():
        print(f"error: --dir not found or not a directory: {out_dir}", file=sys.stderr)
        return 2

    jobs_dir = resolve_jobs_dir(out_dir, args.jobs_dir)
    if not jobs_dir.is_dir():
        print(
            f"error: jobs directory not found: {jobs_dir}\n"
            "Pass --jobs-dir to point at the Harbor results directory.",
            file=sys.stderr,
        )
        return 2

    job_index = index_job_dirs(jobs_dir)
    task_dirs = sorted(p for p in out_dir.iterdir() if p.is_dir() and is_task_dir(p))

    rows: list[dict] = []
    skipped = 0
    for task_dir in task_dirs:
        instance_id = task_dir.name
        agent_jobs = job_index.get(instance_id, {})
        if task_in_progress(agent_jobs):
            skipped += 1
            if args.verbose:
                print(f"  [SKIP] {instance_id} (in progress)")
            continue
        row = {"instance_id": instance_id, "path": str(task_dir.resolve())}
        rows.append(row)
        if args.verbose:
            print(f"  [KEEP] {instance_id}")

    write_jsonl_atomic(args.out, rows)

    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(
        f"[{stamp}] {len(rows)}/{len(task_dirs)} instance(s) "
        f"({skipped} in progress) from {out_dir} -> {args.out}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
