#!/usr/bin/env python3
"""Write a JSONL of every successfully-validated task, for periodic (hourly) runs.

Scans a directory of Harbor-format task folders (default: ``tasks``) and records
each task whose validation succeeded — the latest NOP run scored reward 0 AND
the latest Oracle run scored reward 1 — into a JSONL file. Each line is:

    {"instance_id": "<task folder name>", "path": "<absolute path to the folder>"}

The ``instance_id`` is the task folder name (e.g. ``xtuc__webassemblyjs-968``),
which is the identifier Harbor uses as its ``task_id``.

Success detection mirrors ``extract_successful.py`` and reuses swegen's own
``parse_harbor_outcome`` so reward parsing matches the rest of the pipeline.

The output file is written atomically (temp file + rename), so a reader never
sees a partially-written file even while this runs on a schedule.

Usage:
    python collect_successful_instances.py
    python collect_successful_instances.py --dir tasks --out /path/to/out.jsonl
    python collect_successful_instances.py --jobs-dir .swegen/harbor-jobs -v

Run hourly via cron, e.g.:
    7 * * * * cd /shared_workspace_mfs/alex/swe-gen-mod/SWE-gen && \
        .venv/bin/python collect_successful_instances.py >> /tmp/collect_successful.log 2>&1
"""

from __future__ import annotations

import argparse
import glob as _glob
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Default destination for the JSONL of successful instances.
DEFAULT_OUT = Path("/shared_workspace_mfs/alex/swe-gen-oss-successful.jsonl")

# Make the in-repo `swegen` package importable when run from the repo root.
_SRC = Path(__file__).resolve().parent / "src"
if _SRC.is_dir():
    sys.path.insert(0, str(_SRC))

try:
    from swegen.tools.harbor_runner import parse_harbor_outcome
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


def _latest_result(jobs_dir: Path, task_id: str, agent: str) -> Path | None:
    """Most-recent ``result.json`` for ``<task_id>-<agent>-N`` runs, by mtime."""
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
    """True when the latest NOP reward == 0 AND the latest Oracle reward == 1."""
    nop_path = _latest_result(jobs_dir, task_id, "nop")
    oracle_path = _latest_result(jobs_dir, task_id, "oracle")
    nop_reward = parse_harbor_outcome(nop_path).reward if nop_path else None
    oracle_reward = parse_harbor_outcome(oracle_path).reward if oracle_path else None
    return nop_reward == 0 and oracle_reward == 1


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
            "Collect successful (NOP reward=0 AND Oracle reward=1) tasks into a "
            "JSONL of {instance_id, path}."
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
        help="Print each successful instance as it is found.",
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

    task_dirs = sorted(p for p in out_dir.iterdir() if p.is_dir() and is_task_dir(p))

    rows: list[dict] = []
    for task_dir in task_dirs:
        instance_id = task_dir.name
        if task_succeeded(jobs_dir, instance_id):
            row = {"instance_id": instance_id, "path": str(task_dir.resolve())}
            rows.append(row)
            if args.verbose:
                print(f"  [PASS] {instance_id}")

    write_jsonl_atomic(args.out, rows)

    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(
        f"[{stamp}] {len(rows)}/{len(task_dirs)} successful instance(s) "
        f"from {out_dir} -> {args.out}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
