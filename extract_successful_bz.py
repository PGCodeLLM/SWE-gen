#!/usr/bin/env python3
"""Zip up the Harbor task folders that passed validation (no modifications).

Scans an output directory of generated Harbor-format task folders (the same
layout `swegen create` writes: each `<task_id>/` has a `task.toml` and an
`environment/` dir) and archives every task whose validation *succeeded* into a
single timestamped zip. Unlike `extract_successful.py`, this variant archives
each task exactly as-is — it does NOT rewrite the Dockerfile base image or swap
in the obs_download.py acquisition step.

A task counts as successful when BOTH hold, using the most recent run of each:
  - NOP    run reward == 0  (the unmodified/buggy code fails the tests)
  - Oracle run reward == 1  (the reference fix passes the tests)

Rewards are read from Harbor's job results under the jobs directory
(`<dir>/../.swegen/harbor-jobs` by default), reusing swegen's own
`parse_harbor_outcome` so schema handling matches the rest of the pipeline.

Usage:
    python extract_successful_bz.py --dir tasks
    python extract_successful_bz.py --dir tasks --jobs-dir .swegen/harbor-jobs --out out.zip
    python extract_successful_bz.py --dir tasks --dry-run -v
"""

from __future__ import annotations

import argparse
import glob as _glob
import sys
import zipfile
from datetime import datetime
from pathlib import Path

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
    """Most-recent ``result.json`` for ``<task_id>-<agent>-N`` runs, by mtime.

    Harbor writes a timestamped subdir inside each ``--jobs-dir`` run, so we
    rglob for the job-level result.json and keep the newest across all attempts.
    """
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


def task_status(jobs_dir: Path, task_id: str) -> tuple[bool, object, object]:
    """Return ``(succeeded, nop_reward, oracle_reward)`` for one task.

    ``nop_reward`` / ``oracle_reward`` are the parsed rewards (0/1/None) of the
    latest run of each agent; ``succeeded`` is ``nop == 0 and oracle == 1``.
    """
    nop_path = _latest_result(jobs_dir, task_id, "nop")
    oracle_path = _latest_result(jobs_dir, task_id, "oracle")
    nop_reward = parse_harbor_outcome(nop_path).reward if nop_path else None
    oracle_reward = parse_harbor_outcome(oracle_path).reward if oracle_path else None
    succeeded = nop_reward == 0 and oracle_reward == 1
    return succeeded, nop_reward, oracle_reward


def resolve_jobs_dir(out_dir: Path, override: Path | None) -> Path:
    """Pick the Harbor jobs directory, mirroring how swegen lays it out.

    Precedence: explicit ``--jobs-dir`` > ``<dir>/../.swegen/harbor-jobs``
    (swegen's default, since the dataset dir's parent holds ``.swegen``) >
    ``<dir>/.swegen/harbor-jobs``.
    """
    if override is not None:
        return override
    sibling = out_dir.parent / ".swegen" / "harbor-jobs"
    if sibling.is_dir():
        return sibling
    nested = out_dir / ".swegen" / "harbor-jobs"
    if nested.is_dir():
        return nested
    return sibling  # report the swegen-default path in the error below


def add_dir_to_zip(zf: zipfile.ZipFile, task_dir: Path, root: Path) -> int:
    """Add ``task_dir`` recursively to ``zf``; arcnames are relative to ``root``.

    The archive therefore contains ``<task_id>/...`` entries. Returns the number
    of files written.
    """
    count = 0
    for item in sorted(task_dir.rglob("*")):
        if item.is_symlink() or not item.is_file():
            continue
        zf.write(item, item.relative_to(root).as_posix())
        count += 1
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Zip all successful (NOP reward=0 AND Oracle reward=1) Harbor task "
            "folders from --dir into a timestamped archive, unmodified."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--dir",
        required=True,
        type=Path,
        help="Output directory containing Harbor-format task folders.",
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
        default=None,
        help="Output zip path (default: ./<timestamp>_bz.zip in the current dir).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List the successful tasks but do not write a zip.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print the NOP/Oracle reward decision for every task.",
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
    if not task_dirs:
        print(f"No Harbor-format task folders found under {out_dir}.")
        return 1

    successful: list[Path] = []
    for task_dir in task_dirs:
        task_id = task_dir.name
        ok, nop_reward, oracle_reward = task_status(jobs_dir, task_id)
        if ok:
            successful.append(task_dir)
        if args.verbose:
            mark = "PASS" if ok else "skip"
            print(
                f"  [{mark}] {task_id}: nop={nop_reward} oracle={oracle_reward}"
            )

    print(
        f"Scanned {len(task_dirs)} task folder(s) in {out_dir}; "
        f"{len(successful)} successful (nop=0 & oracle=1)."
    )

    if not successful:
        print("Nothing to archive.")
        return 0

    if args.dry_run:
        print("Dry run — would archive:")
        for task_dir in successful:
            print(f"  {task_dir.name}")
        return 0

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_zip = args.out if args.out is not None else Path(f"{timestamp}_bz.zip")
    out_zip.parent.mkdir(parents=True, exist_ok=True)

    total_files = 0
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for task_dir in successful:
            n = add_dir_to_zip(zf, task_dir, out_dir)
            total_files += n
            print(f"  + {task_dir.name} ({n} file(s))")

    print(
        f"Wrote {out_zip} — {len(successful)} task(s), {total_files} file(s)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
