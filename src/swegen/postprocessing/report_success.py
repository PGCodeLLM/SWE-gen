#!/usr/bin/env python3
"""Report which generated tasks pass validation and zip the successful ones.

A task is considered *successful* when, in its **own** harbor jobs:

* every ``nop`` trial scored reward ``0.0`` (the no-op/buggy code never passes
  the test), and
* at least one ``oracle`` trial scored reward ``1.0`` (the gold patch passes).

Reward data is not written back into the ``tasks/`` directory; it lives only in
``<jobs-dir>/<job>/<timestamp>/result.json`` under
``stats.evals.<agent>__tasks.reward_stats.reward`` (a mapping of reward value ->
list of trial ids like ``owner__repo-pr__<suffix>``).

Two pitfalls this script handles:

* **Batching** -- one job scores many unrelated tasks together, so the job name
  is meaningless. We only read a task's own ``<task>-nop-*`` / ``<task>-oracle-*``
  jobs and only count trials whose task matches.
* **Filler errors** -- a task also appears as a filler trial inside other tasks'
  jobs, where its environment is not built and it logs ``RuntimeError``. Those
  incidental errors are ignored by scoping to the task's own jobs.

Usage::

    python -m swegen.postprocessing.report_success -o successful.zip
    python -m swegen.postprocessing.report_success --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
import zipfile
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Report tasks that pass validation (nop reward=0, oracle reward=1) "
            "and zip the successful task directories."
        ),
    )
    parser.add_argument(
        "-o",
        "--output",
        default="successful_tasks.zip",
        help="Output .zip path for the successful task directories "
        "(default: successful_tasks.zip).",
    )
    parser.add_argument(
        "--tasks-dir",
        default="tasks",
        help="Directory containing generated task folders (default: tasks).",
    )
    parser.add_argument(
        "--jobs-dir",
        default=".swegen/harbor-jobs",
        help="Directory containing harbor job results "
        "(default: .swegen/harbor-jobs).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print the success/failure report; do not write the zip.",
    )
    return parser.parse_args()


def _trial_task(trial_id: str) -> str:
    """Strip the trailing ``__<random suffix>`` from a harbor trial id.

    Task names themselves contain ``__`` (``owner__repo-pr``), so we only drop
    the last segment, which is the per-trial random suffix.
    """
    return trial_id.rsplit("__", 1)[0]


def _rewards_from_own_jobs(
    task: str, agent: str, jobs_dir: Path
) -> tuple[list[float], int]:
    """Collect rewards and error count for ``task`` from its own ``agent`` jobs.

    Only ``<task>-<agent>-*`` job directories are inspected, and only trials
    whose task matches ``task`` are counted.
    """
    rewards: list[float] = []
    errors = 0
    for result_file in jobs_dir.glob(f"{task}-{agent}-*/*/result.json"):
        try:
            payload = json.loads(result_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        evals = payload.get("stats", {}).get("evals", {})
        for eval_name, eval_data in evals.items():
            if agent not in eval_name:
                continue
            reward_map = eval_data.get("reward_stats", {}).get("reward", {})
            for value, trials in reward_map.items():
                for trial in trials:
                    if _trial_task(trial) == task:
                        rewards.append(float(value))
            for trials in eval_data.get("exception_stats", {}).values():
                for trial in trials:
                    if _trial_task(trial) == task:
                        errors += 1
    return rewards, errors


def _is_successful(nop: list[float], oracle: list[float]) -> bool:
    nop_ok = bool(nop) and all(value == 0.0 for value in nop)
    oracle_ok = 1.0 in oracle
    return nop_ok and oracle_ok


def _zip_tasks(tasks: list[str], tasks_dir: Path, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for task in tasks:
            task_path = tasks_dir / task
            for file_path in sorted(task_path.rglob("*")):
                if file_path.is_file() or file_path.is_symlink():
                    zf.write(file_path, arcname=str(file_path.relative_to(tasks_dir)))


def main() -> int:
    args = _parse_args()
    tasks_dir = Path(args.tasks_dir)
    jobs_dir = Path(args.jobs_dir)

    if not tasks_dir.is_dir():
        raise FileNotFoundError(f"tasks directory not found: {tasks_dir}")
    if not jobs_dir.is_dir():
        raise FileNotFoundError(f"jobs directory not found: {jobs_dir}")

    successful: list[str] = []
    for task in sorted(p.name for p in tasks_dir.iterdir() if p.is_dir()):
        nop, nop_err = _rewards_from_own_jobs(task, "nop", jobs_dir)
        oracle, oracle_err = _rewards_from_own_jobs(task, "oracle", jobs_dir)
        ok = _is_successful(nop, oracle)
        if ok:
            successful.append(task)
        status = "PASS" if ok else "fail"
        nop_str = sorted(set(nop)) or "-"
        oracle_str = sorted(set(oracle)) or "-"
        print(
            f"[{status}] {task:<48} "
            f"nop={nop_str}/err{nop_err}  oracle={oracle_str}/err{oracle_err}"
        )

    total = sum(1 for p in tasks_dir.iterdir() if p.is_dir())
    print(f"\n{len(successful)}/{total} tasks successful "
          f"(nop reward=0 always, oracle reward=1 at least once)")

    if args.dry_run:
        print("Dry run: no zip written.")
        return 0

    if not successful:
        print("No successful tasks to zip; skipping zip creation.")
        return 0

    output = Path(args.output)
    _zip_tasks(successful, tasks_dir, output)
    print(f"Wrote {len(successful)} task(s) to {output}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001 - surface a clean CLI error
        print(f"report_success.py failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
