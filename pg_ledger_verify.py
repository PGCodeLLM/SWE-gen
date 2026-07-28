#!/usr/bin/env python3
"""Verify Postgres ledger parity against the source JSONL files.

For each ledger file, compare the "latest wins" view produced by the Postgres
backend (``LedgerRepo(...).load_latest()``) against the original JSONL reader
(``load_latest_postchecks`` for instance-keyed ledgers, a generic latest-by-line
reader for the rest). Mismatches indicate either a backfill gap or a dedup
ordering bug.

The comparison is on the *keyed view* (one record per instance / task), which is
what the pipeline actually consumes — raw row order is intentionally allowed to
differ (append-don't-upsert, shard reordering).

Usage:
    python pg_ledger_verify.py --run-dir runs/20260716-sol-max-full-16w
    python pg_ledger_verify.py --root runs
    python pg_ledger_verify.py --file runs/.../postcheck-status.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Make the in-repo `swegen` package importable when run from the repo root.
_SRC = Path(__file__).resolve().parent / "src"
if _SRC.is_dir():
    sys.path.insert(0, str(_SRC))

from swegen.ledger_repo import LedgerRepo, _resolve  # noqa: E402


def _jsonl_latest_instance_keyed(path: Path) -> dict[str, dict]:
    """Reference reader for instance-keyed ledgers (postcheck-style dedup)."""
    from slurm_validation_worker import load_latest_postchecks

    return load_latest_postchecks(path)


def _jsonl_latest_task_keyed(path: Path) -> dict[str, dict]:
    """Reference reader for the create-success ledger, keyed by task_id."""
    latest: dict[str, dict] = {}
    sequence: dict[str, tuple] = {}
    if not path.is_file():
        return latest
    with path.open(encoding="utf-8", errors="replace") as fh:
        for line_index, line in enumerate(fh):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            key = record.get("task_id") or record.get("instance")
            if not isinstance(key, str) or not key:
                harbor = record.get("harbor")
                key = Path(harbor).name if isinstance(harbor, str) else None
            if not key:
                continue
            ts = record.get("ts") or record.get("timestamp") or ""
            seq = (ts if isinstance(ts, str) else "", line_index)
            if key not in sequence or seq >= sequence[key]:
                latest[key] = record
                sequence[key] = seq
    return latest


def _jsonl_latest_generic(path: Path, key_field: str) -> dict[str, dict]:
    """Reference reader for simple append ledgers, latest-by-line per key."""
    latest: dict[str, dict] = {}
    if not path.is_file():
        return latest
    with path.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            key = record.get(key_field)
            if not isinstance(key, str) or not key:
                continue
            latest[key] = record  # last wins
    return latest


def _pg_latest(path: Path) -> dict[str, dict]:
    """Latest view from Postgres for the given ledger path."""
    repo = LedgerRepo(path)
    if repo.backend != "postgres":
        raise RuntimeError(
            f"backend is not postgres (SWEGEN_LEDGER_BACKEND={repo.backend!r}); "
            "set the env to postgres to verify the PG path"
        )
    return repo.load_latest()


def _keyed_view(pg: dict[str, dict], path: Path) -> tuple[dict[str, dict], str]:
    """Return (view, key_field) — pg latest is already keyed by instance/task.

    For non-instance-keyed tables load_latest() still returns a dict keyed by the
    row's instance field; we just use whatever keys it produced.
    """
    return pg, "instance"


def _reference_view(path: Path, table: str) -> dict[str, dict]:
    if table in ("postcheck_status", "reward_backfill_status"):
        return _jsonl_latest_instance_keyed(path)
    if table == "create_success":
        return _jsonl_latest_task_keyed(path)
    if table == "pushed_images":
        return _jsonl_latest_generic(path, "instance")
    # blacklist / stage3_reward_guard / orchestrator_* — best-effort by instance.
    return _jsonl_latest_generic(path, "instance")


def _find_files(roots: list[Path]) -> list[Path]:
    from backfill_jsonl_to_pg import _iter_ledger_files

    return _iter_ledger_files(roots)


def _diff(pg: dict[str, dict], ref: dict[str, dict]) -> tuple[set, set, dict]:
    pg_keys = set(pg)
    ref_keys = set(ref)
    only_pg = pg_keys - ref_keys
    only_ref = ref_keys - pg_keys
    differing: dict[str, tuple] = {}
    for key in pg_keys & ref_keys:
        # Normalize: compare the JSON payload of the records (order-insensitive).
        a = json.dumps(pg[key], sort_keys=True, default=str)
        b = json.dumps(ref[key], sort_keys=True, default=str)
        if a != b:
            differing[key] = (pg[key], ref[key])
    return only_pg, only_ref, differing


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run-dir", action="append", default=[])
    parser.add_argument("--root", action="append", default=[])
    parser.add_argument("--file", action="append", default=[])
    parser.add_argument(
        "--show-diffs",
        type=int,
        default=3,
        help="How many per-key diffs to print in detail (0 to suppress).",
    )
    args = parser.parse_args(argv)

    roots = [Path(p) for p in (args.run_dir + args.root + args.file)]
    if not roots:
        parser.error("at least one of --run-dir/--root/--file is required")

    files = _find_files(roots)
    if not files:
        print("No ledger files found.", file=sys.stderr)
        return 1

    exit_code = 0
    for path in files:
        try:
            table, _event = _resolve(path)
        except KeyError:
            continue
        try:
            pg = _pg_latest(path)
        except Exception as exc:
            print(f"  ERROR reading PG for {path}: {exc}", file=sys.stderr)
            exit_code = 1
            continue
        try:
            ref = _reference_view(path, table)
        except Exception as exc:
            print(f"  ERROR reading JSONL for {path}: {exc}", file=sys.stderr)
            exit_code = 1
            continue

        only_pg, only_ref, differing = _diff(pg, ref)
        status = "OK" if not (only_pg or only_ref or differing) else "MISMATCH"
        print(
            f"  {status:8s} {path}\n"
            f"           table={table} pg_keys={len(pg)} jsonl_keys={len(ref)} "
            f"only_pg={len(only_pg)} only_jsonl={len(only_ref)} differing={len(differing)}"
        )
        if status == "MISMATCH":
            exit_code = 1
            shown = 0
            for key in sorted(only_ref):
                if shown >= args.show_diffs:
                    break
                print(f"             only-in-jsonl: {key}")
                shown += 1
            shown = 0
            for key in sorted(only_pg):
                if shown >= args.show_diffs:
                    break
                print(f"             only-in-pg:    {key}")
                shown += 1
            shown = 0
            for key, (a, b) in differing.items():
                if shown >= args.show_diffs:
                    break
                print(f"             differs:       {key}")
                print(f"               pg:    {json.dumps(a, sort_keys=True, default=str)[:200]}")
                print(f"               jsonl: {json.dumps(b, sort_keys=True, default=str)[:200]}")
                shown += 1

    return exit_code


if __name__ == "__main__":
    from swegen import db

    try:
        raise SystemExit(main())
    finally:
        try:
            db.close_pool()
        except Exception:
            pass
