#!/usr/bin/env python3
"""Idempotently backfill JSONL ledgers into the swegen Postgres database.

Every worker now writes to Postgres (see ``swegen.ledger_repo``), but the
historical ``.jsonl`` ledgers accumulated under run directories still hold the
only record of past pipeline state. This script imports those files into their
target tables so reads against Postgres see the full history.

Idempotency: each imported row is tagged with its origin
(``source_file`` + ``source_line``) and every table has a partial unique index
on that pair, so re-running the import is a no-op for rows already loaded.

Usage:
    # Import every known ledger file under a run dir
    python backfill_jsonl_to_pg.py --run-dir runs/20260716-sol-max-full-16w

    # Import across many run dirs
    python backfill_jsonl_to_pg.py --root runs --root /data/work/alex/old-runs

    # Import one specific file
    python backfill_jsonl_to_pg.py --file runs/.../postcheck-status.jsonl

    # Dry run (report what would be imported, write nothing)
    python backfill_jsonl_to_pg.py --run-dir runs/... --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

# Make the in-repo `swegen` package importable when run from the repo root.
_SRC = Path(__file__).resolve().parent / "src"
if _SRC.is_dir():
    sys.path.insert(0, str(_SRC))

from swegen import db  # noqa: E402
from swegen.ledger_repo import (  # noqa: E402
    _PUSHED_IMAGE_PREFIXES,
    _TABLE_REGISTRY,
    _resolve,
    backfill_rows,
)

# Snapshot copies of ledgers taken before a maintenance action, e.g.
# orchestrator-progress.before-failure-clear-20260716T123736Z.jsonl. These hold
# superseded state and must NOT be backfilled (latest-wins reads could let a
# stale snapshot win). Matched against the file stem.
_SNAPSHOT_RE = re.compile(r"\.(before|after)-")


def _is_snapshot(path: Path) -> bool:
    return bool(_SNAPSHOT_RE.search(path.stem))


def _resolve_table(path: Path) -> str | None:
    """Return the target table for a ledger file, or None if it isn't a ledger."""
    if _is_snapshot(path):
        return None
    try:
        table, _event = _resolve(path)
    except KeyError:
        # _resolve already accepts the pushed-image prefixes and orchestrator
        # shard suffixes, so this only fires for genuinely unknown files.
        stem = path.stem
        for prefix in _PUSHED_IMAGE_PREFIXES:
            if stem.startswith(prefix):
                return _TABLE_REGISTRY[prefix][0]
        return None
    return table


def _iter_ledger_files(roots: list[Path]) -> list[Path]:
    """Find all ledger files under the given roots (dedup, sorted).

    Uses a single os.walk per root (faster than one rglob glob pass per
    pattern when the tree is large). A file qualifies if its stem matches a
    known ledger and it is not a timestamped snapshot.
    """
    found: set[Path] = set()
    for root in roots:
        if root.is_file():
            if _resolve_table(root) is not None and not _is_snapshot(root):
                found.add(root.resolve())
            continue
        if not root.is_dir():
            continue
        for _dirpath, _dirnames, filenames in os.walk(root):
            for fname in filenames:
                if not fname.endswith(".jsonl"):
                    continue
                p = Path(_dirpath) / fname
                if _resolve_table(p) is None or _is_snapshot(p):
                    continue
                found.add(p.resolve())
    return sorted(found)


def backfill_file(path: Path, *, dry_run: bool) -> tuple[int, int, int]:
    """Backfill a single JSONL file. Returns (imported, skipped, malformed)."""
    table = _resolve_table(path)
    if table is None:
        return (0, 0, 0)
    source_file = str(path)
    malformed = 0
    rows: list[tuple[dict, int]] = []
    with path.open(encoding="utf-8", errors="replace") as fh:
        for line_no, raw in enumerate(fh, start=1):
            stripped = raw.strip()
            if not stripped:
                continue
            try:
                record = json.loads(stripped)
            except json.JSONDecodeError:
                malformed += 1
                continue
            if not isinstance(record, dict):
                malformed += 1
                continue
            rows.append((record, line_no))
    if dry_run:
        return (len(rows), 0, malformed)
    try:
        imported, skipped = backfill_rows(
            table, rows, source_file=source_file
        )
    except Exception as exc:  # pragma: no cover - DB error
        print(f"  ERROR {path}: {exc}", file=sys.stderr)
        return (0, 0, malformed)
    return (imported, skipped, malformed)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--run-dir",
        action="append",
        default=[],
        help="A run directory to scan for ledger files (repeatable).",
    )
    parser.add_argument(
        "--root",
        action="append",
        default=[],
        help="A directory tree to recursively scan for ledgers (repeatable).",
    )
    parser.add_argument(
        "--file",
        action="append",
        default=[],
        help="A specific JSONL ledger file to import (repeatable).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be imported without writing to the DB.",
    )
    args = parser.parse_args(argv)

    roots: list[Path] = []
    for rd in args.run_dir:
        roots.append(Path(rd))
    for r in args.root:
        roots.append(Path(r))
    for f in args.file:
        roots.append(Path(f))

    if not roots:
        parser.error("at least one of --run-dir/--root/--file is required")

    files = _iter_ledger_files(roots)
    if not files:
        print("No ledger files found under the given paths.", file=sys.stderr)
        return 1

    print(f"Found {len(files)} ledger file(s).", file=sys.stderr)
    if not args.dry_run:
        # Touch the pool early so a bad DSN fails fast with a clear message.
        db.get_pool()

    total_imported = total_skipped = total_malformed = 0
    for path in files:
        table = _resolve_table(path)
        imported, skipped, malformed = backfill_file(path, dry_run=args.dry_run)
        total_imported += imported
        total_skipped += skipped
        total_malformed += malformed
        verb = "would import" if args.dry_run else "imported"
        print(
            f"  {path}\n      -> {table}: {verb} {imported}, skipped {skipped}, "
            f"malformed {malformed}"
        )

    label = "DRY RUN " if args.dry_run else ""
    print(
        f"\n{label}Done. imported={total_imported} skipped={total_skipped} "
        f"malformed={total_malformed}",
        file=sys.stderr,
    )
    if not args.dry_run:
        db.close_pool()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
