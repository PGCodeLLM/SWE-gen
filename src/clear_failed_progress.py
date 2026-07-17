#!/usr/bin/env python3
"""Compact SWE-gen JSONL state files to their latest successful instances.

The orchestrator keeps its progress and instance-status files open for append.
This utility therefore rewrites each file *in place* instead of replacing its
inode, allowing a paused writer to resume safely on the compacted file.  The
caller must briefly stop every process that can append to the supplied files.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CompactResult:
    path: Path
    backup: Path | None
    input_lines: int
    latest_instances: int
    retained_successes: int
    dropped_failures: int


def latest_instance_records(path: Path) -> tuple[list[dict[str, Any]], int, int]:
    """Return latest successful records in last-update order plus counts."""
    latest: dict[str, tuple[int, dict[str, Any]]] = {}
    input_lines = 0
    with path.open(encoding="utf-8", errors="replace") as stream:
        for index, line in enumerate(stream):
            input_lines += 1
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            instance = record.get("instance")
            status = record.get("status")
            if isinstance(instance, str) and status in {"success", "failure"}:
                latest[instance] = (index, record)

    retained = sorted(
        ((index, record) for index, record in latest.values() if record.get("status") == "success"),
        key=lambda item: item[0],
    )
    records = [record for _index, record in retained]
    dropped_failures = sum(record.get("status") == "failure" for _index, record in latest.values())
    return records, input_lines, dropped_failures


def backup_path(path: Path, tag: str) -> Path:
    return path.with_name(f"{path.stem}.before-failure-clear-{tag}{path.suffix}")


def rewrite_in_place(path: Path, records: list[dict[str, Any]]) -> None:
    """Preserve the inode used by an already-open O_APPEND writer."""
    with path.open("r+", encoding="utf-8") as stream:
        stream.seek(0)
        for record in records:
            stream.write(json.dumps(record, separators=(",", ":")) + "\n")
        stream.truncate()
        stream.flush()
        os.fsync(stream.fileno())


def compact_file(path: Path, tag: str, *, dry_run: bool = False) -> CompactResult:
    records, input_lines, dropped_failures = latest_instance_records(path)
    backup = backup_path(path, tag)
    if not dry_run:
        if backup.exists():
            raise FileExistsError(f"backup already exists: {backup}")
        shutil.copy2(path, backup)
        rewrite_in_place(path, records)
    return CompactResult(
        path=path,
        backup=None if dry_run else backup,
        input_lines=input_lines,
        latest_instances=len(records) + dropped_failures,
        retained_successes=len(records),
        dropped_failures=dropped_failures,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument(
        "--tag",
        default=datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
        help="Suffix used for immutable backup copies",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    for path in args.paths:
        if not path.is_file():
            raise SystemExit(f"state file not found: {path}")

    results = [compact_file(path, args.tag, dry_run=args.dry_run) for path in args.paths]
    for result in results:
        print(
            json.dumps(
                {
                    "path": str(result.path),
                    "backup": str(result.backup) if result.backup else None,
                    "input_lines": result.input_lines,
                    "latest_instances": result.latest_instances,
                    "retained_successes": result.retained_successes,
                    "dropped_failures": result.dropped_failures,
                    "dry_run": args.dry_run,
                },
                sort_keys=True,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
