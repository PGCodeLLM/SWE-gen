#!/usr/bin/env python3
"""Generate deterministic, whole-repository orchestrator shards.

Repositories are assigned with longest-processing-time (LPT) scheduling: the
largest repository groups are assigned first to the shard with the fewest PRs.
All PRs for a repository therefore stay in exactly one shard, preventing two
orchestrators from touching the same shared repository cache concurrently.

The default layout creates the six four-worker r3 groups used by the current
SG/HK deployment. Custom layouts support later revisions and the DE route.
The manifest contains only non-secret launch metadata.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_SHARD_NAMES = (
    "r3-sg-a",
    "r3-hk-a",
    "r3-sg-b",
    "r3-hk-b",
    "r3-sg-c",
    "r3-hk-c",
)
DEFAULT_RUN_NAME = "20260716-sol-max-full-16w"
SHARD_NAME_RE = re.compile(r"^r(?P<revision>[0-9]+)-(?P<route>sg|hk|de)-(?P<suffix>[a-z0-9-]+)$")
ROUTE_ENV_FILES = {"sg": ".env", "hk": ".env_hk", "de": ".env_de"}


@dataclass(frozen=True)
class Record:
    repo: str
    pull_number: str
    raw_line: str
    source_line: int

    @property
    def pair(self) -> tuple[str, str]:
        return self.repo, self.pull_number


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_records(path: Path) -> list[Record]:
    """Parse JSONL while preserving every non-blank source line verbatim."""
    records: list[Record] = []
    seen_pairs: dict[tuple[str, str], int] = {}
    with path.open(encoding="utf-8") as fh:
        for line_number, raw in enumerate(fh, 1):
            line = raw.rstrip("\r\n")
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"{path}:{line_number}: invalid JSON: {error}") from error
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{line_number}: expected a JSON object")
            repo = str(value.get("repo", "")).strip()
            pull_number = str(value.get("pull_number", "")).strip()
            if not repo or not pull_number:
                raise ValueError(f"{path}:{line_number}: repo and pull_number must be non-empty")
            pair = (repo, pull_number)
            previous_line = seen_pairs.get(pair)
            if previous_line is not None:
                raise ValueError(
                    f"{path}:{line_number}: duplicate {repo}#{pull_number}; "
                    f"first seen on line {previous_line}"
                )
            seen_pairs[pair] = line_number
            records.append(Record(repo, pull_number, line, line_number))
    if not records:
        raise ValueError(f"{path}: no records found")
    return records


def lpt_partition(records: Sequence[Record], shard_count: int) -> list[list[Record]]:
    """Partition records by whole repository using deterministic LPT."""
    if shard_count < 1:
        raise ValueError("shard_count must be positive")

    groups: dict[str, list[Record]] = {}
    for record in records:
        groups.setdefault(record.repo, []).append(record)

    shards: list[list[Record]] = [[] for _ in range(shard_count)]
    loads = [0] * shard_count
    for repo in sorted(groups, key=lambda item: (-len(groups[item]), item)):
        target = min(range(shard_count), key=lambda index: (loads[index], index))
        shards[target].extend(groups[repo])
        loads[target] += len(groups[repo])
    return shards


def validate_partition(
    source_records: Sequence[Record], shards: Sequence[Sequence[Record]]
) -> None:
    """Independently verify exact pair coverage and unique repository ownership."""
    source_pairs = {record.pair for record in source_records}
    output_pairs: set[tuple[str, str]] = set()
    repo_owners: dict[str, int] = {}

    for shard_index, records in enumerate(shards):
        for record in records:
            if record.pair in output_pairs:
                raise ValueError(f"duplicate output pair: {record.repo}#{record.pull_number}")
            output_pairs.add(record.pair)
            owner = repo_owners.setdefault(record.repo, shard_index)
            if owner != shard_index:
                raise ValueError(
                    f"repository {record.repo!r} appears in shards {owner} and {shard_index}"
                )

    missing = source_pairs - output_pairs
    unexpected = output_pairs - source_pairs
    if missing or unexpected:
        raise ValueError(
            f"partition coverage mismatch: missing={len(missing)}, unexpected={len(unexpected)}"
        )


def _runtime_layout(name: str, run_name: str, workers: int) -> dict[str, Any]:
    match = SHARD_NAME_RE.fullmatch(name)
    if match is None:
        raise ValueError(f"invalid shard name {name!r}; expected r<revision>-(sg|hk|de)-<id>")
    revision = f"r{match.group('revision')}"
    route = match.group("route")
    suffix = match.group("suffix")
    group = f"{route}-{suffix}"
    run_dir = Path("runs") / run_name
    return {
        "route": route.upper(),
        "proxy_env_file": ROUTE_ENV_FILES[route],
        "workers": workers,
        "session": f"swegen-{group}-{revision}",
        "log_dir": str(run_dir / f"orchestrator-logs-{group}-{revision}-{workers}w"),
        "launch_log": str(run_dir / f"orchestrator-{group}-{revision}-{workers}w-launch.log"),
        "progress_jsonl": str(run_dir / f"orchestrator-progress-{group}-{revision}.jsonl"),
        "instance_status_jsonl": str(
            run_dir / f"orchestrator-instance-status-{group}-{revision}.jsonl"
        ),
    }


def generate_shards(
    source_path: Path,
    output_dir: Path,
    *,
    names: Sequence[str] = DEFAULT_SHARD_NAMES,
    manifest_name: str = "r3-shards-manifest.json",
    run_name: str = DEFAULT_RUN_NAME,
    workers: int = 4,
) -> dict[str, Any]:
    """Generate shard JSONL files and return/write their deterministic manifest."""
    if len(set(names)) != len(names):
        raise ValueError("shard names must be unique")
    if workers < 1:
        raise ValueError("workers must be positive")
    layouts = [_runtime_layout(name, run_name, workers) for name in names]

    records = read_records(source_path)
    shards = lpt_partition(records, len(names))
    validate_partition(records, shards)
    output_dir.mkdir(parents=True, exist_ok=True)

    shard_records: list[dict[str, Any]] = []
    for name, layout, shard in zip(names, layouts, shards, strict=True):
        path = output_dir / f"{name}.jsonl"
        content = "".join(f"{record.raw_line}\n" for record in shard).encode()
        path.write_bytes(content)
        repos = {record.repo for record in shard}
        shard_records.append(
            {
                "name": name,
                "path": str(path),
                "entries": len(shard),
                "repositories": len(repos),
                "sha256": sha256_bytes(content),
                **layout,
            }
        )

    loads = [len(shard) for shard in shards]
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "algorithm": "whole-repository-lpt-v1",
        "source": str(source_path),
        "source_sha256": sha256_bytes(source_path.read_bytes()),
        "run_name": run_name,
        "totals": {
            "entries": len(records),
            "repositories": len({record.repo for record in records}),
            "unique_repo_pr_pairs": len({record.pair for record in records}),
        },
        "balance": {
            "minimum_entries": min(loads),
            "maximum_entries": max(loads),
            "entry_spread": max(loads) - min(loads),
        },
        "shards": shard_records,
    }
    manifest_path = output_dir / manifest_name
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="Source PR-task JSONL")
    parser.add_argument("output_dir", type=Path, help="Destination shard directory")
    parser.add_argument(
        "--names",
        nargs="+",
        default=list(DEFAULT_SHARD_NAMES),
        help="Ordered shard names (default: alternating SG/HK r3 groups)",
    )
    parser.add_argument("--manifest-name", default="r3-shards-manifest.json")
    parser.add_argument("--run-name", default=DEFAULT_RUN_NAME)
    parser.add_argument("--workers", type=int, default=4)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = generate_shards(
        args.source,
        args.output_dir,
        names=args.names,
        manifest_name=args.manifest_name,
        run_name=args.run_name,
        workers=args.workers,
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
