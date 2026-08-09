#!/usr/bin/env python3
"""Export every Harbor task whose Push stage succeeded into a single zip.

Task files live in PostgreSQL (``pipeline_task_files``), not on disk, so the
archive is built by streaming rows out of the database. Each task becomes one
directory holding its stored files plus the SWR image reference recorded by the
Push stage, and a top-level ``manifest.json`` indexes the whole set.

Rows are streamed with a server-side cursor and written straight into the zip:
the full export is over a gigabyte, so nothing is accumulated in memory.
"""

from __future__ import annotations

import argparse
import json
import os
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

_ENVIRONMENT_DOCKERFILE = "environment/Dockerfile"
_ENVIRONMENT_DOCKERFILE_SOURCE = "environment/Dockerfile.source"
# The pushed SWR image already contains the built environment, so the exported
# Dockerfile only has to pull it. Dockerfile.source keeps the original
# build-from-source recipe alongside, since the image is the only other record
# of how the environment was produced.
_THIN_DOCKERFILE = 'FROM {image}\nWORKDIR /app/src\nCMD ["sleep", "infinity"]\n'

_PUSHED_TASKS_SQL = """
    SELECT DISTINCT ON (result.task_id, result.task_version)
        result.task_id,
        result.task_version,
        task.repo,
        task.pr,
        result.finished_at,
        result.result
    FROM pipeline_stage_results AS result
    JOIN pipeline_tasks AS task
      ON task.task_id = result.task_id
     AND task.task_version = result.task_version
    WHERE result.stage = 'push'
      AND result.status = 'succeeded'
      AND (%(pushed_through)s::timestamptz IS NULL
           OR result.finished_at <= %(pushed_through)s)
      AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset(%(excluded_tasks)s::jsonb)
               AS excluded(task_id text, task_version integer)
          WHERE excluded.task_id = result.task_id
            AND excluded.task_version = result.task_version
      )
    ORDER BY result.task_id, result.task_version, result.finished_at DESC
"""
_TASK_FILES_SQL = """
    SELECT file.task_id, file.task_version, file.path, file.content, file.mode
    FROM pipeline_task_files AS file
    JOIN (
        SELECT DISTINCT task_id, task_version
        FROM pipeline_stage_results
        WHERE stage = 'push'
          AND status = 'succeeded'
          AND (%(pushed_through)s::timestamptz IS NULL
               OR finished_at <= %(pushed_through)s)
          AND NOT EXISTS (
              SELECT 1
              FROM jsonb_to_recordset(%(excluded_tasks)s::jsonb)
                   AS excluded(task_id text, task_version integer)
              WHERE excluded.task_id = pipeline_stage_results.task_id
                AND excluded.task_version = pipeline_stage_results.task_version
          )
    ) AS pushed
      ON pushed.task_id = file.task_id
     AND pushed.task_version = file.task_version
    ORDER BY file.task_id, file.task_version, file.path
"""


def _connection_string() -> str:
    missing = [
        name
        for name in ("SWEGEN_PG_HOST", "SWEGEN_PG_USER", "SWEGEN_PG_DB", "SWEGEN_PG_PASSWORD")
        if not os.environ.get(name, "").strip()
    ]
    if missing:
        raise SystemExit(f"missing required environment: {', '.join(missing)}")
    return (
        f"host={os.environ['SWEGEN_PG_HOST']} "
        f"port={os.environ.get('SWEGEN_PG_PORT', '5432')} "
        f"user={os.environ['SWEGEN_PG_USER']} "
        f"dbname={os.environ['SWEGEN_PG_DB']} "
        f"password={os.environ['SWEGEN_PG_PASSWORD']}"
    )


def _task_directory(task_id: str, task_version: int) -> str:
    # Task directories sit at the archive root so extraction yields the Harbor
    # tasks directly, with no wrapper directory to strip first.
    return f"{task_id}__v{task_version}"


def _parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"invalid ISO-8601 timestamp: {value!r}") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a UTC offset")
    return parsed.astimezone(UTC)


def _read_manifest(path: Path) -> dict[str, Any]:
    try:
        if zipfile.is_zipfile(path):
            with zipfile.ZipFile(path) as archive:
                payload = archive.read("manifest.json")
        else:
            payload = path.read_bytes()
        manifest = json.loads(payload)
    except (OSError, KeyError, json.JSONDecodeError, zipfile.BadZipFile) as error:
        raise SystemExit(f"cannot read exclusion manifest from {path}: {error}") from error
    if not isinstance(manifest, dict):
        raise SystemExit(f"exclusion manifest must be a JSON object: {path}")
    return manifest


def _load_excluded_tasks(path: Path | None) -> set[tuple[str, int]]:
    if path is None:
        return set()
    manifest = _read_manifest(path)
    raw_tasks = manifest.get("tasks")
    if not isinstance(raw_tasks, list):
        raise SystemExit(f"exclusion manifest has no tasks array: {path}")

    identities: set[tuple[str, int]] = set()
    for index, entry in enumerate(raw_tasks):
        if not isinstance(entry, dict):
            raise SystemExit(f"exclusion manifest task {index} is not an object: {path}")
        task_id = entry.get("task_id")
        task_version = entry.get("task_version")
        if (
            not isinstance(task_id, str)
            or not task_id
            or isinstance(task_version, bool)
            or not isinstance(task_version, int)
            or task_version <= 0
        ):
            raise SystemExit(f"exclusion manifest task {index} has an invalid identity: {path}")
        identity = (task_id, task_version)
        if identity in identities:
            raise SystemExit(f"exclusion manifest contains duplicate task {identity!r}: {path}")
        identities.add(identity)
    return identities


def _query_parameters(
    excluded_tasks: set[tuple[str, int]], pushed_through: datetime | None
) -> dict[str, object]:
    exclusions = [
        {"task_id": task_id, "task_version": task_version}
        for task_id, task_version in sorted(excluded_tasks)
    ]
    return {
        "excluded_tasks": json.dumps(exclusions, separators=(",", ":")),
        "pushed_through": pushed_through,
    }


def export(
    destination: Path,
    *,
    exclude_manifest: Path | None = None,
    pushed_through: datetime | None = None,
    expected_task_count: int | None = None,
) -> dict[str, object]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, object]] = []
    file_count = 0
    byte_count = 0
    excluded_tasks = _load_excluded_tasks(exclude_manifest)
    query_parameters = _query_parameters(excluded_tasks, pushed_through)

    with psycopg.connect(_connection_string(), row_factory=dict_row) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        with connection.cursor(name="pushed_tasks") as cursor:
            tasks = {
                (row["task_id"], row["task_version"]): row
                for row in cursor.execute(_PUSHED_TASKS_SQL, query_parameters)
            }
        if not tasks:
            raise SystemExit("no tasks with a succeeded Push stage were found")
        if expected_task_count is not None and len(tasks) != expected_task_count:
            raise SystemExit(
                f"selected {len(tasks)} tasks, expected {expected_task_count}; archive not written"
            )

        with zipfile.ZipFile(
            destination, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6
        ) as archive:
            # Named cursor => server-side streaming; the file bodies total well
            # over a gigabyte and must not be materialised at once.
            with connection.cursor(name="pushed_task_files") as cursor:
                cursor.itersize = 200
                for row in cursor.execute(_TASK_FILES_SQL, query_parameters):
                    key = (row["task_id"], row["task_version"])
                    directory = _task_directory(*key)
                    path = row["path"]
                    # The stored Dockerfile builds the environment from source.
                    # These images are already in SWR, so it is kept as
                    # Dockerfile.source for reproducibility and the thin
                    # pull-only form takes its place below.
                    if path == _ENVIRONMENT_DOCKERFILE:
                        path = _ENVIRONMENT_DOCKERFILE_SOURCE
                    info = zipfile.ZipInfo(f"{directory}/{path}")
                    # Preserve the stored mode so solve.sh and friends stay
                    # executable after extraction.
                    info.external_attr = (row["mode"] & 0o7777) << 16
                    info.compress_type = zipfile.ZIP_DEFLATED
                    archive.writestr(info, bytes(row["content"]))
                    file_count += 1
                    byte_count += len(row["content"])

            for (task_id, task_version), task in sorted(tasks.items()):
                payload = task["result"] if isinstance(task["result"], dict) else {}
                entry = {
                    "task_id": task_id,
                    "task_version": task_version,
                    "repo": task["repo"],
                    "pr": task["pr"],
                    "directory": _task_directory(task_id, task_version),
                    "pushed_at": task["finished_at"].isoformat() if task["finished_at"] else None,
                    "swr_image": payload.get("remote_tag"),
                    "registry": payload.get("registry"),
                    "suffix": payload.get("suffix"),
                    "remote_buildkit": payload.get("remote_buildkit", False),
                }
                entries.append(entry)
                archive.writestr(
                    f"{entry['directory']}/swr-image.json",
                    json.dumps(entry, indent=2, sort_keys=True) + "\n",
                )
                if entry["swr_image"]:
                    archive.writestr(
                        f"{entry['directory']}/{_ENVIRONMENT_DOCKERFILE}",
                        _THIN_DOCKERFILE.format(image=entry["swr_image"]),
                    )
                    file_count += 1

            manifest = {
                "generated_at": datetime.now(UTC).isoformat(),
                "source": "pipeline_stage_results stage=push status=succeeded",
                "excluded_manifest": str(exclude_manifest) if exclude_manifest else None,
                "excluded_task_count": len(excluded_tasks),
                "pushed_through": pushed_through.isoformat() if pushed_through else None,
                "task_count": len(entries),
                "file_count": file_count,
                "uncompressed_bytes": byte_count,
                "tasks": entries,
            }
            archive.writestr("manifest.json", json.dumps(manifest, indent=2) + "\n")

    return {
        "task_count": len(entries),
        "file_count": file_count,
        "uncompressed_bytes": byte_count,
        "missing_swr_image": sum(1 for entry in entries if not entry["swr_image"]),
        "excluded_task_count": len(excluded_tasks),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--exclude-manifest",
        type=Path,
        help="Exclude task identities from a manifest JSON or a zip containing manifest.json",
    )
    parser.add_argument(
        "--pushed-through",
        type=_parse_timestamp,
        help="Include successful Push results through this inclusive ISO-8601 timestamp",
    )
    parser.add_argument(
        "--expected-task-count",
        type=int,
        help="Abort before writing unless the selected task count matches",
    )
    arguments = parser.parse_args()
    if arguments.expected_task_count is not None and arguments.expected_task_count <= 0:
        parser.error("--expected-task-count must be positive")
    summary = export(
        arguments.output.resolve(),
        exclude_manifest=(
            arguments.exclude_manifest.resolve() if arguments.exclude_manifest else None
        ),
        pushed_through=arguments.pushed_through,
        expected_task_count=arguments.expected_task_count,
    )
    size = arguments.output.resolve().stat().st_size
    print(f"tasks              : {summary['task_count']}")
    print(f"files              : {summary['file_count']}")
    print(f"uncompressed bytes : {summary['uncompressed_bytes']:,}")
    print(f"tasks missing image: {summary['missing_swr_image']}")
    print(f"tasks excluded     : {summary['excluded_task_count']}")
    print(f"archive bytes      : {size:,}")
    print(f"archive            : {arguments.output.resolve()}")


if __name__ == "__main__":
    main()
