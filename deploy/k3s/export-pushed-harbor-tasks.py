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
    ORDER BY result.task_id, result.task_version, result.finished_at DESC
"""
_TASK_FILES_SQL = """
    SELECT file.task_id, file.task_version, file.path, file.content, file.mode
    FROM pipeline_task_files AS file
    JOIN (
        SELECT DISTINCT task_id, task_version
        FROM pipeline_stage_results
        WHERE stage = 'push' AND status = 'succeeded'
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


def export(destination: Path) -> dict[str, object]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, object]] = []
    file_count = 0
    byte_count = 0

    with psycopg.connect(_connection_string(), row_factory=dict_row) as connection:
        with connection.cursor(name="pushed_tasks") as cursor:
            tasks = {
                (row["task_id"], row["task_version"]): row
                for row in cursor.execute(_PUSHED_TASKS_SQL)
            }
        if not tasks:
            raise SystemExit("no tasks with a succeeded Push stage were found")

        with zipfile.ZipFile(
            destination, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6
        ) as archive:
            # Named cursor => server-side streaming; the file bodies total well
            # over a gigabyte and must not be materialised at once.
            with connection.cursor(name="pushed_task_files") as cursor:
                cursor.itersize = 200
                for row in cursor.execute(_TASK_FILES_SQL):
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
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()
    summary = export(arguments.output.resolve())
    size = arguments.output.resolve().stat().st_size
    print(f"tasks              : {summary['task_count']}")
    print(f"files              : {summary['file_count']}")
    print(f"uncompressed bytes : {summary['uncompressed_bytes']:,}")
    print(f"tasks missing image: {summary['missing_swr_image']}")
    print(f"archive bytes      : {size:,}")
    print(f"archive            : {arguments.output.resolve()}")


if __name__ == "__main__":
    main()
