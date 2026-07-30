#!/usr/bin/env python3
"""Continuously fill the distributed generate queue from ``public.pr_tasks``.

This replaces the deprecated batch orchestrator.  It does not run ``swegen
create`` and it has no local worker pool.  Its only responsibilities are to
select eligible PRs, enforce the configured generate-stage backlog limit, and
atomically create ``pipeline_tasks`` plus their first PGMQ messages.
"""

from __future__ import annotations

import argparse
import logging
import re
import signal
import threading
from dataclasses import dataclass
from typing import Protocol

from swegen import db
from swegen.model_settings import AutoqueueSettings, load_autoqueue_settings
from swegen.pipeline.cli import (
    ENQUEUE_ADVISORY_LOCK_KEYS,
    PipelineTaskAlreadyExists,
    enqueue_pipeline_task,
)
from swegen.queueing.pgmq import ConnectionLike, PgmqQueue

LOGGER = logging.getLogger("swegen.autoqueue")

_RELATION_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\Z")


class CursorLike(Protocol):
    rowcount: int

    def fetchone(self) -> object | None: ...

    def fetchall(self) -> list[object]: ...


class AutoqueueConnection(ConnectionLike, Protocol):
    def execute(self, query: str, params: tuple[object, ...] | None = None) -> CursorLike: ...


@dataclass(frozen=True, slots=True)
class AutoqueueResult:
    queue_limit: int
    queued_before: int
    enqueued: tuple[str, ...]

    @property
    def available_before(self) -> int:
        return max(0, self.queue_limit - self.queued_before)


def _quoted_relation(relation: str) -> str:
    if _RELATION_RE.fullmatch(relation) is None:
        raise ValueError("source_table must be a safe schema-qualified relation")
    schema, table = relation.split(".", 1)
    return f'"{schema}"."{table}"'


def _row_value(row: object, index: int, name: str) -> object:
    if isinstance(row, dict):
        return row[name]
    if isinstance(row, (tuple, list)):
        return row[index]
    raise RuntimeError(f"unsupported database row type: {type(row).__name__}")


def _candidate_sql(settings: AutoqueueSettings) -> tuple[str, tuple[object, ...]]:
    relation = _quoted_relation(settings.source_table)
    clauses = [
        "COALESCE(source.swegen_bz_passed, FALSE) = FALSE",
        "COALESCE(source.swegen_retries, 0) < %s",
        "(source.unlock_time IS NULL OR source.unlock_time <= CURRENT_TIMESTAMP)",
        "LOWER(COALESCE(source.pr_category, '')) = ANY(%s)",
        "NOT EXISTS ("
        "SELECT 1 FROM public.pipeline_tasks task "
        "WHERE task.repo = LOWER(source.repo) "
        "AND task.pr = source.pull_number "
        "AND task.task_version = %s)",
    ]
    params: list[object] = [
        settings.max_retries,
        list(settings.pr_categories),
        settings.task_version,
    ]
    if settings.require_obs:
        clauses.append("source.obs_exists = TRUE")
    if settings.exclude_languages:
        clauses.append("NOT (LOWER(COALESCE(source.primary_language, '')) = ANY(%s))")
        params.append(list(settings.exclude_languages))
    sql = (
        "SELECT source.repo, source.pull_number "
        f"FROM {relation} AS source "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY COALESCE(source.swegen_retries, 0), source.repo, source.pull_number DESC "
        "FOR UPDATE OF source SKIP LOCKED LIMIT %s"
    )
    return sql, tuple(params)


def fill_generate_queue(
    connection: AutoqueueConnection,
    settings: AutoqueueSettings,
    *,
    queue: PgmqQueue | None = None,
) -> AutoqueueResult:
    """Fill available generate slots in one advisory-locked database transaction."""

    queue_adapter = queue or PgmqQueue()
    relation = _quoted_relation(settings.source_table)
    candidate_sql, candidate_params = _candidate_sql(settings)
    enqueued: list[str] = []

    with connection.transaction():
        connection.execute(
            "SELECT pg_advisory_xact_lock(%s, %s)",
            ENQUEUE_ADVISORY_LOCK_KEYS,
        )
        queued_row = connection.execute(
            "SELECT count(*) AS queued "
            "FROM public.pipeline_tasks "
            "WHERE state = 'queued' AND current_stage = 'generate'"
        ).fetchone()
        if queued_row is None:
            raise RuntimeError("generate backlog count returned no row")
        queued_before = int(_row_value(queued_row, 0, "queued"))
        capacity = max(0, settings.queue_limit - queued_before)
        if capacity == 0:
            return AutoqueueResult(settings.queue_limit, queued_before, ())

        candidates = connection.execute(
            candidate_sql,
            (*candidate_params, capacity),
        ).fetchall()
        for row in candidates:
            repo = str(_row_value(row, 0, "repo"))
            pr = int(_row_value(row, 1, "pull_number"))
            try:
                result = enqueue_pipeline_task(
                    connection,
                    repo=repo,
                    pr=pr,
                    task_version=settings.task_version,
                    queue=queue_adapter,
                )
            except PipelineTaskAlreadyExists:
                # The shared enqueue lock covers supported callers; retain the
                # uniqueness fallback for pre-existing rows or direct SQL writers.
                continue
            connection.execute(
                f"UPDATE {relation} SET "
                "instance_id = COALESCE(NULLIF(instance_id, ''), %s), "
                "swegen_retries = COALESCE(swegen_retries, 0) + 1 "
                "WHERE repo = %s AND pull_number = %s",
                (result.task.task_id, repo, pr),
            )
            enqueued.append(result.task.task_id)

    return AutoqueueResult(settings.queue_limit, queued_before, tuple(enqueued))


def run_forever(settings: AutoqueueSettings, stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            with db.connection() as connection:
                result = fill_generate_queue(connection, settings)
            LOGGER.info(
                "generate backlog %d/%d; enqueued %d task(s)",
                result.queued_before,
                result.queue_limit,
                len(result.enqueued),
            )
        except Exception:
            LOGGER.exception("autoqueue iteration failed")
        stop_event.wait(settings.poll_seconds)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--once",
        action="store_true",
        help="Fill the queue once and exit instead of polling continuously.",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_autoqueue_settings()

    if args.once:
        with db.connection() as connection:
            result = fill_generate_queue(connection, settings)
        print(
            f"generate backlog {result.queued_before}/{result.queue_limit}; "
            f"enqueued {len(result.enqueued)} task(s)"
        )
        return 0

    stop_event = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_args: stop_event.set())
    signal.signal(signal.SIGINT, lambda *_args: stop_event.set())
    run_forever(settings, stop_event)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
