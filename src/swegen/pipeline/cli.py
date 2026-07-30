"""Minimal operator CLI for enqueueing and inspecting distributed tasks."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid4

import typer

from swegen import db
from swegen.pipeline.models import PipelineTask
from swegen.queueing.models import PipelineStage, QueueMessage, QueueMetrics, QueueName
from swegen.queueing.pgmq import ConnectionLike, PgmqQueue

ENQUEUE_ADVISORY_LOCK_KEYS = (0x53574547, 2)  # "SWEG", enqueue generation 2

_INSERT_TASK_SQL = """
    INSERT INTO pipeline_tasks (
        task_id, task_version, repo, pr, trace_id, state, current_stage,
        created_at, updated_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
    RETURNING task_id, task_version
"""
_TASKS_SQL = """
    SELECT task_id, task_version, repo, pr, trace_id, state, current_stage
    FROM pipeline_tasks
    ORDER BY task_id, task_version
"""
_TASKS_FILTERED_SQL = """
    SELECT task_id, task_version, repo, pr, trace_id, state, current_stage
    FROM pipeline_tasks
    WHERE task_id = %s
    ORDER BY task_id, task_version
"""
_STAGE_RESULTS_SQL = """
    SELECT task_id, task_version, stage, attempt, status, worker_id, node_name,
           started_at, finished_at
    FROM pipeline_stage_results
    ORDER BY task_id, task_version,
        CASE stage
            WHEN 'generate' THEN 1
            WHEN 'validate' THEN 2
            WHEN 'reward' THEN 3
            WHEN 'push' THEN 4
        END,
        attempt
"""
_STAGE_RESULTS_FILTERED_SQL = """
    SELECT task_id, task_version, stage, attempt, status, worker_id, node_name,
           started_at, finished_at
    FROM pipeline_stage_results
    WHERE task_id = %s
    ORDER BY task_id, task_version,
        CASE stage
            WHEN 'generate' THEN 1
            WHEN 'validate' THEN 2
            WHEN 'reward' THEN 3
            WHEN 'push' THEN 4
        END,
        attempt
"""
_FILE_COUNTS_SQL = """
    SELECT task_id, task_version, count(*) AS file_count
    FROM pipeline_task_files
    GROUP BY task_id, task_version
    ORDER BY task_id, task_version
"""
_FILE_COUNTS_FILTERED_SQL = """
    SELECT task_id, task_version, count(*) AS file_count
    FROM pipeline_task_files
    WHERE task_id = %s
    GROUP BY task_id, task_version
    ORDER BY task_id, task_version
"""


class StatusCursorLike(Protocol):
    def fetchall(self) -> list[object]: ...


class StatusConnectionLike(ConnectionLike, Protocol):
    def execute(
        self,
        query: str,
        params: Sequence[object] | None = None,
    ) -> StatusCursorLike: ...


class PipelineEnqueueError(RuntimeError):
    """Raised when enqueue cannot prove the durable task/message identity."""


class PipelineTaskAlreadyExists(PipelineEnqueueError):
    """Raised when a task ID or repository/PR/version is already present."""


@dataclass(frozen=True, slots=True)
class EnqueueResult:
    """The task and first queue delivery created by one atomic enqueue."""

    task: PipelineTask
    message: QueueMessage
    pgmq_msg_id: int


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _row_value(row: object, index: int, name: str) -> object:
    if isinstance(row, Mapping):
        try:
            return row[name]
        except KeyError as error:
            raise PipelineEnqueueError(f"database row is missing {name!r}") from error
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        try:
            return row[index]
        except IndexError as error:
            raise PipelineEnqueueError(f"database row is missing column {index}") from error
    raise PipelineEnqueueError(f"unsupported database row type: {type(row).__name__}")


def enqueue_pipeline_task(
    connection: ConnectionLike,
    *,
    repo: str,
    pr: int,
    task_version: int = 1,
    queue: PgmqQueue | None = None,
    uuid_factory: Callable[[], UUID] = uuid4,
    now_factory: Callable[[], datetime] = _utc_now,
) -> EnqueueResult:
    """Insert one queued task and its first PGMQ delivery atomically."""

    trace_id = uuid_factory()
    canonical_repo = repo.lower()
    task = PipelineTask(
        task_id=f"{canonical_repo.replace('/', '__')}-{pr}",
        task_version=task_version,
        repo=canonical_repo,
        pr=pr,
        trace_id=trace_id,
    )
    enqueued_at = now_factory()
    message = QueueMessage(
        event_id=uuid_factory(),
        task_id=task.task_id,
        task_version=task.task_version,
        stage=PipelineStage.GENERATE,
        attempt=1,
        trace_id=task.trace_id,
        enqueued_at=enqueued_at,
    )
    queue_adapter = queue or PgmqQueue()

    with connection.transaction():
        connection.execute(
            "SELECT pg_advisory_xact_lock(%s, %s)",
            ENQUEUE_ADVISORY_LOCK_KEYS,
        )
        inserted = connection.execute(
            _INSERT_TASK_SQL,
            (
                task.task_id,
                task.task_version,
                task.repo,
                task.pr,
                task.trace_id,
                task.state.value,
                task.current_stage.value,
                enqueued_at,
                enqueued_at,
            ),
        ).fetchone()
        if inserted is None:
            raise PipelineTaskAlreadyExists(
                f"pipeline task {task.task_id} v{task.task_version} already exists"
            )

        inserted_identity = (
            _row_value(inserted, 0, "task_id"),
            _row_value(inserted, 1, "task_version"),
        )
        expected_identity = (task.task_id, task.task_version)
        if inserted_identity != expected_identity:
            raise PipelineEnqueueError(
                "pipeline task insert returned an unexpected task identity: "
                f"expected {expected_identity!r}, got {inserted_identity!r}"
            )

        pgmq_msg_id = queue_adapter.send(connection, message)

    return EnqueueResult(task=task, message=message, pgmq_msg_id=pgmq_msg_id)


def _fetch_rows(
    connection: StatusConnectionLike,
    unfiltered_sql: str,
    filtered_sql: str,
    task_id: str | None,
) -> list[object]:
    if task_id is None:
        return connection.execute(unfiltered_sql).fetchall()
    return connection.execute(filtered_sql, (task_id,)).fetchall()


def _status_value(row: object, index: int, name: str) -> object:
    try:
        return _row_value(row, index, name)
    except PipelineEnqueueError as error:
        raise RuntimeError(str(error)) from error


def _display(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _render_task(row: object) -> str:
    return (
        f"{_display(_status_value(row, 0, 'task_id'))} "
        f"v{_display(_status_value(row, 1, 'task_version'))} "
        f"repo={_display(_status_value(row, 2, 'repo'))} "
        f"pr={_display(_status_value(row, 3, 'pr'))} "
        f"state={_display(_status_value(row, 5, 'state'))} "
        f"stage={_display(_status_value(row, 6, 'current_stage'))} "
        f"trace={_display(_status_value(row, 4, 'trace_id'))}"
    )


def _render_stage_result(row: object) -> str:
    return (
        f"{_display(_status_value(row, 0, 'task_id'))} "
        f"v{_display(_status_value(row, 1, 'task_version'))} "
        f"stage={_display(_status_value(row, 2, 'stage'))} "
        f"attempt={_display(_status_value(row, 3, 'attempt'))} "
        f"status={_display(_status_value(row, 4, 'status'))} "
        f"worker={_display(_status_value(row, 5, 'worker_id'))} "
        f"node={_display(_status_value(row, 6, 'node_name'))} "
        f"started={_display(_status_value(row, 7, 'started_at'))} "
        f"finished={_display(_status_value(row, 8, 'finished_at'))}"
    )


def _render_file_count(row: object) -> str:
    return (
        f"{_display(_status_value(row, 0, 'task_id'))} "
        f"v{_display(_status_value(row, 1, 'task_version'))} "
        f"files={_display(_status_value(row, 2, 'file_count'))}"
    )


def _render_queue(metrics: QueueMetrics) -> str:
    return (
        f"{metrics.queue.value} length={metrics.queue_length} "
        f"visible={metrics.visible_length} total={metrics.total_messages} "
        f"newest_age_s={_display(metrics.newest_message_age_seconds)} "
        f"oldest_age_s={_display(metrics.oldest_message_age_seconds)} "
        f"scraped={metrics.scraped_at.isoformat()}"
    )


def render_pipeline_status(
    connection: StatusConnectionLike,
    *,
    task_id: str | None = None,
    queue: PgmqQueue | None = None,
) -> str:
    """Return deterministic ledger rows and metrics for all fixed queues."""

    task_rows = _fetch_rows(connection, _TASKS_SQL, _TASKS_FILTERED_SQL, task_id)
    stage_rows = _fetch_rows(
        connection,
        _STAGE_RESULTS_SQL,
        _STAGE_RESULTS_FILTERED_SQL,
        task_id,
    )
    file_rows = _fetch_rows(
        connection,
        _FILE_COUNTS_SQL,
        _FILE_COUNTS_FILTERED_SQL,
        task_id,
    )
    queue_adapter = queue or PgmqQueue()
    queue_metrics = [queue_adapter.metrics(connection, queue_name) for queue_name in QueueName]

    lines = ["TASKS"]
    lines.extend((_render_task(row) for row in task_rows) if task_rows else ("(no tasks)",))
    lines.append("STAGE RESULTS")
    lines.extend(
        (_render_stage_result(row) for row in stage_rows) if stage_rows else ("(no stage results)",)
    )
    lines.append("TASK FILES")
    lines.extend(
        (_render_file_count(row) for row in file_rows) if file_rows else ("(no stored files)",)
    )
    lines.append("QUEUES")
    lines.extend(_render_queue(metrics) for metrics in queue_metrics)
    return "\n".join(lines)


app = typer.Typer(
    no_args_is_help=True,
    add_completion=False,
    help="Operate the PostgreSQL/PGMQ SWE-gen pipeline.",
)


@app.command()
def enqueue(
    repo: str = typer.Option(..., "--repo", help="GitHub repository in OWNER/REPO form"),
    pr: int = typer.Option(..., "--pr", help="Merged pull request number"),
    task_version: int = typer.Option(1, "--task-version", help="Task version"),
) -> None:
    """Atomically create a task and enqueue its generate stage."""

    try:
        with db.connection() as connection:
            result = enqueue_pipeline_task(
                connection,
                repo=repo,
                pr=pr,
                task_version=task_version,
            )
    except PipelineTaskAlreadyExists as error:
        typer.echo(f"error: {error}", err=True)
        raise typer.Exit(1) from error
    except ValueError as error:
        typer.echo(f"error: {error}", err=True)
        raise typer.Exit(2) from error

    typer.echo(
        f"enqueued {result.task.task_id} v{result.task.task_version} "
        f"trace={result.task.trace_id} queue={QueueName.GENERATE.value} "
        f"msg_id={result.pgmq_msg_id}"
    )


@app.command()
def status(
    task_id: str | None = typer.Option(None, "--task-id", help="Filter by canonical task ID"),
) -> None:
    """Print task state, stage outcomes, file counts, and queue metrics."""

    with db.connection() as connection:
        report = render_pipeline_status(connection, task_id=task_id)
    typer.echo(report)


if __name__ == "__main__":  # pragma: no cover
    app()
