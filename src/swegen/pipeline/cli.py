"""Minimal operator CLI for enqueueing and inspecting distributed tasks."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
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
from swegen.tools.buildkit_intermediates import (
    BuildkitIntermediateError,
    claim_intermediate,
    complete_intermediate,
    fail_intermediate,
    inspect_manifest_digest,
    list_intermediates,
    touch_intermediate,
)
from swegen.tools.remote_buildkit import DEFAULT_ORPHAN_MIN_AGE_SECONDS, sweep_orphaned_builds

_DEFAULT_NAMESPACE = "swegen-pipeline"
_SWEEP_PREVIEW_LIMIT = 20
_KUBECTL_TIMEOUT_SECONDS = 60

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
            WHEN 'repair' THEN 3
            WHEN 'reward' THEN 4
            WHEN 'reward_repair' THEN 5
            WHEN 'push' THEN 6
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
            WHEN 'repair' THEN 3
            WHEN 'reward' THEN 4
            WHEN 'reward_repair' THEN 5
            WHEN 'push' THEN 6
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


def _live_worker_pod_names(namespace: str) -> list[str]:
    """Return every Pod name currently present in ``namespace``.

    The sweep treats absence from this list as proof a worker is gone, so this
    must fail loudly rather than return a partial list. Every Pod is returned
    regardless of phase: a Pending or Terminating worker still owns its rows.
    """

    executable = shutil.which("kubectl") or shutil.which("k3s")
    if executable is None:
        raise ValueError("neither kubectl nor k3s is on PATH")
    command = [executable, "get", "pods", "-n", namespace, "-o", "json"]
    if executable.endswith("k3s"):
        command.insert(1, "kubectl")
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=_KUBECTL_TIMEOUT_SECONDS,
    )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise ValueError(f"kubectl returned unparsable JSON: {error}") from error
    items = payload.get("items")
    if not isinstance(items, list):
        raise ValueError("kubectl response has no 'items' list")
    names = [
        item["metadata"]["name"]
        for item in items
        if isinstance(item, dict)
        and isinstance(item.get("metadata"), dict)
        and isinstance(item["metadata"].get("name"), str)
    ]
    if len(names) != len(items):
        raise ValueError("kubectl returned Pod entries without a usable name")
    return names


app = typer.Typer(
    no_args_is_help=True,
    add_completion=False,
    help="Operate the PostgreSQL/PGMQ SWE-gen pipeline.",
)
buildkit_intermediate_app = typer.Typer(
    no_args_is_help=True,
    help="List, claim, and register repository dependency intermediates.",
)
app.add_typer(buildkit_intermediate_app, name="buildkit-intermediate")


def _json_object(raw_value: str | None) -> dict[str, object]:
    if raw_value is None or not raw_value.strip():
        return {}
    try:
        value = json.loads(raw_value)
    except json.JSONDecodeError as error:
        raise ValueError(f"metadata must be valid JSON: {error}") from error
    if not isinstance(value, dict):
        raise ValueError("metadata must be a JSON object")
    return value


def _json_echo(value: object) -> None:
    typer.echo(json.dumps(value, default=str, sort_keys=True, separators=(",", ":")))


@buildkit_intermediate_app.command("list")
def buildkit_intermediate_list(
    repo: str = typer.Option(..., "--repo", help="GitHub repository in OWNER/REPO form"),
    dependency_key: str | None = typer.Option(
        None, "--dependency-key", help="Optional lock/dependency SHA-256"
    ),
    include_all: bool = typer.Option(
        False, "--all", help="Include building, failed, and retired records"
    ),
) -> None:
    """List reusable dependency images before attempting another cold build."""

    try:
        with db.connection() as connection:
            rows = list_intermediates(
                connection,
                repo=repo,
                dependency_key=dependency_key,
                ready_only=not include_all,
            )
    except ValueError as error:
        typer.echo(f"error: {error}", err=True)
        raise typer.Exit(2) from error
    _json_echo(rows)


@buildkit_intermediate_app.command("claim")
def buildkit_intermediate_claim(
    repo: str = typer.Option(..., "--repo"),
    dependency_key: str = typer.Option(..., "--dependency-key"),
    build_key: str = typer.Option(..., "--build-key"),
    cold_build_seconds: float = typer.Option(..., "--cold-build-seconds"),
    claim_owner: str | None = typer.Option(None, "--claim-owner"),
    commit_sha: str | None = typer.Option(None, "--commit-sha"),
    lockfile_path: str | None = typer.Option(None, "--lockfile-path"),
    lockfile_sha256: str | None = typer.Option(None, "--lockfile-sha256"),
    dockerfile_sha256: str | None = typer.Option(None, "--dockerfile-sha256"),
    source_task_id: str | None = typer.Option(None, "--source-task-id"),
    source_task_version: int | None = typer.Option(None, "--source-task-version"),
    metadata_json: str | None = typer.Option(None, "--metadata-json"),
) -> None:
    """Atomically reserve one dependency/build key, suppressing duplicate builds."""

    owner = claim_owner or os.environ.get("POD_NAME") or os.environ.get("HOSTNAME") or "operator"
    try:
        metadata = _json_object(metadata_json)
        with db.connection() as connection:
            row = claim_intermediate(
                connection,
                repo=repo,
                dependency_key=dependency_key,
                build_key=build_key,
                claim_owner=owner,
                cold_build_seconds=cold_build_seconds,
                commit_sha=commit_sha,
                lockfile_path=lockfile_path,
                lockfile_sha256=lockfile_sha256,
                dockerfile_sha256=dockerfile_sha256,
                source_task_id=source_task_id,
                source_task_version=source_task_version,
                metadata=metadata,
            )
    except (BuildkitIntermediateError, ValueError) as error:
        typer.echo(f"error: {error}", err=True)
        raise typer.Exit(2) from error
    _json_echo(row)


@buildkit_intermediate_app.command("complete")
def buildkit_intermediate_complete(
    claim_token: str = typer.Option(..., "--claim-token"),
    image_ref: str = typer.Option(..., "--image-ref"),
    build_seconds: float = typer.Option(..., "--build-seconds"),
    cold_build_seconds: float = typer.Option(..., "--cold-build-seconds"),
    image_digest: str | None = typer.Option(
        None,
        "--image-digest",
        help="Expected digest; omit to use the verified registry manifest digest",
    ),
    metadata_json: str | None = typer.Option(None, "--metadata-json"),
) -> None:
    """Verify the pushed manifest and make the claimed intermediate reusable."""

    try:
        metadata = _json_object(metadata_json)
        verified_digest = inspect_manifest_digest(image_ref)
        if image_digest is not None and image_digest.strip().lower() != verified_digest:
            raise BuildkitIntermediateError(
                f"registry manifest digest {verified_digest} does not match {image_digest}"
            )
        with db.connection() as connection:
            row = complete_intermediate(
                connection,
                claim_token=claim_token,
                image_ref=image_ref,
                image_digest=verified_digest,
                build_seconds=build_seconds,
                cold_build_seconds=cold_build_seconds,
                metadata=metadata,
            )
    except (BuildkitIntermediateError, ValueError, OSError, subprocess.SubprocessError) as error:
        typer.echo(f"error: {error}", err=True)
        raise typer.Exit(2) from error
    _json_echo(row)


@buildkit_intermediate_app.command("fail")
def buildkit_intermediate_fail(
    claim_token: str = typer.Option(..., "--claim-token"),
    error: str = typer.Option(..., "--error"),
) -> None:
    """Release a failed claim so another Repair worker may retry it later."""

    try:
        with db.connection() as connection:
            row = fail_intermediate(connection, claim_token=claim_token, error=error)
    except (BuildkitIntermediateError, ValueError) as caught:
        typer.echo(f"error: {caught}", err=True)
        raise typer.Exit(2) from caught
    _json_echo(row)


@buildkit_intermediate_app.command("use")
def buildkit_intermediate_use(
    intermediate_id: int = typer.Option(..., "--id"),
) -> None:
    """Record that a Repair worker selected a ready intermediate."""

    try:
        with db.connection() as connection:
            row = touch_intermediate(connection, intermediate_id=intermediate_id)
    except (BuildkitIntermediateError, ValueError) as error:
        typer.echo(f"error: {error}", err=True)
        raise typer.Exit(2) from error
    _json_echo(row)


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


@app.command("sweep-remote-builds")
def sweep_remote_builds(
    namespace: str = typer.Option(
        _DEFAULT_NAMESPACE, "--namespace", help="Namespace holding the worker Pods"
    ),
    min_age_seconds: float = typer.Option(
        DEFAULT_ORPHAN_MIN_AGE_SECONDS,
        "--min-age-seconds",
        help="Only sweep rows untouched for at least this long",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Report what would be swept without writing"
    ),
) -> None:
    """Mark remote builds whose submitting worker Pod no longer exists as orphaned."""

    try:
        live_worker_ids = _live_worker_pod_names(namespace)
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        typer.echo(f"error: could not list Pods in {namespace}: {error}", err=True)
        raise typer.Exit(1) from error

    try:
        with db.connection() as connection:
            swept = sweep_orphaned_builds(
                connection,
                live_worker_ids,
                min_age_seconds=min_age_seconds,
            )
            if dry_run:
                connection.rollback()
    except ValueError as error:
        typer.echo(f"error: {error}", err=True)
        raise typer.Exit(2) from error

    verb = "would sweep" if dry_run else "swept"
    typer.echo(f"{verb} {len(swept)} orphaned remote build(s); {len(live_worker_ids)} live Pods")
    for row in swept[:_SWEEP_PREVIEW_LIMIT]:
        typer.echo(f"  {row['request_id']} was={row['status']} worker={row['worker_id']}")
    if len(swept) > _SWEEP_PREVIEW_LIMIT:
        typer.echo(f"  ... and {len(swept) - _SWEEP_PREVIEW_LIMIT} more")


if __name__ == "__main__":  # pragma: no cover
    app()
