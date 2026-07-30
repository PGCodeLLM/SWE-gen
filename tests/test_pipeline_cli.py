import json
from collections import deque
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from uuid import UUID

import pytest
from typer.testing import CliRunner

from swegen.pipeline import cli
from swegen.pipeline.cli import (
    PipelineEnqueueError,
    PipelineTaskAlreadyExists,
    enqueue_pipeline_task,
    render_pipeline_status,
)
from swegen.queueing.models import QueueName

TRACE_ID = UUID("11111111-1111-4111-8111-111111111111")
EVENT_ID = UUID("22222222-2222-4222-8222-222222222222")
ENQUEUED_AT = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
SCRAPED_AT = datetime(2026, 7, 29, 12, 5, tzinfo=UTC)


def normalize_sql(sql: str) -> str:
    return " ".join(sql.split())


class FakeCursor:
    def __init__(self, rows: Sequence[object]) -> None:
        self.rows = list(rows)

    def fetchone(self) -> object | None:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[object]:
        return self.rows.copy()


class RecordingConnection:
    def __init__(self, *results: Sequence[object]) -> None:
        self.results = deque(results)
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.events: list[str] = []

    def execute(self, query: str, params: Sequence[object] | None = None) -> FakeCursor:
        normalized_query = normalize_sql(query)
        parameters = tuple(params or ())
        self.calls.append((normalized_query, parameters))
        if normalized_query.startswith("SELECT pg_advisory_xact_lock"):
            self.events.append("enqueue-lock")
            return FakeCursor(())
        if normalized_query.startswith("INSERT INTO pipeline_tasks"):
            self.events.append("insert-task")
        elif "pgmq.send" in normalized_query:
            self.events.append("send")
        if not self.results:
            raise AssertionError(f"No result configured for SQL: {normalized_query}")
        return FakeCursor(self.results.popleft())

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.events.append("transaction-enter")
        try:
            yield
        except BaseException:
            self.events.append("transaction-rollback")
            raise
        else:
            self.events.append("transaction-exit")


class EchoingEnqueueConnection(RecordingConnection):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, query: str, params: Sequence[object] | None = None) -> FakeCursor:
        normalized_query = normalize_sql(query)
        parameters = tuple(params or ())
        self.calls.append((normalized_query, parameters))
        if normalized_query.startswith("SELECT pg_advisory_xact_lock"):
            self.events.append("enqueue-lock")
            return FakeCursor(())
        if normalized_query.startswith("INSERT INTO pipeline_tasks"):
            self.events.append("insert-task")
            return FakeCursor(({"task_id": parameters[0], "task_version": parameters[1]},))
        if "pgmq.send" in normalized_query:
            self.events.append("send")
            return FakeCursor(({"send": 71},))
        raise AssertionError(f"Unexpected SQL: {normalized_query}")


def uuid_factory() -> Iterator[UUID]:
    yield TRACE_ID
    yield EVENT_ID


def metric_row(queue: QueueName, *, length: int) -> tuple[object, ...]:
    return (
        queue.value,
        length,
        2 if length else None,
        5 if length else None,
        length + 10,
        SCRAPED_AT,
        length,
    )


def test_enqueue_inserts_task_and_sends_generate_message_in_one_transaction() -> None:
    connection = RecordingConnection(
        ({"task_id": "ticketmaster__aurora-13", "task_version": 1},),
        ({"send": 71},),
    )
    generated_uuids = uuid_factory()

    result = enqueue_pipeline_task(
        connection,
        repo="ticketmaster/aurora",
        pr=13,
        uuid_factory=lambda: next(generated_uuids),
        now_factory=lambda: ENQUEUED_AT,
    )

    assert result.task.task_id == "ticketmaster__aurora-13"
    assert result.task.task_version == 1
    assert result.task.trace_id == TRACE_ID
    assert result.message.event_id == EVENT_ID
    assert result.message.trace_id == TRACE_ID
    assert result.message.stage.value == "generate"
    assert result.message.attempt == 1
    assert result.message.enqueued_at == ENQUEUED_AT
    assert result.pgmq_msg_id == 71
    assert connection.events == [
        "transaction-enter",
        "enqueue-lock",
        "insert-task",
        "send",
        "transaction-exit",
    ]

    lock_sql, lock_params = connection.calls[0]
    assert "pg_advisory_xact_lock" in lock_sql
    assert lock_params == cli.ENQUEUE_ADVISORY_LOCK_KEYS

    insert_sql, insert_params = connection.calls[1]
    assert "ON CONFLICT DO NOTHING" in insert_sql
    assert "RETURNING task_id, task_version" in insert_sql
    assert insert_params == (
        "ticketmaster__aurora-13",
        1,
        "ticketmaster/aurora",
        13,
        TRACE_ID,
        "queued",
        "generate",
        ENQUEUED_AT,
        ENQUEUED_AT,
    )

    send_sql, send_params = connection.calls[2]
    assert "pgmq.send" in send_sql
    assert send_params[0] == QueueName.GENERATE.value
    assert send_params[2] == 0
    payload = json.loads(str(send_params[1]))
    assert payload == {
        "schema_version": 1,
        "event_id": str(EVENT_ID),
        "task_id": "ticketmaster__aurora-13",
        "task_version": 1,
        "stage": "generate",
        "attempt": 1,
        "trace_id": str(TRACE_ID),
        "enqueued_at": ENQUEUED_AT.isoformat().replace("+00:00", "Z"),
    }


def test_enqueue_canonicalizes_mixed_case_repository_identity() -> None:
    connection = EchoingEnqueueConnection()
    generated_uuids = uuid_factory()

    result = enqueue_pipeline_task(
        connection,
        repo="TicketMaster/Aurora",
        pr=13,
        uuid_factory=lambda: next(generated_uuids),
        now_factory=lambda: ENQUEUED_AT,
    )

    assert result.task.repo == "ticketmaster/aurora"
    assert result.task.task_id == "ticketmaster__aurora-13"
    assert connection.calls[1][1][2] == "ticketmaster/aurora"
    payload = json.loads(str(connection.calls[2][1][1]))
    assert payload["task_id"] == "ticketmaster__aurora-13"


def test_enqueue_duplicate_rolls_back_without_sending() -> None:
    connection = RecordingConnection(())
    generated_uuids = uuid_factory()

    with pytest.raises(PipelineTaskAlreadyExists, match="ticketmaster__aurora-13 v1"):
        enqueue_pipeline_task(
            connection,
            repo="ticketmaster/aurora",
            pr=13,
            uuid_factory=lambda: next(generated_uuids),
            now_factory=lambda: ENQUEUED_AT,
        )

    assert len(connection.calls) == 2
    assert connection.events == [
        "transaction-enter",
        "enqueue-lock",
        "insert-task",
        "transaction-rollback",
    ]


def test_enqueue_rejects_an_unexpected_insert_identity_without_sending() -> None:
    connection = RecordingConnection(
        ({"task_id": "other__repo-13", "task_version": 1},),
    )
    generated_uuids = uuid_factory()

    with pytest.raises(PipelineEnqueueError, match="unexpected task identity"):
        enqueue_pipeline_task(
            connection,
            repo="ticketmaster/aurora",
            pr=13,
            uuid_factory=lambda: next(generated_uuids),
            now_factory=lambda: ENQUEUED_AT,
        )

    assert len(connection.calls) == 2
    assert "send" not in connection.events
    assert connection.events[-1] == "transaction-rollback"


def test_enqueue_uses_pipeline_task_model_for_repo_validation() -> None:
    connection = RecordingConnection()

    with pytest.raises(ValueError, match="OWNER/REPO"):
        enqueue_pipeline_task(connection, repo="not/a/repo", pr=13)

    assert connection.calls == []
    assert connection.events == []


def test_status_filters_all_ledger_queries_and_reports_fixed_queues() -> None:
    connection = RecordingConnection(
        (
            {
                "task_id": "ticketmaster__aurora-13",
                "task_version": 1,
                "repo": "ticketmaster/aurora",
                "pr": 13,
                "trace_id": TRACE_ID,
                "state": "running",
                "current_stage": "validate",
            },
        ),
        (
            {
                "task_id": "ticketmaster__aurora-13",
                "task_version": 1,
                "stage": "generate",
                "attempt": 1,
                "status": "succeeded",
                "worker_id": "generate-0",
                "node_name": "node-generate",
                "started_at": ENQUEUED_AT,
                "finished_at": datetime(2026, 7, 29, 12, 3, tzinfo=UTC),
            },
        ),
        (
            {
                "task_id": "ticketmaster__aurora-13",
                "task_version": 1,
                "file_count": 7,
            },
        ),
        *((metric_row(queue, length=index),) for index, queue in enumerate(QueueName)),
    )

    output = render_pipeline_status(connection, task_id="ticketmaster__aurora-13")

    assert output == "\n".join(
        [
            "TASKS",
            (
                "ticketmaster__aurora-13 v1 repo=ticketmaster/aurora pr=13 "
                f"state=running stage=validate trace={TRACE_ID}"
            ),
            "STAGE RESULTS",
            (
                "ticketmaster__aurora-13 v1 stage=generate attempt=1 status=succeeded "
                "worker=generate-0 node=node-generate "
                "started=2026-07-29T12:00:00+00:00 finished=2026-07-29T12:03:00+00:00"
            ),
            "TASK FILES",
            "ticketmaster__aurora-13 v1 files=7",
            "QUEUES",
            (
                "swegen_generate length=0 visible=0 total=10 "
                "newest_age_s=- oldest_age_s=- scraped=2026-07-29T12:05:00+00:00"
            ),
            (
                "swegen_validate length=1 visible=1 total=11 "
                "newest_age_s=2 oldest_age_s=5 scraped=2026-07-29T12:05:00+00:00"
            ),
            (
                "swegen_reward length=2 visible=2 total=12 "
                "newest_age_s=2 oldest_age_s=5 scraped=2026-07-29T12:05:00+00:00"
            ),
            (
                "swegen_push length=3 visible=3 total=13 "
                "newest_age_s=2 oldest_age_s=5 scraped=2026-07-29T12:05:00+00:00"
            ),
            (
                "swegen_dead length=4 visible=4 total=14 "
                "newest_age_s=2 oldest_age_s=5 scraped=2026-07-29T12:05:00+00:00"
            ),
        ]
    )

    for query, params in connection.calls[:3]:
        assert "WHERE task_id = %s" in query
        assert params == ("ticketmaster__aurora-13",)
    assert [params for _, params in connection.calls[3:]] == [(queue.value,) for queue in QueueName]


def test_status_prints_explicit_empty_sections() -> None:
    connection = RecordingConnection(
        (),
        (),
        (),
        *((metric_row(queue, length=0),) for queue in QueueName),
    )

    output = render_pipeline_status(connection)

    assert "TASKS\n(no tasks)" in output
    assert "STAGE RESULTS\n(no stage results)" in output
    assert "TASK FILES\n(no stored files)" in output
    for query, params in connection.calls[:3]:
        assert "WHERE task_id = %s" not in query
        assert params == ()


def test_enqueue_command_uses_default_database_connection(monkeypatch) -> None:
    connection = RecordingConnection(
        ({"task_id": "ticketmaster__aurora-13", "task_version": 1},),
        ({"send": 71},),
    )

    @contextmanager
    def fake_connection() -> Iterator[RecordingConnection]:
        yield connection

    monkeypatch.setattr(cli.db, "connection", fake_connection)

    result = CliRunner().invoke(
        cli.app,
        ["enqueue", "--repo", "ticketmaster/aurora", "--pr", "13"],
    )

    assert result.exit_code == 0
    assert "enqueued ticketmaster__aurora-13 v1" in result.stdout
    assert "queue=swegen_generate" in result.stdout


def test_enqueue_command_reports_duplicate_without_a_traceback(monkeypatch) -> None:
    connection = RecordingConnection(())

    @contextmanager
    def fake_connection() -> Iterator[RecordingConnection]:
        yield connection

    monkeypatch.setattr(cli.db, "connection", fake_connection)

    result = CliRunner().invoke(
        cli.app,
        ["enqueue", "--repo", "ticketmaster/aurora", "--pr", "13"],
    )

    assert result.exit_code == 1
    assert "already exists" in result.output
    assert "Traceback" not in result.output


def test_status_command_uses_default_database_connection(monkeypatch) -> None:
    connection = RecordingConnection(
        (),
        (),
        (),
        *((metric_row(queue, length=0),) for queue in QueueName),
    )

    @contextmanager
    def fake_connection() -> Iterator[RecordingConnection]:
        yield connection

    monkeypatch.setattr(cli.db, "connection", fake_connection)

    result = CliRunner().invoke(cli.app, ["status", "--task-id", "missing-task"])

    assert result.exit_code == 0
    assert result.stdout.startswith("TASKS\n(no tasks)\n")
    for _, params in connection.calls[:3]:
        assert params == ("missing-task",)


def test_status_rows_do_not_include_stored_error_or_result_payloads() -> None:
    secret = "SWEGEN_PG_PASSWORD=do-not-print"
    connection = RecordingConnection(
        (
            {
                "task_id": "ticketmaster__aurora-13",
                "task_version": 1,
                "repo": "ticketmaster/aurora",
                "pr": 13,
                "trace_id": TRACE_ID,
                "state": "failed",
                "current_stage": "generate",
                "last_error": secret,
            },
        ),
        (
            {
                "task_id": "ticketmaster__aurora-13",
                "task_version": 1,
                "stage": "generate",
                "attempt": 1,
                "status": "failed",
                "worker_id": "generate-0",
                "node_name": "node-generate",
                "started_at": ENQUEUED_AT,
                "finished_at": ENQUEUED_AT,
                "error": secret,
                "result": {"token": secret},
            },
        ),
        (),
        *((metric_row(queue, length=0),) for queue in QueueName),
    )

    output = render_pipeline_status(connection)

    assert secret not in output
    task_projection = connection.calls[0][0].partition(" FROM ")[0]
    stage_projection = connection.calls[1][0].partition(" FROM ")[0]
    assert "last_error" not in task_projection
    assert "error" not in stage_projection
    assert "result" not in stage_projection


def test_insert_and_status_queries_are_parameterized() -> None:
    connection = RecordingConnection(
        ({"task_id": "owner__repo-7", "task_version": 2},),
        ({"send": 9},),
    )
    generated_uuids = uuid_factory()

    enqueue_pipeline_task(
        connection,
        repo="owner/repo",
        pr=7,
        task_version=2,
        uuid_factory=lambda: next(generated_uuids),
        now_factory=lambda: ENQUEUED_AT,
    )

    query, params = connection.calls[0]
    assert "owner/repo" not in query
    assert "owner__repo-7" not in query
    assert query.count("%s") == len(params)


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"task_id": "owner__repo-7", "task_version": 2}, ("owner__repo-7", 2)),
        (("owner__repo-7", 2), ("owner__repo-7", 2)),
    ],
)
def test_enqueue_accepts_database_mapping_or_sequence_rows(
    row: Mapping[str, object] | tuple[object, ...],
    expected: tuple[str, int],
) -> None:
    connection = RecordingConnection((row,), ({"send": 9},))
    generated_uuids = uuid_factory()

    result = enqueue_pipeline_task(
        connection,
        repo="owner/repo",
        pr=7,
        task_version=2,
        uuid_factory=lambda: next(generated_uuids),
        now_factory=lambda: ENQUEUED_AT,
    )

    assert (result.task.task_id, result.task.task_version) == expected
