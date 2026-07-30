from collections import deque
from collections.abc import Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from importlib.resources import files
from uuid import UUID

import pytest
from pydantic import ValidationError

from swegen.queueing import (
    ClaimedMessage,
    PgmqQueue,
    PipelineStage,
    QueueMessage,
    QueueMetrics,
    QueueName,
    QueueOperationError,
    RetryDisposition,
    queue_for_stage,
)

EVENT_ID = UUID("11111111-1111-4111-8111-111111111111")
TRACE_ID = UUID("22222222-2222-4222-8222-222222222222")
ENQUEUED_AT = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)
PGMQ_ENQUEUED_AT = datetime(2026, 7, 28, 12, 0, 1, tzinfo=UTC)
VISIBLE_AT = datetime(2026, 7, 28, 12, 5, tzinfo=UTC)


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
        self.calls.append((normalized_query, tuple(params or ())))
        for operation in ("send", "set_vt", "archive", "metrics"):
            if f"pgmq.{operation}" in normalized_query:
                self.events.append(operation)
                break
        if not self.results:
            raise AssertionError(f"No result configured for SQL: {normalized_query}")
        return FakeCursor(self.results.popleft())

    @contextmanager
    def transaction(self):
        self.events.append("transaction-enter")
        try:
            yield
        except BaseException:
            self.events.append("transaction-rollback")
            raise
        else:
            self.events.append("transaction-exit")


def message_data(**overrides: object) -> dict[str, object]:
    data: dict[str, object] = {
        "schema_version": 1,
        "event_id": EVENT_ID,
        "task_id": "owner__repo-123",
        "task_version": 1,
        "stage": PipelineStage.GENERATE,
        "attempt": 1,
        "trace_id": TRACE_ID,
        "enqueued_at": ENQUEUED_AT,
    }
    data.update(overrides)
    return data


def queue_message(**overrides: object) -> QueueMessage:
    return QueueMessage.model_validate(message_data(**overrides))


def pgmq_row(queue_message_value: QueueMessage, **overrides: object) -> tuple[object, ...]:
    values: dict[str, object] = {
        "msg_id": 71,
        "read_ct": 2,
        "enqueued_at": PGMQ_ENQUEUED_AT,
        "vt": VISIBLE_AT,
        "message": queue_message_value.model_dump(mode="json"),
    }
    values.update(overrides)
    return (
        values["msg_id"],
        values["read_ct"],
        values["enqueued_at"],
        values["vt"],
        values["message"],
    )


def claimed_message(
    stage: PipelineStage = PipelineStage.GENERATE,
    *,
    queue: QueueName | None = None,
    read_count: int = 1,
) -> ClaimedMessage:
    return ClaimedMessage(
        queue=queue or queue_for_stage(stage),
        msg_id=71,
        read_count=read_count,
        enqueued_at=PGMQ_ENQUEUED_AT,
        visible_at=VISIBLE_AT,
        message=queue_message(stage=stage),
    )


def successor_message(
    current: ClaimedMessage,
    **overrides: object,
) -> QueueMessage:
    next_stage = current.message.stage.next_stage
    if next_stage is None:
        raise ValueError("Final-stage messages have no successor")
    data: dict[str, object] = {
        "event_id": UUID("33333333-3333-4333-8333-333333333333"),
        "task_id": current.message.task_id,
        "task_version": current.message.task_version,
        "stage": next_stage,
        "attempt": 1,
        "trace_id": current.message.trace_id,
        "enqueued_at": datetime(2026, 7, 28, 12, 1, tzinfo=UTC),
    }
    data.update(overrides)
    return queue_message(**data)


def test_queue_message_accepts_the_version_one_identifier_contract() -> None:
    message = QueueMessage.model_validate(message_data())

    assert message.schema_version == 1
    assert message.task_id == "owner__repo-123"
    assert message.event_id == EVENT_ID
    assert message.trace_id == TRACE_ID


def test_queue_message_is_immutable() -> None:
    message = QueueMessage.model_validate(message_data())

    with pytest.raises(ValidationError):
        message.task_id = "different"  # type: ignore[misc]


def test_queue_message_rejects_unknown_payload_fields() -> None:
    with pytest.raises(ValidationError, match="artifact_path"):
        QueueMessage.model_validate(message_data(artifact_path="/tmp/task.zip"))


def test_queue_message_rejects_a_naive_enqueue_timestamp() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        QueueMessage.model_validate(message_data(enqueued_at=datetime(2026, 7, 28, 12, 0)))


def test_queue_message_rejects_a_blank_task_id() -> None:
    with pytest.raises(ValidationError, match="task_id"):
        QueueMessage.model_validate(message_data(task_id="   "))


@pytest.mark.parametrize(("field", "value"), [("task_version", 0), ("attempt", 0)])
def test_queue_message_requires_positive_counters(field: str, value: int) -> None:
    with pytest.raises(ValidationError, match=field):
        QueueMessage.model_validate(message_data(**{field: value}))


def test_pipeline_stages_map_to_fixed_queues_in_order() -> None:
    assert queue_for_stage(PipelineStage.GENERATE) is QueueName.GENERATE
    assert queue_for_stage(PipelineStage.VALIDATE) is QueueName.VALIDATE
    assert queue_for_stage(PipelineStage.REPAIR) is QueueName.REPAIR
    assert queue_for_stage(PipelineStage.REWARD) is QueueName.REWARD
    assert queue_for_stage(PipelineStage.PUSH) is QueueName.PUSH

    assert PipelineStage.GENERATE.next_stage is PipelineStage.VALIDATE
    assert PipelineStage.VALIDATE.next_stage is PipelineStage.REWARD
    assert PipelineStage.REPAIR.next_stage is PipelineStage.VALIDATE
    assert PipelineStage.REWARD.next_stage is PipelineStage.PUSH
    assert PipelineStage.PUSH.next_stage is None


def test_all_pgmq_queue_names_fit_the_extension_limit() -> None:
    assert {queue.value for queue in QueueName} == {
        "swegen_generate",
        "swegen_validate",
        "swegen_repair",
        "swegen_reward",
        "swegen_push",
        "swegen_dead",
    }
    assert all(len(queue.value) <= 47 for queue in QueueName)


def test_send_uses_the_message_stage_queue_and_jsonb_payload() -> None:
    message = queue_message()
    connection = RecordingConnection([(401,)])

    msg_id = PgmqQueue().send(connection, message)

    assert msg_id == 401
    assert connection.calls == [
        (
            "SELECT * FROM pgmq.send(%s, %s::jsonb, %s)",
            ("swegen_generate", message.model_dump_json(), 0),
        )
    ]


def test_send_rejects_a_negative_delay_before_executing_sql() -> None:
    connection = RecordingConnection()

    with pytest.raises(ValueError, match="delay_seconds"):
        PgmqQueue().send(connection, queue_message(), delay_seconds=-1)

    assert connection.calls == []


def test_claim_decodes_tuple_rows_from_the_stage_queue() -> None:
    message = queue_message(stage=PipelineStage.VALIDATE)
    connection = RecordingConnection([pgmq_row(message)])

    claims = PgmqQueue().claim(
        connection,
        PipelineStage.VALIDATE,
        visibility_timeout_seconds=300,
        quantity=1,
    )

    assert claims == [
        ClaimedMessage(
            queue=QueueName.VALIDATE,
            msg_id=71,
            read_count=2,
            enqueued_at=PGMQ_ENQUEUED_AT,
            visible_at=VISIBLE_AT,
            message=message,
        )
    ]
    assert connection.calls == [
        (
            "SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.read(%s, %s, %s)",
            ("swegen_validate", 300, 1),
        )
    ]


def test_claim_uses_long_poll_when_requested() -> None:
    connection = RecordingConnection([])

    claims = PgmqQueue().claim(
        connection,
        PipelineStage.REWARD,
        visibility_timeout_seconds=600,
        quantity=4,
        max_poll_seconds=5,
        poll_interval_ms=250,
    )

    assert claims == []
    assert connection.calls == [
        (
            "SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.read_with_poll(%s, %s, %s, %s, %s)",
            ("swegen_reward", 600, 4, 5, 250),
        )
    ]


def test_claim_rejects_a_payload_routed_to_the_wrong_stage_queue() -> None:
    connection = RecordingConnection([pgmq_row(queue_message(stage=PipelineStage.GENERATE))])

    with pytest.raises(QueueOperationError, match="does not match"):
        PgmqQueue().claim(
            connection,
            PipelineStage.VALIDATE,
            visibility_timeout_seconds=300,
        )


def test_claim_revalidates_payloads_read_from_postgresql() -> None:
    invalid_payload = queue_message().model_dump(mode="json")
    invalid_payload["artifact_path"] = "/tmp/task.zip"
    connection = RecordingConnection([pgmq_row(queue_message(), message=invalid_payload)])

    with pytest.raises(ValidationError, match="artifact_path"):
        PgmqQueue().claim(
            connection,
            PipelineStage.GENERATE,
            visibility_timeout_seconds=300,
        )


def test_heartbeat_decodes_mapping_rows_and_updates_visibility() -> None:
    message = queue_message(stage=PipelineStage.REWARD)
    heartbeat_visible_at = datetime(2026, 7, 28, 12, 10, tzinfo=UTC)
    connection = RecordingConnection(
        [pgmq_row(message)],
        [
            {
                "msg_id": 71,
                "read_ct": 2,
                "enqueued_at": PGMQ_ENQUEUED_AT,
                "vt": heartbeat_visible_at,
                "message": message.model_dump(mode="json"),
            }
        ],
    )
    queue = PgmqQueue()
    claim = queue.claim(
        connection,
        PipelineStage.REWARD,
        visibility_timeout_seconds=300,
    )[0]

    updated = queue.heartbeat(connection, claim, visibility_timeout_seconds=600)

    assert updated.visible_at == heartbeat_visible_at
    assert connection.calls[-1] == (
        "SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.set_vt(%s, %s, %s)",
        ("swegen_reward", 71, 600),
    )


def test_archive_requires_pgmq_to_confirm_the_message_was_archived() -> None:
    message = queue_message(stage=PipelineStage.PUSH)
    connection = RecordingConnection([pgmq_row(message)], [(False,)])
    queue = PgmqQueue()
    claim = queue.claim(
        connection,
        PipelineStage.PUSH,
        visibility_timeout_seconds=300,
    )[0]

    with pytest.raises(QueueOperationError, match="archive"):
        queue.archive(connection, claim)

    assert connection.calls[-1] == (
        "SELECT pgmq.archive(%s, %s)",
        ("swegen_push", 71),
    )


def test_metrics_decodes_the_named_pgmq_metrics_record() -> None:
    scraped_at = datetime(2026, 7, 28, 12, 15, tzinfo=UTC)
    connection = RecordingConnection(
        [
            {
                "queue_name": "swegen_reward",
                "queue_length": 12,
                "newest_msg_age_sec": 3,
                "oldest_msg_age_sec": 91,
                "total_messages": 1_205,
                "scrape_time": scraped_at,
                "queue_visible_length": 8,
            }
        ]
    )

    metrics = PgmqQueue().metrics(connection, QueueName.REWARD)

    assert metrics == QueueMetrics(
        queue=QueueName.REWARD,
        queue_length=12,
        visible_length=8,
        newest_message_age_seconds=3,
        oldest_message_age_seconds=91,
        total_messages=1_205,
        scraped_at=scraped_at,
    )
    assert connection.calls == [
        (
            "SELECT queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages, scrape_time, queue_visible_length FROM pgmq.metrics(%s)",
            ("swegen_reward",),
        )
    ]


def test_complete_and_handoff_is_one_ordered_transaction() -> None:
    current = claimed_message(PipelineStage.GENERATE)
    successor = successor_message(current)
    connection = RecordingConnection([(402,)], [(True,)])

    def complete_stage(conn: RecordingConnection, claim: ClaimedMessage) -> bool:
        assert conn is connection
        assert claim is current
        conn.events.append("ledger")
        return True

    next_msg_id = PgmqQueue().complete_and_handoff(
        connection,
        current,
        successor,
        complete_stage=complete_stage,
    )

    assert next_msg_id == 402
    assert connection.events == [
        "transaction-enter",
        "ledger",
        "send",
        "archive",
        "transaction-exit",
    ]


def test_complete_and_handoff_archives_a_duplicate_without_fanning_out() -> None:
    current = claimed_message(PipelineStage.VALIDATE)
    successor = successor_message(current)
    connection = RecordingConnection([(True,)])

    def complete_stage(conn: RecordingConnection, claim: ClaimedMessage) -> bool:
        conn.events.append("ledger-duplicate")
        return False

    next_msg_id = PgmqQueue().complete_and_handoff(
        connection,
        current,
        successor,
        complete_stage=complete_stage,
    )

    assert next_msg_id is None
    assert connection.events == [
        "transaction-enter",
        "ledger-duplicate",
        "archive",
        "transaction-exit",
    ]
    assert all("pgmq.send" not in query for query, _ in connection.calls)


def test_complete_and_handoff_archives_the_final_stage_without_a_send() -> None:
    current = claimed_message(PipelineStage.PUSH)
    connection = RecordingConnection([(True,)])

    def complete_stage(conn: RecordingConnection, claim: ClaimedMessage) -> bool:
        conn.events.append("ledger")
        return True

    next_msg_id = PgmqQueue().complete_and_handoff(
        connection,
        current,
        None,
        complete_stage=complete_stage,
    )

    assert next_msg_id is None
    assert connection.events == [
        "transaction-enter",
        "ledger",
        "archive",
        "transaction-exit",
    ]


@pytest.mark.parametrize(
    "successor_overrides",
    [
        {"stage": PipelineStage.REWARD},
        {"event_id": EVENT_ID},
        {"task_id": "different-task"},
        {"task_version": 2},
        {"trace_id": UUID("44444444-4444-4444-8444-444444444444")},
    ],
)
def test_complete_and_handoff_rejects_a_mismatched_successor(
    successor_overrides: dict[str, object],
) -> None:
    current = claimed_message(PipelineStage.GENERATE)
    successor = successor_message(current, **successor_overrides)
    connection = RecordingConnection()

    with pytest.raises(QueueOperationError, match="successor"):
        PgmqQueue().complete_and_handoff(
            connection,
            current,
            successor,
            complete_stage=lambda _connection, _claim: True,
        )

    assert connection.events == []
    assert connection.calls == []


def test_complete_and_handoff_requires_a_successor_before_the_final_stage() -> None:
    current = claimed_message(PipelineStage.REWARD)
    connection = RecordingConnection()

    with pytest.raises(QueueOperationError, match="requires a successor"):
        PgmqQueue().complete_and_handoff(
            connection,
            current,
            None,
            complete_stage=lambda _connection, _claim: True,
        )


def test_complete_and_handoff_rejects_a_claim_from_the_wrong_queue() -> None:
    current = claimed_message(PipelineStage.GENERATE, queue=QueueName.REWARD)
    connection = RecordingConnection()

    with pytest.raises(QueueOperationError, match="current stage"):
        PgmqQueue().complete_and_handoff(
            connection,
            current,
            successor_message(current),
            complete_stage=lambda _connection, _claim: True,
        )

    assert connection.events == []


def test_complete_and_handoff_rolls_back_when_the_ledger_callback_fails() -> None:
    current = claimed_message(PipelineStage.GENERATE)
    connection = RecordingConnection()

    def fail_completion(conn: RecordingConnection, claim: ClaimedMessage) -> bool:
        conn.events.append("ledger")
        raise RuntimeError("ledger write failed")

    with pytest.raises(RuntimeError, match="ledger write failed"):
        PgmqQueue().complete_and_handoff(
            connection,
            current,
            successor_message(current),
            complete_stage=fail_completion,
        )

    assert connection.events == ["transaction-enter", "ledger", "transaction-rollback"]


def test_complete_and_handoff_rejects_a_non_boolean_ledger_result() -> None:
    current = claimed_message(PipelineStage.GENERATE)
    connection = RecordingConnection([(True,)])

    def invalid_completion_result(conn: RecordingConnection, claim: ClaimedMessage) -> bool:
        conn.events.append("ledger")
        return None  # type: ignore[return-value]

    with pytest.raises(QueueOperationError, match="boolean"):
        PgmqQueue().complete_and_handoff(
            connection,
            current,
            successor_message(current),
            complete_stage=invalid_completion_result,
        )

    assert connection.events == ["transaction-enter", "ledger", "transaction-rollback"]


def test_complete_and_handoff_rolls_back_when_archive_fails() -> None:
    current = claimed_message(PipelineStage.GENERATE)
    connection = RecordingConnection([(402,)], [(False,)])

    def complete_stage(conn: RecordingConnection, claim: ClaimedMessage) -> bool:
        conn.events.append("ledger")
        return True

    with pytest.raises(QueueOperationError, match="archive"):
        PgmqQueue().complete_and_handoff(
            connection,
            current,
            successor_message(current),
            complete_stage=complete_stage,
        )

    assert connection.events == [
        "transaction-enter",
        "ledger",
        "send",
        "archive",
        "transaction-rollback",
    ]


def test_complete_terminal_records_and_archives_without_successor() -> None:
    current = claimed_message(PipelineStage.REWARD)
    connection = RecordingConnection([(True,)])

    def complete_stage(conn: RecordingConnection, claim: ClaimedMessage) -> bool:
        assert conn is connection
        assert claim is current
        conn.events.append("callback")
        return True

    newly_completed = PgmqQueue().complete_terminal(
        connection,
        current,
        complete_stage=complete_stage,
    )

    assert newly_completed is True
    assert connection.events == [
        "transaction-enter",
        "callback",
        "archive",
        "transaction-exit",
    ]
    assert all("pgmq.send" not in query for query, _ in connection.calls)


def test_complete_terminal_archives_a_duplicate_without_successor() -> None:
    current = claimed_message(PipelineStage.VALIDATE)
    connection = RecordingConnection([(True,)])

    def complete_stage(conn: RecordingConnection, claim: ClaimedMessage) -> bool:
        conn.events.append("callback-duplicate")
        return False

    newly_completed = PgmqQueue().complete_terminal(
        connection,
        current,
        complete_stage=complete_stage,
    )

    assert newly_completed is False
    assert connection.events == [
        "transaction-enter",
        "callback-duplicate",
        "archive",
        "transaction-exit",
    ]
    assert all("pgmq.send" not in query for query, _ in connection.calls)


def test_complete_terminal_rejects_a_non_boolean_callback_result() -> None:
    current = claimed_message(PipelineStage.REWARD)
    connection = RecordingConnection()

    def invalid_completion_result(conn: RecordingConnection, claim: ClaimedMessage) -> bool:
        conn.events.append("callback")
        return None  # type: ignore[return-value]

    with pytest.raises(
        QueueOperationError, match="Terminal completion callback must return a boolean"
    ):
        PgmqQueue().complete_terminal(
            connection,
            current,
            complete_stage=invalid_completion_result,
        )

    assert connection.events == ["transaction-enter", "callback", "transaction-rollback"]
    assert connection.calls == []


def test_retry_before_the_delivery_limit_only_changes_visibility() -> None:
    current = claimed_message(PipelineStage.VALIDATE, read_count=2)
    retry_visible_at = datetime(2026, 7, 28, 12, 6, tzinfo=UTC)
    connection = RecordingConnection([pgmq_row(current.message, read_ct=2, vt=retry_visible_at)])

    disposition = PgmqQueue().retry_or_dead_letter(
        connection,
        current,
        max_deliveries=3,
        retry_visibility_timeout_seconds=60,
    )

    assert disposition is RetryDisposition.RETRY
    assert connection.events == ["set_vt"]
    assert connection.calls == [
        (
            "SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.set_vt(%s, %s, %s)",
            ("swegen_validate", 71, 60),
        )
    ]


def test_delivery_limit_dead_letters_and_archives_in_one_transaction() -> None:
    current = claimed_message(PipelineStage.REWARD, read_count=3)
    connection = RecordingConnection([(901,)], [(True,)])

    disposition = PgmqQueue().retry_or_dead_letter(
        connection,
        current,
        max_deliveries=3,
        retry_visibility_timeout_seconds=60,
    )

    assert disposition is RetryDisposition.DEAD_LETTER
    assert connection.events == [
        "transaction-enter",
        "send",
        "archive",
        "transaction-exit",
    ]
    assert connection.calls[0] == (
        "SELECT * FROM pgmq.send(%s, %s::jsonb, %s)",
        ("swegen_dead", current.message.model_dump_json(), 0),
    )


def test_terminal_failure_callback_runs_before_dead_letter_send() -> None:
    current = claimed_message(PipelineStage.GENERATE, read_count=4)
    connection = RecordingConnection([(902,)], [(True,)])

    def record_terminal_failure(conn: RecordingConnection, claim: ClaimedMessage) -> bool:
        assert claim is current
        conn.events.append("ledger-dead")
        return True

    disposition = PgmqQueue().retry_or_dead_letter(
        connection,
        current,
        max_deliveries=3,
        retry_visibility_timeout_seconds=60,
        record_terminal_failure=record_terminal_failure,
    )

    assert disposition is RetryDisposition.DEAD_LETTER
    assert connection.events == [
        "transaction-enter",
        "ledger-dead",
        "send",
        "archive",
        "transaction-exit",
    ]


def test_duplicate_terminal_failure_is_archived_without_another_dead_letter() -> None:
    current = claimed_message(PipelineStage.PUSH, read_count=3)
    connection = RecordingConnection([(True,)])

    def record_terminal_failure(conn: RecordingConnection, claim: ClaimedMessage) -> bool:
        conn.events.append("ledger-dead-duplicate")
        return False

    disposition = PgmqQueue().retry_or_dead_letter(
        connection,
        current,
        max_deliveries=3,
        retry_visibility_timeout_seconds=60,
        record_terminal_failure=record_terminal_failure,
    )

    assert disposition is RetryDisposition.DEAD_LETTER
    assert connection.events == [
        "transaction-enter",
        "ledger-dead-duplicate",
        "archive",
        "transaction-exit",
    ]
    assert all("pgmq.send" not in query for query, _ in connection.calls)


def test_dead_letter_rejects_a_non_boolean_ledger_result() -> None:
    current = claimed_message(PipelineStage.PUSH, read_count=3)
    connection = RecordingConnection([(True,)])

    def invalid_terminal_result(conn: RecordingConnection, claim: ClaimedMessage) -> bool:
        conn.events.append("ledger-dead")
        return None  # type: ignore[return-value]

    with pytest.raises(QueueOperationError, match="boolean"):
        PgmqQueue().retry_or_dead_letter(
            connection,
            current,
            max_deliveries=3,
            retry_visibility_timeout_seconds=60,
            record_terminal_failure=invalid_terminal_result,
        )

    assert connection.events == ["transaction-enter", "ledger-dead", "transaction-rollback"]


@pytest.mark.parametrize(
    ("max_deliveries", "retry_delay", "error_field"),
    [(0, 60, "max_deliveries"), (3, 0, "retry_visibility_timeout_seconds")],
)
def test_retry_policy_rejects_non_positive_limits_before_sql(
    max_deliveries: int,
    retry_delay: int,
    error_field: str,
) -> None:
    connection = RecordingConnection()

    with pytest.raises(ValueError, match=error_field):
        PgmqQueue().retry_or_dead_letter(
            connection,
            claimed_message(read_count=1),
            max_deliveries=max_deliveries,
            retry_visibility_timeout_seconds=retry_delay,
        )

    assert connection.events == []
    assert connection.calls == []


def test_bootstrap_sql_accepts_supported_extension_or_complete_sql_only_api() -> None:
    sql = files("swegen.queueing").joinpath("bootstrap.sql").read_text()
    normalized_sql = sql.upper()

    assert "PG_EXTENSION" in normalized_sql
    assert "EXTVERSION" in normalized_sql
    assert "1.5.0" in sql
    for signature in (
        "pgmq.create(text)",
        "pgmq.send(text,jsonb,integer)",
        "pgmq.read(text,integer,integer,jsonb)",
        "pgmq.read_with_poll(text,integer,integer,integer,integer,jsonb)",
        "pgmq.set_vt(text,bigint,integer)",
        "pgmq.archive(text,bigint)",
        "pgmq.metrics(text)",
    ):
        assert f"TO_REGPROCEDURE('{signature.upper()}')" in normalized_sql

    assert "PG_TYPE" in normalized_sql
    assert "PG_ATTRIBUTE" in normalized_sql
    assert "METRICS_RESULT" in normalized_sql
    assert "QUEUE_VISIBLE_LENGTH" in normalized_sql
    assert "SQL-ONLY" in normalized_sql

    assert normalized_sql.count("SELECT PGMQ.CREATE(") == 6
    for queue in QueueName:
        assert f"'{queue.value}'" in sql

    assert "CREATE EXTENSION" not in normalized_sql
    assert "CREATE TABLE" not in normalized_sql
    assert "PASSWORD" not in normalized_sql
