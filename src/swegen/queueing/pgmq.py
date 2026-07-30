"""Small, transaction-friendly adapter around PGMQ's SQL API."""

from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from typing import Protocol

from swegen.queueing.models import (
    ClaimedMessage,
    PipelineStage,
    QueueMessage,
    QueueMetrics,
    QueueName,
    RetryDisposition,
    queue_for_handoff,
    queue_for_stage,
    queues_for_stage,
)

MAX_DELAY_SECONDS = 86_400
MAX_VISIBILITY_TIMEOUT_SECONDS = 86_400
MAX_CLAIM_QUANTITY = 1_000
MAX_POLL_SECONDS = 60
MIN_POLL_INTERVAL_MS = 10
MAX_POLL_INTERVAL_MS = 10_000
MAX_DELIVERIES = 1_000


_SEND_SQL = "SELECT * FROM pgmq.send(%s, %s::jsonb, %s)"
_READ_SQL = """
    SELECT msg_id, read_ct, enqueued_at, vt, message
    FROM pgmq.read(%s, %s, %s)
"""
_READ_WITH_POLL_SQL = """
    SELECT msg_id, read_ct, enqueued_at, vt, message
    FROM pgmq.read_with_poll(%s, %s, %s, %s, %s)
"""
_SET_VT_SQL = """
    SELECT msg_id, read_ct, enqueued_at, vt, message
    FROM pgmq.set_vt(%s, %s, %s)
"""
_ARCHIVE_SQL = "SELECT pgmq.archive(%s, %s)"
_METRICS_SQL = """
    SELECT queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec,
           total_messages, scrape_time, queue_visible_length
    FROM pgmq.metrics(%s)
"""


class CursorLike(Protocol):
    """Subset of a psycopg cursor used by this adapter."""

    def fetchone(self) -> object | None: ...

    def fetchall(self) -> list[object]: ...


class ConnectionLike(Protocol):
    """Subset of a psycopg connection used by queue operations."""

    def execute(self, query: str, params: Sequence[object] | None = None) -> CursorLike: ...

    def transaction(self) -> AbstractContextManager[object]: ...


class StageCompletion(Protocol):
    """Idempotent ledger mutation performed inside a queue transaction."""

    def __call__(self, connection: ConnectionLike, claim: ClaimedMessage, /) -> bool: ...


class QueueOperationError(RuntimeError):
    """Raised when PGMQ cannot confirm a requested queue operation."""


def _bounded_integer(
    name: str,
    value: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _row_value(row: object, index: int, name: str) -> object:
    if isinstance(row, Mapping):
        try:
            return row[name]
        except KeyError as error:
            raise QueueOperationError(f"PGMQ row is missing {name!r}") from error

    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        try:
            return row[index]
        except IndexError as error:
            raise QueueOperationError(f"PGMQ row is missing column {index}") from error

    raise QueueOperationError(f"Unsupported PGMQ row type: {type(row).__name__}")


def _scalar(row: object | None, operation: str) -> object:
    if row is None:
        raise QueueOperationError(f"PGMQ {operation} returned no row")
    if isinstance(row, Mapping):
        if operation in row:
            return row[operation]
        if len(row) == 1:
            return next(iter(row.values()))
        raise QueueOperationError(f"PGMQ {operation} returned an ambiguous row")
    return _row_value(row, 0, operation)


def _decode_payload(raw_message: object) -> QueueMessage:
    if isinstance(raw_message, (str, bytes, bytearray)):
        return QueueMessage.model_validate_json(raw_message)
    return QueueMessage.model_validate(raw_message)


def _require_boolean_callback_result(operation: str, result: object) -> bool:
    if not isinstance(result, bool):
        raise QueueOperationError(f"{operation} callback must return a boolean")
    return result


class PgmqQueue:
    """Execute PGMQ operations on a caller-owned PostgreSQL connection."""

    def send(
        self,
        connection: ConnectionLike,
        message: QueueMessage,
        *,
        delay_seconds: int = 0,
    ) -> int:
        """Send an identifier-only message to its stage queue.

        The caller owns the transaction and must commit before consumers can
        observe the message.
        """

        delay_seconds = _bounded_integer(
            "delay_seconds",
            delay_seconds,
            minimum=0,
            maximum=MAX_DELAY_SECONDS,
        )
        return self._send_to_queue(
            connection,
            queue_for_stage(message.stage),
            message,
            delay_seconds=delay_seconds,
        )

    def claim(
        self,
        connection: ConnectionLike,
        stage: PipelineStage,
        *,
        visibility_timeout_seconds: int,
        quantity: int = 1,
        max_poll_seconds: int = 0,
        poll_interval_ms: int = 100,
    ) -> list[ClaimedMessage]:
        """Claim visible messages from one stage, optionally using long polling.

        The caller must commit this short operation before doing stage work;
        keeping the transaction open would hide the visibility update.
        """

        visibility_timeout_seconds = _bounded_integer(
            "visibility_timeout_seconds",
            visibility_timeout_seconds,
            minimum=1,
            maximum=MAX_VISIBILITY_TIMEOUT_SECONDS,
        )
        quantity = _bounded_integer("quantity", quantity, minimum=1, maximum=MAX_CLAIM_QUANTITY)
        max_poll_seconds = _bounded_integer(
            "max_poll_seconds", max_poll_seconds, minimum=0, maximum=MAX_POLL_SECONDS
        )
        accepted_queues = queues_for_stage(stage)
        for priority_queue in accepted_queues[:-1]:
            claims = self._claim_from_queue(
                connection,
                priority_queue,
                visibility_timeout_seconds=visibility_timeout_seconds,
                quantity=quantity,
                max_poll_seconds=0,
                poll_interval_ms=poll_interval_ms,
            )
            if claims:
                return claims

        return self._claim_from_queue(
            connection,
            accepted_queues[-1],
            visibility_timeout_seconds=visibility_timeout_seconds,
            quantity=quantity,
            max_poll_seconds=max_poll_seconds,
            poll_interval_ms=poll_interval_ms,
        )

    def _claim_from_queue(
        self,
        connection: ConnectionLike,
        queue: QueueName,
        *,
        visibility_timeout_seconds: int,
        quantity: int,
        max_poll_seconds: int,
        poll_interval_ms: int,
    ) -> list[ClaimedMessage]:
        if max_poll_seconds:
            poll_interval_ms = _bounded_integer(
                "poll_interval_ms",
                poll_interval_ms,
                minimum=MIN_POLL_INTERVAL_MS,
                maximum=MAX_POLL_INTERVAL_MS,
            )
            cursor = connection.execute(
                _READ_WITH_POLL_SQL,
                (
                    queue.value,
                    visibility_timeout_seconds,
                    quantity,
                    max_poll_seconds,
                    poll_interval_ms,
                ),
            )
        else:
            cursor = connection.execute(
                _READ_SQL,
                (queue.value, visibility_timeout_seconds, quantity),
            )

        return [self._decode_claim(row, queue) for row in cursor.fetchall()]

    def heartbeat(
        self,
        connection: ConnectionLike,
        claim: ClaimedMessage,
        *,
        visibility_timeout_seconds: int,
    ) -> ClaimedMessage:
        """Move a live claim's visibility deadline into the future.

        The caller must commit promptly so other consumers observe the update.
        """

        visibility_timeout_seconds = _bounded_integer(
            "visibility_timeout_seconds",
            visibility_timeout_seconds,
            minimum=1,
            maximum=MAX_VISIBILITY_TIMEOUT_SECONDS,
        )
        row = connection.execute(
            _SET_VT_SQL,
            (claim.queue.value, claim.msg_id, visibility_timeout_seconds),
        ).fetchone()
        if row is None:
            raise QueueOperationError(
                f"PGMQ set_vt returned no message for {claim.queue.value}/{claim.msg_id}"
            )
        updated = self._decode_claim(row, claim.queue)
        if updated.msg_id != claim.msg_id:
            raise QueueOperationError("PGMQ set_vt returned a different message ID")
        return updated

    def archive(self, connection: ConnectionLike, claim: ClaimedMessage) -> None:
        """Archive a successfully handled message, requiring PGMQ confirmation."""

        archived = _scalar(
            connection.execute(
                _ARCHIVE_SQL,
                (claim.queue.value, claim.msg_id),
            ).fetchone(),
            "archive",
        )
        if archived is not True:
            raise QueueOperationError(f"PGMQ archive failed for {claim.queue.value}/{claim.msg_id}")

    def metrics(self, connection: ConnectionLike, queue: QueueName) -> QueueMetrics:
        """Read one queue's operational metrics snapshot."""

        row = connection.execute(_METRICS_SQL, (queue.value,)).fetchone()
        if row is None:
            raise QueueOperationError(f"PGMQ metrics returned no row for {queue.value}")

        metrics = QueueMetrics(
            queue=_row_value(row, 0, "queue_name"),
            queue_length=_row_value(row, 1, "queue_length"),
            newest_message_age_seconds=_row_value(row, 2, "newest_msg_age_sec"),
            oldest_message_age_seconds=_row_value(row, 3, "oldest_msg_age_sec"),
            total_messages=_row_value(row, 4, "total_messages"),
            scraped_at=_row_value(row, 5, "scrape_time"),
            visible_length=_row_value(row, 6, "queue_visible_length"),
        )
        if metrics.queue is not queue:
            raise QueueOperationError(
                f"PGMQ metrics returned {metrics.queue.value} for requested {queue.value}"
            )
        return metrics

    def complete_and_handoff(
        self,
        connection: ConnectionLike,
        current: ClaimedMessage,
        successor: QueueMessage | None,
        *,
        complete_stage: StageCompletion,
    ) -> int | None:
        """Persist completion, emit one successor, and archive atomically.

        The ledger callback returns ``True`` only when it records a new
        task/stage attempt. A duplicate callback result still archives the
        duplicate delivery but deliberately emits no additional successor.
        """

        self._validate_current_claim(current)
        self._validate_successor(current.message, successor)

        with connection.transaction():
            newly_completed = _require_boolean_callback_result(
                "Stage completion",
                complete_stage(connection, current),
            )
            next_msg_id = None
            if newly_completed and successor is not None:
                next_msg_id = self._send_to_queue(
                    connection,
                    queue_for_handoff(current.message.stage, successor.stage),
                    successor,
                    delay_seconds=0,
                )
            self.archive(connection, current)
            return next_msg_id

    def complete_terminal(
        self,
        connection: ConnectionLike,
        current: ClaimedMessage,
        *,
        complete_stage: StageCompletion,
    ) -> bool:
        self._validate_current_claim(current)
        with connection.transaction():
            newly_completed = _require_boolean_callback_result(
                "Terminal completion", complete_stage(connection, current)
            )
            self.archive(connection, current)
            return newly_completed

    def retry_or_dead_letter(
        self,
        connection: ConnectionLike,
        current: ClaimedMessage,
        *,
        max_deliveries: int,
        retry_visibility_timeout_seconds: int,
        record_terminal_failure: StageCompletion | None = None,
    ) -> RetryDisposition:
        """Retry in place or atomically move a terminal delivery to dead-letter.

        The caller must commit a retry disposition promptly; terminal
        dead-letter operations are grouped in ``connection.transaction()``.
        """

        max_deliveries = _bounded_integer(
            "max_deliveries",
            max_deliveries,
            minimum=1,
            maximum=MAX_DELIVERIES,
        )
        retry_visibility_timeout_seconds = _bounded_integer(
            "retry_visibility_timeout_seconds",
            retry_visibility_timeout_seconds,
            minimum=1,
            maximum=MAX_VISIBILITY_TIMEOUT_SECONDS,
        )
        self._validate_current_claim(current)

        if current.read_count < max_deliveries:
            self.heartbeat(
                connection,
                current,
                visibility_timeout_seconds=retry_visibility_timeout_seconds,
            )
            return RetryDisposition.RETRY

        with connection.transaction():
            newly_terminal = (
                _require_boolean_callback_result(
                    "Terminal failure",
                    record_terminal_failure(connection, current),
                )
                if record_terminal_failure is not None
                else True
            )
            if newly_terminal:
                self._send_to_queue(
                    connection,
                    QueueName.DEAD,
                    current.message,
                    delay_seconds=0,
                )
            self.archive(connection, current)
        return RetryDisposition.DEAD_LETTER

    def _send_to_queue(
        self,
        connection: ConnectionLike,
        queue: QueueName,
        message: QueueMessage,
        *,
        delay_seconds: int,
    ) -> int:
        row = connection.execute(
            _SEND_SQL,
            (queue.value, message.model_dump_json(), delay_seconds),
        ).fetchone()
        raw_msg_id = _scalar(row, "send")
        if isinstance(raw_msg_id, bool) or not isinstance(raw_msg_id, int) or raw_msg_id <= 0:
            raise QueueOperationError(f"PGMQ send returned invalid message ID: {raw_msg_id!r}")
        return raw_msg_id

    def _decode_claim(self, row: object, queue: QueueName) -> ClaimedMessage:
        message = _decode_payload(_row_value(row, 4, "message"))
        accepted_queues = queues_for_stage(message.stage)
        if queue is not QueueName.DEAD and queue not in accepted_queues:
            raise QueueOperationError(
                f"Message stage {message.stage.value} does not match queue {queue.value}"
            )

        return ClaimedMessage(
            queue=queue,
            msg_id=_row_value(row, 0, "msg_id"),
            read_count=_row_value(row, 1, "read_ct"),
            enqueued_at=_row_value(row, 2, "enqueued_at"),
            visible_at=_row_value(row, 3, "vt"),
            message=message,
        )

    @staticmethod
    def _validate_current_claim(current: ClaimedMessage) -> None:
        accepted_queues = queues_for_stage(current.message.stage)
        if current.queue not in accepted_queues:
            raise QueueOperationError(
                f"Queue {current.queue.value} does not match current stage "
                f"{current.message.stage.value}"
            )

    @staticmethod
    def _validate_successor(
        current: QueueMessage,
        successor: QueueMessage | None,
    ) -> None:
        expected_stage = current.stage.next_stage
        if expected_stage is None:
            if successor is not None:
                raise QueueOperationError("Final stage must not have a successor")
            return

        if successor is None:
            raise QueueOperationError(
                f"Stage {current.stage.value} requires a successor at {expected_stage.value}"
            )

        matching_identity = (
            successor.stage is expected_stage
            and successor.event_id != current.event_id
            and successor.task_id == current.task_id
            and successor.task_version == current.task_version
            and successor.trace_id == current.trace_id
        )
        if not matching_identity:
            raise QueueOperationError(
                "Invalid successor: expected the next stage for the same task version and trace"
            )
