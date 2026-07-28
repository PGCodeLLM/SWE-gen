from __future__ import annotations

from collections import deque
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from shutil import rmtree
from threading import Event, Lock
from time import monotonic
from typing import Any
from uuid import UUID

import pytest

from swegen import db as db_module
from swegen.pipeline import worker as worker_module
from swegen.pipeline.models import PipelineTask, StageExecution, TaskFile
from swegen.pipeline.worker import (
    ClaimHeartbeat,
    PipelineWorker,
    WorkerSettings,
    _safe_error_text,
)
from swegen.queueing.models import (
    ClaimedMessage,
    PipelineStage,
    QueueMessage,
    RetryDisposition,
    queue_for_stage,
)
from swegen.queueing.pgmq import (
    MAX_DELIVERIES,
    MAX_POLL_SECONDS,
    MAX_VISIBILITY_TIMEOUT_SECONDS,
)

EVENT_ID = UUID("11111111-1111-4111-8111-111111111111")
NEXT_EVENT_ID = UUID("33333333-3333-4333-8333-333333333333")
TRACE_ID = UUID("22222222-2222-4222-8222-222222222222")
ENQUEUED_AT = datetime(2026, 7, 28, 11, 59, tzinfo=UTC)
VISIBLE_AT = datetime(2026, 7, 28, 12, 5, tzinfo=UTC)
STARTED_AT = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)
HANDOFF_AT = datetime(2026, 7, 28, 12, 1, tzinfo=UTC)


def pipeline_task(stage: PipelineStage = PipelineStage.GENERATE) -> PipelineTask:
    return PipelineTask(
        task_id="owner__repo-123",
        task_version=1,
        repo="owner/repo",
        pr=123,
        trace_id=TRACE_ID,
        current_stage=stage,
    )


def pipeline_claim(
    stage: PipelineStage = PipelineStage.GENERATE,
    *,
    read_count: int = 1,
    msg_id: int = 71,
) -> ClaimedMessage:
    message = QueueMessage(
        event_id=EVENT_ID,
        task_id="owner__repo-123",
        task_version=1,
        stage=stage,
        attempt=2,
        trace_id=TRACE_ID,
        enqueued_at=ENQUEUED_AT,
    )
    return ClaimedMessage(
        queue=queue_for_stage(stage),
        msg_id=msg_id,
        read_count=read_count,
        enqueued_at=ENQUEUED_AT,
        visible_at=VISIBLE_AT,
        message=message,
    )


def reward_claim(*, read_count: int = 1) -> ClaimedMessage:
    return pipeline_claim(PipelineStage.REWARD, read_count=read_count)


def task_file(
    path: str = "instruction.md",
    content: bytes = b"fix the bug\n",
    mode: int = 0o644,
) -> TaskFile:
    return TaskFile(
        path=path,
        content=content,
        mode=mode,
        sha256=sha256(content).hexdigest(),
    )


class FakeConnection:
    def __init__(self, name: str, events: list[str]) -> None:
        self.name = name
        self.events = events
        self.active = False
        self.exited = False

    def __enter__(self) -> FakeConnection:
        if self.active:
            raise AssertionError("connection context cannot be re-entered")
        self.active = True
        self.events.append(f"{self.name}:enter")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        self.active = False
        self.exited = True
        self.events.append(f"{self.name}:exit")


class RecordingConnectionFactory:
    def __init__(self) -> None:
        self.connections: list[FakeConnection] = []
        self.events: list[str] = []
        self._lock = Lock()

    def __call__(self) -> FakeConnection:
        with self._lock:
            connection = FakeConnection(
                f"connection-{len(self.connections) + 1}",
                self.events,
            )
            self.connections.append(connection)
            return connection


class FakeQueue:
    def __init__(self, claims: Sequence[ClaimedMessage] = ()) -> None:
        self.claims = deque(claims)
        self.claim_calls: list[tuple[FakeConnection, PipelineStage, dict[str, int]]] = []
        self.heartbeat_calls: list[tuple[FakeConnection, ClaimedMessage, int]] = []
        self.handoffs: list[tuple[ClaimedMessage, QueueMessage | None]] = []
        self.completed_terminal: list[ClaimedMessage] = []
        self.retries: list[ClaimedMessage] = []
        self.dead_letters: list[ClaimedMessage] = []

    def claim(
        self,
        connection: FakeConnection,
        stage: PipelineStage,
        *,
        visibility_timeout_seconds: int,
        quantity: int,
        max_poll_seconds: int,
    ) -> list[ClaimedMessage]:
        self.claim_calls.append(
            (
                connection,
                stage,
                {
                    "visibility_timeout_seconds": visibility_timeout_seconds,
                    "quantity": quantity,
                    "max_poll_seconds": max_poll_seconds,
                },
            )
        )
        if not self.claims:
            return []
        return [self.claims.popleft()]

    def heartbeat(
        self,
        connection: FakeConnection,
        claim: ClaimedMessage,
        *,
        visibility_timeout_seconds: int,
    ) -> ClaimedMessage:
        self.heartbeat_calls.append((connection, claim, visibility_timeout_seconds))
        return claim

    def complete_and_handoff(
        self,
        connection: FakeConnection,
        current: ClaimedMessage,
        successor: QueueMessage | None,
        *,
        complete_stage: Callable[[FakeConnection, ClaimedMessage], bool],
    ) -> int | None:
        assert complete_stage(connection, current) is True
        self.handoffs.append((current, successor))
        return 501 if successor is not None else None

    def complete_terminal(
        self,
        connection: FakeConnection,
        current: ClaimedMessage,
        *,
        complete_stage: Callable[[FakeConnection, ClaimedMessage], bool],
    ) -> bool:
        assert complete_stage(connection, current) is True
        self.completed_terminal.append(current)
        return True

    def retry_or_dead_letter(
        self,
        connection: FakeConnection,
        current: ClaimedMessage,
        *,
        max_deliveries: int,
        retry_visibility_timeout_seconds: int,
        record_terminal_failure: Callable[[FakeConnection, ClaimedMessage], bool] | None = None,
    ) -> RetryDisposition:
        assert retry_visibility_timeout_seconds == 300
        if current.read_count < max_deliveries:
            self.retries.append(current)
            return RetryDisposition.RETRY
        assert record_terminal_failure is not None
        assert record_terminal_failure(connection, current) is True
        self.dead_letters.append(current)
        return RetryDisposition.DEAD_LETTER


class FakeStore:
    def __init__(
        self,
        task: PipelineTask,
        files: Sequence[TaskFile] = (),
    ) -> None:
        self.task = task
        self.files = tuple(files)
        self.get_task_calls: list[tuple[FakeConnection, str, int]] = []
        self.load_files_calls: list[tuple[FakeConnection, str, int]] = []
        self.stage_results: list[
            tuple[
                FakeConnection,
                ClaimedMessage,
                StageExecution,
                datetime,
                str,
                str,
            ]
        ] = []
        self.terminal_failures: list[
            tuple[FakeConnection, ClaimedMessage, str, datetime, str, str]
        ] = []

    def get_task(
        self,
        connection: FakeConnection,
        task_id: str,
        task_version: int,
    ) -> PipelineTask:
        self.get_task_calls.append((connection, task_id, task_version))
        return self.task

    def load_files(
        self,
        connection: FakeConnection,
        task_id: str,
        task_version: int,
    ) -> tuple[TaskFile, ...]:
        self.load_files_calls.append((connection, task_id, task_version))
        return self.files

    def record_stage_result(
        self,
        connection: FakeConnection,
        claim: ClaimedMessage,
        execution: StageExecution,
        *,
        started_at: datetime,
        worker_id: str,
        node_name: str,
    ) -> bool:
        self.stage_results.append((connection, claim, execution, started_at, worker_id, node_name))
        return True

    def record_terminal_failure(
        self,
        connection: FakeConnection,
        claim: ClaimedMessage,
        error: str,
        *,
        started_at: datetime,
        worker_id: str,
        node_name: str,
    ) -> bool:
        self.terminal_failures.append((connection, claim, error, started_at, worker_id, node_name))
        return True


class FakeAction:
    def __init__(
        self,
        result: StageExecution | None = None,
        *,
        error: Exception | None = None,
        side_effect: Callable[[PipelineTask, Path], None] | None = None,
    ) -> None:
        self.result = result or StageExecution.succeeded({"ok": True})
        self.error = error
        self.side_effect = side_effect
        self.calls: list[tuple[PipelineTask, Path]] = []

    def __call__(self, task: PipelineTask, workspace: Path) -> StageExecution:
        self.calls.append((task, workspace))
        if self.side_effect is not None:
            self.side_effect(task, workspace)
        if self.error is not None:
            raise self.error
        return self.result


class FakeHeartbeat:
    def __init__(
        self,
        failure: str | None = None,
        *,
        running_after_exit: bool = False,
    ) -> None:
        self.entered = False
        self.exited = False
        self.failure = failure
        self.running_after_exit = running_after_exit

    @property
    def is_running(self) -> bool:
        return self.exited and self.running_after_exit

    def __enter__(self) -> FakeHeartbeat:
        self.entered = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        self.exited = True


class FakeHeartbeatFactory:
    def __init__(
        self,
        failure: str | None = None,
        *,
        running_after_exit: bool = False,
    ) -> None:
        self.instances: list[FakeHeartbeat] = []
        self.calls: list[dict[str, Any]] = []
        self.failure = failure
        self.running_after_exit = running_after_exit

    def __call__(self, **kwargs: Any) -> FakeHeartbeat:
        self.calls.append(kwargs)
        heartbeat = FakeHeartbeat(
            self.failure,
            running_after_exit=self.running_after_exit,
        )
        self.instances.append(heartbeat)
        return heartbeat


class SequenceClock:
    def __init__(self, *values: datetime) -> None:
        self.values = deque(values)

    def __call__(self) -> datetime:
        if not self.values:
            raise AssertionError("clock called more times than expected")
        return self.values.popleft()


def make_worker(
    tmp_path: Path,
    *,
    stage: PipelineStage = PipelineStage.REWARD,
    claim: ClaimedMessage | None = None,
    claims: Sequence[ClaimedMessage] | None = None,
    action: FakeAction | Callable[[PipelineTask, Path], StageExecution] | None = None,
    task: PipelineTask | None = None,
    files: Sequence[TaskFile] = (),
    clock: Callable[[], datetime] | None = None,
    stop_event: Event | None = None,
    heartbeat_factory: Callable[..., AbstractContextManager[object]] | None = None,
    queue: FakeQueue | None = None,
    workspace_factory: Callable[..., AbstractContextManager[Path]] | None = None,
) -> PipelineWorker:
    selected_claims = list(claims or ([] if claim is None else [claim]))
    queue = queue or FakeQueue(selected_claims)
    store = FakeStore(task or pipeline_task(stage), files)
    return PipelineWorker(
        stage=stage,
        connection_factory=RecordingConnectionFactory(),
        queue=queue,
        store=store,
        action=action or FakeAction(),
        settings=WorkerSettings(workspace_root=tmp_path),
        worker_id="worker-1",
        node_name="node-a",
        clock=clock or (lambda: STARTED_AT),
        uuid_factory=lambda: NEXT_EVENT_ID,
        stop_event=stop_event,
        heartbeat_factory=heartbeat_factory or FakeHeartbeatFactory(),
        workspace_factory=workspace_factory,
    )


def test_worker_settings_use_resilient_defaults(tmp_path: Path) -> None:
    settings = WorkerSettings(workspace_root=tmp_path)

    assert settings.claim_quantity == 1
    assert settings.visibility_timeout_seconds == 300
    assert settings.heartbeat_interval_seconds == 60
    assert settings.poll_seconds == 10
    assert settings.max_deliveries == 3
    assert settings.retry_visibility_timeout_seconds == 300
    assert settings.workspace_root == tmp_path


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("claim_quantity", 0),
        ("visibility_timeout_seconds", 0),
        ("heartbeat_interval_seconds", 0),
        ("poll_seconds", -1),
        ("max_deliveries", 0),
        ("retry_visibility_timeout_seconds", 0),
    ],
)
def test_worker_settings_reject_invalid_nonpositive_values(
    tmp_path: Path,
    field: str,
    value: int,
) -> None:
    with pytest.raises(ValueError, match=field):
        WorkerSettings(workspace_root=tmp_path, **{field: value})


def test_worker_settings_require_heartbeat_before_visibility_expiry(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="heartbeat_interval_seconds"):
        WorkerSettings(
            workspace_root=tmp_path,
            visibility_timeout_seconds=60,
            heartbeat_interval_seconds=60,
        )


def test_worker_settings_enforce_the_single_claim_mvp(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="claim_quantity"):
        WorkerSettings(workspace_root=tmp_path, claim_quantity=2)


def test_worker_settings_reject_a_non_finite_heartbeat_interval(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="heartbeat_interval_seconds"):
        WorkerSettings(workspace_root=tmp_path, heartbeat_interval_seconds=float("nan"))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("visibility_timeout_seconds", MAX_VISIBILITY_TIMEOUT_SECONDS + 1),
        ("poll_seconds", MAX_POLL_SECONDS + 1),
        ("max_deliveries", MAX_DELIVERIES + 1),
        ("retry_visibility_timeout_seconds", MAX_VISIBILITY_TIMEOUT_SECONDS + 1),
    ],
)
def test_worker_settings_reject_values_above_pgmq_bounds(
    tmp_path: Path,
    field: str,
    value: int,
) -> None:
    with pytest.raises(ValueError, match=field):
        WorkerSettings(workspace_root=tmp_path, **{field: value})


@pytest.mark.parametrize(
    "credential_text",
    [
        "SWEGEN_PG_PASSWORD=hunter2",
        "password=hunter2",
        "AWS_SECRET_ACCESS_KEY=hunter2",
        "client_secret=hunter2",
        "access_key=hunter2",
        "token=hunter2",
        '{"password": "hunter2"}',
        "postgresql://root:hunter2@db.example/swegen",
        "host=db user=root password=hunter2 dbname=swegen",
    ],
)
def test_safe_error_text_redacts_worker_local_credential_forms(
    credential_text: str,
) -> None:
    safe_error = _safe_error_text(RuntimeError(f"failed with {credential_text}"))

    assert "hunter2" not in safe_error
    assert "<REDACTED>" in safe_error


def test_safe_error_text_normalizes_controls_before_logging_and_persistence() -> None:
    safe_error = _safe_error_text(
        RuntimeError("first line\npassword=hunter2\rsecond\x00\tthird" + "x" * 5_000)
    )

    assert "hunter2" not in safe_error
    assert all(ord(character) >= 32 and ord(character) != 127 for character in safe_error)
    assert len(safe_error) <= 4_000


def test_empty_poll_returns_false_without_loading_a_task(tmp_path: Path) -> None:
    worker = make_worker(tmp_path)

    assert worker.run_once() is False

    assert worker.store.get_task_calls == []
    assert worker.queue.handoffs == []
    assert worker.queue.claim_calls[0][2] == {
        "visibility_timeout_seconds": 300,
        "quantity": 1,
        "max_poll_seconds": 10,
    }
    assert worker.connection_factory.connections[0].exited is True


def test_claim_and_load_connections_close_before_stage_execution(tmp_path: Path) -> None:
    claim = reward_claim()

    def assert_short_transactions_closed(task: PipelineTask, workspace: Path) -> None:
        assert task.current_stage is PipelineStage.REWARD
        assert workspace.is_dir()
        assert len(worker.connection_factory.connections) == 2
        assert all(
            connection.exited and not connection.active
            for connection in worker.connection_factory.connections
        )

    action = FakeAction(side_effect=assert_short_transactions_closed)
    worker = make_worker(tmp_path, action=action, claim=claim)

    assert worker.run_once() is True


def test_claim_heartbeat_uses_short_independent_connections_and_stops() -> None:
    action_active = Event()
    heartbeat_seen = Event()
    connection_factory = RecordingConnectionFactory()
    claim = reward_claim()

    class HeartbeatQueue(FakeQueue):
        def heartbeat(
            self,
            connection: FakeConnection,
            current: ClaimedMessage,
            *,
            visibility_timeout_seconds: int,
        ) -> ClaimedMessage:
            assert action_active.wait(timeout=1)
            updated = super().heartbeat(
                connection,
                current,
                visibility_timeout_seconds=visibility_timeout_seconds,
            )
            heartbeat_seen.set()
            return updated

    queue = HeartbeatQueue()
    heartbeat = ClaimHeartbeat(
        connection_factory=connection_factory,
        queue=queue,
        claim=claim,
        visibility_timeout_seconds=300,
        interval_seconds=0.01,
    )

    with heartbeat:
        action_active.set()
        assert heartbeat_seen.wait(timeout=1)
        assert heartbeat.is_running is True

    assert heartbeat.is_running is False
    assert queue.heartbeat_calls
    assert all(connection.exited for connection in connection_factory.connections)
    assert all(call[2] == 300 for call in queue.heartbeat_calls)


def test_claim_heartbeat_exit_is_bounded_when_the_database_call_remains_blocked() -> None:
    heartbeat_started = Event()
    release_heartbeat = Event()
    heartbeat_finished = Event()
    connection_factory = RecordingConnectionFactory()
    claim = reward_claim()

    class BlockingHeartbeatQueue(FakeQueue):
        def heartbeat(
            self,
            connection: FakeConnection,
            current: ClaimedMessage,
            *,
            visibility_timeout_seconds: int,
        ) -> ClaimedMessage:
            heartbeat_started.set()
            release_heartbeat.wait(timeout=2)
            heartbeat_finished.set()
            return current

    heartbeat = ClaimHeartbeat(
        connection_factory=connection_factory,
        queue=BlockingHeartbeatQueue(),
        claim=claim,
        visibility_timeout_seconds=300,
        interval_seconds=0.01,
        join_timeout_seconds=0.01,
    )

    started = monotonic()
    with heartbeat:
        assert heartbeat_started.wait(timeout=1)
    elapsed = monotonic() - started

    assert elapsed < 0.5
    assert heartbeat.lease_lost is True
    assert heartbeat.failure is not None
    assert "bounded cancellation" in heartbeat.failure

    release_heartbeat.set()
    assert heartbeat_finished.wait(timeout=1)


def test_claim_heartbeat_cancels_a_blocked_active_connection_on_exit() -> None:
    heartbeat_started = Event()
    unblock_heartbeat = Event()
    force_cleanup = Event()
    heartbeat_finished = Event()
    events: list[str] = []
    claim = reward_claim()

    class CancellableConnection(FakeConnection):
        def __init__(self) -> None:
            super().__init__("heartbeat-connection", events)
            self.cancel_timeouts: list[float] = []
            self.cancelled = False

        def cancel_safe(self, *, timeout: float) -> None:
            self.cancel_timeouts.append(timeout)
            self.cancelled = True
            unblock_heartbeat.set()

    connection = CancellableConnection()

    class CancellableConnectionFactory:
        def __call__(self) -> CancellableConnection:
            return connection

    class BlockingHeartbeatQueue(FakeQueue):
        def __init__(self) -> None:
            super().__init__()
            self.visibility_mutations = 0

        def heartbeat(
            self,
            current_connection: CancellableConnection,
            current: ClaimedMessage,
            *,
            visibility_timeout_seconds: int,
        ) -> ClaimedMessage:
            heartbeat_started.set()
            unblock_heartbeat.wait(timeout=1)
            try:
                if current_connection.cancelled:
                    raise RuntimeError("heartbeat query cancelled")
                if not force_cleanup.is_set():
                    self.visibility_mutations += 1
                return current
            finally:
                heartbeat_finished.set()

    queue = BlockingHeartbeatQueue()
    heartbeat = ClaimHeartbeat(
        connection_factory=CancellableConnectionFactory(),
        queue=queue,
        claim=claim,
        visibility_timeout_seconds=300,
        interval_seconds=0.01,
        join_timeout_seconds=0.1,
        cancel_timeout_seconds=0.05,
    )

    try:
        with heartbeat:
            assert heartbeat_started.wait(timeout=1)

        assert connection.cancel_timeouts == [0.05]
        assert heartbeat.is_running is False
        assert connection.exited is True
        assert queue.visibility_mutations == 0
    finally:
        force_cleanup.set()
        unblock_heartbeat.set()
        assert heartbeat_finished.wait(timeout=1)


def test_claim_heartbeat_does_not_use_unbounded_cancel_after_cancel_safe_failure() -> None:
    heartbeat_started = Event()
    unblock_heartbeat = Event()
    heartbeat_finished = Event()
    events: list[str] = []
    claim = reward_claim()

    class FailingSafeCancelConnection(FakeConnection):
        def __init__(self) -> None:
            super().__init__("safe-cancel-failure", events)
            self.cancel_safe_calls: list[float] = []
            self.cancel_calls = 0

        def cancel_safe(self, *, timeout: float) -> None:
            self.cancel_safe_calls.append(timeout)
            raise TimeoutError("bounded cancellation timed out")

        def cancel(self) -> None:
            self.cancel_calls += 1
            unblock_heartbeat.set()

    connection = FailingSafeCancelConnection()

    class ConnectionFactory:
        def __call__(self) -> FailingSafeCancelConnection:
            return connection

    class BlockingQueue(FakeQueue):
        def heartbeat(
            self,
            current_connection: FailingSafeCancelConnection,
            current: ClaimedMessage,
            *,
            visibility_timeout_seconds: int,
        ) -> ClaimedMessage:
            heartbeat_started.set()
            unblock_heartbeat.wait(timeout=1)
            heartbeat_finished.set()
            return current

    heartbeat = ClaimHeartbeat(
        connection_factory=ConnectionFactory(),
        queue=BlockingQueue(),
        claim=claim,
        visibility_timeout_seconds=300,
        interval_seconds=0.01,
        join_timeout_seconds=0.01,
        cancel_timeout_seconds=0.01,
    )

    try:
        with heartbeat:
            assert heartbeat_started.wait(timeout=1)

        assert connection.cancel_safe_calls == [0.01]
        assert connection.cancel_calls == 0
        assert heartbeat.failure is not None
    finally:
        unblock_heartbeat.set()
        assert heartbeat_finished.wait(timeout=1)


def test_claim_heartbeat_rolls_back_a_query_returning_after_exit() -> None:
    heartbeat_started = Event()
    release_query = Event()
    connection_exited = Event()
    events: list[str] = []
    claim = reward_claim()

    class TransactionalConnection(FakeConnection):
        def __init__(self) -> None:
            super().__init__("transactional-heartbeat", events)
            self.pending_visibility = False
            self.committed_visibility = 0
            self.rollbacks = 0

        def cancel_safe(self, *, timeout: float) -> None:
            raise TimeoutError("query did not cancel before timeout")

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            traceback: object | None,
        ) -> None:
            if exc_type is None and self.pending_visibility:
                self.committed_visibility += 1
            elif exc_type is not None:
                self.rollbacks += 1
            self.pending_visibility = False
            super().__exit__(exc_type, exc, traceback)
            connection_exited.set()

    connection = TransactionalConnection()

    class ConnectionFactory:
        def __call__(self) -> TransactionalConnection:
            return connection

    class LateReturningQueue(FakeQueue):
        def heartbeat(
            self,
            current_connection: TransactionalConnection,
            current: ClaimedMessage,
            *,
            visibility_timeout_seconds: int,
        ) -> ClaimedMessage:
            heartbeat_started.set()
            assert release_query.wait(timeout=1)
            current_connection.pending_visibility = True
            return current

    heartbeat = ClaimHeartbeat(
        connection_factory=ConnectionFactory(),
        queue=LateReturningQueue(),
        claim=claim,
        visibility_timeout_seconds=300,
        interval_seconds=0.01,
        join_timeout_seconds=0.01,
        cancel_timeout_seconds=0.01,
    )

    with heartbeat:
        assert heartbeat_started.wait(timeout=1)

    release_query.set()
    assert connection_exited.wait(timeout=1)
    assert connection.committed_visibility == 0
    assert connection.rollbacks == 1


def test_claim_heartbeat_does_not_run_after_a_late_connection_acquisition() -> None:
    acquisition_started = Event()
    release_acquisition = Event()
    connection_exited = Event()
    heartbeat_called = Event()
    events: list[str] = []
    claim = reward_claim()

    class LateConnection(FakeConnection):
        def __enter__(self) -> LateConnection:
            acquisition_started.set()
            assert release_acquisition.wait(timeout=1)
            return super().__enter__()

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            traceback: object | None,
        ) -> None:
            super().__exit__(exc_type, exc, traceback)
            connection_exited.set()

    connection = LateConnection("late-connection", events)

    class LateConnectionFactory:
        def __call__(self) -> LateConnection:
            return connection

    class RecordingHeartbeatQueue(FakeQueue):
        def heartbeat(
            self,
            current_connection: LateConnection,
            current: ClaimedMessage,
            *,
            visibility_timeout_seconds: int,
        ) -> ClaimedMessage:
            heartbeat_called.set()
            return current

    heartbeat = ClaimHeartbeat(
        connection_factory=LateConnectionFactory(),
        queue=RecordingHeartbeatQueue(),
        claim=claim,
        visibility_timeout_seconds=300,
        interval_seconds=0.01,
        join_timeout_seconds=0.01,
    )

    with heartbeat:
        assert acquisition_started.wait(timeout=1)

    release_acquisition.set()
    assert connection_exited.wait(timeout=1)
    assert heartbeat_called.is_set() is False


def test_claim_heartbeat_detects_changed_delivery_ownership() -> None:
    heartbeat_seen = Event()
    claim = reward_claim(read_count=1)

    class ReclaimedHeartbeatQueue(FakeQueue):
        def heartbeat(
            self,
            connection: FakeConnection,
            current: ClaimedMessage,
            *,
            visibility_timeout_seconds: int,
        ) -> ClaimedMessage:
            heartbeat_seen.set()
            return current.model_copy(update={"read_count": current.read_count + 1})

    heartbeat = ClaimHeartbeat(
        connection_factory=RecordingConnectionFactory(),
        queue=ReclaimedHeartbeatQueue(),
        claim=claim,
        visibility_timeout_seconds=300,
        interval_seconds=0.01,
    )

    with heartbeat:
        assert heartbeat_seen.wait(timeout=1)

    assert heartbeat.lease_lost is True
    assert heartbeat.failure is not None
    assert "ownership" in heartbeat.failure


@pytest.mark.parametrize("action_raises", [False, True])
def test_worker_leaves_a_failed_heartbeat_for_pgmq_recovery_without_stale_mutation(
    tmp_path: Path,
    action_raises: bool,
) -> None:
    action = (
        FakeAction(error=RuntimeError("action also failed"))
        if action_raises
        else FakeAction(StageExecution.succeeded({"score": 1}))
    )
    worker = make_worker(
        tmp_path,
        action=action,
        claim=reward_claim(),
        heartbeat_factory=FakeHeartbeatFactory("lease ownership was lost"),
    )

    assert worker.run_once() is True

    assert worker.queue.handoffs == []
    assert worker.queue.completed_terminal == []
    assert worker.queue.retries == []
    assert worker.queue.dead_letters == []
    assert worker.store.stage_results == []
    assert worker.store.terminal_failures == []


def test_worker_quarantines_after_a_heartbeat_thread_outlives_join(
    tmp_path: Path,
) -> None:
    first_claim = reward_claim()
    second_claim = pipeline_claim(PipelineStage.REWARD, msg_id=72)
    action = FakeAction(StageExecution.succeeded({"score": 1}))
    worker = make_worker(
        tmp_path,
        claims=[first_claim, second_claim],
        action=action,
        heartbeat_factory=FakeHeartbeatFactory(
            "visibility heartbeat did not stop before the join timeout",
            running_after_exit=True,
        ),
    )

    assert worker.run_once() is True
    assert worker.run_once() is False

    assert worker.stop_event.is_set() is True
    assert len(worker.queue.claim_calls) == 1
    assert len(action.calls) == 1
    assert worker.queue.handoffs == []
    assert worker.queue.retries == []


def test_success_records_result_and_constructs_the_exact_sole_successor(
    tmp_path: Path,
) -> None:
    claim = reward_claim()
    execution = StageExecution.succeeded({"score": 1})
    worker = make_worker(
        tmp_path,
        action=FakeAction(execution),
        claim=claim,
        clock=SequenceClock(STARTED_AT, HANDOFF_AT),
    )

    assert worker.run_once() is True

    assert worker.queue.handoffs == [
        (
            claim,
            QueueMessage(
                schema_version=1,
                event_id=NEXT_EVENT_ID,
                task_id=claim.message.task_id,
                task_version=claim.message.task_version,
                stage=PipelineStage.PUSH,
                attempt=1,
                trace_id=claim.message.trace_id,
                enqueued_at=HANDOFF_AT,
            ),
        )
    ]
    assert NEXT_EVENT_ID != claim.message.event_id
    recorded = worker.store.stage_results
    assert recorded == [
        (
            recorded[0][0],
            claim,
            execution,
            STARTED_AT,
            "worker-1",
            "node-a",
        )
    ]


def test_final_push_success_completes_without_a_successor(tmp_path: Path) -> None:
    claim = pipeline_claim(PipelineStage.PUSH)
    worker = make_worker(
        tmp_path,
        stage=PipelineStage.PUSH,
        claim=claim,
        action=FakeAction(StageExecution.succeeded({"remote_tag": "registry/task:1"})),
    )

    assert worker.run_once() is True

    assert worker.queue.handoffs == [(claim, None)]


def test_rejected_execution_archives_without_handoff(tmp_path: Path) -> None:
    action = FakeAction(StageExecution.rejected({"reason": "hacking"}))
    worker = make_worker(tmp_path, action=action, claim=reward_claim())

    assert worker.run_once() is True
    assert worker.queue.completed_terminal == [reward_claim()]
    assert worker.queue.handoffs == []


def test_action_exception_retries_in_place_before_the_delivery_limit(tmp_path: Path) -> None:
    claim = reward_claim(read_count=2)
    worker = make_worker(
        tmp_path,
        action=FakeAction(error=RuntimeError("temporary outage")),
        claim=claim,
    )

    assert worker.run_once() is True

    assert worker.queue.retries == [claim]
    assert worker.queue.dead_letters == []
    assert worker.store.terminal_failures == []


def test_exhausted_action_exception_records_redacted_failure_and_dead_letters(
    tmp_path: Path,
) -> None:
    claim = reward_claim(read_count=3)
    secret = "ghp_abcdefghijklmnopqrstuvwxyz"
    worker = make_worker(
        tmp_path,
        action=FakeAction(error=RuntimeError(f"credential={secret} " + "x" * 10_000)),
        claim=claim,
    )

    assert worker.run_once() is True

    assert worker.queue.dead_letters == [claim]
    assert worker.queue.retries == []
    assert len(worker.store.terminal_failures) == 1
    stored_error = worker.store.terminal_failures[0][2]
    assert secret not in stored_error
    assert "<REDACTED>" in stored_error
    assert len(stored_error) <= 4_000


def test_stop_request_during_action_allows_completion_but_prevents_future_claims(
    tmp_path: Path,
) -> None:
    stop_event = Event()

    def request_stop(task: PipelineTask, workspace: Path) -> None:
        assert workspace.is_dir()
        stop_event.set()

    action = FakeAction(
        StageExecution.succeeded({"score": 1}),
        side_effect=request_stop,
    )
    claim = reward_claim()
    worker = make_worker(
        tmp_path,
        action=action,
        claim=claim,
        stop_event=stop_event,
    )

    worker.run_forever()

    assert worker.queue.handoffs[0][0] == claim
    assert len(action.calls) == 1
    assert len(worker.queue.claim_calls) == 1
    assert worker.run_once() is False
    assert len(worker.queue.claim_calls) == 1


def test_stop_request_during_long_poll_releases_claim_without_starting_action(
    tmp_path: Path,
) -> None:
    stop_event = Event()
    claim = reward_claim()

    class StopBeforeReturningClaimQueue(FakeQueue):
        def claim(
            self,
            connection: FakeConnection,
            stage: PipelineStage,
            *,
            visibility_timeout_seconds: int,
            quantity: int,
            max_poll_seconds: int,
        ) -> list[ClaimedMessage]:
            claims = super().claim(
                connection,
                stage,
                visibility_timeout_seconds=visibility_timeout_seconds,
                quantity=quantity,
                max_poll_seconds=max_poll_seconds,
            )
            stop_event.set()
            return claims

    queue = StopBeforeReturningClaimQueue([claim])
    action = FakeAction(StageExecution.succeeded({"score": 1}))
    worker = make_worker(
        tmp_path,
        action=action,
        stop_event=stop_event,
        queue=queue,
    )

    assert worker.run_once() is False

    assert action.calls == []
    assert queue.heartbeat_calls == [(worker.connection_factory.connections[1], claim, 1)]
    assert worker.connection_factory.connections[0].exited is True
    assert worker.connection_factory.connections[1].exited is True
    assert queue.handoffs == []
    assert queue.retries == []
    assert queue.dead_letters == []


def test_each_delivery_uses_and_cleans_a_fresh_materialized_workspace(
    tmp_path: Path,
) -> None:
    first_claim = pipeline_claim(PipelineStage.VALIDATE, msg_id=71)
    second_claim = pipeline_claim(PipelineStage.VALIDATE, msg_id=72)
    expected_file = task_file("tests/test.sh", b"#!/bin/sh\nexit 0\n", 0o755)
    observed_workspaces: list[Path] = []

    def inspect_workspace(task: PipelineTask, workspace: Path) -> None:
        observed_workspaces.append(workspace)
        materialized = workspace / "tasks" / task.task_id / expected_file.path
        assert materialized.read_bytes() == expected_file.content
        assert materialized.stat().st_mode & 0o777 == expected_file.mode

    worker = make_worker(
        tmp_path,
        stage=PipelineStage.VALIDATE,
        claims=[first_claim, second_claim],
        action=FakeAction(side_effect=inspect_workspace),
        files=[expected_file],
    )

    assert worker.run_once() is True
    assert worker.run_once() is True

    assert len(observed_workspaces) == 2
    assert observed_workspaces[0] != observed_workspaces[1]
    assert all(not workspace.exists() for workspace in observed_workspaces)
    assert len(worker.store.load_files_calls) == 2


def test_generate_starts_without_loading_or_materializing_stored_files(
    tmp_path: Path,
) -> None:
    claim = pipeline_claim(PipelineStage.GENERATE)

    def inspect_empty_generate_workspace(task: PipelineTask, workspace: Path) -> None:
        assert not (workspace / "tasks" / task.task_id).exists()

    execution = StageExecution.succeeded({"generated": True}, [task_file()])
    worker = make_worker(
        tmp_path,
        stage=PipelineStage.GENERATE,
        claim=claim,
        action=FakeAction(execution, side_effect=inspect_empty_generate_workspace),
        files=[task_file("stale.txt", b"must not load")],
    )

    assert worker.run_once() is True

    assert worker.store.load_files_calls == []


def test_runtime_worker_uses_one_explicitly_bounded_pool_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool_timeouts: list[float | None] = []
    generic_connection_calls: list[None] = []

    class FakePool:
        def connection(self, timeout: float | None = None) -> FakeConnection:
            pool_timeouts.append(timeout)
            return FakeConnection(f"pool-{len(pool_timeouts)}", [])

    def generic_connection() -> FakeConnection:
        generic_connection_calls.append(None)
        return FakeConnection("generic", [])

    monkeypatch.setattr(db_module, "get_pool", lambda: FakePool())
    monkeypatch.setattr(db_module, "connection", generic_connection)
    monkeypatch.setattr(worker_module, "_load_stage_action", lambda stage: FakeAction())

    worker = worker_module._build_runtime_worker(PipelineStage.REWARD)
    first_context = worker.connection_factory()
    second_context = worker.connection_factory()

    assert first_context is not second_context
    assert pool_timeouts == [2.0, 2.0]
    assert generic_connection_calls == []


def test_cleanup_failure_after_durable_completion_does_not_retry_or_dead_letter(
    tmp_path: Path,
) -> None:
    claim = reward_claim()

    class CleanupFailureWorkspaceFactory:
        def __init__(self) -> None:
            self.path: Path | None = None

        def __call__(self, *, root: Path, prefix: str) -> AbstractContextManager[Path]:
            factory = self

            class CleanupFailureWorkspace:
                def __enter__(self) -> Path:
                    factory.path = root / f"{prefix}workspace"
                    factory.path.mkdir()
                    return factory.path

                def __exit__(
                    self,
                    exc_type: type[BaseException] | None,
                    exc: BaseException | None,
                    traceback: object | None,
                ) -> None:
                    assert factory.path is not None
                    rmtree(factory.path)
                    raise RuntimeError("workspace cleanup failed")

            return CleanupFailureWorkspace()

    workspace_factory = CleanupFailureWorkspaceFactory()
    worker = make_worker(
        tmp_path,
        action=FakeAction(StageExecution.succeeded({"score": 1})),
        claim=claim,
        workspace_factory=workspace_factory,
    )

    assert worker.run_once() is True

    assert len(worker.queue.handoffs) == 1
    assert worker.queue.handoffs[0][0] == claim
    assert worker.queue.retries == []
    assert worker.queue.dead_letters == []
    assert len(worker.store.stage_results) == 1
    assert workspace_factory.path is not None
    assert workspace_factory.path.exists() is False
