"""Long-lived PGMQ stage worker with visibility heartbeats."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import tempfile
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, Thread
from types import FrameType
from typing import Any, Protocol
from uuid import UUID, uuid4

from swegen.create.claude_code_utils import redact_sensitive_text
from swegen.pipeline.models import PipelineTask, StageExecution, TaskFile
from swegen.pipeline.task_store import TaskStore, materialize_task_files
from swegen.queueing.models import ClaimedMessage, PipelineStage, QueueMessage
from swegen.queueing.pgmq import PgmqQueue

LOGGER = logging.getLogger(__name__)
MAX_WORKER_ERROR_CHARS = 4_000

type ConnectionFactory = Callable[[], AbstractContextManager[Any]]


class StageAction(Protocol):
    """Execute one pipeline stage inside a per-delivery workspace."""

    def __call__(self, task: PipelineTask, workspace: Path, /) -> StageExecution: ...


class HeartbeatFactory(Protocol):
    """Build the visibility guard used around one stage action."""

    def __call__(
        self,
        *,
        connection_factory: ConnectionFactory,
        queue: PgmqQueue,
        claim: ClaimedMessage,
        visibility_timeout_seconds: int,
        interval_seconds: float,
    ) -> AbstractContextManager[object]: ...


def _positive_integer(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _positive_interval(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return float(value)


def _safe_error_text(error: BaseException) -> str:
    """Return a bounded credential-redacted exception summary."""

    detail = str(error).strip()
    summary = type(error).__name__ if not detail else f"{type(error).__name__}: {detail}"
    return redact_sensitive_text(summary)[:MAX_WORKER_ERROR_CHARS]


def _first_identity(*environment_names: str) -> str:
    for name in environment_names:
        value = os.environ.get(name, "").strip()
        if value:
            return value[:255]
    hostname = socket.gethostname().strip()
    return (hostname or "unknown-worker")[:255]


def _default_workspace_root() -> Path:
    configured = os.environ.get("SWEGEN_WORKSPACE_ROOT", "").strip()
    return Path(configured) if configured else Path(tempfile.gettempdir()) / "swegen-worker"


@dataclass(frozen=True, slots=True)
class WorkerSettings:
    """Timing, retry, and workspace settings for one stage worker."""

    claim_quantity: int = 1
    visibility_timeout_seconds: int = 300
    heartbeat_interval_seconds: float = 60
    poll_seconds: int = 10
    max_deliveries: int = 3
    retry_visibility_timeout_seconds: int = 300
    workspace_root: Path = field(default_factory=_default_workspace_root)

    def __post_init__(self) -> None:
        claim_quantity = _positive_integer("claim_quantity", self.claim_quantity)
        visibility = _positive_integer(
            "visibility_timeout_seconds", self.visibility_timeout_seconds
        )
        heartbeat = _positive_interval(
            "heartbeat_interval_seconds", self.heartbeat_interval_seconds
        )
        poll_seconds = _positive_integer("poll_seconds", self.poll_seconds)
        max_deliveries = _positive_integer("max_deliveries", self.max_deliveries)
        retry_visibility = _positive_integer(
            "retry_visibility_timeout_seconds",
            self.retry_visibility_timeout_seconds,
        )
        if heartbeat >= visibility:
            raise ValueError(
                "heartbeat_interval_seconds must be shorter than visibility_timeout_seconds"
            )
        try:
            workspace_root = Path(self.workspace_root)
        except TypeError as error:
            raise ValueError("workspace_root must be a filesystem path") from error

        object.__setattr__(self, "claim_quantity", claim_quantity)
        object.__setattr__(self, "visibility_timeout_seconds", visibility)
        object.__setattr__(self, "heartbeat_interval_seconds", heartbeat)
        object.__setattr__(self, "poll_seconds", poll_seconds)
        object.__setattr__(self, "max_deliveries", max_deliveries)
        object.__setattr__(
            self,
            "retry_visibility_timeout_seconds",
            retry_visibility,
        )
        object.__setattr__(self, "workspace_root", workspace_root)


class ClaimHeartbeat:
    """Extend one PGMQ claim using independent, promptly committed connections."""

    def __init__(
        self,
        *,
        connection_factory: ConnectionFactory,
        queue: PgmqQueue,
        claim: ClaimedMessage,
        visibility_timeout_seconds: int,
        interval_seconds: float,
    ) -> None:
        self._connection_factory = connection_factory
        self._queue = queue
        self._claim = claim
        self._visibility_timeout_seconds = _positive_integer(
            "visibility_timeout_seconds", visibility_timeout_seconds
        )
        self._interval_seconds = _positive_interval("interval_seconds", interval_seconds)
        self._stopped = Event()
        self._thread: Thread | None = None

    @property
    def is_running(self) -> bool:
        """Return whether the background heartbeat thread is alive."""

        return self._thread is not None and self._thread.is_alive()

    def __enter__(self) -> ClaimHeartbeat:
        if self._thread is not None:
            raise RuntimeError("ClaimHeartbeat contexts cannot be reused")
        self._thread = Thread(
            target=self._run,
            name=f"pgmq-heartbeat-{self._claim.queue.value}-{self._claim.msg_id}",
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        self._stopped.set()
        if self._thread is not None:
            self._thread.join()

    def _run(self) -> None:
        current = self._claim
        while not self._stopped.wait(self._interval_seconds):
            try:
                with self._connection_factory() as connection:
                    current = self._queue.heartbeat(
                        connection,
                        current,
                        visibility_timeout_seconds=self._visibility_timeout_seconds,
                    )
            except Exception as error:  # keep stage work alive; PGMQ remains crash recovery
                LOGGER.error(
                    "visibility heartbeat failed for %s/%s: %s",
                    current.queue.value,
                    current.msg_id,
                    _safe_error_text(error),
                )


class PipelineWorker:
    """Claim, execute, and atomically complete messages for one pipeline stage."""

    def __init__(
        self,
        *,
        stage: PipelineStage,
        connection_factory: ConnectionFactory,
        queue: PgmqQueue,
        store: TaskStore,
        action: StageAction,
        settings: WorkerSettings | None = None,
        worker_id: str | None = None,
        node_name: str | None = None,
        clock: Callable[[], datetime] | None = None,
        uuid_factory: Callable[[], UUID] | None = None,
        stop_event: Event | None = None,
        heartbeat_factory: HeartbeatFactory | None = None,
    ) -> None:
        self.stage = PipelineStage(stage)
        self.connection_factory = connection_factory
        self.queue = queue
        self.store = store
        self.action = action
        self.settings = settings or WorkerSettings()
        self.worker_id = (
            worker_id or _first_identity("SWEGEN_WORKER_ID", "POD_NAME", "HOSTNAME")
        ).strip()
        self.node_name = (node_name or _first_identity("SWEGEN_NODE_NAME", "NODE_NAME")).strip()
        if not self.worker_id:
            raise ValueError("worker_id must be nonblank")
        if not self.node_name:
            raise ValueError("node_name must be nonblank")
        self._clock = clock or (lambda: datetime.now(UTC))
        self._uuid_factory = uuid_factory or uuid4
        self.stop_event = stop_event or Event()
        self._heartbeat_factory = heartbeat_factory or ClaimHeartbeat

    def request_stop(
        self,
        signum: int | None = None,
        frame: FrameType | None = None,
    ) -> None:
        """Prevent future claims without interrupting an active delivery."""

        del signum, frame
        self.stop_event.set()

    def run_once(self) -> bool:
        """Claim and process available deliveries, returning whether work was found."""

        if self.stop_event.is_set():
            return False

        with self.connection_factory() as connection:
            claims = self.queue.claim(
                connection,
                self.stage,
                visibility_timeout_seconds=self.settings.visibility_timeout_seconds,
                quantity=self.settings.claim_quantity,
                max_poll_seconds=self.settings.poll_seconds,
            )

        if not claims:
            return False
        for claim in claims:
            self._process_claim(claim)
        return True

    def run_forever(self) -> None:
        """Poll until a stop request arrives, allowing any active action to finish."""

        while not self.stop_event.is_set():
            try:
                self.run_once()
            except Exception as error:
                LOGGER.error("pipeline worker poll failed: %s", _safe_error_text(error))
                self.stop_event.wait(self.settings.poll_seconds)

    def _process_claim(self, claim: ClaimedMessage) -> None:
        started_at = self._now()
        try:
            self.settings.workspace_root.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(
                dir=self.settings.workspace_root,
                prefix=f"{self.stage.value}-{claim.msg_id}-",
            ) as temporary_directory:
                workspace = Path(temporary_directory)
                with self._heartbeat_factory(
                    connection_factory=self.connection_factory,
                    queue=self.queue,
                    claim=claim,
                    visibility_timeout_seconds=self.settings.visibility_timeout_seconds,
                    interval_seconds=self.settings.heartbeat_interval_seconds,
                ):
                    task, files = self._load_task(claim)
                    tasks_directory = workspace / "tasks"
                    tasks_directory.mkdir()
                    if claim.message.stage is not PipelineStage.GENERATE:
                        materialize_task_files(files, tasks_directory / task.task_id)
                    execution = self.action(task, workspace)
                    if not isinstance(execution, StageExecution):
                        raise TypeError("stage action must return a StageExecution")
                self._complete(claim, execution, started_at=started_at)
        except Exception as error:
            self._retry_or_dead_letter(claim, error, started_at=started_at)

    def _load_task(self, claim: ClaimedMessage) -> tuple[PipelineTask, tuple[TaskFile, ...]]:
        message = claim.message
        with self.connection_factory() as connection:
            task = self.store.get_task(
                connection,
                message.task_id,
                message.task_version,
            )
            files = (
                ()
                if message.stage is PipelineStage.GENERATE
                else self.store.load_files(
                    connection,
                    message.task_id,
                    message.task_version,
                )
            )

        if (
            task.task_id != message.task_id
            or task.task_version != message.task_version
            or task.trace_id != message.trace_id
            or task.current_stage is not message.stage
        ):
            raise RuntimeError("claimed message does not match the authoritative task state")
        return task, tuple(files)

    def _complete(
        self,
        claim: ClaimedMessage,
        execution: StageExecution,
        *,
        started_at: datetime,
    ) -> None:
        def record_stage_result(connection: Any, current: ClaimedMessage) -> bool:
            return self.store.record_stage_result(
                connection,
                current,
                execution,
                started_at=started_at,
                worker_id=self.worker_id,
                node_name=self.node_name,
            )

        with self.connection_factory() as connection:
            if execution.should_handoff:
                self.queue.complete_and_handoff(
                    connection,
                    claim,
                    self._successor(claim),
                    complete_stage=record_stage_result,
                )
            else:
                self.queue.complete_terminal(
                    connection,
                    claim,
                    complete_stage=record_stage_result,
                )

    def _successor(self, claim: ClaimedMessage) -> QueueMessage | None:
        message = claim.message
        next_stage = message.stage.next_stage
        if next_stage is None:
            return None
        return QueueMessage(
            schema_version=1,
            event_id=self._uuid_factory(),
            task_id=message.task_id,
            task_version=message.task_version,
            stage=next_stage,
            attempt=1,
            trace_id=message.trace_id,
            enqueued_at=self._now(),
        )

    def _retry_or_dead_letter(
        self,
        claim: ClaimedMessage,
        error: Exception,
        *,
        started_at: datetime,
    ) -> None:
        safe_error = _safe_error_text(error)
        LOGGER.error(
            "stage %s delivery %s/%s failed: %s",
            self.stage.value,
            claim.queue.value,
            claim.msg_id,
            safe_error,
        )

        def record_terminal_failure(connection: Any, current: ClaimedMessage) -> bool:
            return self.store.record_terminal_failure(
                connection,
                current,
                safe_error,
                started_at=started_at,
                worker_id=self.worker_id,
                node_name=self.node_name,
            )

        with self.connection_factory() as connection:
            self.queue.retry_or_dead_letter(
                connection,
                claim,
                max_deliveries=self.settings.max_deliveries,
                retry_visibility_timeout_seconds=(self.settings.retry_visibility_timeout_seconds),
                record_terminal_failure=record_terminal_failure,
            )

    def _now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("worker clock must return a timezone-aware datetime")
        return value


def _parse_stage(value: str) -> PipelineStage:
    try:
        return PipelineStage(value)
    except ValueError as error:
        choices = ", ".join(stage.value for stage in PipelineStage)
        raise argparse.ArgumentTypeError(f"stage must be one of: {choices}") from error


def _load_stage_action(stage: PipelineStage) -> StageAction:
    # Task 5 supplies this module. Keep the import lazy so Task 4 remains importable.
    from swegen.pipeline.actions import action_for_stage

    return action_for_stage(stage)


def _build_runtime_worker(stage: PipelineStage) -> PipelineWorker:
    from swegen.db import connection

    return PipelineWorker(
        stage=stage,
        connection_factory=connection,
        queue=PgmqQueue(),
        store=TaskStore(),
        action=_load_stage_action(stage),
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run one configured stage worker via ``python -m``."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", required=True, type=_parse_stage)
    arguments = parser.parse_args(argv)

    worker = _build_runtime_worker(arguments.stage)
    signal.signal(signal.SIGTERM, worker.request_stop)
    signal.signal(signal.SIGINT, worker.request_stop)
    worker.run_forever()
    return 0


__all__ = [
    "ClaimHeartbeat",
    "PipelineWorker",
    "StageAction",
    "WorkerSettings",
    "main",
]


if __name__ == "__main__":  # pragma: no cover - exercised by container runtime
    raise SystemExit(main())
