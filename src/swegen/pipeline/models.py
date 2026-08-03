"""Immutable records shared by pipeline storage, workers, and stage actions."""

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from math import isfinite
from types import MappingProxyType
from uuid import UUID

from swegen.queueing.models import PipelineStage

_TASK_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_REPO_PART_PATTERN = re.compile(r"[A-Za-z0-9_.-]+\Z")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")

type JsonScalar = str | int | float | bool | None
type FrozenJsonValue = JsonScalar | tuple[FrozenJsonValue, ...] | Mapping[str, FrozenJsonValue]


class PipelineTaskState(StrEnum):
    """Durable lifecycle states for one versioned pipeline task."""

    QUEUED = "queued"
    RUNNING = "running"
    REJECTED = "rejected"
    FAILED = "failed"
    COMPLETED = "completed"


class StageResultStatus(StrEnum):
    """Terminal outcomes of one stage attempt."""

    SUCCEEDED = "succeeded"
    REJECTED = "rejected"
    FAILED = "failed"


def _require_positive_int(name: str, value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def _validate_task_id(task_id: object) -> None:
    if not isinstance(task_id, str) or _TASK_ID_PATTERN.fullmatch(task_id) is None:
        raise ValueError("task_id must be a safe non-empty identifier")


def _validate_repo(repo: object) -> None:
    if not isinstance(repo, str):
        raise ValueError("repo must use the OWNER/REPO form")
    parts = repo.split("/")
    if (
        len(parts) != 2
        or any(part in {"", ".", ".."} for part in parts)
        or any(_REPO_PART_PATTERN.fullmatch(part) is None for part in parts)
    ):
        raise ValueError("repo must use the safe OWNER/REPO form")


def _validate_task_file_path(path: object) -> None:
    if not isinstance(path, str) or not path or path.startswith("/") or "\\" in path:
        raise ValueError("path must be a non-empty relative POSIX path")
    if any(part in {"", ".", ".."} for part in path.split("/")):
        raise ValueError("path must not contain empty, '.' or '..' components")


def _freeze_json_value(value: object, active_containers: set[int]) -> FrozenJsonValue:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not isfinite(value):
            raise ValueError("result must contain only finite JSON-safe numbers")
        return value
    if isinstance(value, Mapping):
        container_id = id(value)
        if container_id in active_containers:
            raise ValueError("result must not contain cyclic JSON-safe containers")
        active_containers.add(container_id)
        try:
            frozen: dict[str, FrozenJsonValue] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise ValueError("result JSON-safe object keys must be strings")
                frozen[key] = _freeze_json_value(item, active_containers)
            return MappingProxyType(frozen)
        finally:
            active_containers.remove(container_id)
    if isinstance(value, (list, tuple)):
        container_id = id(value)
        if container_id in active_containers:
            raise ValueError("result must not contain cyclic JSON-safe containers")
        active_containers.add(container_id)
        try:
            return tuple(_freeze_json_value(item, active_containers) for item in value)
        finally:
            active_containers.remove(container_id)
    raise ValueError(f"result contains a non-JSON-safe value: {type(value).__name__}")


def _freeze_json_object(value: object) -> Mapping[str, FrozenJsonValue]:
    if not isinstance(value, Mapping):
        raise ValueError("result must be a JSON-safe object")
    frozen = _freeze_json_value(value, set())
    if not isinstance(frozen, Mapping):
        raise ValueError("result must be a JSON-safe object")
    return frozen


def _plain_json_value(value: FrozenJsonValue) -> object:
    if isinstance(value, Mapping):
        return {key: _plain_json_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_plain_json_value(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class TaskFile:
    """One regular task file stored as bytes and permission bits."""

    path: str
    content: bytes
    mode: int
    sha256: str
    size_bytes: int | None = None

    def __post_init__(self) -> None:
        _validate_task_file_path(self.path)
        if not isinstance(self.content, bytes):
            raise ValueError("content must be bytes")
        if (
            isinstance(self.mode, bool)
            or not isinstance(self.mode, int)
            or not 0 <= self.mode <= 0o777
        ):
            raise ValueError("mode must contain only regular permission bits")
        if not isinstance(self.sha256, str) or _SHA256_PATTERN.fullmatch(self.sha256) is None:
            raise ValueError("sha256 must be a lowercase 64-character hexadecimal digest")

        actual_size = len(self.content)
        if self.size_bytes is None:
            object.__setattr__(self, "size_bytes", actual_size)
        elif (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or self.size_bytes < 0
            or self.size_bytes != actual_size
        ):
            raise ValueError("size_bytes must equal the content length")


@dataclass(frozen=True, slots=True)
class PipelineTask:
    """Authoritative identity and current state for one task version."""

    task_id: str
    task_version: int
    repo: str
    pr: int
    trace_id: UUID
    state: PipelineTaskState = PipelineTaskState.QUEUED
    current_stage: PipelineStage = PipelineStage.GENERATE
    last_error: str | None = None
    last_reason: str | None = None

    def __post_init__(self) -> None:
        _validate_task_id(self.task_id)
        _require_positive_int("task_version", self.task_version)
        _validate_repo(self.repo)
        _require_positive_int("pr", self.pr)
        if not isinstance(self.trace_id, UUID):
            raise ValueError("trace_id must be a UUID")
        try:
            object.__setattr__(self, "state", PipelineTaskState(self.state))
        except (TypeError, ValueError) as error:
            raise ValueError("state must be a fixed pipeline task state") from error
        try:
            object.__setattr__(self, "current_stage", PipelineStage(self.current_stage))
        except (TypeError, ValueError) as error:
            raise ValueError("current_stage must be a fixed pipeline stage") from error
        for name in ("last_error", "last_reason"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"{name} must be a string or None")


@dataclass(frozen=True, slots=True)
class StageExecution:
    """Terminal stage output and optional replacement task files."""

    status: StageResultStatus
    result: Mapping[str, FrozenJsonValue]
    files: tuple[TaskFile, ...] = ()

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "status", StageResultStatus(self.status))
        except (TypeError, ValueError) as error:
            raise ValueError("status must be a fixed stage result status") from error
        frozen_result = _freeze_json_object(self.result)
        files = tuple(self.files)
        if not all(isinstance(task_file, TaskFile) for task_file in files):
            raise ValueError("files must contain only TaskFile records")
        object.__setattr__(self, "result", frozen_result)
        object.__setattr__(self, "files", files)

    @property
    def should_handoff(self) -> bool:
        """Return whether the worker should enqueue the sole successor stage."""

        return self.status is StageResultStatus.SUCCEEDED

    def result_json(self) -> dict[str, object]:
        """Return a fresh mutable plain object suitable for JSONB serialization."""

        return {key: _plain_json_value(value) for key, value in self.result.items()}

    @classmethod
    def succeeded(
        cls,
        result: Mapping[str, object],
        files: Iterable[TaskFile] = (),
    ) -> "StageExecution":
        """Build a successful execution that may hand off generated files."""

        return cls(StageResultStatus.SUCCEEDED, dict(result), tuple(files))

    @classmethod
    def rejected(cls, result: Mapping[str, object]) -> "StageExecution":
        """Build an expected policy rejection with no successor handoff."""

        return cls(StageResultStatus.REJECTED, dict(result))

    @classmethod
    def failed(cls, result: Mapping[str, object]) -> "StageExecution":
        """Build a terminal execution failure with no successor handoff."""

        return cls(StageResultStatus.FAILED, dict(result))


__all__ = [
    "PipelineTask",
    "PipelineTaskState",
    "StageExecution",
    "StageResultStatus",
    "TaskFile",
]
