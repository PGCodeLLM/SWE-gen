"""Safe filesystem views and transaction-friendly PostgreSQL task storage."""

from __future__ import annotations

import json
import os
import re
import stat
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Protocol
from uuid import UUID

from swegen.create.claude_code_utils import redact_sensitive_text
from swegen.pipeline.models import (
    PipelineTask,
    PipelineTaskState,
    StageExecution,
    StageResultStatus,
    TaskFile,
)
from swegen.queueing.models import ClaimedMessage, PipelineStage, queue_for_stage

DEFAULT_MAX_FILE_BYTES = 128 * 1024 * 1024
DEFAULT_MAX_TASK_BYTES = 512 * 1024 * 1024
MAX_STORED_ERROR_CHARS = 4_000

_TASK_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_DIRECTORY_OPEN_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_CLOEXEC", 0)
)
_FILE_READ_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NONBLOCK", 0)
)
_FILE_WRITE_FLAGS = (
    os.O_WRONLY
    | os.O_CREAT
    | os.O_EXCL
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_CLOEXEC", 0)
)

_GET_TASK_SQL = """
    SELECT task_id, task_version, repo, pr, trace_id, state, current_stage
    FROM pipeline_tasks
    WHERE task_id = %s AND task_version = %s
"""
_LOAD_FILES_SQL = """
    SELECT path, content, mode, sha256, size_bytes
    FROM pipeline_task_files
    WHERE task_id = %s AND task_version = %s
    ORDER BY path
"""
_DELETE_FILES_SQL = """
    DELETE FROM pipeline_task_files
    WHERE task_id = %s AND task_version = %s
"""
_INSERT_FILE_SQL = """
    INSERT INTO pipeline_task_files (
        task_id, task_version, path, content, mode, size_bytes, sha256
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
_INSERT_STAGE_RESULT_SQL = """
    INSERT INTO pipeline_stage_results (
        task_id, task_version, stage, attempt, status,
        pgmq_msg_id, pgmq_read_count, worker_id, node_name,
        started_at, finished_at, result, error
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
    ON CONFLICT (task_id, task_version, stage, attempt) DO NOTHING
    RETURNING task_id, task_version, stage, attempt
"""
_UPDATE_TASK_SQL = """
    UPDATE pipeline_tasks
    SET state = %s, current_stage = %s, updated_at = %s, finished_at = %s,
        last_error = %s, last_reason = %s
    WHERE task_id = %s AND task_version = %s AND trace_id = %s AND current_stage = %s
"""
_INSERT_PUSHED_IMAGE_SQL = """
    INSERT INTO pushed_images (
        instance, registry, suffix, swr_url, pushed, event, payload
    ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
"""


class CursorLike(Protocol):
    """Subset of a psycopg cursor used by the task store."""

    rowcount: int

    def fetchone(self) -> object | None: ...

    def fetchall(self) -> list[object]: ...


class ConnectionLike(Protocol):
    """Subset of a caller-owned psycopg connection used by the task store."""

    def execute(self, query: str, params: Sequence[object] | None = None) -> CursorLike: ...


class TaskStoreError(RuntimeError):
    """Base error for invalid or inconsistent task storage operations."""


class TaskNotFoundError(TaskStoreError):
    """Raised when a requested task version does not exist."""


class TaskFileError(TaskStoreError):
    """Raised when task files cannot be captured or materialized safely."""


def _require_positive_limit(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise TaskFileError(f"{name} must be a positive integer")
    return value


def _safe_relative_path(path: object) -> str:
    if not isinstance(path, str) or not path or path.startswith("/") or "\\" in path:
        raise TaskFileError("task file path must be a non-empty relative POSIX path")
    parts = path.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise TaskFileError("task file path must not contain empty, '.' or '..' components")
    try:
        path.encode("utf-8")
    except UnicodeEncodeError as error:
        raise TaskFileError("task file path must be valid UTF-8") from error
    return path


def _same_entry(first: os.stat_result, second: os.stat_result) -> bool:
    return first.st_dev == second.st_dev and first.st_ino == second.st_ino


def _read_captured_file(
    directory_fd: int,
    name: str,
    relative_path: str,
    observed: os.stat_result,
    *,
    max_file_bytes: int,
) -> tuple[bytes, int]:
    try:
        file_fd = os.open(name, _FILE_READ_FLAGS, dir_fd=directory_fd)
    except OSError as error:
        raise TaskFileError(f"cannot safely open task file {relative_path!r}: {error}") from error

    try:
        before = os.fstat(file_fd)
        if not stat.S_ISREG(before.st_mode):
            raise TaskFileError(f"task entry {relative_path!r} is not a regular file")
        if not _same_entry(observed, before):
            raise TaskFileError(f"task file {relative_path!r} changed while being inspected")
        if before.st_size > max_file_bytes:
            raise TaskFileError(
                f"task file {relative_path!r} exceeds the per-file limit of {max_file_bytes} bytes"
            )

        chunks: list[bytes] = []
        captured_size = 0
        while True:
            chunk = os.read(file_fd, min(1024 * 1024, max_file_bytes + 1 - captured_size))
            if not chunk:
                break
            chunks.append(chunk)
            captured_size += len(chunk)
            if captured_size > max_file_bytes:
                raise TaskFileError(
                    f"task file {relative_path!r} exceeds the per-file limit of "
                    f"{max_file_bytes} bytes"
                )

        after = os.fstat(file_fd)
        content = b"".join(chunks)
        if (
            not _same_entry(before, after)
            or before.st_size != len(content)
            or after.st_size != len(content)
            or before.st_mtime_ns != after.st_mtime_ns
            or before.st_ctime_ns != after.st_ctime_ns
        ):
            raise TaskFileError(
                f"task file {relative_path!r} changed size or content while being captured"
            )
        return content, after.st_mode & 0o777
    except OSError as error:
        raise TaskFileError(f"cannot read task file {relative_path!r}: {error}") from error
    finally:
        os.close(file_fd)


def _capture_directory(
    directory_fd: int,
    relative_parts: tuple[str, ...],
    captured: list[TaskFile],
    total_bytes: int,
    *,
    max_file_bytes: int,
    max_task_bytes: int,
) -> int:
    try:
        with os.scandir(directory_fd) as entries:
            ordered_entries = sorted(entries, key=lambda entry: entry.name)
    except OSError as error:
        location = "/".join(relative_parts) or "."
        raise TaskFileError(f"cannot scan task directory {location!r}: {error}") from error

    for entry in ordered_entries:
        relative_path = _safe_relative_path("/".join((*relative_parts, entry.name)))
        try:
            observed = entry.stat(follow_symlinks=False)
        except OSError as error:
            raise TaskFileError(f"cannot inspect task entry {relative_path!r}: {error}") from error

        if stat.S_ISLNK(observed.st_mode):
            raise TaskFileError(f"task entry {relative_path!r} is a symlink")
        if stat.S_ISDIR(observed.st_mode):
            try:
                child_fd = os.open(entry.name, _DIRECTORY_OPEN_FLAGS, dir_fd=directory_fd)
            except OSError as error:
                raise TaskFileError(
                    f"cannot safely open task directory {relative_path!r}: {error}"
                ) from error
            try:
                opened = os.fstat(child_fd)
                if not stat.S_ISDIR(opened.st_mode) or not _same_entry(observed, opened):
                    raise TaskFileError(
                        f"task directory {relative_path!r} changed while being inspected"
                    )
                total_bytes = _capture_directory(
                    child_fd,
                    (*relative_parts, entry.name),
                    captured,
                    total_bytes,
                    max_file_bytes=max_file_bytes,
                    max_task_bytes=max_task_bytes,
                )
            finally:
                os.close(child_fd)
            continue
        if not stat.S_ISREG(observed.st_mode):
            raise TaskFileError(f"task entry {relative_path!r} is not a regular file")

        if total_bytes + observed.st_size > max_task_bytes:
            raise TaskFileError(f"task files exceed the total task limit of {max_task_bytes} bytes")
        content, mode = _read_captured_file(
            directory_fd,
            entry.name,
            relative_path,
            observed,
            max_file_bytes=max_file_bytes,
        )
        total_bytes += len(content)
        if total_bytes > max_task_bytes:
            raise TaskFileError(f"task files exceed the total task limit of {max_task_bytes} bytes")
        captured.append(
            TaskFile(
                path=relative_path,
                content=content,
                mode=mode,
                sha256=sha256(content).hexdigest(),
            )
        )
    return total_bytes


def capture_task_files(
    root: Path,
    *,
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES,
    max_task_bytes: int = DEFAULT_MAX_TASK_BYTES,
) -> tuple[TaskFile, ...]:
    """Capture one task root as deterministic regular-file records.

    Directory traversal is descriptor-relative and every opened entry uses
    ``O_NOFOLLOW`` where the platform provides it. This keeps symlink changes
    and common path races from redirecting capture outside ``root``.
    """

    max_file_bytes = _require_positive_limit("max_file_bytes", max_file_bytes)
    max_task_bytes = _require_positive_limit("max_task_bytes", max_task_bytes)
    try:
        root = Path(root)
        root_stat = os.lstat(root)
    except FileNotFoundError as error:
        raise TaskFileError(f"task root is not a directory: {root}") from error
    except (OSError, TypeError) as error:
        raise TaskFileError(f"cannot inspect task root {root!r}: {error}") from error

    if stat.S_ISLNK(root_stat.st_mode):
        raise TaskFileError(f"task root must not be a symlink: {root}")
    if not stat.S_ISDIR(root_stat.st_mode):
        raise TaskFileError(f"task root is not a directory: {root}")

    try:
        root_fd = os.open(root, _DIRECTORY_OPEN_FLAGS)
    except OSError as error:
        raise TaskFileError(f"cannot safely open task root {root}: {error}") from error
    try:
        opened = os.fstat(root_fd)
        if not stat.S_ISDIR(opened.st_mode) or not _same_entry(root_stat, opened):
            raise TaskFileError("task root changed while being inspected")
        captured: list[TaskFile] = []
        _capture_directory(
            root_fd,
            (),
            captured,
            0,
            max_file_bytes=max_file_bytes,
            max_task_bytes=max_task_bytes,
        )
        return tuple(sorted(captured, key=lambda task_file: task_file.path))
    finally:
        os.close(root_fd)


def _validate_task_files(
    files: Iterable[TaskFile],
    *,
    verify_integrity: bool,
) -> tuple[TaskFile, ...]:
    try:
        task_files = tuple(files)
    except TypeError as error:
        raise TaskFileError("files must be an iterable of TaskFile records") from error

    seen: set[str] = set()
    for task_file in task_files:
        if not isinstance(task_file, TaskFile):
            raise TaskFileError("files must contain only TaskFile records")
        path = _safe_relative_path(task_file.path)
        if path in seen:
            raise TaskFileError(f"duplicate task file path: {path}")
        seen.add(path)
        if not isinstance(task_file.content, bytes):
            raise TaskFileError(f"task file {path!r} content must be bytes")
        if (
            isinstance(task_file.mode, bool)
            or not isinstance(task_file.mode, int)
            or not 0 <= task_file.mode <= 0o777
        ):
            raise TaskFileError(f"task file {path!r} mode must contain permission bits only")
        if (
            isinstance(task_file.size_bytes, bool)
            or not isinstance(task_file.size_bytes, int)
            or task_file.size_bytes < 0
            or task_file.size_bytes != len(task_file.content)
        ):
            raise TaskFileError(f"task file {path!r} stored size does not match its content")
        if (
            not isinstance(task_file.sha256, str)
            or _SHA256_PATTERN.fullmatch(task_file.sha256) is None
        ):
            raise TaskFileError(f"task file {path!r} has an invalid digest")
        if verify_integrity and sha256(task_file.content).hexdigest() != task_file.sha256:
            raise TaskFileError(f"task file {path!r} digest does not match its content")

    for path in seen:
        parts = path.split("/")
        for length in range(1, len(parts)):
            if "/".join(parts[:length]) in seen:
                raise TaskFileError(f"task file path conflicts with a parent file: {path}")
    return tuple(sorted(task_files, key=lambda task_file: task_file.path))


def _reject_symlinked_destination_components(destination: Path) -> Path:
    absolute = Path(os.path.abspath(destination))
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current /= part
        try:
            current_stat = os.lstat(current)
        except FileNotFoundError:
            break
        except OSError as error:
            raise TaskFileError(f"cannot inspect destination path {current}: {error}") from error
        if stat.S_ISLNK(current_stat.st_mode):
            raise TaskFileError(f"destination path must not contain a symlink: {current}")
    return absolute


def _prepare_destination(destination: Path) -> int:
    destination = _reject_symlinked_destination_components(destination)
    try:
        destination_stat = os.lstat(destination)
    except FileNotFoundError:
        try:
            os.mkdir(destination, 0o755)
        except OSError as error:
            raise TaskFileError(
                f"cannot create destination directory {destination}: {error}"
            ) from error
    except OSError as error:
        raise TaskFileError(
            f"cannot inspect destination directory {destination}: {error}"
        ) from error
    else:
        if stat.S_ISLNK(destination_stat.st_mode):
            raise TaskFileError(f"destination must not be a symlink: {destination}")
        if not stat.S_ISDIR(destination_stat.st_mode):
            raise TaskFileError(f"destination is not a directory: {destination}")
        try:
            with os.scandir(destination) as entries:
                if next(entries, None) is not None:
                    raise TaskFileError(f"destination directory must be empty: {destination}")
        except TaskFileError:
            raise
        except OSError as error:
            raise TaskFileError(
                f"cannot scan destination directory {destination}: {error}"
            ) from error

    try:
        destination_fd = os.open(destination, _DIRECTORY_OPEN_FLAGS)
    except OSError as error:
        raise TaskFileError(
            f"cannot safely open destination directory {destination}: {error}"
        ) from error
    try:
        with os.scandir(destination_fd) as entries:
            if next(entries, None) is not None:
                raise TaskFileError(f"destination directory must be empty: {destination}")
    except BaseException:
        os.close(destination_fd)
        raise
    return destination_fd


def _write_all(file_fd: int, content: bytes) -> None:
    view = memoryview(content)
    written = 0
    while written < len(view):
        count = os.write(file_fd, view[written:])
        if count <= 0:
            raise OSError("write returned no progress")
        written += count


def _materialize_one(destination_fd: int, task_file: TaskFile) -> None:
    parts = task_file.path.split("/")
    parent_fd = os.dup(destination_fd)
    try:
        for part in parts[:-1]:
            try:
                os.mkdir(part, 0o755, dir_fd=parent_fd)
            except FileExistsError:
                pass
            child_fd = os.open(part, _DIRECTORY_OPEN_FLAGS, dir_fd=parent_fd)
            os.close(parent_fd)
            parent_fd = child_fd

        file_fd = os.open(parts[-1], _FILE_WRITE_FLAGS, 0o600, dir_fd=parent_fd)
        try:
            _write_all(file_fd, task_file.content)
            os.fchmod(file_fd, task_file.mode)
        finally:
            os.close(file_fd)
    except OSError as error:
        raise TaskFileError(
            f"cannot safely materialize task file {task_file.path!r}: {error}"
        ) from error
    finally:
        os.close(parent_fd)


def materialize_task_files(files: Iterable[TaskFile], destination: Path) -> None:
    """Write verified task records into a new or empty disposable directory."""

    task_files = _validate_task_files(files, verify_integrity=True)
    try:
        destination = Path(destination)
    except TypeError as error:
        raise TaskFileError("destination must be a filesystem path") from error
    destination_fd = _prepare_destination(destination)
    try:
        for task_file in task_files:
            _materialize_one(destination_fd, task_file)
    finally:
        os.close(destination_fd)


def _require_task_identity(task_id: object, task_version: object) -> tuple[str, int]:
    if not isinstance(task_id, str) or _TASK_ID_PATTERN.fullmatch(task_id) is None:
        raise ValueError("task_id must be a safe non-empty identifier")
    if isinstance(task_version, bool) or not isinstance(task_version, int) or task_version <= 0:
        raise ValueError("task_version must be a positive integer")
    return task_id, task_version


def _row_value(row: object, index: int, name: str) -> object:
    if isinstance(row, Mapping):
        try:
            return row[name]
        except KeyError as error:
            raise TaskStoreError(f"database row is missing {name!r}") from error
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        try:
            return row[index]
        except IndexError as error:
            raise TaskStoreError(f"database row is missing column {index}") from error
    raise TaskStoreError(f"unsupported database row type: {type(row).__name__}")


def _decode_task(row: object) -> PipelineTask:
    try:
        raw_trace_id = _row_value(row, 4, "trace_id")
        trace_id = raw_trace_id if isinstance(raw_trace_id, UUID) else UUID(str(raw_trace_id))
        return PipelineTask(
            task_id=_row_value(row, 0, "task_id"),  # type: ignore[arg-type]
            task_version=_row_value(row, 1, "task_version"),  # type: ignore[arg-type]
            repo=_row_value(row, 2, "repo"),  # type: ignore[arg-type]
            pr=_row_value(row, 3, "pr"),  # type: ignore[arg-type]
            trace_id=trace_id,
            state=_row_value(row, 5, "state"),  # type: ignore[arg-type]
            current_stage=_row_value(row, 6, "current_stage"),  # type: ignore[arg-type]
        )
    except TaskStoreError:
        raise
    except (TypeError, ValueError) as error:
        raise TaskStoreError(f"invalid pipeline task row: {error}") from error


def _decode_task_file(row: object) -> TaskFile:
    try:
        raw_content = _row_value(row, 1, "content")
        if isinstance(raw_content, memoryview):
            content = raw_content.tobytes()
        elif isinstance(raw_content, bytearray):
            content = bytes(raw_content)
        else:
            content = raw_content
        return TaskFile(
            path=_row_value(row, 0, "path"),  # type: ignore[arg-type]
            content=content,  # type: ignore[arg-type]
            mode=_row_value(row, 2, "mode"),  # type: ignore[arg-type]
            sha256=_row_value(row, 3, "sha256"),  # type: ignore[arg-type]
            size_bytes=_row_value(row, 4, "size_bytes"),  # type: ignore[arg-type]
        )
    except TaskStoreError:
        raise
    except (TypeError, ValueError) as error:
        raise TaskStoreError(f"invalid pipeline task file row: {error}") from error


def _json_payload(value: dict[str, object]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _require_aware_datetime(name: str, value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value


def _require_nonblank(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonblank string")
    return value.strip()


def _bounded_redacted_error(error: object) -> str:
    error = _require_nonblank("error", error)
    return redact_sensitive_text(error)[:MAX_STORED_ERROR_CHARS]


def _result_text(result: Mapping[str, object], key: str) -> str | None:
    value = result.get(key)
    if value is None:
        return None
    text = value if isinstance(value, str) else str(value)
    if not text.strip():
        return None
    return redact_sensitive_text(text)[:MAX_STORED_ERROR_CHARS]


def _validate_claim(claim: ClaimedMessage) -> None:
    if not isinstance(claim, ClaimedMessage):
        raise TypeError("claim must be a ClaimedMessage")
    message = claim.message
    _require_task_identity(message.task_id, message.task_version)
    if (
        isinstance(message.attempt, bool)
        or not isinstance(message.attempt, int)
        or message.attempt <= 0
    ):
        raise ValueError("attempt must be a positive integer")
    if not isinstance(message.trace_id, UUID):
        raise ValueError("trace_id must be a UUID")
    expected_queue = queue_for_stage(message.stage)
    if claim.queue is not expected_queue:
        raise TaskStoreError(
            f"claim queue {claim.queue.value!r} does not match stage {message.stage.value!r}"
        )
    if claim.msg_id <= 0 or claim.read_count <= 0:
        raise ValueError("claim PGMQ identity values must be positive")


def _returned_stage_identity(row: object) -> tuple[object, object, object, object]:
    return (
        _row_value(row, 0, "task_id"),
        _row_value(row, 1, "task_version"),
        _row_value(row, 2, "stage"),
        _row_value(row, 3, "attempt"),
    )


def _push_inventory_fields(result: Mapping[str, object]) -> tuple[str, str | None, str]:
    remote_tag = result.get("remote_tag")
    if not isinstance(remote_tag, str) or not remote_tag.strip():
        raise TaskStoreError("successful push result requires a nonblank remote_tag")
    registry = result.get("registry")
    if registry is not None and (not isinstance(registry, str) or not registry.strip()):
        raise TaskStoreError("push result registry must be a nonblank string when supplied")
    suffix = result.get("suffix", "")
    if not isinstance(suffix, str):
        raise TaskStoreError("push result suffix must be a string when supplied")
    return remote_tag.strip(), registry.strip() if isinstance(registry, str) else None, suffix


class TaskStore:
    """Store pipeline tasks using a connection and transaction owned by the caller."""

    def __init__(self, *, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock or (lambda: datetime.now(UTC))

    def get_task(
        self,
        connection: ConnectionLike,
        task_id: str,
        task_version: int,
    ) -> PipelineTask:
        """Load one authoritative task version."""

        task_id, task_version = _require_task_identity(task_id, task_version)
        row = connection.execute(_GET_TASK_SQL, (task_id, task_version)).fetchone()
        if row is None:
            raise TaskNotFoundError(f"task {task_id!r} version {task_version} was not found")
        task = _decode_task(row)
        if task.task_id != task_id or task.task_version != task_version:
            raise TaskStoreError("database returned a different task identity")
        return task

    def load_files(
        self,
        connection: ConnectionLike,
        task_id: str,
        task_version: int,
    ) -> tuple[TaskFile, ...]:
        """Load a task version's files in deterministic path order."""

        task_id, task_version = _require_task_identity(task_id, task_version)
        rows = connection.execute(_LOAD_FILES_SQL, (task_id, task_version)).fetchall()
        files = tuple(_decode_task_file(row) for row in rows)
        return tuple(sorted(files, key=lambda task_file: task_file.path))

    def replace_files(
        self,
        connection: ConnectionLike,
        task: PipelineTask,
        files: tuple[TaskFile, ...],
    ) -> None:
        """Replace all normalized files for one task version without committing."""

        if not isinstance(task, PipelineTask):
            raise TypeError("task must be a PipelineTask")
        task_files = _validate_task_files(files, verify_integrity=False)
        self._replace_files_by_identity(
            connection,
            task.task_id,
            task.task_version,
            task_files,
        )

    def _replace_files_by_identity(
        self,
        connection: ConnectionLike,
        task_id: str,
        task_version: int,
        files: tuple[TaskFile, ...],
    ) -> None:
        connection.execute(_DELETE_FILES_SQL, (task_id, task_version))
        for task_file in files:
            connection.execute(
                _INSERT_FILE_SQL,
                (
                    task_id,
                    task_version,
                    task_file.path,
                    task_file.content,
                    task_file.mode,
                    task_file.size_bytes,
                    task_file.sha256,
                ),
            )

    def record_stage_result(
        self,
        connection: ConnectionLike,
        claim: ClaimedMessage,
        execution: StageExecution,
        *,
        started_at: datetime,
        worker_id: str,
        node_name: str,
    ) -> bool:
        """Record one idempotent stage result and mutate its task when newly inserted."""

        _validate_claim(claim)
        if not isinstance(execution, StageExecution):
            raise TypeError("execution must be a StageExecution")
        started_at = _require_aware_datetime("started_at", started_at)
        worker_id = _require_nonblank("worker_id", worker_id)
        node_name = _require_nonblank("node_name", node_name)
        finished_at = _require_aware_datetime("clock result", self._clock()).astimezone(UTC)
        if finished_at < started_at:
            raise ValueError("started_at must not be after the completion clock")

        message = claim.message
        task_files = _validate_task_files(execution.files, verify_integrity=False)
        generate_success = (
            message.stage is PipelineStage.GENERATE
            and execution.status is StageResultStatus.SUCCEEDED
        )
        if generate_success and not task_files:
            raise TaskFileError("successful generate requires non-empty task files")
        if task_files and not generate_success:
            raise TaskFileError("only successful generate results may contain task files")

        result = execution.result_json()
        push_fields: tuple[str, str | None, str] | None = None
        if message.stage is PipelineStage.PUSH and execution.status is StageResultStatus.SUCCEEDED:
            push_fields = _push_inventory_fields(result)

        error: str | None = None
        if execution.status is StageResultStatus.REJECTED:
            error = _result_text(result, "reason")
        elif execution.status is StageResultStatus.FAILED:
            error = _result_text(result, "error")

        payload = _json_payload(result)
        inserted_row = connection.execute(
            _INSERT_STAGE_RESULT_SQL,
            (
                message.task_id,
                message.task_version,
                message.stage.value,
                message.attempt,
                execution.status.value,
                claim.msg_id,
                claim.read_count,
                worker_id,
                node_name,
                started_at,
                finished_at,
                payload,
                error,
            ),
        ).fetchone()
        if inserted_row is None:
            return False

        expected_identity = (
            message.task_id,
            message.task_version,
            message.stage.value,
            message.attempt,
        )
        if _returned_stage_identity(inserted_row) != expected_identity:
            raise TaskStoreError("stage result returned a different task/stage attempt identity")

        if generate_success:
            self._replace_files_by_identity(
                connection,
                message.task_id,
                message.task_version,
                task_files,
            )

        if execution.status is StageResultStatus.SUCCEEDED:
            successor = message.stage.next_stage
            state = PipelineTaskState.COMPLETED if successor is None else PipelineTaskState.QUEUED
            current_stage = message.stage if successor is None else successor
            task_finished_at = finished_at if successor is None else None
            last_error = None
            last_reason = None
        elif execution.status is StageResultStatus.REJECTED:
            state = PipelineTaskState.REJECTED
            current_stage = message.stage
            task_finished_at = finished_at
            last_error = None
            last_reason = error
        else:
            state = PipelineTaskState.FAILED
            current_stage = message.stage
            task_finished_at = finished_at
            last_error = error
            last_reason = None

        update_cursor = connection.execute(
            _UPDATE_TASK_SQL,
            (
                state.value,
                current_stage.value,
                finished_at,
                task_finished_at,
                last_error,
                last_reason,
                message.task_id,
                message.task_version,
                message.trace_id,
                message.stage.value,
            ),
        )
        if update_cursor.rowcount != 1:
            raise TaskStoreError(
                "stage completion must update exactly one task matching its identity and stage"
            )

        if push_fields is not None:
            remote_tag, registry, suffix = push_fields
            connection.execute(
                _INSERT_PUSHED_IMAGE_SQL,
                (
                    message.task_id,
                    registry,
                    suffix,
                    remote_tag,
                    True,
                    "pushed_images",
                    payload,
                ),
            )
        return True

    def record_terminal_failure(
        self,
        connection: ConnectionLike,
        claim: ClaimedMessage,
        error: str,
        *,
        started_at: datetime,
        worker_id: str,
        node_name: str,
    ) -> bool:
        """Record an exhausted execution error under the claim's idempotency key."""

        safe_error = _bounded_redacted_error(error)
        return self.record_stage_result(
            connection,
            claim,
            StageExecution.failed({"error": safe_error}),
            started_at=started_at,
            worker_id=worker_id,
            node_name=node_name,
        )


__all__ = [
    "DEFAULT_MAX_FILE_BYTES",
    "DEFAULT_MAX_TASK_BYTES",
    "MAX_STORED_ERROR_CHARS",
    "TaskFileError",
    "TaskNotFoundError",
    "TaskStore",
    "TaskStoreError",
    "capture_task_files",
    "materialize_task_files",
]
