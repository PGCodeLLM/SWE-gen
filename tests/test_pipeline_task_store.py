import json
import os
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from uuid import UUID

import pytest

from swegen.pipeline.models import (
    PipelineTask,
    PipelineTaskState,
    StageExecution,
    TaskFile,
)
from swegen.queueing.models import (
    ClaimedMessage,
    PipelineStage,
    QueueMessage,
    QueueName,
    queue_for_stage,
)

EVENT_ID = UUID("11111111-1111-4111-8111-111111111111")
TRACE_ID = UUID("22222222-2222-4222-8222-222222222222")
SECRET_TOKEN = "ghp_abcdefghijklmnopqrstuvwxyz"
STARTED_AT = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)
FINISHED_AT = datetime(2026, 7, 28, 12, 5, tzinfo=UTC)
ENQUEUED_AT = datetime(2026, 7, 28, 11, 59, tzinfo=UTC)
VISIBLE_AT = datetime(2026, 7, 28, 12, 10, tzinfo=UTC)


def normalize_sql(sql: str) -> str:
    return " ".join(sql.split())


GET_TASK_SQL = normalize_sql(
    """
    SELECT task_id, task_version, repo, pr, trace_id, state, current_stage
    FROM pipeline_tasks
    WHERE task_id = %s AND task_version = %s
    """
)
LOAD_FILES_SQL = normalize_sql(
    """
    SELECT path, content, mode, sha256, size_bytes
    FROM pipeline_task_files
    WHERE task_id = %s AND task_version = %s
    ORDER BY path
    """
)
DELETE_FILES_SQL = normalize_sql(
    """
    DELETE FROM pipeline_task_files
    WHERE task_id = %s AND task_version = %s
    """
)
INSERT_FILE_SQL = normalize_sql(
    """
    INSERT INTO pipeline_task_files (
        task_id, task_version, path, content, mode, size_bytes, sha256
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
)
INSERT_STAGE_RESULT_SQL = normalize_sql(
    """
    INSERT INTO pipeline_stage_results (
        task_id, task_version, stage, attempt, status,
        pgmq_msg_id, pgmq_read_count, worker_id, node_name,
        started_at, finished_at, result, error
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
    ON CONFLICT (task_id, task_version, stage, attempt) DO NOTHING
    RETURNING task_id, task_version, stage, attempt
    """
)
UPDATE_TASK_SQL = normalize_sql(
    """
    UPDATE pipeline_tasks
    SET state = %s, current_stage = %s, updated_at = %s, finished_at = %s,
        last_error = %s, last_reason = %s
    WHERE task_id = %s AND task_version = %s AND trace_id = %s AND current_stage = %s
    """
)
INSERT_PUSHED_IMAGE_SQL = normalize_sql(
    """
    INSERT INTO pushed_images (
        instance, registry, suffix, swr_url, pushed, event, payload
    ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
    """
)


@dataclass(frozen=True)
class CursorResult:
    rows: tuple[object, ...] = ()
    rowcount: int = 0


class FakeCursor:
    def __init__(self, result: CursorResult) -> None:
        self.rows = deque(result.rows)
        self.rowcount = result.rowcount

    def fetchone(self) -> object | None:
        return self.rows.popleft() if self.rows else None

    def fetchall(self) -> list[object]:
        rows = list(self.rows)
        self.rows.clear()
        return rows


class RecordingConnection:
    def __init__(self, *results: CursorResult | Sequence[object]) -> None:
        self.results = deque(
            result if isinstance(result, CursorResult) else CursorResult(tuple(result), len(result))
            for result in results
        )
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, params: Sequence[object] | None = None) -> FakeCursor:
        normalized_query = normalize_sql(query)
        self.calls.append((normalized_query, tuple(params or ())))
        if not self.results:
            raise AssertionError(f"No result configured for SQL: {normalized_query}")
        return FakeCursor(self.results.popleft())

    def commit(self) -> None:
        raise AssertionError("TaskStore must not commit a caller-owned transaction")

    def transaction(self) -> None:
        raise AssertionError("TaskStore must not open a transaction")


def json_payload(value: dict[str, object]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def make_task(**overrides: object) -> PipelineTask:
    values: dict[str, object] = {
        "task_id": "owner__repo-123",
        "task_version": 1,
        "repo": "owner/repo",
        "pr": 123,
        "trace_id": TRACE_ID,
        "state": PipelineTaskState.QUEUED,
        "current_stage": PipelineStage.GENERATE,
    }
    values.update(overrides)
    return PipelineTask(**values)  # type: ignore[arg-type]


def make_task_file(path: str = "instruction.md", content: bytes = b"bug\n", mode: int = 0o644):
    return TaskFile(
        path=path,
        content=content,
        mode=mode,
        sha256=sha256(content).hexdigest(),
    )


def forge_task_file(
    *,
    path: str = "instruction.md",
    content: bytes = b"bug\n",
    mode: int = 0o644,
    digest: str | None = None,
    size_bytes: int | None = None,
) -> TaskFile:
    task_file = object.__new__(TaskFile)
    object.__setattr__(task_file, "path", path)
    object.__setattr__(task_file, "content", content)
    object.__setattr__(task_file, "mode", mode)
    object.__setattr__(task_file, "sha256", digest or sha256(content).hexdigest())
    object.__setattr__(task_file, "size_bytes", len(content) if size_bytes is None else size_bytes)
    return task_file


def make_claim(
    stage: PipelineStage = PipelineStage.GENERATE,
    *,
    queue: QueueName | None = None,
    task_id: str = "owner__repo-123",
    task_version: int = 1,
    attempt: int = 1,
    trace_id: UUID = TRACE_ID,
    msg_id: int = 71,
    read_count: int = 2,
) -> ClaimedMessage:
    message = QueueMessage(
        event_id=EVENT_ID,
        task_id=task_id,
        task_version=task_version,
        stage=stage,
        attempt=attempt,
        trace_id=trace_id,
        enqueued_at=ENQUEUED_AT,
    )
    return ClaimedMessage(
        queue=queue or queue_for_stage(stage),
        msg_id=msg_id,
        read_count=read_count,
        enqueued_at=ENQUEUED_AT,
        visible_at=VISIBLE_AT,
        message=message,
    )


def inserted_stage_result(claim: ClaimedMessage) -> CursorResult:
    message = claim.message
    return CursorResult(
        ((message.task_id, message.task_version, message.stage.value, message.attempt),),
        rowcount=1,
    )


def stage_result_params(
    claim: ClaimedMessage,
    execution: StageExecution,
    *,
    error: str | None = None,
) -> tuple[object, ...]:
    message = claim.message
    return (
        message.task_id,
        message.task_version,
        message.stage.value,
        message.attempt,
        execution.status.value,
        claim.msg_id,
        claim.read_count,
        "worker-1",
        "node-a",
        STARTED_AT,
        FINISHED_AT,
        json_payload(execution.result_json()),
        error,
    )


def test_capture_task_files_preserves_nested_binary_files_and_modes(tmp_path: Path) -> None:
    from swegen.pipeline.task_store import capture_task_files

    root = tmp_path / "task"
    (root / "tests").mkdir(parents=True)
    script = b"#!/bin/sh\nexit 0\n"
    binary = b"\x00\xffpayload"
    (root / "tests" / "test.sh").write_bytes(script)
    (root / "tests" / "test.sh").chmod(0o755)
    (root / "artifact.bin").write_bytes(binary)
    (root / "empty.txt").write_bytes(b"")

    files = capture_task_files(root)

    assert [task_file.path for task_file in files] == [
        "artifact.bin",
        "empty.txt",
        "tests/test.sh",
    ]
    assert files[0].content == binary
    assert files[0].sha256 == sha256(binary).hexdigest()
    assert files[1].size_bytes == 0
    assert files[2].content == script
    assert files[2].mode == 0o755


def test_capture_task_files_discards_special_mode_bits(tmp_path: Path) -> None:
    from swegen.pipeline.task_store import capture_task_files

    root = tmp_path / "task"
    root.mkdir()
    script = root / "solve.sh"
    script.write_bytes(b"#!/bin/sh\n")
    script.chmod(0o4755)

    (captured,) = capture_task_files(root)

    assert captured.mode == 0o755


def test_capture_task_files_rejects_metadata_changes_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from swegen.pipeline import task_store

    root = tmp_path / "task"
    root.mkdir()
    target = root / "instruction.md"
    target.write_bytes(b"bug description\n")
    real_read = task_store.os.read
    changed = False

    def read_then_change_metadata(file_descriptor: int, size: int) -> bytes:
        nonlocal changed
        content = real_read(file_descriptor, size)
        if content and not changed:
            metadata = target.stat()
            os.utime(
                target,
                ns=(metadata.st_atime_ns, metadata.st_mtime_ns + 1_000_000_000),
            )
            changed = True
        return content

    monkeypatch.setattr(task_store.os, "read", read_then_change_metadata)

    with pytest.raises(task_store.TaskFileError, match="changed.*captur"):
        task_store.capture_task_files(root)

    assert changed is True


@pytest.mark.parametrize("root_kind", ["missing", "file", "symlink"])
def test_capture_task_files_requires_a_real_directory(tmp_path: Path, root_kind: str) -> None:
    from swegen.pipeline.task_store import TaskFileError, capture_task_files

    root = tmp_path / "task"
    if root_kind == "file":
        root.write_text("not a directory")
    elif root_kind == "symlink":
        target = tmp_path / "target"
        target.mkdir()
        root.symlink_to(target, target_is_directory=True)

    with pytest.raises(TaskFileError, match="directory|symlink"):
        capture_task_files(root)


@pytest.mark.parametrize("link_is_directory", [False, True])
def test_capture_task_files_rejects_symlinks_in_the_tree(
    tmp_path: Path, link_is_directory: bool
) -> None:
    from swegen.pipeline.task_store import TaskFileError, capture_task_files

    root = tmp_path / "task"
    root.mkdir()
    target = root / "target"
    if link_is_directory:
        target.mkdir()
    else:
        target.write_text("target")
    (root / "link").symlink_to(target, target_is_directory=link_is_directory)

    with pytest.raises(TaskFileError, match="symlink"):
        capture_task_files(root)


def test_capture_task_files_rejects_non_regular_entries(tmp_path: Path) -> None:
    from swegen.pipeline.task_store import TaskFileError, capture_task_files

    root = tmp_path / "task"
    root.mkdir()
    os.mkfifo(root / "runtime.pipe")

    with pytest.raises(TaskFileError, match="regular"):
        capture_task_files(root)


def test_capture_task_files_rejects_unsafe_posix_paths(tmp_path: Path) -> None:
    from swegen.pipeline.task_store import TaskFileError, capture_task_files

    root = tmp_path / "task"
    root.mkdir()
    (root / "unsafe\\name").write_text("x")

    with pytest.raises(TaskFileError, match="path"):
        capture_task_files(root)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"max_file_bytes": 0}, "max_file_bytes"),
        ({"max_file_bytes": True}, "max_file_bytes"),
        ({"max_task_bytes": -1}, "max_task_bytes"),
    ],
)
def test_capture_task_files_requires_positive_limits(
    tmp_path: Path, kwargs: dict[str, object], match: str
) -> None:
    from swegen.pipeline.task_store import TaskFileError, capture_task_files

    root = tmp_path / "task"
    root.mkdir()

    with pytest.raises(TaskFileError, match=match):
        capture_task_files(root, **kwargs)  # type: ignore[arg-type]


def test_capture_task_files_enforces_file_and_total_limits(tmp_path: Path) -> None:
    from swegen.pipeline.task_store import TaskFileError, capture_task_files

    root = tmp_path / "task"
    root.mkdir()
    (root / "a").write_bytes(b"abc")
    (root / "b").write_bytes(b"def")

    with pytest.raises(TaskFileError, match="per-file"):
        capture_task_files(root, max_file_bytes=2, max_task_bytes=20)
    with pytest.raises(TaskFileError, match="total"):
        capture_task_files(root, max_file_bytes=3, max_task_bytes=5)


def test_materialize_task_files_writes_a_verified_empty_destination(tmp_path: Path) -> None:
    from swegen.pipeline.task_store import materialize_task_files

    destination = tmp_path / "out"
    destination.mkdir()
    files = (
        make_task_file("tests/test.sh", b"#!/bin/sh\n", 0o755),
        make_task_file("artifact.bin", b"\x00\xff", 0o600),
        make_task_file("empty", b"", 0o640),
    )

    materialize_task_files(files, destination)

    assert (destination / "tests" / "test.sh").read_bytes() == b"#!/bin/sh\n"
    assert (destination / "tests" / "test.sh").stat().st_mode & 0o777 == 0o755
    assert (destination / "artifact.bin").read_bytes() == b"\x00\xff"
    assert (destination / "artifact.bin").stat().st_mode & 0o777 == 0o600
    assert (destination / "empty").read_bytes() == b""


@pytest.mark.parametrize(
    "task_file",
    [
        forge_task_file(path="../escape"),
        forge_task_file(path="/absolute"),
        forge_task_file(path="tests\\test.sh"),
    ],
)
def test_materialize_task_files_rejects_unsafe_paths(tmp_path: Path, task_file: TaskFile) -> None:
    from swegen.pipeline.task_store import TaskFileError, materialize_task_files

    with pytest.raises(TaskFileError, match="path"):
        materialize_task_files((task_file,), tmp_path / "out")

    assert not (tmp_path / "out").exists()
    assert not (tmp_path / "escape").exists()


def test_materialize_task_files_rejects_nul_path_before_creating_destination(
    tmp_path: Path,
) -> None:
    from swegen.pipeline.task_store import TaskFileError, materialize_task_files

    destination = tmp_path / "out"
    task_file = make_task_file("unsafe\x00name")

    with pytest.raises(TaskFileError, match="path"):
        materialize_task_files((task_file,), destination)

    assert not destination.exists()


def test_materialize_task_files_rejects_duplicate_paths_before_writing(tmp_path: Path) -> None:
    from swegen.pipeline.task_store import TaskFileError, materialize_task_files

    files = (make_task_file("same"), make_task_file("same", b"other"))

    with pytest.raises(TaskFileError, match="duplicate"):
        materialize_task_files(files, tmp_path / "out")

    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize(
    "task_file",
    [
        forge_task_file(digest="0" * 64),
        forge_task_file(size_bytes=999),
    ],
)
def test_materialize_task_files_verifies_metadata_before_writing(
    tmp_path: Path, task_file: TaskFile
) -> None:
    from swegen.pipeline.task_store import TaskFileError, materialize_task_files

    with pytest.raises(TaskFileError, match="digest|size"):
        materialize_task_files((task_file,), tmp_path / "out")

    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("destination_kind", ["nonempty", "file", "symlink"])
def test_materialize_task_files_rejects_a_nonempty_or_unsafe_destination(
    tmp_path: Path, destination_kind: str
) -> None:
    from swegen.pipeline.task_store import TaskFileError, materialize_task_files

    destination = tmp_path / "out"
    if destination_kind == "nonempty":
        destination.mkdir()
        (destination / "existing").write_text("keep")
    elif destination_kind == "file":
        destination.write_text("not a directory")
    else:
        target = tmp_path / "target"
        target.mkdir()
        destination.symlink_to(target, target_is_directory=True)

    with pytest.raises(TaskFileError, match="empty|directory|symlink"):
        materialize_task_files((make_task_file(),), destination)


def test_materialize_task_files_rejects_symlinked_destination_parents(tmp_path: Path) -> None:
    from swegen.pipeline.task_store import TaskFileError, materialize_task_files

    outside = tmp_path / "outside"
    outside.mkdir()
    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(outside, target_is_directory=True)

    with pytest.raises(TaskFileError, match="symlink"):
        materialize_task_files((make_task_file(),), linked_parent / "out")

    assert not (outside / "out").exists()


def test_materialize_task_files_rejects_an_ancestor_swap_without_writing_outside(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from swegen.pipeline import task_store

    ancestor = tmp_path / "ancestor"
    ancestor.mkdir()
    original_ancestor = tmp_path / "ancestor-original"
    outside = tmp_path / "outside"
    outside.mkdir()
    destination = ancestor / "out"
    real_open = task_store.os.open
    real_mkdir = task_store.os.mkdir
    swapped = False

    def swap_ancestor() -> None:
        nonlocal swapped
        ancestor.rename(original_ancestor)
        ancestor.symlink_to(outside, target_is_directory=True)
        swapped = True

    def racing_open(
        path: os.PathLike[str] | str,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        if not swapped and dir_fd is not None and os.fspath(path) == ancestor.name:
            swap_ancestor()
        return real_open(path, flags, mode, dir_fd=dir_fd)

    def racing_mkdir(
        path: os.PathLike[str] | str,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        if not swapped and dir_fd is None and os.fspath(path) == str(destination):
            swap_ancestor()
        real_mkdir(path, mode, dir_fd=dir_fd)

    monkeypatch.setattr(task_store.os, "open", racing_open)
    monkeypatch.setattr(task_store.os, "mkdir", racing_mkdir)

    with pytest.raises(task_store.TaskFileError, match="destination|symlink|open"):
        task_store.materialize_task_files((make_task_file(),), destination)

    assert swapped is True
    assert not (outside / "out" / "instruction.md").exists()


@pytest.mark.parametrize("preexisting_destination", [False, True])
def test_materialize_task_files_cleans_staging_after_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    preexisting_destination: bool,
) -> None:
    from swegen.pipeline import task_store

    destination = tmp_path / "out"
    if preexisting_destination:
        destination.mkdir()
    files = (
        make_task_file("a.txt", b"first"),
        make_task_file("b.txt", b"second"),
    )
    real_write_all = task_store._write_all
    real_write = task_store.os.write
    write_count = 0

    def fail_during_second_write(file_descriptor: int, content: bytes) -> None:
        nonlocal write_count
        write_count += 1
        if write_count == 2:
            real_write(file_descriptor, content[:1])
            raise OSError("injected mid-write failure")
        real_write_all(file_descriptor, content)

    monkeypatch.setattr(task_store, "_write_all", fail_during_second_write)

    with pytest.raises(task_store.TaskFileError, match="injected mid-write failure"):
        task_store.materialize_task_files(files, destination)

    if preexisting_destination:
        assert destination.is_dir()
        assert list(destination.iterdir()) == []
    else:
        assert not destination.exists()
    assert not any(path.name.startswith(".swegen-task-") for path in tmp_path.iterdir())


@pytest.mark.parametrize(
    "row",
    [
        {
            "task_id": "owner__repo-123",
            "task_version": 1,
            "repo": "owner/repo",
            "pr": 123,
            "trace_id": TRACE_ID,
            "state": "queued",
            "current_stage": "generate",
        },
        (
            "owner__repo-123",
            1,
            "owner/repo",
            123,
            TRACE_ID,
            "queued",
            "generate",
        ),
    ],
)
def test_get_task_decodes_mapping_and_tuple_rows(row: object) -> None:
    from swegen.pipeline.task_store import TaskStore

    connection = RecordingConnection([row])

    task = TaskStore().get_task(connection, "owner__repo-123", 1)

    assert task == make_task()
    assert connection.calls == [(GET_TASK_SQL, ("owner__repo-123", 1))]


def test_get_task_raises_a_clear_not_found_error() -> None:
    from swegen.pipeline.task_store import TaskNotFoundError, TaskStore

    connection = RecordingConnection([])

    with pytest.raises(TaskNotFoundError, match="owner__repo-123.*version 1"):
        TaskStore().get_task(connection, "owner__repo-123", 1)


@pytest.mark.parametrize(("task_id", "task_version"), [("../bad", 1), ("ok", 0)])
def test_task_store_rejects_invalid_lookup_identity_before_sql(
    task_id: str, task_version: int
) -> None:
    from swegen.pipeline.task_store import TaskStore

    connection = RecordingConnection()

    with pytest.raises(ValueError, match="task_id|task_version"):
        TaskStore().get_task(connection, task_id, task_version)

    assert connection.calls == []


def test_load_files_decodes_rows_and_returns_path_order() -> None:
    from swegen.pipeline.task_store import TaskStore

    a = b"a"
    b = b"b"
    connection = RecordingConnection(
        [
            ("z/file", memoryview(b), 0o600, sha256(b).hexdigest(), 1),
            {
                "path": "a/file",
                "content": a,
                "mode": 0o644,
                "sha256": sha256(a).hexdigest(),
                "size_bytes": 1,
            },
        ]
    )

    files = TaskStore().load_files(connection, "owner__repo-123", 1)

    assert files == (
        make_task_file("a/file", a, 0o644),
        make_task_file("z/file", b, 0o600),
    )
    assert connection.calls == [(LOAD_FILES_SQL, ("owner__repo-123", 1))]


def test_replace_files_deletes_then_inserts_every_file_without_committing() -> None:
    from swegen.pipeline.task_store import TaskStore

    task = make_task()
    a = make_task_file("a", b"a", 0o600)
    z = make_task_file("z", b"z", 0o755)
    connection = RecordingConnection(CursorResult(), CursorResult(), CursorResult())

    TaskStore().replace_files(connection, task, (z, a))

    assert connection.calls == [
        (DELETE_FILES_SQL, (task.task_id, task.task_version)),
        (
            INSERT_FILE_SQL,
            (task.task_id, task.task_version, "a", b"a", 0o600, 1, a.sha256),
        ),
        (
            INSERT_FILE_SQL,
            (task.task_id, task.task_version, "z", b"z", 0o755, 1, z.sha256),
        ),
    ]


def test_replace_files_rejects_duplicate_paths_before_sql() -> None:
    from swegen.pipeline.task_store import TaskFileError, TaskStore

    connection = RecordingConnection()
    duplicate = (make_task_file("same"), make_task_file("same", b"different"))

    with pytest.raises(TaskFileError, match="duplicate"):
        TaskStore().replace_files(connection, make_task(), duplicate)

    assert connection.calls == []


def test_replace_files_rejects_digest_mismatch_before_sql() -> None:
    from swegen.pipeline.task_store import TaskFileError, TaskStore

    connection = RecordingConnection()
    task_file = forge_task_file(digest="0" * 64)

    with pytest.raises(TaskFileError, match="digest"):
        TaskStore().replace_files(connection, make_task(), (task_file,))

    assert connection.calls == []


def test_record_generate_success_replaces_files_and_queues_validate_state() -> None:
    from swegen.pipeline.task_store import TaskStore

    claim = make_claim(PipelineStage.GENERATE)
    task_file = make_task_file("instruction.md")
    execution = StageExecution.succeeded({"task_path": "generated"}, (task_file,))
    connection = RecordingConnection(
        inserted_stage_result(claim),
        CursorResult(),
        CursorResult(),
        CursorResult(rowcount=1),
    )

    inserted = TaskStore(clock=lambda: FINISHED_AT).record_stage_result(
        connection,
        claim,
        execution,
        started_at=STARTED_AT,
        worker_id="worker-1",
        node_name="node-a",
    )

    assert inserted is True
    assert connection.calls == [
        (INSERT_STAGE_RESULT_SQL, stage_result_params(claim, execution)),
        (DELETE_FILES_SQL, (claim.message.task_id, claim.message.task_version)),
        (
            INSERT_FILE_SQL,
            (
                claim.message.task_id,
                claim.message.task_version,
                task_file.path,
                task_file.content,
                task_file.mode,
                task_file.size_bytes,
                task_file.sha256,
            ),
        ),
        (
            UPDATE_TASK_SQL,
            (
                "queued",
                "validate",
                FINISHED_AT,
                None,
                None,
                None,
                claim.message.task_id,
                claim.message.task_version,
                claim.message.trace_id,
                "generate",
            ),
        ),
    ]


def test_record_generate_success_rejects_digest_mismatch_before_sql() -> None:
    from swegen.pipeline.task_store import TaskFileError, TaskStore

    claim = make_claim(PipelineStage.GENERATE)
    task_file = forge_task_file(digest="0" * 64)
    execution = StageExecution.succeeded({"task_path": "generated"}, (task_file,))
    connection = RecordingConnection()

    with pytest.raises(TaskFileError, match="digest"):
        TaskStore(clock=lambda: FINISHED_AT).record_stage_result(
            connection,
            claim,
            execution,
            started_at=STARTED_AT,
            worker_id="worker-1",
            node_name="node-a",
        )

    assert connection.calls == []


def test_duplicate_stage_result_has_no_task_file_or_inventory_mutations() -> None:
    from swegen.pipeline.task_store import TaskStore

    claim = make_claim(PipelineStage.GENERATE)
    execution = StageExecution.succeeded({"task_path": "generated"}, (make_task_file(),))
    connection = RecordingConnection([])

    inserted = TaskStore(clock=lambda: FINISHED_AT).record_stage_result(
        connection,
        claim,
        execution,
        started_at=STARTED_AT,
        worker_id="worker-1",
        node_name="node-a",
    )

    assert inserted is False
    assert connection.calls == [(INSERT_STAGE_RESULT_SQL, stage_result_params(claim, execution))]


@pytest.mark.parametrize(
    ("stage", "successor"),
    [
        (PipelineStage.VALIDATE, PipelineStage.REWARD),
        (PipelineStage.REWARD, PipelineStage.PUSH),
    ],
)
def test_successful_middle_stages_queue_the_exact_successor(
    stage: PipelineStage, successor: PipelineStage
) -> None:
    from swegen.pipeline.task_store import TaskStore

    claim = make_claim(stage)
    execution = StageExecution.succeeded({"ok": True})
    connection = RecordingConnection(inserted_stage_result(claim), CursorResult(rowcount=1))

    assert TaskStore(clock=lambda: FINISHED_AT).record_stage_result(
        connection,
        claim,
        execution,
        started_at=STARTED_AT,
        worker_id="worker-1",
        node_name="node-a",
    )
    assert connection.calls == [
        (INSERT_STAGE_RESULT_SQL, stage_result_params(claim, execution)),
        (
            UPDATE_TASK_SQL,
            (
                "queued",
                successor.value,
                FINISHED_AT,
                None,
                None,
                None,
                claim.message.task_id,
                claim.message.task_version,
                claim.message.trace_id,
                stage.value,
            ),
        ),
    ]


def test_successful_push_completes_the_task_and_appends_inventory() -> None:
    from swegen.pipeline.task_store import TaskStore

    claim = make_claim(PipelineStage.PUSH)
    result = {
        "remote_tag": "swr.example/swegen/owner__repo-123:v1",
        "registry": "swr",
        "suffix": "_platform",
        "already_present": False,
    }
    execution = StageExecution.succeeded(result)
    payload = json_payload(execution.result_json())
    connection = RecordingConnection(
        inserted_stage_result(claim),
        CursorResult(rowcount=1),
        CursorResult(),
    )

    assert TaskStore(clock=lambda: FINISHED_AT).record_stage_result(
        connection,
        claim,
        execution,
        started_at=STARTED_AT,
        worker_id="worker-1",
        node_name="node-a",
    )
    assert connection.calls == [
        (INSERT_STAGE_RESULT_SQL, stage_result_params(claim, execution)),
        (
            UPDATE_TASK_SQL,
            (
                "completed",
                "push",
                FINISHED_AT,
                FINISHED_AT,
                None,
                None,
                claim.message.task_id,
                claim.message.task_version,
                claim.message.trace_id,
                "push",
            ),
        ),
        (
            INSERT_PUSHED_IMAGE_SQL,
            (
                claim.message.task_id,
                "swr",
                "_platform",
                result["remote_tag"],
                True,
                "pushed_images",
                payload,
            ),
        ),
    ]


def test_expected_rejection_marks_the_task_rejected_without_files() -> None:
    from swegen.pipeline.task_store import TaskStore

    claim = make_claim(PipelineStage.REWARD)
    execution = StageExecution.rejected({"reason": "reward hacking detected"})
    connection = RecordingConnection(inserted_stage_result(claim), CursorResult(rowcount=1))

    assert TaskStore(clock=lambda: FINISHED_AT).record_stage_result(
        connection,
        claim,
        execution,
        started_at=STARTED_AT,
        worker_id="worker-1",
        node_name="node-a",
    )
    assert connection.calls == [
        (
            INSERT_STAGE_RESULT_SQL,
            stage_result_params(claim, execution, error="reward hacking detected"),
        ),
        (
            UPDATE_TASK_SQL,
            (
                "rejected",
                "reward",
                FINISHED_AT,
                FINISHED_AT,
                None,
                "reward hacking detected",
                claim.message.task_id,
                claim.message.task_version,
                claim.message.trace_id,
                "reward",
            ),
        ),
    ]


@pytest.mark.parametrize("status", ["succeeded", "rejected", "failed"])
def test_record_stage_result_redacts_nested_json_without_mutating_execution(status: str) -> None:
    from swegen.pipeline.task_store import TaskStore

    result: dict[str, object] = {
        "message": f"token {SECRET_TOKEN}",
        "nested": {
            f"secret-{SECRET_TOKEN}": [
                SECRET_TOKEN,
                {"authorization": f"Bearer {SECRET_TOKEN}"},
                7,
                True,
                None,
            ]
        },
    }
    if status == "rejected":
        result["reason"] = f"rejected {SECRET_TOKEN}"
        execution = StageExecution.rejected(result)
    elif status == "failed":
        result["error"] = f"failed {SECRET_TOKEN}"
        execution = StageExecution.failed(result)
    else:
        execution = StageExecution.succeeded(result)
    original_result = execution.result_json()
    claim = make_claim(PipelineStage.VALIDATE)
    connection = RecordingConnection(inserted_stage_result(claim), CursorResult(rowcount=1))

    assert TaskStore(clock=lambda: FINISHED_AT).record_stage_result(
        connection,
        claim,
        execution,
        started_at=STARTED_AT,
        worker_id="worker-1",
        node_name="node-a",
    )

    stored_payload = json.loads(connection.calls[0][1][-2])
    rendered_payload = json.dumps(stored_payload)
    assert SECRET_TOKEN not in rendered_payload
    assert "<REDACTED>" in rendered_payload
    redacted_items = stored_payload["nested"]["secret-<REDACTED>"]
    assert redacted_items[2:] == [7, True, None]
    assert original_result == execution.result_json()
    assert SECRET_TOKEN in json.dumps(original_result)
    stored_error = connection.calls[0][1][-1]
    if status == "succeeded":
        assert stored_error is None
    else:
        assert SECRET_TOKEN not in stored_error
        assert "<REDACTED>" in stored_error


def test_successful_push_redacts_stage_and_inventory_payloads() -> None:
    from swegen.pipeline.task_store import TaskStore

    claim = make_claim(PipelineStage.PUSH)
    execution = StageExecution.succeeded(
        {
            "remote_tag": "swr.example/swegen/owner__repo-123:v1",
            "details": {"tokens": [SECRET_TOKEN, 3]},
        }
    )
    connection = RecordingConnection(
        inserted_stage_result(claim),
        CursorResult(rowcount=1),
        CursorResult(),
    )

    assert TaskStore(clock=lambda: FINISHED_AT).record_stage_result(
        connection,
        claim,
        execution,
        started_at=STARTED_AT,
        worker_id="worker-1",
        node_name="node-a",
    )

    stage_payload = connection.calls[0][1][-2]
    inventory_payload = connection.calls[2][1][-1]
    assert stage_payload == inventory_payload
    assert SECRET_TOKEN not in stage_payload
    assert json.loads(stage_payload)["details"] == {"tokens": ["<REDACTED>", 3]}


def test_record_terminal_failure_redacts_bounds_and_marks_the_task_failed() -> None:
    from swegen.pipeline.task_store import MAX_STORED_ERROR_CHARS, TaskStore

    claim = make_claim(PipelineStage.VALIDATE, read_count=3)
    raw_error = "ghp_abcdefghijklmnopqrstuvwxyz " + "x" * (MAX_STORED_ERROR_CHARS + 100)
    connection = RecordingConnection(inserted_stage_result(claim), CursorResult(rowcount=1))

    assert TaskStore(clock=lambda: FINISHED_AT).record_terminal_failure(
        connection,
        claim,
        raw_error,
        started_at=STARTED_AT,
        worker_id="worker-1",
        node_name="node-a",
    )
    insert_params = connection.calls[0][1]
    stored_error = insert_params[-1]
    assert isinstance(stored_error, str)
    assert len(stored_error) <= MAX_STORED_ERROR_CHARS
    assert "ghp_" not in stored_error
    assert "<REDACTED>" in stored_error
    assert json.loads(insert_params[-2]) == {"error": stored_error}
    assert insert_params[6] == 3
    assert connection.calls[1] == (
        UPDATE_TASK_SQL,
        (
            "failed",
            "validate",
            FINISHED_AT,
            FINISHED_AT,
            stored_error,
            None,
            claim.message.task_id,
            claim.message.task_version,
            claim.message.trace_id,
            "validate",
        ),
    )


def test_duplicate_terminal_failure_is_a_noop_after_the_idempotency_insert() -> None:
    from swegen.pipeline.task_store import TaskStore

    claim = make_claim(PipelineStage.VALIDATE, read_count=3)
    connection = RecordingConnection([])

    assert not TaskStore(clock=lambda: FINISHED_AT).record_terminal_failure(
        connection,
        claim,
        "runner failed",
        started_at=STARTED_AT,
        worker_id="worker-1",
        node_name="node-a",
    )
    assert len(connection.calls) == 1
    assert connection.calls[0][0] == INSERT_STAGE_RESULT_SQL


def test_successful_generation_requires_nonempty_files() -> None:
    from swegen.pipeline.task_store import TaskFileError, TaskStore

    claim = make_claim(PipelineStage.GENERATE)
    execution = StageExecution.succeeded({})
    connection = RecordingConnection()

    with pytest.raises(TaskFileError, match="non-empty"):
        TaskStore(clock=lambda: FINISHED_AT).record_stage_result(
            connection,
            claim,
            execution,
            started_at=STARTED_AT,
            worker_id="worker-1",
            node_name="node-a",
        )
    assert connection.calls == []


@pytest.mark.parametrize(
    ("stage", "execution"),
    [
        (PipelineStage.VALIDATE, StageExecution.succeeded({}, (make_task_file(),))),
        (PipelineStage.GENERATE, StageExecution.rejected({"reason": "no"})),
        (PipelineStage.GENERATE, StageExecution.failed({"error": "bad"})),
    ],
)
def test_stage_results_reject_files_outside_successful_generation(
    stage: PipelineStage, execution: StageExecution
) -> None:
    from swegen.pipeline.task_store import TaskFileError, TaskStore

    if not execution.files:
        execution = StageExecution(execution.status, execution.result, (make_task_file(),))
    connection = RecordingConnection()

    with pytest.raises(TaskFileError, match="only successful generate"):
        TaskStore(clock=lambda: FINISHED_AT).record_stage_result(
            connection,
            make_claim(stage),
            execution,
            started_at=STARTED_AT,
            worker_id="worker-1",
            node_name="node-a",
        )

    assert connection.calls == []


def test_successful_push_requires_a_nonblank_remote_tag() -> None:
    from swegen.pipeline.task_store import TaskStore, TaskStoreError

    connection = RecordingConnection()

    with pytest.raises(TaskStoreError, match="remote_tag"):
        TaskStore(clock=lambda: FINISHED_AT).record_stage_result(
            connection,
            make_claim(PipelineStage.PUSH),
            StageExecution.succeeded({"registry": "swr"}),
            started_at=STARTED_AT,
            worker_id="worker-1",
            node_name="node-a",
        )

    assert connection.calls == []


def test_record_stage_result_rejects_a_claim_routed_to_the_wrong_queue() -> None:
    from swegen.pipeline.task_store import TaskStore, TaskStoreError

    claim = make_claim(PipelineStage.VALIDATE, queue=QueueName.GENERATE)
    connection = RecordingConnection()

    with pytest.raises(TaskStoreError, match="queue.*stage"):
        TaskStore(clock=lambda: FINISHED_AT).record_stage_result(
            connection,
            claim,
            StageExecution.succeeded({"ok": True}),
            started_at=STARTED_AT,
            worker_id="worker-1",
            node_name="node-a",
        )

    assert connection.calls == []


def test_record_stage_result_requires_exactly_one_matching_task_update() -> None:
    from swegen.pipeline.task_store import TaskStore, TaskStoreError

    claim = make_claim(PipelineStage.VALIDATE)
    execution = StageExecution.succeeded({"ok": True})
    connection = RecordingConnection(inserted_stage_result(claim), CursorResult(rowcount=0))

    with pytest.raises(TaskStoreError, match="exactly one task"):
        TaskStore(clock=lambda: FINISHED_AT).record_stage_result(
            connection,
            claim,
            execution,
            started_at=STARTED_AT,
            worker_id="worker-1",
            node_name="node-a",
        )

    assert connection.calls[-1][1][-4:] == (
        claim.message.task_id,
        claim.message.task_version,
        claim.message.trace_id,
        claim.message.stage.value,
    )


@pytest.mark.parametrize(
    ("started_at", "worker_id", "node_name"),
    [
        (datetime(2026, 7, 28, 12, 0), "worker-1", "node-a"),
        (FINISHED_AT + timedelta(seconds=1), "worker-1", "node-a"),
        (STARTED_AT, " ", "node-a"),
        (STARTED_AT, "worker-1", ""),
    ],
)
def test_record_stage_result_validates_timestamps_and_worker_identity_before_sql(
    started_at: datetime, worker_id: str, node_name: str
) -> None:
    from swegen.pipeline.task_store import TaskStore

    connection = RecordingConnection()

    with pytest.raises(ValueError, match="started_at|worker_id|node_name"):
        TaskStore(clock=lambda: FINISHED_AT).record_stage_result(
            connection,
            make_claim(PipelineStage.VALIDATE),
            StageExecution.succeeded({"ok": True}),
            started_at=started_at,
            worker_id=worker_id,
            node_name=node_name,
        )

    assert connection.calls == []


def test_record_stage_result_requires_an_aware_clock() -> None:
    from swegen.pipeline.task_store import TaskStore

    connection = RecordingConnection()

    with pytest.raises(ValueError, match="clock"):
        TaskStore(clock=lambda: datetime(2026, 7, 28, 12, 5)).record_stage_result(
            connection,
            make_claim(PipelineStage.VALIDATE),
            StageExecution.succeeded({"ok": True}),
            started_at=STARTED_AT,
            worker_id="worker-1",
            node_name="node-a",
        )

    assert connection.calls == []
