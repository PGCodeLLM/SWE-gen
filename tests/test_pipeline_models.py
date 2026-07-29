from collections.abc import Mapping
from dataclasses import FrozenInstanceError
from hashlib import sha256
from pathlib import Path
from uuid import UUID

import pytest

TRACE_ID = UUID("22222222-2222-4222-8222-222222222222")


def schema_sql() -> str:
    schema_path = Path(__file__).parents[1] / "src" / "swegen" / "schema.sql"
    return " ".join(schema_path.read_text().split())


def test_pipeline_states_are_fixed() -> None:
    from swegen.pipeline.models import PipelineTaskState

    assert [state.value for state in PipelineTaskState] == [
        "queued",
        "running",
        "rejected",
        "failed",
        "completed",
    ]


def test_stage_result_states_are_fixed() -> None:
    from swegen.pipeline.models import StageResultStatus

    assert [status.value for status in StageResultStatus] == [
        "succeeded",
        "rejected",
        "failed",
    ]


def test_stage_execution_rejects_without_handoff() -> None:
    from swegen.pipeline.models import StageExecution, StageResultStatus

    execution = StageExecution.rejected({"reason": "NOP reward was 1"})

    assert execution.status is StageResultStatus.REJECTED
    assert execution.result == {"reason": "NOP reward was 1"}
    assert execution.files == ()
    assert execution.should_handoff is False


def test_only_successful_stage_execution_hands_off_files() -> None:
    from swegen.pipeline.models import StageExecution, StageResultStatus, TaskFile

    content = b"#!/bin/sh\nexit 0\n"
    task_file = TaskFile(
        path="tests/test.sh",
        content=content,
        mode=0o755,
        sha256=sha256(content).hexdigest(),
    )

    succeeded = StageExecution.succeeded({"reward": 1}, files=(task_file,))
    failed = StageExecution.failed({"error": "runner crashed"})

    assert succeeded.status is StageResultStatus.SUCCEEDED
    assert succeeded.should_handoff is True
    assert succeeded.files == (task_file,)
    assert failed.status is StageResultStatus.FAILED
    assert failed.should_handoff is False


def test_stage_execution_snapshots_and_recursively_freezes_results() -> None:
    from swegen.pipeline.models import StageExecution

    details = {"nop": 0, "oracle": 1}
    labels = ["validated", "clean"]
    source: dict[str, object] = {
        "reward": 1,
        "details": details,
        "labels": labels,
    }

    execution = StageExecution.succeeded(source)
    source["reward"] = 0
    details["nop"] = 1
    labels.append("mutated")

    assert execution.result["reward"] == 1
    assert execution.result["labels"] == ("validated", "clean")
    stored_details = execution.result["details"]
    assert isinstance(stored_details, Mapping)
    assert stored_details["nop"] == 0

    with pytest.raises(TypeError):
        execution.result["reward"] = 0  # type: ignore[index]
    with pytest.raises(TypeError):
        stored_details["nop"] = 1  # type: ignore[index]

    plain_result = execution.result_json()
    assert plain_result == {
        "reward": 1,
        "details": {"nop": 0, "oracle": 1},
        "labels": ["validated", "clean"],
    }
    plain_result["reward"] = 0
    assert execution.result["reward"] == 1


@pytest.mark.parametrize(
    "result",
    [
        {"value": object()},
        {"value": b"not-json"},
        {"value": {"not", "json"}},
        {"value": float("nan")},
        {1: "keys must be strings"},
    ],
)
def test_stage_execution_rejects_non_json_safe_results(result: dict[object, object]) -> None:
    from swegen.pipeline.models import StageExecution

    with pytest.raises(ValueError, match="JSON-safe"):
        StageExecution.succeeded(result)  # type: ignore[arg-type]


def test_stage_execution_preserves_files_from_a_one_shot_iterable() -> None:
    from swegen.pipeline.models import StageExecution, StageResultStatus, TaskFile

    content = b"test"
    task_file = TaskFile(
        path="tests/test.txt",
        content=content,
        mode=0o644,
        sha256=sha256(content).hexdigest(),
    )
    files = (item for item in (task_file,))

    execution = StageExecution(
        status=StageResultStatus.SUCCEEDED,
        result={},
        files=files,  # type: ignore[arg-type]
    )

    assert execution.files == (task_file,)


def test_pipeline_records_are_immutable() -> None:
    from swegen.pipeline.models import PipelineTask

    task = PipelineTask(
        task_id="owner__repo-123",
        task_version=1,
        repo="owner/repo",
        pr=123,
        trace_id=TRACE_ID,
    )

    with pytest.raises(FrozenInstanceError):
        task.pr = 124  # type: ignore[misc]


@pytest.mark.parametrize(
    "task_id",
    ["", " owner__repo-123", "../owner__repo-123", "owner/repo-123", "owner\\repo-123"],
)
def test_pipeline_task_rejects_unsafe_task_ids(task_id: str) -> None:
    from swegen.pipeline.models import PipelineTask

    with pytest.raises(ValueError, match="task_id"):
        PipelineTask(
            task_id=task_id,
            task_version=1,
            repo="owner/repo",
            pr=123,
            trace_id=TRACE_ID,
        )


@pytest.mark.parametrize("repo", ["", "owner", "/repo", "owner/", "../repo", "owner/repo/extra"])
def test_pipeline_task_requires_an_owner_repo_name(repo: str) -> None:
    from swegen.pipeline.models import PipelineTask

    with pytest.raises(ValueError, match="repo"):
        PipelineTask(
            task_id="owner__repo-123",
            task_version=1,
            repo=repo,
            pr=123,
            trace_id=TRACE_ID,
        )


@pytest.mark.parametrize(("field", "value"), [("task_version", 0), ("pr", 0)])
def test_pipeline_task_requires_positive_identity_numbers(field: str, value: int) -> None:
    from swegen.pipeline.models import PipelineTask

    values = {
        "task_id": "owner__repo-123",
        "task_version": 1,
        "repo": "owner/repo",
        "pr": 123,
        "trace_id": TRACE_ID,
    }
    values[field] = value

    with pytest.raises(ValueError, match=field):
        PipelineTask(**values)  # type: ignore[arg-type]


def test_task_file_derives_and_validates_its_size() -> None:
    from swegen.pipeline.models import TaskFile

    content = b"binary\x00payload"
    task_file = TaskFile(
        path="environment/bug.patch",
        content=content,
        mode=0o644,
        sha256=sha256(content).hexdigest(),
    )

    assert task_file.size_bytes == len(content)

    with pytest.raises(ValueError, match="size_bytes"):
        TaskFile(
            path="environment/bug.patch",
            content=content,
            mode=0o644,
            sha256=sha256(content).hexdigest(),
            size_bytes=len(content) + 1,
        )


@pytest.mark.parametrize(
    "path",
    ["", "/tests/test.sh", "../test.sh", "tests/./test.sh", "tests//test.sh", "tests\\test.sh"],
)
def test_task_file_rejects_non_relative_posix_paths(path: str) -> None:
    from swegen.pipeline.models import TaskFile

    with pytest.raises(ValueError, match="path"):
        TaskFile(path=path, content=b"x", mode=0o644, sha256=sha256(b"x").hexdigest())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("content", "not-bytes"),
        ("mode", 0o1000),
        ("sha256", "0" * 63),
    ],
)
def test_task_file_rejects_invalid_metadata(field: str, value: object) -> None:
    from swegen.pipeline.models import TaskFile

    values: dict[str, object] = {
        "path": "instruction.md",
        "content": b"x",
        "mode": 0o644,
        "sha256": sha256(b"x").hexdigest(),
    }
    values[field] = value

    with pytest.raises(ValueError, match=field):
        TaskFile(**values)  # type: ignore[arg-type]


def test_pipeline_schema_has_named_tables_and_idempotent_stage_key() -> None:
    sql = schema_sql()

    assert "CREATE TABLE IF NOT EXISTS pipeline_tasks" in sql
    assert "CREATE TABLE IF NOT EXISTS pipeline_task_files" in sql
    assert "CREATE TABLE IF NOT EXISTS pipeline_stage_results" in sql
    assert "CREATE TABLE IF NOT EXISTS pipeline_stage_activity" in sql
    assert "CONSTRAINT pk_pipeline_tasks PRIMARY KEY (task_id, task_version)" in sql
    assert (
        "CONSTRAINT pk_pipeline_stage_results PRIMARY KEY (task_id, task_version, stage, attempt)"
    ) in sql
    assert (
        "CONSTRAINT pk_pipeline_stage_activity PRIMARY KEY (task_id, task_version, stage)"
        in sql
    )


def test_pipeline_schema_has_named_identity_and_file_constraints() -> None:
    sql = schema_sql()

    assert ("CONSTRAINT uq_pipeline_tasks_repo_pr_version UNIQUE (repo, pr, task_version)") in sql
    assert "trace_id UUID NOT NULL" in sql
    assert "created_at TIMESTAMPTZ NOT NULL DEFAULT now()" in sql
    assert "updated_at TIMESTAMPTZ NOT NULL DEFAULT now()" in sql
    assert "finished_at TIMESTAMPTZ" in sql
    assert "last_error TEXT" in sql
    assert "last_reason TEXT" in sql
    assert "CONSTRAINT ck_pipeline_tasks_task_version_positive" in sql
    assert "CONSTRAINT ck_pipeline_tasks_pr_positive" in sql
    assert "CONSTRAINT ck_pipeline_tasks_state" in sql
    assert "CONSTRAINT ck_pipeline_tasks_current_stage" in sql
    assert "CONSTRAINT pk_pipeline_task_files PRIMARY KEY (task_id, task_version, path)" in sql
    assert "CONSTRAINT fk_pipeline_task_files_task" in sql
    assert "REFERENCES pipeline_tasks (task_id, task_version) ON DELETE CASCADE" in sql
    assert "path TEXT NOT NULL" in sql
    assert "size_bytes BIGINT NOT NULL" in sql
    assert "CONSTRAINT ck_pipeline_task_files_path_safe" in sql
    assert "CONSTRAINT ck_pipeline_task_files_size_nonnegative" in sql
    assert "CONSTRAINT ck_pipeline_task_files_size_matches_content" in sql
    assert "CONSTRAINT ck_pipeline_task_files_mode" in sql
    assert "CONSTRAINT ck_pipeline_task_files_sha256" in sql


def test_pipeline_schema_has_named_stage_result_constraints_and_indexes() -> None:
    sql = schema_sql()

    assert "CONSTRAINT fk_pipeline_stage_results_task" in sql
    assert "CONSTRAINT ck_pipeline_stage_results_stage" in sql
    assert "CONSTRAINT ck_pipeline_stage_results_attempt_positive" in sql
    assert "CONSTRAINT ck_pipeline_stage_results_status" in sql
    assert "CONSTRAINT ck_pipeline_stage_results_pgmq_msg_id_positive" in sql
    assert "CONSTRAINT ck_pipeline_stage_results_pgmq_read_count_positive" in sql
    assert "pgmq_msg_id BIGINT NOT NULL" in sql
    assert "pgmq_read_count INTEGER NOT NULL" in sql
    assert "worker_id TEXT NOT NULL" in sql
    assert "node_name TEXT NOT NULL" in sql
    assert "error TEXT" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_pipeline_tasks_state_stage" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_pipeline_stage_results_status" in sql


def test_pipeline_schema_has_live_activity_constraints_and_index() -> None:
    sql = schema_sql()

    assert "CONSTRAINT fk_pipeline_stage_activity_task" in sql
    assert "CONSTRAINT ck_pipeline_stage_activity_stage" in sql
    assert "CONSTRAINT ck_pipeline_stage_activity_attempt_positive" in sql
    assert "CONSTRAINT ck_pipeline_stage_activity_pgmq_msg_id_positive" in sql
    assert "CONSTRAINT ck_pipeline_stage_activity_pgmq_read_count_positive" in sql
    assert "CONSTRAINT ck_pipeline_stage_activity_worker_nonblank" in sql
    assert "CONSTRAINT ck_pipeline_stage_activity_node_nonblank" in sql
    assert "CONSTRAINT ck_pipeline_stage_activity_timestamps" in sql
    assert "pgmq_msg_id BIGINT NOT NULL" in sql
    assert "pgmq_read_count INTEGER NOT NULL" in sql
    assert "started_at TIMESTAMPTZ NOT NULL" in sql
    assert "heartbeat_at TIMESTAMPTZ NOT NULL" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_pipeline_stage_activity_heartbeat" in sql


def test_pipeline_schema_uses_the_fixed_state_stage_and_result_values() -> None:
    sql = schema_sql()

    assert "state IN ('queued', 'running', 'rejected', 'failed', 'completed')" in sql
    assert "current_stage IN ('generate', 'validate', 'reward', 'push')" in sql
    assert "stage IN ('generate', 'validate', 'reward', 'push')" in sql
    assert "status IN ('succeeded', 'rejected', 'failed')" in sql
    assert "content BYTEA NOT NULL" in sql
    assert "result JSONB NOT NULL DEFAULT '{}'::jsonb" in sql
    assert "started_at TIMESTAMPTZ NOT NULL" in sql
    assert "finished_at TIMESTAMPTZ NOT NULL" in sql
