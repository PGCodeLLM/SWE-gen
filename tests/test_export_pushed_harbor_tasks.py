from __future__ import annotations

import importlib.util
import json
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

EXPORTER = Path(__file__).resolve().parents[1] / "deploy" / "k3s" / "export-pushed-harbor-tasks.py"


def _load():
    spec = importlib.util.spec_from_file_location("export_pushed_harbor_tasks", EXPORTER)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _manifest(tasks: list[dict[str, object]]) -> dict[str, object]:
    return {"generated_at": "2026-08-03T15:31:38+00:00", "tasks": tasks}


class FakeCursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.itersize = 0
        self.calls: list[tuple[str, dict[str, object] | None]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, query: str, parameters: dict[str, object] | None = None):
        self.calls.append((query, parameters))
        return iter(self.rows)


class FakeConnection:
    def __init__(
        self,
        task_rows: list[dict[str, object]],
        file_rows: list[dict[str, object]],
    ) -> None:
        self.task_cursor = FakeCursor(task_rows)
        self.file_cursor = FakeCursor(file_rows)
        self.statements: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, statement: str) -> None:
        self.statements.append(statement)

    def cursor(self, *, name: str) -> FakeCursor:
        return self.task_cursor if name == "pushed_tasks" else self.file_cursor


def test_load_excluded_tasks_from_json_and_zip(tmp_path: Path) -> None:
    module = _load()
    manifest = _manifest(
        [
            {"task_id": "owner__repo-2", "task_version": 2},
            {"task_id": "owner__repo-1", "task_version": 1},
        ]
    )
    json_path = tmp_path / "manifest.json"
    json_path.write_text(json.dumps(manifest))
    zip_path = tmp_path / "baseline.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("manifest.json", json.dumps(manifest))

    expected = {("owner__repo-1", 1), ("owner__repo-2", 2)}
    assert module._load_excluded_tasks(json_path) == expected
    assert module._load_excluded_tasks(zip_path) == expected


def test_load_excluded_tasks_rejects_state_file_without_tasks(tmp_path: Path) -> None:
    module = _load()
    state_path = tmp_path / "baseline.state.json"
    state_path.write_text(json.dumps({"task_count": 3482}))

    with pytest.raises(SystemExit, match="has no tasks array"):
        module._load_excluded_tasks(state_path)


def test_parse_timestamp_requires_offset_and_normalizes_to_utc() -> None:
    module = _load()

    assert module._parse_timestamp("2026-08-07T17:07:04+08:00") == datetime(
        2026, 8, 7, 9, 7, 4, tzinfo=UTC
    )
    with pytest.raises(Exception, match="UTC offset"):
        module._parse_timestamp("2026-08-07T17:07:04")


def test_export_applies_exclusion_cutoff_and_count_guard(tmp_path: Path, monkeypatch) -> None:
    module = _load()
    cutoff = datetime(2026, 8, 7, 9, 7, 4, tzinfo=UTC)
    pushed_at = datetime(2026, 8, 7, 8, 0, tzinfo=UTC)
    connection = FakeConnection(
        [
            {
                "task_id": "new__task-2",
                "task_version": 1,
                "repo": "new/task",
                "pr": 2,
                "finished_at": pushed_at,
                "result": {"remote_tag": "registry/new-task:2", "registry": "platform"},
            }
        ],
        [
            {
                "task_id": "new__task-2",
                "task_version": 1,
                "path": "environment/Dockerfile",
                "content": b"FROM source\n",
                "mode": 0o644,
            },
            {
                "task_id": "new__task-2",
                "task_version": 1,
                "path": "solution/solve.sh",
                "content": b"#!/bin/sh\n",
                "mode": 0o755,
            },
        ],
    )
    monkeypatch.setattr(module, "_connection_string", lambda: "unused")
    monkeypatch.setattr(module.psycopg, "connect", lambda *_args, **_kwargs: connection)
    baseline = tmp_path / "baseline.json"
    baseline.write_text(json.dumps(_manifest([{"task_id": "old__task-1", "task_version": 1}])))
    output = tmp_path / "delta.zip"

    summary = module.export(
        output,
        exclude_manifest=baseline,
        pushed_through=cutoff,
        expected_task_count=1,
    )

    assert summary["task_count"] == 1
    assert summary["excluded_task_count"] == 1
    assert connection.statements == ["SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"]
    for cursor in (connection.task_cursor, connection.file_cursor):
        parameters = cursor.calls[0][1]
        assert parameters is not None
        assert parameters["pushed_through"] == cutoff
        assert json.loads(parameters["excluded_tasks"]) == [
            {"task_id": "old__task-1", "task_version": 1}
        ]

    with zipfile.ZipFile(output) as archive:
        names = set(archive.namelist())
        assert "new__task-2__v1/environment/Dockerfile" in names
        assert "new__task-2__v1/environment/Dockerfile.source" in names
        assert "new__task-2__v1/swr-image.json" in names
        exported = json.loads(archive.read("manifest.json"))
        assert exported["task_count"] == 1
        assert exported["excluded_task_count"] == 1
        assert exported["pushed_through"] == cutoff.isoformat()
        solve = archive.getinfo("new__task-2__v1/solution/solve.sh")
        assert solve.external_attr >> 16 == 0o755


def test_export_count_mismatch_does_not_create_archive(tmp_path: Path, monkeypatch) -> None:
    module = _load()
    connection = FakeConnection(
        [
            {
                "task_id": "new__task-2",
                "task_version": 1,
                "repo": "new/task",
                "pr": 2,
                "finished_at": datetime(2026, 8, 7, tzinfo=UTC),
                "result": {"remote_tag": "registry/new-task:2"},
            }
        ],
        [],
    )
    monkeypatch.setattr(module, "_connection_string", lambda: "unused")
    monkeypatch.setattr(module.psycopg, "connect", lambda *_args, **_kwargs: connection)
    output = tmp_path / "delta.zip"

    with pytest.raises(SystemExit, match="selected 1 tasks, expected 2"):
        module.export(output, expected_task_count=2)

    assert not output.exists()
    assert connection.file_cursor.calls == []
