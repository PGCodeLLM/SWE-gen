from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from uuid import UUID

from swegen.pipeline.models import PipelineTask


class RecordingConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    @contextmanager
    def transaction(self):
        yield

    def execute(self, query: str, params: tuple[object, ...]):
        self.calls.append((" ".join(query.split()), params))


def make_task() -> PipelineTask:
    return PipelineTask(
        task_id="owner__mixedrepo-42",
        task_version=1,
        repo="owner/mixedrepo",
        pr=42,
        trace_id=UUID("12345678-1234-5678-1234-567812345678"),
    )


def test_export_completed_task_persists_all_variants_and_deletes_only_three_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import completion

    task = make_task()
    task_dir = tmp_path / "workspace" / task.task_id
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "tests").mkdir()
    (task_dir / "solution").mkdir()
    dockerfile = "FROM ubuntu:24.04\nRUN echo original\n"
    test_sh = "#!/bin/sh\npytest -q\n"
    task_toml = (
        'version = "1.0"\n\n'
        "[environment]\n"
        "build_timeout_sec = 600\n\n"
        "[verifier]\n"
        "timeout_sec = 300\n"
    )
    (task_dir / "environment" / "Dockerfile").write_text(dockerfile)
    (task_dir / "environment" / "bug.patch").write_text("keep bug patch")
    (task_dir / "tests" / "test.sh").write_text(test_sh)
    (task_dir / "tests" / "fixture.txt").write_text("keep fixture")
    (task_dir / "task.toml").write_text(task_toml)
    (task_dir / "instruction.md").write_text("keep instruction")
    (task_dir / "solution" / "fix.patch").write_text("keep fix")

    output_dir = tmp_path / "successful_harbor_tasks"
    config = tmp_path / "swegen.toml"
    config.write_text(f'[completed_tasks]\noutput_dir = "{output_dir}"\n')
    monkeypatch.setenv("SWEGEN_CONFIG", str(config))
    monkeypatch.setenv("NODE_NAME", "worker-node-7")
    monkeypatch.setattr(
        completion.db,
        "query_one",
        lambda *args, **kwargs: {
            "repo_full_name": "Owner/MixedRepo",
            "base_commit": "a" * 40,
            "issue_number": None,
        },
    )
    connection = RecordingConnection()

    @contextmanager
    def fake_connection():
        yield connection

    monkeypatch.setattr(completion.db, "connection", fake_connection)

    exported = completion.export_completed_task(
        task,
        task_dir,
        voyager_image_ref="primary.example/team/generated:owner__mixedrepo-42",
        minddistiller_image_ref="mind.example/team/generated:owner__mixedrepo-42",
    )

    assert exported.directory == (output_dir / task.task_id).resolve()
    assert not exported.dockerfile_path.exists()
    assert not exported.config_path.exists()
    assert not exported.test_path.exists()
    assert (exported.directory / "environment" / "bug.patch").read_text() == "keep bug patch"
    assert (exported.directory / "tests" / "fixture.txt").read_text() == "keep fixture"
    assert (exported.directory / "instruction.md").read_text() == "keep instruction"
    assert (exported.directory / "solution" / "fix.patch").read_text() == "keep fix"

    # The authoritative workspace materialization is not changed by export.
    assert (task_dir / "environment" / "Dockerfile").read_text() == dockerfile
    assert (task_dir / "tests" / "test.sh").read_text() == test_sh
    assert (task_dir / "task.toml").read_text() == task_toml

    assert len(connection.calls) == 1
    query, params = connection.calls[0]
    assert "INSERT INTO public.completed_tasks" in query
    assert params[0] == task.task_id
    assert params[1] == "worker-node-7"
    assert params[2] == str(exported.directory)
    assert params[3:6] == (dockerfile, test_sh, task_toml)
    assert params[6] == dockerfile
    assert params[7] == test_sh
    assert 'docker_image = "mind.example/team/generated:owner__mixedrepo-42"' in params[8]
    assert params[9] == "FROM primary.example/team/generated:owner__mixedrepo-42"
    assert params[10] == test_sh
    assert "[cwm_task_metadata]" in params[11]
    assert 'repo_full_name = "Owner/MixedRepo"' in params[11]
    assert f'source_commit = "{"a" * 40}"' in params[11]
    assert "issue_number" not in params[11]


def test_cwm_metadata_includes_issue_number_when_public_supplies_it() -> None:
    from swegen.pipeline.completion import append_cwm_metadata

    rendered = append_cwm_metadata(
        "[environment]\n",
        make_task(),
        repo_full_name="Owner/MixedRepo",
        base_commit="b" * 40,
        issue_number="314",
    )

    assert 'issue_number = "314"' in rendered
