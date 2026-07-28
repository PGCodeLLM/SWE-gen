from __future__ import annotations

from pathlib import Path

import orchestrator
from swegen.database import DatabasePRTask
from swegen.production_quota import ProductionQuota
from swegen.swr import SWRUploadResult


def test_successful_task_copy_is_unchanged(tmp_path: Path):
    entry = orchestrator.Entry(
        repo="Owner/Repo",
        pull_number="42",
        base_commit="a" * 40,
        instance_id="owner__repo-42",
    )
    tasks = tmp_path / "tasks"
    tasks_bz = tmp_path / "tasks_bz"
    source = tasks / entry.instance_id
    environment = source / "environment"
    environment.mkdir(parents=True)
    original_dockerfile = (
        "FROM ubuntu:24.04\n\nRUN git clone https://github.com/Owner/Repo.git /app/src\n"
    )
    original_task_toml = '[metadata]\nname = "original"\n'
    (environment / "Dockerfile").write_text(original_dockerfile)
    (source / "task.toml").write_text(original_task_toml)
    (source / "instruction.md").write_bytes(b"original instruction\n")

    status = orchestrator.copy_successful_task(entry, tasks, tasks_bz)

    copied = tasks_bz / entry.instance_id
    assert status == "original task copied unchanged"
    assert (copied / "environment" / "Dockerfile").read_text() == original_dockerfile
    assert (copied / "task.toml").read_text() == original_task_toml
    assert (copied / "instruction.md").read_bytes() == b"original instruction\n"


def test_tasks_postprocessed_keeps_clone_and_applies_network_git_fixes(
    tmp_path: Path,
    monkeypatch,
):
    entry = orchestrator.Entry(
        repo="Owner/Repo",
        pull_number="42",
        base_commit="a" * 40,
        instance_id="owner__repo-42",
        image_ref=(
            "swr-aifm-code-data-platform-6sudmx.swr-pro.myhuaweicloud.com/"
            "swesandbox/public/repo/platform/pool-00001:owner-repo-v1"
        ),
    )
    tasks = tmp_path / "tasks"
    tasks_postprocessed = tmp_path / "tasks_postprocessed"
    source = tasks / entry.instance_id
    environment = source / "environment"
    environment.mkdir(parents=True)
    (source / "task.toml").write_text("[metadata]\n")
    (environment / "Dockerfile").write_text(
        "FROM ubuntu:24.04\n\n"
        "RUN git clone https://github.com/Owner/Repo.git /app/src\n"
        "RUN cd /app/src && git checkout --detach " + "b" * 40 + "\n"
    )
    monkeypatch.setattr(orchestrator, "fetch_issue_number", lambda *_args: "7")

    status = orchestrator.postprocess_task(entry, tasks, tasks_postprocessed, [])

    dockerfile = (
        tasks_postprocessed / entry.instance_id / "environment" / "Dockerfile"
    ).read_text()
    assert dockerfile.startswith(
        "FROM swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/"
        "swesandbox/public/repo/platform/pool-00001:owner-repo-v1\n"
    )
    assert "# BEGIN SWEGEN PROXY CA SETUP" in dockerfile
    assert "git clone https://github.com/Owner/Repo.git /app/src" in dockerfile
    assert f"git fetch --depth=1 origin {'b' * 40}" in dockerfile
    assert "git reset --hard && git clean -fdx && git checkout --detach" in dockerfile
    assert (
        tasks_postprocessed / entry.instance_id / "environment" / "swegen-proxy-ca.crt"
    ).is_file()
    assert "task.toml updated" in status


def test_postprocessed_from_instruction_requires_database_image_ref():
    for image_ref in ("", "ubuntu:24.04"):
        try:
            orchestrator.postprocessed_from_instruction(image_ref)
        except ValueError as exc:
            assert "database image reference" in str(exc)
        else:
            raise AssertionError(f"expected invalid image ref to fail: {image_ref!r}")


def test_generated_skeleton_installs_proxy_before_network_package_steps():
    from swegen.create.task_skeleton import SkeletonParams, generate_dockerfile

    dockerfile = generate_dockerfile(
        SkeletonParams(
            repo_url="https://github.com/owner/repo.git",
            head_sha="a" * 40,
            base_sha="b" * 40,
            pr_number=1,
        )
    )
    assert dockerfile.index("# BEGIN SWEGEN PROXY CA SETUP") < dockerfile.index(
        "# Base system packages"
    )


class _OnePackageDatabase:
    def __init__(self):
        self.claimed = False
        self.marked = []

    def claim_repo_package(self, **_kwargs):
        if self.claimed:
            return []
        self.claimed = True
        return [
            DatabasePRTask(
                repo="owner/repo",
                pull_number=1,
                base_commit="a" * 40,
                instance_id="owner__repo-1",
                swegen_retries=1,
            )
        ]

    def mark_swegen_passed(self, instance_id):
        self.marked.append(instance_id)


def test_consumer_marks_database_only_after_successful_upload(tmp_path, monkeypatch):
    database = _OnePackageDatabase()
    tasks = tmp_path / "tasks"
    tasks_bz = tmp_path / "tasks_bz"
    tasks_postprocessed = tmp_path / "tasks_postprocessed"
    state = tmp_path / "state"
    logs = tmp_path / "logs"
    for path in (tasks, tasks_bz, tasks_postprocessed, state, logs):
        path.mkdir()
    events = []

    monkeypatch.setattr(
        orchestrator,
        "process_entry",
        lambda *_args, **_kwargs: orchestrator.ProcessResult(
            returncode=0,
            image_names=("local-image:latest",),
        ),
    )
    monkeypatch.setattr(orchestrator, "run_hacking_check", lambda *_args: (True, "clean"))

    def copy_successful(_entry, source_dir, destination_dir):
        assert source_dir == tasks
        assert destination_dir == tasks_bz
        events.append("copy")
        return "original task copied unchanged"

    def postprocess(_entry, source_dir, destination_dir, _tokens):
        assert source_dir == tasks
        assert destination_dir == tasks_postprocessed
        events.append("postprocess")
        return "postprocessed"

    monkeypatch.setattr(
        orchestrator,
        "copy_successful_task",
        copy_successful,
    )
    monkeypatch.setattr(orchestrator, "postprocess_task", postprocess)
    monkeypatch.setattr(orchestrator, "load_swr_settings", lambda: object())
    monkeypatch.setattr(
        orchestrator,
        "upload_image_to_swr",
        lambda *_args: events.append("upload")
        or SWRUploadResult(True, "uploaded", "registry/repository/ea_sz_owner__repo-1:latest"),
    )

    outcomes = orchestrator.run_consumer(
        0,
        database,
        False,
        False,
        10,
        {},
        "swegen",
        logs,
        output_dir=tasks,
        state_dir=state,
        successful_dir=tasks_bz,
        postprocessed_dir=tasks_postprocessed,
    )

    assert outcomes[0].ok
    assert events == ["copy", "postprocess", "upload"]
    assert outcomes[0].successful_copy_status == "original task copied unchanged"
    assert outcomes[0].postprocess_status == "postprocessed"
    assert database.marked == ["owner__repo-1"]
    assert outcomes[0].image_prune_allowed
    assert outcomes[0].swr_remote_ref in outcomes[0].image_names


def test_consumer_retains_image_and_does_not_mark_database_on_upload_failure(
    tmp_path,
    monkeypatch,
):
    database = _OnePackageDatabase()
    paths = [
        tmp_path / name for name in ("tasks", "tasks_bz", "tasks_postprocessed", "state", "logs")
    ]
    for path in paths:
        path.mkdir()
    tasks, tasks_bz, tasks_postprocessed, state, logs = paths

    monkeypatch.setattr(
        orchestrator,
        "process_entry",
        lambda *_args, **_kwargs: orchestrator.ProcessResult(
            returncode=0,
            image_names=("local-image:latest",),
        ),
    )
    monkeypatch.setattr(orchestrator, "run_hacking_check", lambda *_args: (True, "clean"))
    monkeypatch.setattr(
        orchestrator,
        "copy_successful_task",
        lambda *_args: "original task copied unchanged",
    )
    monkeypatch.setattr(orchestrator, "postprocess_task", lambda *_args: "postprocessed")
    monkeypatch.setattr(orchestrator, "load_swr_settings", lambda: object())
    monkeypatch.setattr(
        orchestrator,
        "upload_image_to_swr",
        lambda *_args: SWRUploadResult(False, "push failed", "remote:latest"),
    )

    outcome = orchestrator.run_consumer(
        0,
        database,
        False,
        False,
        10,
        {},
        "swegen",
        logs,
        output_dir=tasks,
        state_dir=state,
        successful_dir=tasks_bz,
        postprocessed_dir=tasks_postprocessed,
    )[0]

    assert not outcome.ok
    assert outcome.failure_reason == "push failed"
    assert not outcome.image_prune_allowed
    assert database.marked == []


class _TwoTaskDatabase(_OnePackageDatabase):
    def __init__(self):
        super().__init__()
        self.released = []

    def claim_repo_package(self, **_kwargs):
        if self.claimed:
            return []
        self.claimed = True
        return [
            DatabasePRTask(
                repo="owner/repo",
                pull_number=number,
                base_commit="a" * 40,
                instance_id=f"owner__repo-{number}",
                swegen_retries=1,
            )
            for number in (1, 2)
        ]

    def release_claims(self, tasks):
        self.released.extend(task.instance_id for task in tasks)
        return len(tasks)


def test_consumer_stops_at_success_quota_and_releases_unprocessed_claims(tmp_path, monkeypatch):
    database = _TwoTaskDatabase()
    tasks = tmp_path / "tasks"
    tasks_bz = tmp_path / "tasks_bz"
    tasks_postprocessed = tmp_path / "tasks_postprocessed"
    state = tmp_path / "state"
    logs = tmp_path / "logs"
    for path in (tasks, tasks_bz, tasks_postprocessed, state, logs):
        path.mkdir()
    quota = ProductionQuota(
        state / "production-quota.json",
        1,
        stale_after_seconds=3600,
        poll_interval=0.01,
    )

    monkeypatch.setattr(
        orchestrator,
        "process_entry",
        lambda *_args, **_kwargs: orchestrator.ProcessResult(
            returncode=0,
            image_names=("local-image:latest",),
        ),
    )
    monkeypatch.setattr(orchestrator, "run_hacking_check", lambda *_args: (True, "clean"))
    monkeypatch.setattr(
        orchestrator,
        "copy_successful_task",
        lambda *_args: "original task copied unchanged",
    )
    monkeypatch.setattr(orchestrator, "postprocess_task", lambda *_args: "postprocessed")
    monkeypatch.setattr(orchestrator, "load_swr_settings", lambda: object())
    monkeypatch.setattr(
        orchestrator,
        "upload_image_to_swr",
        lambda *_args: SWRUploadResult(True, "uploaded", "remote:latest"),
    )

    outcomes = orchestrator.run_consumer(
        0,
        database,
        False,
        False,
        10,
        {},
        "swegen",
        logs,
        output_dir=tasks,
        state_dir=state,
        successful_dir=tasks_bz,
        postprocessed_dir=tasks_postprocessed,
        production_quota=quota,
    )

    assert [outcome.entry.instance_id for outcome in outcomes] == ["owner__repo-1"]
    assert database.marked == ["owner__repo-1"]
    assert database.released == ["owner__repo-2"]
    assert quota.snapshot().successes == 1
