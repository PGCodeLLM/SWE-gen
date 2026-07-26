from __future__ import annotations

from pathlib import Path

import orchestrator
from swegen.database import DatabasePRTask
from swegen.swr import SWRUploadResult


def test_tasks_bz_postprocessing_keeps_clone_and_applies_network_git_fixes(
    tmp_path: Path,
    monkeypatch,
):
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
    (source / "task.toml").write_text("[metadata]\n")
    (environment / "Dockerfile").write_text(
        "FROM ubuntu:24.04\n\n"
        "RUN git clone https://github.com/Owner/Repo.git /app/src\n"
        "RUN cd /app/src && git checkout --detach " + "b" * 40 + "\n"
    )
    monkeypatch.setattr(orchestrator, "fetch_issue_number", lambda *_args: "7")

    status = orchestrator.postprocess_task(entry, tasks, tasks_bz, [])

    dockerfile = (tasks_bz / entry.instance_id / "environment" / "Dockerfile").read_text()
    assert dockerfile.startswith(
        "FROM swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/swesandbox"
    )
    assert "# BEGIN SWEGEN PROXY CA SETUP" in dockerfile
    assert "git clone https://github.com/Owner/Repo.git /app/src" in dockerfile
    assert f"git fetch --depth=1 origin {'b' * 40}" in dockerfile
    assert "git reset --hard && git clean -fdx && git checkout --detach" in dockerfile
    assert (tasks_bz / entry.instance_id / "environment" / "swegen-proxy-ca.crt").is_file()
    assert "task.toml updated" in status


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
    state = tmp_path / "state"
    logs = tmp_path / "logs"
    for path in (tasks, tasks_bz, state, logs):
        path.mkdir()

    monkeypatch.setattr(
        orchestrator,
        "process_entry",
        lambda *_args, **_kwargs: orchestrator.ProcessResult(
            returncode=0,
            image_names=("local-image:latest",),
        ),
    )
    monkeypatch.setattr(orchestrator, "run_hacking_check", lambda *_args: (True, "clean"))
    monkeypatch.setattr(orchestrator, "postprocess_task", lambda *_args: "postprocessed")
    monkeypatch.setattr(orchestrator, "load_swr_settings", lambda: object())
    monkeypatch.setattr(
        orchestrator,
        "upload_image_to_swr",
        lambda *_args: SWRUploadResult(
            True,
            "uploaded",
            "registry/repository/ea_sz_owner__repo-1:latest",
        ),
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
        postprocessed_dir=tasks_bz,
    )

    assert outcomes[0].ok
    assert database.marked == ["owner__repo-1"]
    assert outcomes[0].image_prune_allowed
    assert outcomes[0].swr_remote_ref in outcomes[0].image_names


def test_consumer_retains_image_and_does_not_mark_database_on_upload_failure(
    tmp_path,
    monkeypatch,
):
    database = _OnePackageDatabase()
    paths = [tmp_path / name for name in ("tasks", "tasks_bz", "state", "logs")]
    for path in paths:
        path.mkdir()
    tasks, tasks_bz, state, logs = paths

    monkeypatch.setattr(
        orchestrator,
        "process_entry",
        lambda *_args, **_kwargs: orchestrator.ProcessResult(
            returncode=0,
            image_names=("local-image:latest",),
        ),
    )
    monkeypatch.setattr(orchestrator, "run_hacking_check", lambda *_args: (True, "clean"))
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
        postprocessed_dir=tasks_bz,
    )[0]

    assert not outcome.ok
    assert outcome.failure_reason == "push failed"
    assert not outcome.image_prune_allowed
    assert database.marked == []
