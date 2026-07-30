from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from uuid import UUID

import pytest
from harbor.models.job.result import JobResult, JobStats
from harbor.models.task.id import LocalTaskId
from harbor.models.trial.config import TaskConfig, TrialConfig
from harbor.models.trial.result import AgentInfo, TrialResult
from harbor.models.verifier.result import VerifierResult

from swegen.tools import harbor_runner
from swegen.tools.harbor_runner import HarborRunCancelled, parse_harbor_outcome


def write_job_result(path: Path, reward: int | float) -> None:
    task_path = Path("task")
    trial = TrialResult(
        task_name="task",
        trial_name="trial",
        trial_uri="file:///trial",
        task_id=LocalTaskId(path=task_path),
        task_checksum="checksum",
        config=TrialConfig(task=TaskConfig(path=task_path)),
        agent_info=AgentInfo(name="nop", version="1"),
        verifier_result=VerifierResult(rewards={"reward": reward}),
    )
    result = JobResult(
        id=UUID("12345678-1234-5678-1234-567812345678"),
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        finished_at=datetime(2026, 1, 1, tzinfo=UTC),
        n_total_trials=1,
        stats=JobStats(n_trials=1),
        trial_results=[trial],
    )
    path.write_text(result.model_dump_json())


def write_raw_reward(path: Path, serialized_reward: str) -> None:
    write_job_result(path, 0.5)
    path.write_text(path.read_text().replace('"reward":0.5', f'"reward":{serialized_reward}'))


@pytest.mark.parametrize("reward", [0.5, 1.5])
def test_parse_harbor_outcome_preserves_fractional_reward(
    tmp_path: Path,
    reward: float,
) -> None:
    result_path = tmp_path / "result.json"
    write_job_result(result_path, reward)

    outcome = parse_harbor_outcome(result_path)

    assert outcome.reward == reward
    assert type(outcome.reward) is float


@pytest.mark.parametrize("serialized_reward", ["true", "NaN", "Infinity", "-Infinity"])
def test_parse_harbor_outcome_rejects_bool_and_nonfinite_rewards(
    tmp_path: Path,
    serialized_reward: str,
) -> None:
    result_path = tmp_path / "result.json"
    write_raw_reward(result_path, serialized_reward)

    outcome = parse_harbor_outcome(result_path)

    assert outcome.reward is None


def test_parse_harbor_outcome_rejects_unparseable_reward(tmp_path: Path) -> None:
    result_path = tmp_path / "result.json"
    write_raw_reward(result_path, '"not-a-number"')

    outcome = parse_harbor_outcome(result_path)

    assert outcome.reward is None


def test_run_harbor_agent_cancels_process_group_and_reaps_containers(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cancel_event = Event()
    cancel_event.set()
    reaped: list[str] = []
    monkeypatch.setattr(
        harbor_runner,
        "harbor_cmd_base",
        lambda: [sys.executable, "-c", "import time; time.sleep(60)"],
    )
    monkeypatch.setattr(harbor_runner, "suffixed_docker_config_args", lambda *_: [])
    monkeypatch.setattr(
        harbor_runner,
        "_reap_harbor_containers",
        lambda task_id, _environment: reaped.append(task_id),
    )

    with pytest.raises(HarborRunCancelled, match="worker shutdown"):
        harbor_runner.run_harbor_agent(
            "owner__repo-1",
            tmp_path / "tasks",
            tmp_path / "jobs",
            "nop",
            capture_output=True,
            wall_timeout_seconds=60,
            cancel_event=cancel_event,
        )

    assert reaped == ["owner__repo-1"]


def test_run_harbor_agent_disables_compose_bake_delegation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    check_environment = (
        "import os, sys; "
        "sys.exit(0 if os.environ.get('COMPOSE_BAKE') == 'false' "
        "and os.environ.get('DOCKER_BUILDKIT') == '1' else 7)"
    )
    monkeypatch.setattr(
        harbor_runner,
        "harbor_cmd_base",
        lambda: [sys.executable, "-c", check_environment],
    )
    monkeypatch.setattr(harbor_runner, "suffixed_docker_config_args", lambda *_: [])
    monkeypatch.setattr(harbor_runner, "_reap_harbor_containers", lambda *_: None)

    exit_code, result_path = harbor_runner.run_harbor_agent(
        "owner__repo-1",
        tmp_path / "tasks",
        tmp_path / "jobs",
        "nop",
        capture_output=True,
        wall_timeout_seconds=60,
    )

    assert exit_code == 0
    assert result_path is None


def test_duplicate_compose_builds_keep_only_newest_retry() -> None:
    processes = (
        harbor_runner._ComposeBuildProcess(
            pid=101,
            started_at_ticks=1_000,
            project="owner__repo-1__suffix",
        ),
        harbor_runner._ComposeBuildProcess(
            pid=202,
            started_at_ticks=2_000,
            project="owner__repo-1__suffix",
        ),
        harbor_runner._ComposeBuildProcess(
            pid=303,
            started_at_ticks=500,
            project="another__repo-2__suffix",
        ),
    )

    assert harbor_runner._duplicate_compose_build_pids(processes) == (101,)


def test_duplicate_compose_reaper_escalates_stuck_client(monkeypatch) -> None:
    processes = (
        harbor_runner._ComposeBuildProcess(
            pid=101,
            started_at_ticks=1_000,
            project="owner__repo-1__suffix",
        ),
        harbor_runner._ComposeBuildProcess(
            pid=202,
            started_at_ticks=2_000,
            project="owner__repo-1__suffix",
        ),
    )
    signals: list[tuple[int, int]] = []
    clock = iter((10.0, 10.0 + harbor_runner.COMPOSE_REAP_GRACE_SECONDS))
    monkeypatch.setattr(harbor_runner, "_compose_build_processes", lambda _pgid: processes)
    monkeypatch.setattr(harbor_runner.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(
        harbor_runner.os,
        "kill",
        lambda pid, selected_signal: signals.append((pid, selected_signal)),
    )
    terminating_since: dict[int, float] = {}

    harbor_runner._reap_duplicate_compose_builds(999, terminating_since)
    harbor_runner._reap_duplicate_compose_builds(999, terminating_since)

    assert signals == [(101, harbor_runner.signal.SIGTERM), (101, harbor_runner.signal.SIGKILL)]
