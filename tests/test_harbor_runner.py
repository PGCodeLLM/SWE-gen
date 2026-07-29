from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from harbor.models.job.result import JobResult, JobStats
from harbor.models.task.id import LocalTaskId
from harbor.models.trial.config import TaskConfig, TrialConfig
from harbor.models.trial.result import AgentInfo, TrialResult
from harbor.models.verifier.result import VerifierResult

from swegen.tools.harbor_runner import parse_harbor_outcome


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
