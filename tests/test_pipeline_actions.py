from __future__ import annotations

import sys
from pathlib import Path
from uuid import UUID

import pytest

from reward_hacking_detector.hacking import HackCheckResult
from swegen.pipeline.models import PipelineTask, StageResultStatus
from swegen.queueing.models import PipelineStage
from swegen.tools.harbor_runner import HarborOutcome

_ACTION_ENVIRONMENT_NAMES = (
    "SWEGEN_REPO_CACHE_DIR",
    "SWEGEN_CC_TIMEOUT_SECONDS",
    "SWEGEN_GENERATE_TIMEOUT_SECONDS",
    "SWEGEN_HARBOR_TIMEOUT_SECONDS",
    "SWEGEN_REWARD_ENDPOINT",
    "SWEGEN_REWARD_PRIMARY_MODEL",
    "SWEGEN_REWARD_FALLBACK_MODEL",
    "SWEGEN_REWARD_API_KEY",
    "SWEGEN_SWR_HOST",
    "SWEGEN_SWR_REPOSITORY",
    "SWEGEN_SWR_REGISTRY",
    "SWEGEN_SWR_SUFFIX",
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
)


@pytest.fixture(autouse=True)
def clear_pipeline_action_environment(monkeypatch) -> None:
    for name in _ACTION_ENVIRONMENT_NAMES:
        monkeypatch.delenv(name, raising=False)


def make_task() -> PipelineTask:
    return PipelineTask(
        task_id="owner__repo-42",
        task_version=1,
        repo="owner/repo",
        pr=42,
        trace_id=UUID("12345678-1234-5678-1234-567812345678"),
    )


def test_build_generate_command_uses_relay_flags_and_workspace_paths(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline.actions import build_generate_command

    monkeypatch.delenv("SWEGEN_REPO_CACHE_DIR", raising=False)

    command = build_generate_command(make_task(), tmp_path)

    assert command[:2] == ["swegen", "create"]
    assert command[command.index("--repo") + 1] == "owner/repo"
    assert command[command.index("--pr") + 1] == "42"
    assert command[command.index("--output") + 1] == str(tmp_path / "tasks")
    assert command[command.index("--state-dir") + 1] == str(tmp_path / ".swegen")
    assert command[command.index("--repo-cache-dir") + 1] == str(tmp_path / ".swegen" / "repos")
    assert command[command.index("--cc-timeout") + 1] == "10800"
    assert {
        "--no-validate",
        "--force",
        "--no-require-minimum-difficulty",
        "--no-require-issue",
    } <= set(command)


def test_build_generate_command_uses_configured_persistent_repo_cache(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline.actions import build_generate_command

    persistent_cache = tmp_path.parent / "persistent-repos"
    monkeypatch.setenv("SWEGEN_REPO_CACHE_DIR", str(persistent_cache))

    command = build_generate_command(make_task(), tmp_path)

    assert command[command.index("--repo-cache-dir") + 1] == str(persistent_cache)


@pytest.mark.parametrize("value", ["0", "-1", "1.5", "not-a-number"])
def test_build_generate_command_rejects_invalid_cc_timeout(
    tmp_path: Path,
    monkeypatch,
    value: str,
) -> None:
    from swegen.pipeline.actions import build_generate_command

    monkeypatch.setenv("SWEGEN_CC_TIMEOUT_SECONDS", value)

    with pytest.raises(ValueError, match="positive integer"):
        build_generate_command(make_task(), tmp_path)


def test_build_generate_command_uses_configured_cc_timeout(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline.actions import build_generate_command

    monkeypatch.setenv("SWEGEN_CC_TIMEOUT_SECONDS", "12000")

    command = build_generate_command(make_task(), tmp_path)

    assert command[command.index("--cc-timeout") + 1] == "12000"


def test_generate_outer_timeout_default_exceeds_claude_timeout_default() -> None:
    from swegen.pipeline import actions

    assert actions.DEFAULT_GENERATE_TIMEOUT_SECONDS > actions.DEFAULT_CC_TIMEOUT_SECONDS


def test_generate_action_captures_generated_task_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task_dir = tmp_path / "tasks" / make_task().task_id
    monkeypatch.setattr(
        actions,
        "build_generate_command",
        lambda task, workspace: [
            sys.executable,
            "-c",
            (
                "from pathlib import Path; import sys; "
                "p=Path(sys.argv[1]); p.mkdir(parents=True); "
                "(p/'instruction.md').write_text('fixed'); print('x' * 100)"
            ),
            str(task_dir),
        ],
    )
    monkeypatch.setattr(
        actions,
        "MAX_COMMAND_LOG_BYTES",
        16,
        raising=False,
    )

    execution = actions.generate_action(make_task(), tmp_path)

    assert execution.status is StageResultStatus.SUCCEEDED
    assert [(task_file.path, task_file.content) for task_file in execution.files] == [
        ("instruction.md", b"fixed")
    ]
    result = execution.result_json()
    assert result["duration_seconds"] >= 0
    assert result["file_count"] == 1
    assert result["total_bytes"] == 5
    assert result["log_bytes"] == 16
    assert result["output_truncated"] is True
    assert (tmp_path / ".swegen" / "logs" / "generate.log").stat().st_size == 16


def test_generate_action_raises_when_create_command_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    monkeypatch.setattr(
        actions,
        "build_generate_command",
        lambda task, workspace: [sys.executable, "-c", "raise SystemExit(7)"],
    )

    with pytest.raises(RuntimeError, match="status 7"):
        actions.generate_action(make_task(), tmp_path)


def test_generate_action_raises_when_expected_task_directory_is_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    monkeypatch.setattr(
        actions,
        "build_generate_command",
        lambda task, workspace: [sys.executable, "-c", "pass"],
    )

    with pytest.raises(RuntimeError, match="expected generated task directory"):
        actions.generate_action(make_task(), tmp_path)


def test_validate_action_accepts_exact_nop_zero_and_oracle_one(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    (tmp_path / "tasks" / task.task_id).mkdir(parents=True)
    calls: list[str] = []

    def fake_run_harbor_agent(
        task_id,
        dataset_path,
        jobs_dir,
        agent,
        **kwargs,
    ):
        assert task_id == task.task_id
        assert dataset_path == tmp_path / "tasks"
        assert jobs_dir == tmp_path / ".swegen" / "harbor-jobs"
        assert kwargs["capture_output"] is False
        assert kwargs["delete_after"] is False
        assert kwargs["wall_timeout_seconds"] > 0
        calls.append(agent)
        result_path = tmp_path / f"{agent}.json"
        result_path.write_text("{}")
        return 0, result_path

    monkeypatch.setattr(
        actions,
        "run_harbor_agent",
        fake_run_harbor_agent,
        raising=False,
    )
    monkeypatch.setattr(
        actions,
        "parse_harbor_outcome",
        lambda path: HarborOutcome(reward=0 if path.name == "nop.json" else 1, error=None),
        raising=False,
    )

    execution = actions.validate_action(task, tmp_path)

    assert calls == ["nop", "oracle"]
    assert execution.status is StageResultStatus.SUCCEEDED
    assert execution.result_json() == {"nop_reward": 0, "oracle_reward": 1}


def test_validate_action_rejects_unexpected_nop_reward_without_oracle(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    (tmp_path / "tasks" / task.task_id).mkdir(parents=True)
    calls: list[str] = []

    def fake_run_harbor_agent(task_id, dataset_path, jobs_dir, agent, **kwargs):
        calls.append(agent)
        return 0, tmp_path / f"{agent}.json"

    monkeypatch.setattr(actions, "run_harbor_agent", fake_run_harbor_agent)
    monkeypatch.setattr(
        actions,
        "parse_harbor_outcome",
        lambda path: HarborOutcome(reward=1, error=None),
    )

    execution = actions.validate_action(task, tmp_path)

    assert calls == ["nop"]
    assert execution.status is StageResultStatus.REJECTED
    assert execution.result_json() == {
        "reason": "unexpected_nop_reward",
        "nop_reward": 1,
    }


def test_validate_action_rejects_unexpected_oracle_reward(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    (tmp_path / "tasks" / task.task_id).mkdir(parents=True)

    def fake_run_harbor_agent(task_id, dataset_path, jobs_dir, agent, **kwargs):
        return 0, tmp_path / f"{agent}.json"

    monkeypatch.setattr(actions, "run_harbor_agent", fake_run_harbor_agent)
    monkeypatch.setattr(
        actions,
        "parse_harbor_outcome",
        lambda path: HarborOutcome(reward=0, error=None),
    )

    execution = actions.validate_action(task, tmp_path)

    assert execution.status is StageResultStatus.REJECTED
    assert execution.result_json() == {
        "reason": "unexpected_oracle_reward",
        "nop_reward": 0,
        "oracle_reward": 0,
    }


@pytest.mark.parametrize(
    ("exit_code", "outcome"),
    [
        pytest.param(2, HarborOutcome(reward=0, error=None), id="nonzero-exit"),
        pytest.param(0, HarborOutcome(reward=None, error=None), id="missing-outcome"),
        pytest.param(0, HarborOutcome(reward=0, error="docker failed"), id="execution-error"),
    ],
)
def test_validate_action_raises_for_harbor_infrastructure_failures(
    tmp_path: Path,
    monkeypatch,
    exit_code: int,
    outcome: HarborOutcome,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    (tmp_path / "tasks" / task.task_id).mkdir(parents=True)
    calls: list[str] = []

    def fake_run_harbor_agent(task_id, dataset_path, jobs_dir, agent, **kwargs):
        calls.append(agent)
        return exit_code, tmp_path / "result.json"

    monkeypatch.setattr(actions, "run_harbor_agent", fake_run_harbor_agent)
    monkeypatch.setattr(actions, "parse_harbor_outcome", lambda path: outcome)

    with pytest.raises(RuntimeError, match="Harbor nop"):
        actions.validate_action(task, tmp_path)

    assert calls == ["nop"]


def test_reward_action_raises_when_test_bundle_is_empty(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    (tmp_path / "tasks" / task.task_id).mkdir(parents=True)
    monkeypatch.setattr(actions, "build_test_bundle", lambda task_dir: "", raising=False)

    with pytest.raises(RuntimeError, match="test bundle"):
        actions.reward_action(task, tmp_path)


def test_reward_action_raises_when_detector_returns_infrastructure_error(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    task_dir = tmp_path / "tasks" / task.task_id
    task_dir.mkdir(parents=True)
    monkeypatch.setenv("SWEGEN_REWARD_API_KEY", "dedicated-secret")
    monkeypatch.setenv("OPENAI_API_KEY", "generic-secret")
    monkeypatch.setattr(actions, "build_test_bundle", lambda path: "test bundle")

    async def fake_check(test_bundle, primary, fallback, task_id, instance_dir):
        assert test_bundle == "test bundle"
        assert primary.endpoint == "https://arcyleung-ubuntu.tailb940e6.ts.net"
        assert primary.model == "gpt-5.3-codex-spark"
        assert fallback.model == "gpt-5.6-sol"
        assert primary.api_key == fallback.api_key == "dedicated-secret"
        assert task_id == task.task_id
        assert instance_dir == task_dir
        verdict = HackCheckResult(
            is_hacking=False,
            reason="unavailable",
            prompt="must-not-be-stored",
            raw_response="must-not-be-stored",
            error="network unavailable",
        )
        return primary, verdict, [(primary, verdict)]

    monkeypatch.setattr(
        actions,
        "check_instance_with_fallback",
        fake_check,
        raising=False,
    )

    with pytest.raises(RuntimeError, match="reward-hacking checker"):
        actions.reward_action(task, tmp_path)


def test_reward_action_rejects_hacking_with_compact_evidence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    (tmp_path / "tasks" / task.task_id).mkdir(parents=True)
    monkeypatch.setenv("SWEGEN_REWARD_ENDPOINT", "https://reward.example")
    monkeypatch.setenv("SWEGEN_REWARD_PRIMARY_MODEL", "primary-model")
    monkeypatch.setenv("SWEGEN_REWARD_FALLBACK_MODEL", "fallback-model")
    monkeypatch.setenv("SWEGEN_REWARD_API_KEY", "secret-key")
    monkeypatch.setattr(actions, "build_test_bundle", lambda path: "test bundle")

    async def fake_check(test_bundle, primary, fallback, **kwargs):
        assert primary.endpoint == fallback.endpoint == "https://reward.example"
        verdict = HackCheckResult(
            is_hacking=True,
            reason="tests inspect source text",
            test_framework="pytest",
            prompt="raw prompt secret-key",
            raw_response="raw response secret-key",
        )
        return primary, verdict, [(primary, verdict)]

    monkeypatch.setattr(actions, "check_instance_with_fallback", fake_check)

    execution = actions.reward_action(task, tmp_path)

    assert execution.status is StageResultStatus.REJECTED
    assert execution.result_json() == {
        "selected_model": "primary-model",
        "attempted_models": ["primary-model"],
        "used_fallback": False,
        "framework": "pytest",
        "reason": "tests inspect source text",
    }
    assert "secret-key" not in repr(execution.result_json())


def test_reward_action_succeeds_with_clean_fallback_verdict(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    (tmp_path / "tasks" / task.task_id).mkdir(parents=True)
    monkeypatch.setenv("SWEGEN_REWARD_PRIMARY_MODEL", "primary-model")
    monkeypatch.setenv("SWEGEN_REWARD_FALLBACK_MODEL", "fallback-model")
    monkeypatch.setattr(actions, "build_test_bundle", lambda path: "test bundle")

    async def fake_check(test_bundle, primary, fallback, **kwargs):
        primary_error = HackCheckResult(
            is_hacking=False,
            reason="primary unavailable",
            error="retryable",
            prompt="discarded primary prompt",
            raw_response="discarded primary response",
        )
        clean = HackCheckResult(
            is_hacking=False,
            reason="tests execute behavior",
            test_framework="pytest",
            prompt="discarded fallback prompt",
            raw_response="discarded fallback response",
        )
        return fallback, clean, [(primary, primary_error), (fallback, clean)]

    monkeypatch.setattr(actions, "check_instance_with_fallback", fake_check)

    execution = actions.reward_action(task, tmp_path)

    assert execution.status is StageResultStatus.SUCCEEDED
    assert execution.result_json() == {
        "selected_model": "fallback-model",
        "attempted_models": ["primary-model", "fallback-model"],
        "used_fallback": True,
        "framework": "pytest",
        "reason": "tests execute behavior",
    }
    assert "discarded" not in repr(execution.result_json())


def test_push_action_skips_build_when_remote_manifest_exists(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    (tmp_path / "tasks" / task.task_id / "environment").mkdir(parents=True)
    checked: list[str] = []

    def fake_image_exists(remote_tag):
        checked.append(remote_tag)
        return True

    monkeypatch.setattr(
        actions,
        "image_exists_in_registry",
        fake_image_exists,
        raising=False,
    )
    monkeypatch.setattr(
        actions,
        "build_image_direct",
        lambda *args, **kwargs: pytest.fail("build must be skipped"),
        raising=False,
    )
    monkeypatch.setattr(
        actions,
        "remove_local_image",
        lambda tag: pytest.fail("no local image should be removed"),
        raising=False,
    )

    execution = actions.push_action(task, tmp_path)

    remote_tag = (
        "swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/"
        "swesandbox/public/swe-gen/feature-implementation/generated:owner__repo-42"
    )
    assert checked == [remote_tag]
    assert execution.status is StageResultStatus.SUCCEEDED
    assert execution.result_json() == {
        "remote_tag": remote_tag,
        "registry": "platform",
        "suffix": "_platform",
        "skipped": True,
        "already_present": True,
    }


def test_push_action_builds_pushes_and_removes_local_image(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    task_dir = tmp_path / "tasks" / task.task_id
    (task_dir / "environment").mkdir(parents=True)
    monkeypatch.setenv("SWEGEN_SWR_HOST", "registry.example")
    monkeypatch.setenv("SWEGEN_SWR_REPOSITORY", "team/generated")
    monkeypatch.setenv("SWEGEN_SWR_REGISTRY", "custom")
    monkeypatch.setenv("SWEGEN_SWR_SUFFIX", "_custom")
    for name in (
        "http_proxy",
        "https_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "no_proxy",
        "NO_PROXY",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example:8080")
    monkeypatch.setenv("NO_PROXY", ".example")
    removed: list[str] = []
    pushed: list[tuple[str, str]] = []

    monkeypatch.setattr(actions, "image_exists_in_registry", lambda remote_tag: False)
    monkeypatch.setattr(actions, "local_image_tag", lambda task_id: "local-source:latest")

    def fake_build(instance, directory, proxy_env, log):
        assert instance == task.task_id
        assert directory == task_dir
        assert proxy_env == {
            "HTTPS_PROXY": "http://proxy.example:8080",
            "NO_PROXY": ".example",
        }
        return "local-source:latest"

    def fake_push(local_tag, remote_tag, log):
        pushed.append((local_tag, remote_tag))
        return True

    monkeypatch.setattr(actions, "build_image_direct", fake_build)
    monkeypatch.setattr(actions, "push_to_registry", fake_push)
    monkeypatch.setattr(actions, "remove_local_image", removed.append)

    execution = actions.push_action(task, tmp_path)

    remote_tag = "registry.example/team/generated:owner__repo-42"
    assert pushed == [("local-source:latest", remote_tag)]
    assert removed == ["local-source:latest"]
    assert execution.status is StageResultStatus.SUCCEEDED
    assert execution.result_json() == {
        "remote_tag": remote_tag,
        "registry": "custom",
        "suffix": "_custom",
        "skipped": False,
        "already_present": False,
    }


def test_push_action_removes_local_image_when_push_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    (tmp_path / "tasks" / task.task_id / "environment").mkdir(parents=True)
    removed: list[str] = []
    monkeypatch.setattr(actions, "image_exists_in_registry", lambda remote_tag: False)
    monkeypatch.setattr(actions, "local_image_tag", lambda task_id: "local-source:latest")
    monkeypatch.setattr(
        actions,
        "build_image_direct",
        lambda *args, **kwargs: "local-source:latest",
    )
    monkeypatch.setattr(actions, "push_to_registry", lambda *args, **kwargs: False)
    monkeypatch.setattr(actions, "remove_local_image", removed.append)

    with pytest.raises(RuntimeError, match="image push failed"):
        actions.push_action(task, tmp_path)

    assert removed == ["local-source:latest"]


def test_action_for_stage_maps_all_pipeline_stages_and_rejects_unknown() -> None:
    from swegen.pipeline import actions

    assert actions.action_for_stage(PipelineStage.GENERATE) is actions.generate_action
    assert actions.action_for_stage(PipelineStage.VALIDATE) is actions.validate_action
    assert actions.action_for_stage(PipelineStage.REWARD) is actions.reward_action
    assert actions.action_for_stage(PipelineStage.PUSH) is actions.push_action

    with pytest.raises(ValueError, match="unsupported pipeline stage"):
        actions.action_for_stage("unknown")  # type: ignore[arg-type]
