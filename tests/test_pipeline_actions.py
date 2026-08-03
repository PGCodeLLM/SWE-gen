from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path
from types import SimpleNamespace
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
    "SWEGEN_REPAIR_TIMEOUT_SECONDS",
    "SWEGEN_REWARD_REPAIR_TIMEOUT_SECONDS",
    "SWEGEN_REWARD_ENDPOINT",
    "SWEGEN_REWARD_PRIMARY_MODEL",
    "SWEGEN_REWARD_FALLBACK_MODEL",
    "SWEGEN_REWARD_API_KEY",
    "SWEGEN_REWARD_ALLOW_PROVIDER_KEY_FALLBACK",
    "SWEGEN_SWR_HOST",
    "SWEGEN_SWR_REPOSITORY",
    "SWEGEN_SWR_REGISTRY",
    "SWEGEN_SWR_SUFFIX",
    "GITHUB_TOKEN",
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


def process_is_running(pid: int) -> bool:
    try:
        stat_fields = Path(f"/proc/{pid}/stat").read_text().split()
    except FileNotFoundError:
        return False
    return len(stat_fields) > 2 and stat_fields[2] != "Z"


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


def test_generate_environment_selects_stable_token_from_config_pool(monkeypatch) -> None:
    from swegen.pipeline import actions

    tokens = ["github-token-a", "github-token-b", "github-token-c"]
    monkeypatch.setattr(actions, "load_github_tokens", lambda: tokens)

    environment = actions._generate_environment(make_task())

    assert environment["GITHUB_TOKEN"] == tokens[make_task().trace_id.int % len(tokens)]
    assert "GITHUB_TOKEN" not in os.environ


def test_generate_environment_preserves_explicit_github_token(monkeypatch) -> None:
    from swegen.pipeline import actions

    monkeypatch.setenv("GITHUB_TOKEN", "explicit-token")
    monkeypatch.setattr(
        actions,
        "load_github_tokens",
        lambda: pytest.fail("config token pool must not be loaded"),
    )

    environment = actions._generate_environment(make_task())

    assert environment["GITHUB_TOKEN"] == "explicit-token"


def test_proxy_ca_environment_is_injected_into_legacy_task_dockerfile(
    tmp_path: Path,
) -> None:
    from swegen.pipeline import actions

    task_dir = tmp_path / "tasks" / make_task().task_id
    environment_dir = task_dir / "environment"
    environment_dir.mkdir(parents=True)
    (environment_dir / "swegen-proxy-ca.crt").write_text("proxy-ca")
    dockerfile = environment_dir / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n\n"
        "COPY swegen-proxy-ca.crt /tmp/swegen-proxy-ca.crt\n\n"
        "RUN apt-get update \\\n"
        "    && update-ca-certificates \\\n"
        "    && rm /tmp/swegen-proxy-ca.crt\n\n"
        "RUN npm install --global yarn@1.22.22\n\n"
        "WORKDIR /app\n"
    )

    assert actions._ensure_proxy_ca_runtime_environment(task_dir) is True
    assert actions._ensure_proxy_ca_runtime_environment(task_dir) is False

    rendered = dockerfile.read_text()
    trusted_ca = "/usr/local/share/ca-certificates/swegen-proxy-ca.crt"
    assert f"GIT_SSL_CAINFO={trusted_ca}" in rendered
    assert "UV_NATIVE_TLS=true" in rendered
    assert f"NODE_EXTRA_CA_CERTS={trusted_ca}" in rendered
    assert f"NPM_CONFIG_CAFILE={trusted_ca}" in rendered
    assert "NPM_CONFIG_LEGACY_PEER_DEPS=true" in rendered
    assert "cat /tmp/swegen-proxy-ca.crt >> /etc/ssl/certs/ca-certificates.crt" in rendered
    assert "NPM_CONFIG_FETCH_RETRIES=5" in rendered
    assert "NPM_CONFIG_MAXSOCKETS=4" in rendered
    assert "YARN_NETWORK_TIMEOUT=600000" in rendered
    assert "YARN_NETWORK_CONCURRENCY=4" in rendered
    assert rendered.count("NODE_EXTRA_CA_CERTS=") == 1
    assert rendered.index("NODE_EXTRA_CA_CERTS=") < rendered.index("update-ca-certificates")
    assert rendered.index("NODE_EXTRA_CA_CERTS=") < rendered.index("npm install --global yarn")


def test_proxy_ca_environment_precedes_npm_in_ca_install_instruction(
    tmp_path: Path,
) -> None:
    from swegen.pipeline import actions

    task_dir = tmp_path / "tasks" / make_task().task_id
    environment_dir = task_dir / "environment"
    environment_dir.mkdir(parents=True)
    (environment_dir / "swegen-proxy-ca.crt").write_text("proxy-ca")
    dockerfile = environment_dir / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n\n"
        "COPY swegen-proxy-ca.crt /tmp/swegen-proxy-ca.crt\n\n"
        "RUN update-ca-certificates \\\n"
        "    && npm install --global npm@6.14.18\n\n"
        "WORKDIR /app\n"
    )

    assert actions._ensure_proxy_ca_runtime_environment(task_dir) is True

    rendered = dockerfile.read_text()
    assert rendered.index("GIT_SSL_CAINFO=") < rendered.index("RUN update-ca-certificates")
    assert rendered.index("UV_NATIVE_TLS=true") < rendered.index("RUN update-ca-certificates")
    assert rendered.index("NODE_EXTRA_CA_CERTS=") < rendered.index("RUN update-ca-certificates")
    assert rendered.index("NPM_CONFIG_CAFILE=") < rendered.index("npm install --global npm")


def test_proxy_ca_environment_duplicates_late_assignments_before_ca_install(
    tmp_path: Path,
) -> None:
    from swegen.pipeline import actions

    task_dir = tmp_path / "tasks" / make_task().task_id
    environment_dir = task_dir / "environment"
    environment_dir.mkdir(parents=True)
    (environment_dir / "swegen-proxy-ca.crt").write_text("proxy-ca")
    dockerfile = environment_dir / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n\n"
        "RUN update-ca-certificates && npm install --global npm@6.14.18\n\n"
        "ENV NODE_EXTRA_CA_CERTS=/usr/local/share/ca-certificates/swegen-proxy-ca.crt \\\n"
        "    NPM_CONFIG_CAFILE=/usr/local/share/ca-certificates/swegen-proxy-ca.crt\n"
    )

    assert actions._ensure_proxy_ca_runtime_environment(task_dir) is True
    assert actions._ensure_proxy_ca_runtime_environment(task_dir) is False

    rendered = dockerfile.read_text()
    assert rendered.index("GIT_SSL_CAINFO=") < rendered.index("RUN update-ca-certificates")
    assert rendered.index("UV_NATIVE_TLS=true") < rendered.index("RUN update-ca-certificates")
    assert rendered.index("NODE_EXTRA_CA_CERTS=") < rendered.index("RUN update-ca-certificates")
    assert rendered.index("NPM_CONFIG_CAFILE=") < rendered.index("npm install --global npm")


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
    log_path = tmp_path / ".swegen" / "logs" / "generate.log"
    log_text = log_path.read_text()
    assert 0 < result["log_bytes"] <= 16
    assert result["log_bytes"] == log_path.stat().st_size
    assert log_text
    assert set(log_text) == {"x"}
    assert log_text == log_text.strip()
    assert result["output_truncated"] is True


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


def test_logged_command_timeout_includes_redacted_tail(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    secret = "sk-abcdefghijklmnopqrstuvwxyz"
    monkeypatch.setattr(actions, "_COMMAND_STOP_GRACE_SECONDS", 0.2)

    with pytest.raises(TimeoutError) as raised:
        actions._run_logged_command(
            [
                sys.executable,
                "-c",
                (f"import time; print('OPENAI_API_KEY={secret}', flush=True); time.sleep(60)"),
            ],
            cwd=tmp_path,
            log_path=tmp_path / "timeout.log",
            timeout_seconds=0.1,
        )

    message = str(raised.value)
    assert "<REDACTED>" in message
    assert secret not in message


def test_logged_command_terminates_background_descendant_after_parent_success(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    monkeypatch.setattr(actions, "_COMMAND_STOP_GRACE_SECONDS", 0.2)
    log_path = tmp_path / "background.log"
    child_pid = 0
    try:
        actions._run_logged_command(
            [
                sys.executable,
                "-c",
                (
                    "import subprocess, sys; "
                    "child=subprocess.Popen([sys.executable, '-c', "
                    "'import time; time.sleep(60)']); "
                    "print(child.pid, flush=True)"
                ),
            ],
            cwd=tmp_path,
            log_path=log_path,
            timeout_seconds=2,
        )
        child_pid = int(log_path.read_text().strip())
        deadline = time.monotonic() + 2
        while process_is_running(child_pid) and time.monotonic() < deadline:
            time.sleep(0.02)
        assert not process_is_running(child_pid)
    finally:
        if child_pid and process_is_running(child_pid):
            os.kill(child_pid, signal.SIGKILL)


def test_logged_command_keeps_bounded_redacted_tail_on_nonzero_exit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    secret = "sk-abcdefghijklmnopqrstuvwxyz"
    log_path = tmp_path / "failure.log"
    monkeypatch.setattr(actions, "MAX_COMMAND_LOG_BYTES", 128)

    with pytest.raises(RuntimeError) as raised:
        actions._run_logged_command(
            [
                sys.executable,
                "-c",
                (
                    "print('A' * 4096); "
                    f"print('OPENAI_API_KEY={secret}'); "
                    "print('TAIL-END'); raise SystemExit(7)"
                ),
            ],
            cwd=tmp_path,
            log_path=log_path,
            timeout_seconds=2,
        )

    message = str(raised.value)
    logged = log_path.read_text()
    assert "TAIL-END" in message
    assert "<REDACTED>" in message
    assert secret not in message
    assert logged.rstrip().endswith("TAIL-END")
    assert secret not in logged
    assert log_path.stat().st_size <= 128


def test_logged_command_opens_log_before_spawning(tmp_path: Path) -> None:
    from swegen.pipeline import actions

    marker = tmp_path / "spawned"
    log_path = tmp_path / "log-directory"
    log_path.mkdir()

    with pytest.raises(RuntimeError, match="open command log"):
        actions._run_logged_command(
            [
                sys.executable,
                "-c",
                (
                    "import os, pathlib, time; "
                    f"pathlib.Path({str(marker)!r}).write_text(str(os.getpid())); "
                    "time.sleep(60)"
                ),
            ],
            cwd=tmp_path,
            log_path=log_path,
            timeout_seconds=2,
        )

    time.sleep(0.2)
    try:
        assert not marker.exists()
    finally:
        if marker.exists():
            os.killpg(int(marker.read_text()), signal.SIGKILL)


def test_logged_command_spawn_failure_leaves_closed_log(tmp_path: Path) -> None:
    from swegen.pipeline import actions

    log_path = tmp_path / "spawn.log"

    with pytest.raises(RuntimeError, match="start command"):
        actions._run_logged_command(
            [str(tmp_path / "missing-executable")],
            cwd=tmp_path,
            log_path=log_path,
            timeout_seconds=2,
        )

    assert log_path.is_file()
    log_path.write_text("closed")


def test_validate_action_accepts_exact_nop_zero_and_oracle_one(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    (tmp_path / "tasks" / task.task_id).mkdir(parents=True)
    calls: list[tuple[str, bool]] = []
    removed: list[str] = []

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
        assert kwargs["delete_after"] is (agent == "oracle")
        assert kwargs["wall_timeout_seconds"] > 0
        calls.append((agent, kwargs["delete_after"]))
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
    monkeypatch.setattr(actions, "remove_local_image", removed.append)

    execution = actions.validate_action(task, tmp_path)

    assert calls == [("nop", False), ("oracle", True)]
    assert removed == [actions.local_image_tag(task.task_id)]
    assert execution.status is StageResultStatus.SUCCEEDED
    assert execution.result_json() == {"nop_reward": 0, "oracle_reward": 1}


def test_validate_action_rejects_unexpected_nop_reward_without_oracle(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    (tmp_path / "tasks" / task.task_id).mkdir(parents=True)
    calls: list[tuple[str, bool]] = []
    removed: list[str] = []

    def fake_run_harbor_agent(task_id, dataset_path, jobs_dir, agent, **kwargs):
        calls.append((agent, kwargs["delete_after"]))
        return 0, tmp_path / f"{agent}.json"

    monkeypatch.setattr(actions, "run_harbor_agent", fake_run_harbor_agent)
    monkeypatch.setattr(
        actions,
        "parse_harbor_outcome",
        lambda path: HarborOutcome(reward=1, error=None),
    )
    monkeypatch.setattr(actions, "remove_local_image", removed.append)

    execution = actions.validate_action(task, tmp_path)

    assert calls == [("nop", False)]
    assert removed == [actions.local_image_tag(task.task_id)]
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
    removed: list[str] = []
    monkeypatch.setattr(actions, "remove_local_image", removed.append)

    execution = actions.validate_action(task, tmp_path)

    assert execution.status is StageResultStatus.REJECTED
    assert execution.result_json() == {
        "reason": "unexpected_oracle_reward",
        "nop_reward": 0,
        "oracle_reward": 0,
    }
    assert removed == [actions.local_image_tag(task.task_id)]


@pytest.mark.parametrize(
    ("exit_code", "outcome", "expected_error"),
    [
        pytest.param(
            2,
            HarborOutcome(reward=0, error=None),
            "Harbor nop exited with status 2",
            id="nonzero-exit",
        ),
        pytest.param(
            0,
            HarborOutcome(reward=None, error=None),
            "Harbor nop produced no parseable reward",
            id="missing-outcome",
        ),
        pytest.param(
            0,
            HarborOutcome(reward=0, error="docker failed"),
            "Harbor nop reported an execution error: docker failed",
            id="execution-error",
        ),
    ],
)
def test_validate_action_raises_for_harbor_infrastructure_failures(
    tmp_path: Path,
    monkeypatch,
    exit_code: int,
    outcome: HarborOutcome,
    expected_error: str,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    (tmp_path / "tasks" / task.task_id).mkdir(parents=True)
    calls: list[str] = []
    removed: list[str] = []

    def fake_run_harbor_agent(task_id, dataset_path, jobs_dir, agent, **kwargs):
        calls.append(agent)
        return exit_code, tmp_path / "result.json"

    monkeypatch.setattr(actions, "run_harbor_agent", fake_run_harbor_agent)
    monkeypatch.setattr(actions, "parse_harbor_outcome", lambda path: outcome)
    monkeypatch.setattr(actions, "remove_local_image", removed.append)

    with pytest.raises(RuntimeError, match=expected_error):
        actions.validate_action(task, tmp_path)

    assert calls == ["nop"]
    assert removed == [actions.local_image_tag(task.task_id)]


def test_validate_action_preserves_actionable_tail_of_long_harbor_error(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    (tmp_path / "tasks" / task.task_id).mkdir(parents=True)
    long_error = (
        "Docker compose build started " + ("build output " * 200) + ("npm ERR! code EMISSINGARG")
    )
    monkeypatch.setattr(
        actions,
        "run_harbor_agent",
        lambda *args, **kwargs: (0, tmp_path / "result.json"),
    )
    monkeypatch.setattr(
        actions,
        "parse_harbor_outcome",
        lambda path: HarborOutcome(reward=0, error=long_error),
    )
    monkeypatch.setattr(actions, "remove_local_image", lambda tag: None)

    with pytest.raises(RuntimeError) as raised:
        actions.validate_action(task, tmp_path)

    message = str(raised.value)
    assert "Docker compose build started" in message
    assert "npm ERR! code EMISSINGARG" in message
    assert len(message) <= 1100


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
        assert primary.endpoint == "http://1.95.77.23:3000"
        assert primary.model == "gpt-5.6-sol"
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

    with pytest.raises(RuntimeError, match="network unavailable") as raised:
        actions.reward_action(task, tmp_path)

    assert "dedicated-secret" not in str(raised.value)


def test_reward_action_requires_dedicated_key_by_default(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    (tmp_path / "tasks" / task.task_id).mkdir(parents=True)
    monkeypatch.setenv("OPENAI_API_KEY", "provider-secret")
    monkeypatch.setattr(actions, "build_test_bundle", lambda path: "test bundle")
    monkeypatch.setattr(
        actions,
        "check_instance_with_fallback",
        lambda *args, **kwargs: pytest.fail("checker must not run without an approved key"),
    )

    with pytest.raises(RuntimeError, match="SWEGEN_REWARD_API_KEY"):
        actions.reward_action(task, tmp_path)


def test_reward_action_allows_provider_key_only_with_explicit_opt_in(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    (tmp_path / "tasks" / task.task_id).mkdir(parents=True)
    monkeypatch.setenv("OPENAI_API_KEY", "provider-secret")
    monkeypatch.setenv("SWEGEN_REWARD_ALLOW_PROVIDER_KEY_FALLBACK", "true")
    monkeypatch.setattr(actions, "build_test_bundle", lambda path: "test bundle")

    async def fake_check(test_bundle, primary, fallback, **kwargs):
        assert primary.api_key == fallback.api_key == "provider-secret"
        verdict = HackCheckResult(is_hacking=False, reason="clean")
        return primary, verdict, [(primary, verdict)]

    monkeypatch.setattr(actions, "check_instance_with_fallback", fake_check)

    assert actions.reward_action(task, tmp_path).status is StageResultStatus.SUCCEEDED


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
    monkeypatch.setenv("SWEGEN_REWARD_API_KEY", "dedicated-secret")
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
    removed: list[str] = []
    monkeypatch.setattr(actions, "remove_local_image", removed.append, raising=False)

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
    assert removed == [actions.local_image_tag(task.task_id), remote_tag]


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
    assert removed == ["local-source:latest", remote_tag]
    assert execution.status is StageResultStatus.SUCCEEDED
    assert execution.result_json() == {
        "remote_tag": remote_tag,
        "registry": "custom",
        "suffix": "_custom",
        "skipped": False,
        "already_present": False,
    }


def test_push_action_skips_when_exact_remote_buildkit_image_is_already_pushed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    task_dir = tmp_path / "tasks" / task.task_id
    (task_dir / "environment").mkdir(parents=True)
    remote_tag = "registry.example/team/generated:sha256-abc"
    normalization_calls: list[tuple[str, Path]] = []
    monkeypatch.setenv("SWEGEN_SWR_HOST", "registry.example")
    monkeypatch.setenv("SWEGEN_SWR_REPOSITORY", "team/generated")
    monkeypatch.setattr(
        actions,
        "rewrite_ubuntu_mirrors",
        lambda path: normalization_calls.append(("mirrors", path)),
    )
    monkeypatch.setattr(
        actions,
        "_ensure_proxy_ca_runtime_environment",
        lambda path: normalization_calls.append(("proxy_ca", path)),
    )

    def remote_image(task_id, directory, expected_repository):
        assert normalization_calls == [
            ("mirrors", task_dir / "environment" / "Dockerfile"),
            ("proxy_ca", task_dir),
        ]
        return remote_tag

    monkeypatch.setattr(
        actions,
        "_remote_buildkit_image_for_push",
        remote_image,
    )
    monkeypatch.setattr(
        actions,
        "image_exists_in_registry",
        lambda tag: pytest.fail("task-ID manifest lookup must be skipped"),
    )
    monkeypatch.setattr(
        actions,
        "build_image_direct",
        lambda *args, **kwargs: pytest.fail("local build must be skipped"),
    )
    removed: list[str] = []
    monkeypatch.setattr(actions, "remove_local_image", removed.append)

    execution = actions.push_action(task, tmp_path)

    assert execution.status is StageResultStatus.SUCCEEDED
    assert execution.result_json() == {
        "remote_tag": remote_tag,
        "registry": "platform",
        "suffix": "_platform",
        "skipped": True,
        "already_present": True,
        "remote_buildkit": True,
    }
    assert removed == [
        actions.local_image_tag(task.task_id),
        remote_tag,
        "registry.example/team/generated:owner__repo-42",
    ]
    assert normalization_calls == [
        ("mirrors", task_dir / "environment" / "Dockerfile"),
        ("proxy_ca", task_dir),
    ]


def test_remote_buildkit_push_lookup_accepts_local_overflow_success(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    task_dir = tmp_path / "tasks" / task.task_id
    (task_dir / "environment").mkdir(parents=True)
    config = SimpleNamespace(
        mode="local_overflow",
        dockerfile_registry_rewrites=(),
    )
    remote_ref = "registry.example/team/generated:sha256-exact"
    monkeypatch.setattr(actions.RemoteBuildkitConfig, "from_env", lambda: config)
    monkeypatch.setattr(actions, "context_digest", lambda *args, **kwargs: "a" * 64)
    monkeypatch.setattr(
        actions,
        "find_successful_remote_image",
        lambda **kwargs: remote_ref,
    )

    result = actions._remote_buildkit_image_for_push(
        task.task_id,
        task_dir,
        expected_repository="registry.example/team/generated",
    )

    assert result == remote_ref


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

    remote_tag = (
        "swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/"
        "swesandbox/public/swe-gen/feature-implementation/generated:owner__repo-42"
    )
    assert removed == ["local-source:latest", remote_tag]


def test_push_action_cleans_source_and_remote_aliases_when_build_raises(
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
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("build crashed")),
    )
    monkeypatch.setattr(actions, "remove_local_image", removed.append)

    with pytest.raises(RuntimeError, match="build crashed"):
        actions.push_action(task, tmp_path)

    remote_tag = (
        "swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/"
        "swesandbox/public/swe-gen/feature-implementation/generated:owner__repo-42"
    )
    assert removed == ["local-source:latest", remote_tag]


def test_repair_action_captures_agent_edits_and_returns_to_authoritative_validation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    task_dir = tmp_path / "tasks" / task.task_id
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "tests").mkdir()
    (task_dir / "solution").mkdir()
    (task_dir / "environment" / "Dockerfile").write_text("FROM ubuntu:24.04\n")
    (task_dir / "tests" / "test.sh").write_text("#!/bin/sh\nexit 1\n")
    (task_dir / "tests" / "case.py").write_text("assert False\n")
    (task_dir / "solution" / "solve.sh").write_text("#!/bin/sh\n")
    calls: list[dict[str, object]] = []
    removed: list[str] = []

    def fake_session(**kwargs):
        calls.append(kwargs)
        (task_dir / "tests" / "test.sh").write_text("#!/bin/sh\npytest -q case.py\n")
        return SimpleNamespace(
            success=False,
            nop_passed=True,
            oracle_passed=False,
            error_message="oracle still failed",
        )

    monkeypatch.setattr(actions, "run_claude_code_session", fake_session)
    monkeypatch.setattr(actions, "remove_local_image", removed.append)

    execution = actions.repair_action(task, tmp_path)

    assert execution.status is StageResultStatus.SUCCEEDED
    assert execution.result_json()["agent_reported_success"] is False
    assert execution.result_json()["agent_changed_files"] is True
    captured = {task_file.path: task_file.content for task_file in execution.files}
    assert captured["tests/test.sh"] == b"#!/bin/sh\npytest -q case.py\n"
    assert calls[0]["repair"] is True
    assert calls[0]["validate"] is True
    assert calls[0]["test_files"] == ["tests/case.py"]
    assert calls[0]["timeout"] == actions.DEFAULT_REPAIR_TIMEOUT_SECONDS
    assert removed == [actions.local_image_tag(task.task_id)]


def test_repair_action_does_not_handoff_failed_unchanged_bundle(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    task_dir = tmp_path / "tasks" / task.task_id
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "tests").mkdir()
    (task_dir / "solution").mkdir()
    (task_dir / "environment" / "Dockerfile").write_text("FROM ubuntu:24.04\n")
    (task_dir / "tests" / "test.sh").write_text("#!/bin/sh\nexit 1\n")
    (task_dir / "solution" / "solve.sh").write_text("#!/bin/sh\n")
    removed: list[str] = []
    monkeypatch.setattr(
        actions,
        "run_claude_code_session",
        lambda **kwargs: SimpleNamespace(
            success=False,
            nop_passed=False,
            oracle_passed=False,
            error_message="model endpoint unavailable",
        ),
    )
    monkeypatch.setattr(actions, "remove_local_image", removed.append)

    execution = actions.repair_action(task, tmp_path)

    assert execution.status is StageResultStatus.FAILED
    assert execution.should_handoff is False
    assert execution.files == ()
    assert execution.result_json()["agent_changed_files"] is False
    assert execution.result_json()["error"] == "model endpoint unavailable"
    assert removed == [actions.local_image_tag(task.task_id)]


def test_repair_action_removes_local_image_when_session_raises(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = make_task()
    task_dir = tmp_path / "tasks" / task.task_id
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "tests").mkdir()
    (task_dir / "solution").mkdir()
    (task_dir / "environment" / "Dockerfile").write_text("FROM ubuntu:24.04\n")
    (task_dir / "tests" / "test.sh").write_text("#!/bin/sh\nexit 1\n")
    (task_dir / "solution" / "solve.sh").write_text("#!/bin/sh\n")
    removed: list[str] = []
    monkeypatch.setattr(
        actions,
        "run_claude_code_session",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("repair session crashed")),
    )
    monkeypatch.setattr(actions, "remove_local_image", removed.append)

    with pytest.raises(RuntimeError, match="repair session crashed"):
        actions.repair_action(task, tmp_path)

    assert removed == [actions.local_image_tag(task.task_id)]


def test_reward_repair_action_passes_rejection_reason_and_uses_dedicated_jobs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from swegen.pipeline import actions

    task = PipelineTask(
        task_id="owner__repo-42",
        task_version=1,
        repo="owner/repo",
        pr=42,
        trace_id=UUID("12345678-1234-5678-1234-567812345678"),
        current_stage=PipelineStage.REWARD_REPAIR,
        last_reason="Verifier only checks that the command exits zero.",
    )
    task_dir = tmp_path / "tasks" / task.task_id
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "tests").mkdir()
    (task_dir / "solution").mkdir()
    (task_dir / "environment" / "Dockerfile").write_text("FROM ubuntu:24.04\n")
    (task_dir / "tests" / "test.sh").write_text("#!/bin/sh\nexit 1\n")
    (task_dir / "solution" / "solve.sh").write_text("#!/bin/sh\n")
    calls: list[dict[str, object]] = []
    removed: list[str] = []

    def fake_session(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(
            success=True,
            nop_passed=True,
            oracle_passed=True,
            error_message=None,
        )

    monkeypatch.setattr(actions, "run_claude_code_session", fake_session)
    monkeypatch.setattr(actions, "remove_local_image", removed.append)

    execution = actions.reward_repair_action(task, tmp_path)

    assert execution.status is StageResultStatus.SUCCEEDED
    assert calls[0]["repair"] is True
    assert calls[0]["repair_reason"] == task.last_reason
    assert calls[0]["timeout"] == actions.DEFAULT_REWARD_REPAIR_TIMEOUT_SECONDS
    assert "reward-repair-harbor-jobs" in str(calls[0]["jobs_dir"])
    assert removed == [actions.local_image_tag(task.task_id)]


def test_action_for_stage_maps_all_pipeline_stages_and_rejects_unknown() -> None:
    from swegen.pipeline import actions

    assert actions.action_for_stage(PipelineStage.GENERATE) is actions.generate_action
    assert actions.action_for_stage(PipelineStage.VALIDATE) is actions.validate_action
    assert actions.action_for_stage(PipelineStage.REPAIR) is actions.repair_action
    assert actions.action_for_stage(PipelineStage.REWARD) is actions.reward_action
    assert actions.action_for_stage(PipelineStage.REWARD_REPAIR) is actions.reward_repair_action
    assert actions.action_for_stage(PipelineStage.PUSH) is actions.push_action

    with pytest.raises(ValueError, match="unsupported pipeline stage"):
        actions.action_for_stage("unknown")  # type: ignore[arg-type]
