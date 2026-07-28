"""Concrete actions executed by distributed pipeline workers."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import subprocess
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from math import isfinite
from numbers import Real
from pathlib import Path
from typing import TYPE_CHECKING

from push_all_verified import (
    build_image_direct,
    image_exists_in_registry,
    local_image_tag,
    push_to_registry,
    remove_local_image,
)
from reward_hacking_detector.hacking import (
    LLMConfig,
    build_test_bundle,
    check_instance_with_fallback,
)
from swegen.pipeline.models import PipelineTask, StageExecution
from swegen.pipeline.task_store import capture_task_files
from swegen.queueing.models import PipelineStage
from swegen.tools.harbor_runner import parse_harbor_outcome, run_harbor_agent

if TYPE_CHECKING:
    from swegen.pipeline.worker import StageAction

LOGGER = logging.getLogger(__name__)

MAX_COMMAND_LOG_BYTES = 1024 * 1024
DEFAULT_CC_TIMEOUT_SECONDS = 10800
DEFAULT_GENERATE_TIMEOUT_SECONDS = 14400.0
DEFAULT_HARBOR_TIMEOUT_SECONDS = 3600.0
DEFAULT_REWARD_ENDPOINT = "https://arcyleung-ubuntu.tailb940e6.ts.net"
DEFAULT_REWARD_PRIMARY_MODEL = "gpt-5.3-codex-spark"
DEFAULT_REWARD_FALLBACK_MODEL = "gpt-5.6-sol"
DEFAULT_SWR_HOST = "swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com"
DEFAULT_SWR_REPOSITORY = "swesandbox/public/swe-gen/feature-implementation/generated"
DEFAULT_SWR_REGISTRY = "platform"
DEFAULT_SWR_SUFFIX = "_platform"
_PROXY_ENVIRONMENT_NAMES = (
    "http_proxy",
    "https_proxy",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "no_proxy",
    "NO_PROXY",
)
_COMMAND_READ_BYTES = 64 * 1024
_COMMAND_STOP_GRACE_SECONDS = 10.0


@dataclass(frozen=True, slots=True)
class _CommandSummary:
    duration_seconds: float
    log_bytes: int
    output_truncated: bool


def _environment_timeout(name: str, default: float) -> float:
    raw_value = os.environ.get(name, "").strip()
    if not raw_value:
        return default
    try:
        timeout = float(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} must be a positive finite number") from error
    if not isfinite(timeout) or timeout <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return timeout


def _environment_value(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def _environment_positive_integer(name: str, default: int) -> int:
    raw_value = os.environ.get(name, "").strip()
    try:
        value = int(raw_value) if raw_value else default
    except ValueError as error:
        raise ValueError(f"{name} must be a positive integer") from error
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _compact_text(value: object, max_chars: int = 1000) -> str:
    return " ".join(str(value).split())[:max_chars]


def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=_COMMAND_STOP_GRACE_SECONDS)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    process.wait()


def _run_logged_command(
    command: Sequence[str],
    *,
    cwd: Path,
    log_path: Path,
    timeout_seconds: float,
) -> _CommandSummary:
    """Run a command while streaming output into a size-bounded log file."""

    if not command:
        raise ValueError("command must not be empty")
    if not isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be a positive finite number")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    started_at = time.monotonic()
    process = subprocess.Popen(
        list(command),
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    if process.stdout is None:  # pragma: no cover - guaranteed by stdout=PIPE
        raise RuntimeError("command output pipe was not created")

    output_bytes = 0
    written_bytes = 0
    reader_error: OSError | None = None

    with log_path.open("wb") as log_stream:

        def drain_output() -> None:
            nonlocal output_bytes, written_bytes, reader_error
            try:
                while chunk := process.stdout.read(_COMMAND_READ_BYTES):
                    output_bytes += len(chunk)
                    remaining = MAX_COMMAND_LOG_BYTES - written_bytes
                    if remaining > 0:
                        kept = chunk[:remaining]
                        log_stream.write(kept)
                        written_bytes += len(kept)
            except OSError as error:
                reader_error = error

        reader = threading.Thread(target=drain_output, name="swegen-command-log", daemon=True)
        reader.start()
        try:
            return_code = process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired as error:
            _terminate_process_group(process)
            reader.join(timeout=_COMMAND_STOP_GRACE_SECONDS)
            if reader.is_alive():
                process.stdout.close()
                reader.join(timeout=_COMMAND_STOP_GRACE_SECONDS)
            raise TimeoutError(f"command timed out after {timeout_seconds:g} seconds") from error
        reader.join(timeout=_COMMAND_STOP_GRACE_SECONDS)
        if reader.is_alive():
            process.stdout.close()
            reader.join(timeout=_COMMAND_STOP_GRACE_SECONDS)
            output_bytes = max(output_bytes, MAX_COMMAND_LOG_BYTES + 1)

    duration_seconds = round(time.monotonic() - started_at, 3)
    if reader_error is not None:
        raise RuntimeError("failed while streaming command output") from reader_error
    if return_code != 0:
        raise RuntimeError(f"command exited with status {return_code}")
    return _CommandSummary(
        duration_seconds=duration_seconds,
        log_bytes=written_bytes,
        output_truncated=output_bytes > written_bytes,
    )


def build_generate_command(task: PipelineTask, workspace: Path) -> list[str]:
    """Build the isolated ``swegen create`` command for one pipeline task."""

    state_dir = workspace / ".swegen"
    configured_cache = os.environ.get("SWEGEN_REPO_CACHE_DIR", "").strip()
    repo_cache_dir = Path(configured_cache) if configured_cache else state_dir / "repos"
    cc_timeout = _environment_positive_integer(
        "SWEGEN_CC_TIMEOUT_SECONDS",
        DEFAULT_CC_TIMEOUT_SECONDS,
    )
    return [
        "swegen",
        "create",
        "--repo",
        task.repo,
        "--pr",
        str(task.pr),
        "--output",
        str(workspace / "tasks"),
        "--state-dir",
        str(state_dir),
        "--repo-cache-dir",
        str(repo_cache_dir),
        "--cc-timeout",
        str(cc_timeout),
        "--no-validate",
        "--force",
        "--no-require-minimum-difficulty",
        "--no-require-issue",
    ]


def generate_action(task: PipelineTask, workspace: Path) -> StageExecution:
    """Generate and capture one Harbor task inside the delivery workspace."""

    state_dir = workspace / ".swegen"
    summary = _run_logged_command(
        build_generate_command(task, workspace),
        cwd=workspace,
        log_path=state_dir / "logs" / "generate.log",
        timeout_seconds=_environment_timeout(
            "SWEGEN_GENERATE_TIMEOUT_SECONDS",
            DEFAULT_GENERATE_TIMEOUT_SECONDS,
        ),
    )
    task_dir = workspace / "tasks" / task.task_id
    if not task_dir.is_dir():
        raise RuntimeError(f"expected generated task directory is missing: {task.task_id}")
    files = capture_task_files(task_dir)
    if not files:
        raise RuntimeError(f"generated task directory is empty: {task.task_id}")
    return StageExecution.succeeded(
        {
            "duration_seconds": summary.duration_seconds,
            "file_count": len(files),
            "total_bytes": sum(task_file.size_bytes or 0 for task_file in files),
            "log_bytes": summary.log_bytes,
            "output_truncated": summary.output_truncated,
        },
        files,
    )


def _validation_reward(
    task: PipelineTask,
    workspace: Path,
    agent: str,
) -> int | float:
    exit_code, result_path = run_harbor_agent(
        task.task_id,
        workspace / "tasks",
        workspace / ".swegen" / "harbor-jobs",
        agent,
        capture_output=False,
        delete_after=False,
        wall_timeout_seconds=_environment_timeout(
            "SWEGEN_HARBOR_TIMEOUT_SECONDS",
            DEFAULT_HARBOR_TIMEOUT_SECONDS,
        ),
    )
    if exit_code != 0:
        raise RuntimeError(f"Harbor {agent} exited with status {exit_code}")
    outcome = parse_harbor_outcome(result_path)
    if outcome.error:
        raise RuntimeError(f"Harbor {agent} reported an execution error")
    reward = outcome.reward
    if (
        reward is None
        or isinstance(reward, bool)
        or not isinstance(reward, Real)
        or not isfinite(float(reward))
    ):
        raise RuntimeError(f"Harbor {agent} produced no parseable reward")
    return reward


def validate_action(task: PipelineTask, workspace: Path) -> StageExecution:
    """Require Harbor's NOP baseline to fail and Oracle solution to pass."""

    nop_reward = _validation_reward(task, workspace, "nop")
    if nop_reward != 0:
        return StageExecution.rejected(
            {"reason": "unexpected_nop_reward", "nop_reward": nop_reward}
        )
    oracle_reward = _validation_reward(task, workspace, "oracle")
    if oracle_reward != 1:
        return StageExecution.rejected(
            {
                "reason": "unexpected_oracle_reward",
                "nop_reward": nop_reward,
                "oracle_reward": oracle_reward,
            }
        )
    return StageExecution.succeeded({"nop_reward": nop_reward, "oracle_reward": oracle_reward})


def reward_action(task: PipelineTask, workspace: Path) -> StageExecution:
    """Reject test bundles that the reward-hacking detector flags."""

    task_dir = workspace / "tasks" / task.task_id
    test_bundle = build_test_bundle(task_dir)
    if not test_bundle.strip():
        raise RuntimeError("reward test bundle is empty or unreadable")
    api_key = (
        os.environ.get("SWEGEN_REWARD_API_KEY", "").strip()
        or os.environ.get("OPENAI_API_KEY", "").strip()
        or os.environ.get("ANTHROPIC_API_KEY", "").strip()
    )
    endpoint = _environment_value("SWEGEN_REWARD_ENDPOINT", DEFAULT_REWARD_ENDPOINT)
    primary = LLMConfig(
        name="primary",
        endpoint=endpoint,
        model=_environment_value(
            "SWEGEN_REWARD_PRIMARY_MODEL",
            DEFAULT_REWARD_PRIMARY_MODEL,
        ),
        api_key=api_key,
    )
    fallback = LLMConfig(
        name="fallback",
        endpoint=endpoint,
        model=_environment_value(
            "SWEGEN_REWARD_FALLBACK_MODEL",
            DEFAULT_REWARD_FALLBACK_MODEL,
        ),
        api_key=api_key,
    )
    selected, verdict, attempts = asyncio.run(
        check_instance_with_fallback(
            test_bundle,
            primary,
            fallback,
            task_id=task.task_id,
            instance_dir=task_dir,
        )
    )
    if verdict.error:
        raise RuntimeError("reward-hacking checker returned an infrastructure error")
    evidence = {
        "selected_model": _compact_text(selected.model, 200),
        "attempted_models": [_compact_text(config.model, 200) for config, _result in attempts],
        "used_fallback": selected.model == fallback.model and selected.model != primary.model,
        "framework": _compact_text(verdict.test_framework, 200),
        "reason": _compact_text(verdict.reason),
    }
    if verdict.is_hacking:
        return StageExecution.rejected(evidence)
    return StageExecution.succeeded(evidence)


def push_action(task: PipelineTask, workspace: Path) -> StageExecution:
    """Build and publish a task image to the configured SWR target."""

    host = _environment_value("SWEGEN_SWR_HOST", DEFAULT_SWR_HOST).strip("/")
    repository = _environment_value(
        "SWEGEN_SWR_REPOSITORY",
        DEFAULT_SWR_REPOSITORY,
    ).strip("/")
    registry = _environment_value("SWEGEN_SWR_REGISTRY", DEFAULT_SWR_REGISTRY)
    suffix = _environment_value("SWEGEN_SWR_SUFFIX", DEFAULT_SWR_SUFFIX)
    remote_tag = f"{host}/{repository}:{task.task_id}"
    if image_exists_in_registry(remote_tag):
        return StageExecution.succeeded(
            {
                "remote_tag": remote_tag,
                "registry": registry,
                "suffix": suffix,
                "skipped": True,
                "already_present": True,
            }
        )
    task_dir = workspace / "tasks" / task.task_id
    if not task_dir.is_dir():
        raise RuntimeError(f"materialized task directory is missing: {task.task_id}")
    proxy_environment = {
        name: value for name in _PROXY_ENVIRONMENT_NAMES if (value := os.environ.get(name, ""))
    }
    expected_local_tag = local_image_tag(task.task_id)
    cleanup_tag = expected_local_tag
    try:
        built_tag = build_image_direct(
            task.task_id,
            task_dir,
            proxy_env=proxy_environment,
            log=LOGGER.info,
        )
        if not isinstance(built_tag, str) or not built_tag.strip():
            raise RuntimeError(f"image build failed for {task.task_id}")
        cleanup_tag = built_tag
        if not push_to_registry(built_tag, remote_tag, log=LOGGER.info):
            raise RuntimeError(f"image push failed for {task.task_id}")
        return StageExecution.succeeded(
            {
                "remote_tag": remote_tag,
                "registry": registry,
                "suffix": suffix,
                "skipped": False,
                "already_present": False,
            }
        )
    finally:
        try:
            remove_local_image(cleanup_tag)
        except Exception:
            LOGGER.warning("failed to remove local image %s", cleanup_tag, exc_info=True)


def action_for_stage(stage: PipelineStage) -> StageAction:
    """Return the concrete action registered for a pipeline stage."""

    try:
        normalized_stage = PipelineStage(stage)
    except (TypeError, ValueError) as error:
        raise ValueError(f"unsupported pipeline stage: {stage!r}") from error
    actions = {
        PipelineStage.GENERATE: generate_action,
        PipelineStage.VALIDATE: validate_action,
        PipelineStage.REWARD: reward_action,
        PipelineStage.PUSH: push_action,
    }
    return actions[normalized_stage]


__all__ = [
    "action_for_stage",
    "build_generate_command",
    "generate_action",
    "push_action",
    "reward_action",
    "validate_action",
]
