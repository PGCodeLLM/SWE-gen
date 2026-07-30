"""Concrete actions executed by distributed pipeline workers."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from math import isfinite
from numbers import Real
from pathlib import Path
from threading import Event
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
    check_instance,
)
from swegen.create.claude_code_utils import redact_sensitive_text
from swegen.model_settings import (
    configured_subprocess_env,
    load_github_tokens,
    load_hacking_settings,
    load_swr_target,
)
from swegen.pipeline.completion import export_completed_task, load_minddistiller_login
from swegen.pipeline.models import PipelineTask, StageExecution
from swegen.pipeline.task_store import capture_task_files
from swegen.queueing.models import PipelineStage
from swegen.tools.harbor_runner import parse_harbor_outcome, run_harbor_agent
from swegen.tools.subprocess_utils import run_bounded_command

if TYPE_CHECKING:
    from swegen.pipeline.worker import StageAction

LOGGER = logging.getLogger(__name__)

MAX_COMMAND_LOG_BYTES = 1024 * 1024
DEFAULT_CC_TIMEOUT_SECONDS = 10800
DEFAULT_GENERATE_TIMEOUT_SECONDS = 14400.0
DEFAULT_HARBOR_TIMEOUT_SECONDS = 3600.0
_PROXY_ENVIRONMENT_NAMES = (
    "http_proxy",
    "https_proxy",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "no_proxy",
    "NO_PROXY",
)
_COMMAND_STOP_GRACE_SECONDS = 10.0
_PROXY_CA_FILENAME = "swegen-proxy-ca.crt"
_PROXY_CA_TRUST_PATH = f"/usr/local/share/ca-certificates/{_PROXY_CA_FILENAME}"
_PROXY_PACKAGE_MANAGER_ENVIRONMENT = (
    ("NODE_EXTRA_CA_CERTS", _PROXY_CA_TRUST_PATH),
    ("NPM_CONFIG_CAFILE", _PROXY_CA_TRUST_PATH),
    ("NPM_CONFIG_FETCH_RETRIES", "5"),
    ("NPM_CONFIG_FETCH_RETRY_FACTOR", "2"),
    ("NPM_CONFIG_FETCH_RETRY_MINTIMEOUT", "20000"),
    ("NPM_CONFIG_FETCH_RETRY_MAXTIMEOUT", "120000"),
    ("NPM_CONFIG_MAXSOCKETS", "4"),
    ("YARN_NETWORK_TIMEOUT", "600000"),
    ("YARN_HTTP_TIMEOUT", "600000"),
    ("YARN_NETWORK_CONCURRENCY", "4"),
)


@dataclass(frozen=True, slots=True)
class _CommandSummary:
    duration_seconds: float
    log_bytes: int
    output_truncated: bool
    tail: str


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


def _environment_boolean(name: str, default: bool = False) -> bool:
    raw_value = os.environ.get(name, "").strip().lower()
    if not raw_value:
        return default
    if raw_value in {"1", "true", "yes", "on"}:
        return True
    if raw_value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def _compact_text(value: object, max_chars: int = 1000) -> str:
    return " ".join(str(value).split())[:max_chars]


def _compact_safe_text(value: object, max_chars: int = 1000) -> str:
    return _compact_text(redact_sensitive_text(str(value)), max_chars)


def _compact_safe_error_detail(value: object, max_chars: int = 1000) -> str:
    """Keep the actionable tail of a long, credential-redacted error."""

    text = " ".join(redact_sensitive_text(str(value)).split())
    if len(text) <= max_chars:
        return text
    separator = " ... "
    head_chars = min(240, (max_chars - len(separator)) // 2)
    tail_chars = max_chars - head_chars - len(separator)
    return text[:head_chars] + separator + text[-tail_chars:]


def _run_logged_command(
    command: Sequence[str],
    *,
    cwd: Path,
    log_path: Path,
    timeout_seconds: float,
    env: Mapping[str, str] | None = None,
) -> _CommandSummary:
    """Run a command while streaming output into a size-bounded log file."""

    result = run_bounded_command(
        command,
        cwd=cwd,
        log_path=log_path,
        timeout_seconds=timeout_seconds,
        max_output_bytes=MAX_COMMAND_LOG_BYTES,
        env=env,
        redactor=redact_sensitive_text,
        stop_grace_seconds=_COMMAND_STOP_GRACE_SECONDS,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"command exited with status {result.returncode}; tail: {result.tail or '<empty>'}"
        )
    return _CommandSummary(
        duration_seconds=result.duration_seconds,
        log_bytes=result.log_bytes,
        output_truncated=result.output_truncated,
        tail=result.tail,
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


def _generate_environment(task: PipelineTask) -> dict[str, str]:
    """Return TOML-controlled subprocess settings with a stable GitHub token."""

    environment = configured_subprocess_env(task.task_id)
    tokens = load_github_tokens()
    if tokens:
        environment["GITHUB_TOKEN"] = tokens[task.trace_id.int % len(tokens)]
    else:
        environment.pop("GITHUB_TOKEN", None)
    return environment


def find_docker_compose_files(task_dir: Path) -> tuple[str, ...]:
    """Return Docker Compose YAMLs that make a generated task unsuitable."""

    compose_names = {"docker-compose.yml", "docker-compose.yaml", "compose.yml", "compose.yaml"}
    matches = [
        path.relative_to(task_dir).as_posix()
        for path in task_dir.rglob("*")
        if path.is_file() and path.name.lower() in compose_names
    ]
    return tuple(sorted(matches))


def _ensure_proxy_ca_runtime_environment(task_dir: Path) -> bool:
    """Teach legacy task Dockerfiles to trust the injected CA in Node/npm."""

    environment_dir = task_dir / "environment"
    dockerfile = environment_dir / "Dockerfile"
    proxy_ca = environment_dir / _PROXY_CA_FILENAME
    if not dockerfile.is_file() or not proxy_ca.is_file():
        return False

    text = dockerfile.read_text()
    lines = text.splitlines(keepends=True)
    ca_update_line = next(
        (index for index, line in enumerate(lines) if "update-ca-certificates" in line),
        None,
    )
    if ca_update_line is not None:
        instruction_start = ca_update_line
        while instruction_start > 0 and lines[instruction_start - 1].rstrip().endswith("\\"):
            instruction_start -= 1
        insertion_index = sum(len(line) for line in lines[:instruction_start])
    else:
        workdir_index = text.find("WORKDIR ")
        if workdir_index < 0:
            insertion_index = len(text.rstrip()) + 1
        else:
            insertion_index = workdir_index

    effective_prefix = text[:insertion_index]
    assignments = [
        f"{name}={value}"
        for name, value in _PROXY_PACKAGE_MANAGER_ENVIRONMENT
        if f"{name}=" not in effective_prefix
    ]
    if not assignments:
        return False

    environment = "ENV " + " \\\n    ".join(assignments) + "\n\n"
    updated = text[:insertion_index] + environment + text[insertion_index:]
    dockerfile.write_text(updated)
    return True


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
        env=_generate_environment(task),
    )
    task_dir = workspace / "tasks" / task.task_id
    if not task_dir.is_dir():
        raise RuntimeError(f"expected generated task directory is missing: {task.task_id}")
    _ensure_proxy_ca_runtime_environment(task_dir)
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
    *,
    cancel_event: Event | None = None,
) -> int | float:
    exit_code, result_path = run_harbor_agent(
        task.task_id,
        workspace / "tasks",
        workspace / ".swegen" / "harbor-jobs",
        agent,
        capture_output=False,
        delete_after=agent == "oracle",
        wall_timeout_seconds=_environment_timeout(
            "SWEGEN_HARBOR_TIMEOUT_SECONDS",
            DEFAULT_HARBOR_TIMEOUT_SECONDS,
        ),
        cancel_event=cancel_event,
    )
    if exit_code != 0:
        raise RuntimeError(f"Harbor {agent} exited with status {exit_code}")
    outcome = parse_harbor_outcome(result_path)
    if outcome.error:
        detail = _compact_safe_error_detail(outcome.error, 1000)
        raise RuntimeError(f"Harbor {agent} reported an execution error: {detail or '<unknown>'}")
    reward = outcome.reward
    if (
        reward is None
        or isinstance(reward, bool)
        or not isinstance(reward, Real)
        or not isfinite(float(reward))
    ):
        raise RuntimeError(f"Harbor {agent} produced no parseable reward")
    return reward


def validate_action(
    task: PipelineTask,
    workspace: Path,
    *,
    cancel_event: Event | None = None,
) -> StageExecution:
    """Require Harbor's NOP baseline to fail and Oracle solution to pass."""

    task_dir = workspace / "tasks" / task.task_id
    compose_files = find_docker_compose_files(task_dir)
    if compose_files:
        return StageExecution.rejected(
            {
                "reason": "docker_compose_not_supported",
                "compose_files": list(compose_files),
            }
        )

    _ensure_proxy_ca_runtime_environment(task_dir)
    local_tag = local_image_tag(task.task_id)
    try:
        nop_reward = _validation_reward(
            task,
            workspace,
            "nop",
            cancel_event=cancel_event,
        )
        if nop_reward != 0:
            return StageExecution.rejected(
                {"reason": "unexpected_nop_reward", "nop_reward": nop_reward}
            )
        oracle_reward = _validation_reward(
            task,
            workspace,
            "oracle",
            cancel_event=cancel_event,
        )
        if oracle_reward != 1:
            return StageExecution.rejected(
                {
                    "reason": "unexpected_oracle_reward",
                    "nop_reward": nop_reward,
                    "oracle_reward": oracle_reward,
                }
            )
        return StageExecution.succeeded({"nop_reward": nop_reward, "oracle_reward": oracle_reward})
    finally:
        try:
            remove_local_image(local_tag)
        except Exception as error:
            LOGGER.warning(
                "failed to remove validation image %s: %s",
                local_tag,
                _compact_safe_text(error),
            )


def reward_action(task: PipelineTask, workspace: Path) -> StageExecution:
    """Reject test bundles that the reward-hacking detector flags."""

    task_dir = workspace / "tasks" / task.task_id
    test_bundle = build_test_bundle(task_dir)
    if not test_bundle.strip():
        raise RuntimeError("reward test bundle is empty or unreadable")
    configs = [
        LLMConfig(item.name, item.endpoint, item.model, item.api_key)
        for item in load_hacking_settings()
    ]
    attempts = asyncio.run(check_instance(test_bundle, configs, task_id=task.task_id))
    failures = [(config, verdict) for config, verdict in attempts if verdict.error is not None]
    if failures:
        config, verdict = failures[0]
        raise RuntimeError(
            "reward-hacking checker infrastructure error "
            f"(model={_compact_safe_text(config.model, 200)}): "
            f"{_compact_safe_text(verdict.error, 500)}"
        )
    evidence = {
        "models": [_compact_text(config.model, 200) for config, _verdict in attempts],
        "verdicts": [
            {
                "name": _compact_text(config.name, 200),
                "model": _compact_text(config.model, 200),
                "is_hacking": verdict.is_hacking,
                "framework": _compact_text(verdict.test_framework, 200),
                "reason": _compact_text(verdict.reason),
            }
            for config, verdict in attempts
        ],
    }
    if any(verdict.is_hacking for _config, verdict in attempts):
        return StageExecution.rejected(evidence)
    return StageExecution.succeeded(evidence)


@contextmanager
def minddistiller_docker_environment(credentials_csv: Path, host: str):
    username, password = load_minddistiller_login(credentials_csv, expected_host=host)
    with tempfile.TemporaryDirectory(prefix="swegen-minddistiller-docker-") as temporary:
        config_dir = Path(temporary)
        source_dir = Path(os.environ.get("DOCKER_CONFIG", "/root/.docker"))
        source_config = source_dir / "config.json"
        if source_config.is_file():
            try:
                config = json.loads(source_config.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as error:
                raise RuntimeError(f"could not read Docker config: {error}") from error
        else:
            config = {}
        if not isinstance(config, dict):
            raise RuntimeError("Docker config must contain a JSON object")
        auth = base64.b64encode(f"{username}:{password}".encode()).decode("ascii")
        auths = config.setdefault("auths", {})
        if not isinstance(auths, dict):
            raise RuntimeError("Docker config auths must contain an object")
        auths[host] = {"auth": auth}
        config_path = config_dir / "config.json"
        config_path.write_text(json.dumps(config, sort_keys=True), encoding="utf-8")
        config_path.chmod(0o600)
        environment = dict(os.environ)
        environment["DOCKER_CONFIG"] = str(config_dir)
        yield environment


def push_action(task: PipelineTask, workspace: Path) -> StageExecution:
    """Build once, push to both SWRs, export the task, and persist completion data."""

    primary = load_swr_target("primary")
    minddistiller = load_swr_target("minddistiller")
    if minddistiller.credentials_csv is None:
        raise RuntimeError("[swr.minddistiller].credentials_csv is required")
    primary_tag = primary.image_reference(task.task_id)
    minddistiller_tag = minddistiller.image_reference(task.task_id)
    if primary_tag == minddistiller_tag:
        raise RuntimeError("primary and MindDistiller SWR targets must be different")
    if (primary.registry, primary.suffix) == (
        minddistiller.registry,
        minddistiller.suffix,
    ):
        raise RuntimeError(
            "primary and MindDistiller pushed_images registry/suffix values must be different"
        )
    expected_local_tag = local_image_tag(task.task_id)
    cleanup_tags = [expected_local_tag, primary_tag, minddistiller_tag]
    try:
        with minddistiller_docker_environment(
            minddistiller.credentials_csv,
            minddistiller.host,
        ) as minddistiller_env:
            primary_exists = image_exists_in_registry(primary_tag)
            minddistiller_exists = image_exists_in_registry(
                minddistiller_tag,
                env=minddistiller_env,
            )
            task_dir = workspace / "tasks" / task.task_id
            if not task_dir.is_dir():
                raise RuntimeError(f"materialized task directory is missing: {task.task_id}")
            if not primary_exists or not minddistiller_exists:
                proxy_environment = {
                    name: value
                    for name in _PROXY_ENVIRONMENT_NAMES
                    if (value := os.environ.get(name, ""))
                }
                with tempfile.TemporaryDirectory(
                    prefix=f"push-build-{task.task_id}-",
                    dir=workspace,
                ) as build_directory:
                    build_task_dir = Path(build_directory) / task.task_id
                    shutil.copytree(task_dir, build_task_dir)
                    _ensure_proxy_ca_runtime_environment(build_task_dir)
                    built_tag = build_image_direct(
                        task.task_id,
                        build_task_dir,
                        proxy_env=proxy_environment,
                        log=LOGGER.info,
                    )
                if not isinstance(built_tag, str) or not built_tag.strip():
                    raise RuntimeError(f"image build failed for {task.task_id}")
                if built_tag not in cleanup_tags:
                    cleanup_tags.insert(1, built_tag)
                if not primary_exists and not push_to_registry(
                    built_tag,
                    primary_tag,
                    log=LOGGER.info,
                ):
                    raise RuntimeError(f"primary SWR image push failed for {task.task_id}")
                if not minddistiller_exists and not push_to_registry(
                    built_tag,
                    minddistiller_tag,
                    log=LOGGER.info,
                    env=minddistiller_env,
                ):
                    raise RuntimeError(f"MindDistiller SWR image push failed for {task.task_id}")

            export = export_completed_task(
                task,
                task_dir,
                voyager_image_ref=primary_tag,
                minddistiller_image_ref=minddistiller_tag,
            )
        return StageExecution.succeeded(
            {
                "remote_tag": primary_tag,
                "registry": primary.registry,
                "suffix": primary.suffix,
                "pushed_images": [
                    {
                        "remote_tag": primary_tag,
                        "registry": primary.registry,
                        "suffix": primary.suffix,
                        "already_present": primary_exists,
                    },
                    {
                        "remote_tag": minddistiller_tag,
                        "registry": minddistiller.registry,
                        "suffix": minddistiller.suffix,
                        "already_present": minddistiller_exists,
                    },
                ],
                "harbor_directory": str(export.directory),
            }
        )
    finally:
        for cleanup_tag in cleanup_tags:
            try:
                remove_local_image(cleanup_tag)
            except Exception as error:
                LOGGER.warning(
                    "failed to remove local image alias %s: %s",
                    cleanup_tag,
                    _compact_safe_text(error),
                )


def action_for_stage(
    stage: PipelineStage,
    *,
    cancel_event: Event | None = None,
) -> StageAction:
    """Return the concrete action registered for a pipeline stage."""

    try:
        normalized_stage = PipelineStage(stage)
    except (TypeError, ValueError) as error:
        raise ValueError(f"unsupported pipeline stage: {stage!r}") from error
    validation_action: StageAction = (
        validate_action
        if cancel_event is None
        else partial(validate_action, cancel_event=cancel_event)
    )
    actions = {
        PipelineStage.GENERATE: generate_action,
        PipelineStage.VALIDATE: validation_action,
        PipelineStage.REWARD: reward_action,
        PipelineStage.PUSH: push_action,
    }
    return actions[normalized_stage]


__all__ = [
    "action_for_stage",
    "build_generate_command",
    "find_docker_compose_files",
    "generate_action",
    "minddistiller_docker_environment",
    "push_action",
    "reward_action",
    "validate_action",
]
