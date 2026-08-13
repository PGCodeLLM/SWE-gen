"""Concrete actions executed by distributed pipeline workers."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Mapping, Sequence
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
    check_instance_with_fallback,
)
from swegen.create.claude_code_runner import run_claude_code_session
from swegen.create.claude_code_utils import redact_sensitive_text
from swegen.model_settings import load_github_tokens, required_environment_value
from swegen.pipeline.models import PipelineTask, StageExecution
from swegen.pipeline.task_store import capture_task_files
from swegen.queueing.models import PipelineStage
from swegen.tools.dockerfile_mirrors import rewrite_ubuntu_mirrors
from swegen.tools.harbor_runner import parse_harbor_outcome, run_harbor_agent
from swegen.tools.remote_buildkit import (
    RemoteBuildkitConfig,
    context_digest,
    find_successful_remote_image,
)
from swegen.tools.subprocess_utils import run_bounded_command

if TYPE_CHECKING:
    from swegen.pipeline.worker import StageAction

LOGGER = logging.getLogger(__name__)

MAX_COMMAND_LOG_BYTES = 1024 * 1024
DEFAULT_CC_TIMEOUT_SECONDS = 10800
DEFAULT_GENERATE_TIMEOUT_SECONDS = 14400.0
DEFAULT_REPAIR_TIMEOUT_SECONDS = 14400
DEFAULT_REWARD_REPAIR_TIMEOUT_SECONDS = 14400
DEFAULT_HARBOR_TIMEOUT_SECONDS = 3600.0
# The reward endpoint and its models have no defaults on purpose. Hardcoding
# real values here duplicated the ConfigMap and made an unmounted or stale
# reward Secret look identical to a healthy one, so the stage kept calling a
# delisted model instead of failing. The ConfigMap is now the only source.
_REWARD_SETTINGS_SUPPLIED_BY = (
    "ConfigMap swegen-pipeline-config (deploy/k3s/swegen-pipeline.yaml), "
    "optionally overridden by a swegen-reward-credentials-* Secret"
)
DEFAULT_SWR_HOST = "swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com"
DEFAULT_SWR_REPOSITORY = "swesandbox/public/swe-gen/feature-implementation/generated"
DEFAULT_SWR_REGISTRY = "platform"
DEFAULT_SWR_SUFFIX = "_platform"
# Trajectory registry for dual-push sync. The repository path differs from the
# platform one: every trajectory image ever published lives under
# aifm.coder.exp/swegen/generated (see push_all_verified.DEFAULT_REGISTRIES).
TRAJECTORY_SWR_HOST = "swr-coder-data-trajectory-o84wch.swr-pro.myhuaweicloud.com"
TRAJECTORY_SWR_REPOSITORY = "aifm.coder.exp/swegen/generated"
TRAJECTORY_SWR_REGISTRY = "trajectory"
TRAJECTORY_SWR_SUFFIX = ""
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
    ("GIT_SSL_CAINFO", _PROXY_CA_TRUST_PATH),
    ("UV_NATIVE_TLS", "true"),
    ("NODE_EXTRA_CA_CERTS", _PROXY_CA_TRUST_PATH),
    ("NPM_CONFIG_CAFILE", _PROXY_CA_TRUST_PATH),
    ("NPM_CONFIG_LEGACY_PEER_DEPS", "true"),
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


def _environment_value(name: str, default: str | None = None, *, supplied_by: str = "") -> str:
    """Read a string setting, falling back to ``default`` when one exists.

    Pass ``default=None`` with ``supplied_by`` for a required setting: a missing
    or blank value then raises :class:`MissingRequiredSetting` naming both the
    variable and the Secret/ConfigMap that should have supplied it, instead of
    substituting a literal that hides the misconfiguration.
    """

    if default is None:
        return required_environment_value(name, supplied_by=supplied_by)
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
    command = [
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
    ]
    # Under --no-validate the agent is told not to run Docker, so it never sees
    # a build failure and the continuation loop can only check that the files
    # exist and contain no TODO. Enabling validation lets the agent build, read
    # the error and amend its own Dockerfile before the task ever reaches the
    # validate stage. It costs a build per attempt, so it stays opt-in and
    # requires the docker socket to be mounted into the generate pod.
    if not _environment_boolean("SWEGEN_GENERATE_VALIDATE"):
        command.append("--no-validate")
    command.extend(
        [
            "--force",
            "--no-require-minimum-difficulty",
            "--no-require-issue",
        ]
    )
    return command


def _task_github_token(task: PipelineTask) -> str | None:
    """Pick this task's GitHub token: env override, else the pool by trace_id.

    Deterministic per (task, pool) so nop and oracle passes of the same task use
    the same token, while different tasks spread across the pool. Mirrors the
    selection in :func:`_generate_environment`.
    """

    env_token = os.environ.get("GITHUB_TOKEN", "").strip()
    if env_token:
        return env_token
    tokens = load_github_tokens()
    if not tokens:
        return None
    return tokens[task.trace_id.int % len(tokens)]


def _generate_environment(task: PipelineTask) -> dict[str, str]:
    """Return a subprocess environment with a stable legacy GitHub token."""

    environment = dict(os.environ)
    if environment.get("GITHUB_TOKEN", "").strip():
        return environment
    token = _task_github_token(task)
    if token:
        environment["GITHUB_TOKEN"] = token
    return environment


# Sentinel so the injected git-auth step is found and never duplicated across
# the nop/oracle passes (which rewrite the same Dockerfile twice).
_GITHUB_CREDENTIAL_MARKER = "# swegen: github credential"


def _inject_github_credential(task_dir: Path, token: str | None) -> bool:
    """Authenticate in-Dockerfile ``git clone https://github.com`` clones.

    Task Dockerfiles ``RUN git clone https://github.com/...`` inside the build
    container, which has no GitHub credential locally. When builds ran on the
    remote farm the farm supplied auth; local builds do not, so clones fail with
    ``could not read Username`` or hang until the build timeout.

    Prepend a ``git config --global url.<tokenized>.insteadOf`` so every
    ``https://github.com/`` fetch in the build authenticates. The token is a
    literal in the task's ephemeral Dockerfile (never committed or pushed; the
    validation image is built then deleted), and ``redact_sensitive_text``
    scrubs the tokenized URL from any persisted build log. Idempotent via
    ``_GITHUB_CREDENTIAL_MARKER``.
    """

    if not token:
        return False
    dockerfile = task_dir / "environment" / "Dockerfile"
    if not dockerfile.is_file():
        return False
    text = dockerfile.read_text()
    if _GITHUB_CREDENTIAL_MARKER in text:
        return True
    if "github.com" not in text:
        return False

    rewrite = (
        f'{_GITHUB_CREDENTIAL_MARKER}\n'
        f'RUN git config --global '
        f'url."https://x-access-token:{token}@github.com/".insteadOf '
        f'"https://github.com/"\n'
    )
    # Insert immediately before the first line that clones from github.com, so
    # git is already installed (the clone itself needs it) when the config runs.
    # git config writes the global ~/.gitconfig that every later clone consults.
    lines = text.splitlines(keepends=True)
    insert_at: int | None = None
    for index, line in enumerate(lines):
        if "git clone" in line and "github.com" in line:
            # Walk back over an instruction's continuation lines to the RUN start.
            start = index
            while start > 0 and lines[start - 1].rstrip().endswith("\\"):
                start -= 1
            insert_at = start
            break
    if insert_at is None:
        return False
    updated = "".join(lines[:insert_at]) + rewrite + "".join(lines[insert_at:])
    if updated == text:
        return False
    dockerfile.write_text(updated)
    return True


def _ensure_proxy_ca_runtime_environment(task_dir: Path) -> bool:
    """Teach legacy task Dockerfiles to trust the injected CA in Node/npm."""

    environment_dir = task_dir / "environment"
    dockerfile = environment_dir / "Dockerfile"
    proxy_ca = environment_dir / _PROXY_CA_FILENAME
    if not dockerfile.is_file() or not proxy_ca.is_file():
        return False

    text = dockerfile.read_text()
    updated = text
    temporary_ca = f"/tmp/{_PROXY_CA_FILENAME}"
    system_ca = "/etc/ssl/certs/ca-certificates.crt"
    append_ca_command = f"cat {temporary_ca} >> {system_ca}"
    if temporary_ca in updated and append_ca_command not in updated:
        lines = updated.splitlines(keepends=True)
        for index, line in enumerate(lines):
            if "update-ca-certificates" not in line:
                continue
            if line.rstrip().endswith("\\"):
                indent = line[: len(line) - len(line.lstrip())]
                lines.insert(index + 1, f"{indent}&& {append_ca_command} \\\n")
                updated = "".join(lines)
                break
            inline = "&& update-ca-certificates &&"
            if inline in line:
                lines[index] = line.replace(
                    inline,
                    f"&& update-ca-certificates && {append_ca_command} &&",
                    1,
                )
                updated = "".join(lines)
                break

    lines = updated.splitlines(keepends=True)
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
        workdir_index = updated.find("WORKDIR ")
        if workdir_index < 0:
            insertion_index = len(updated.rstrip()) + 1
        else:
            insertion_index = workdir_index

    effective_prefix = updated[:insertion_index]
    assignments = [
        f"{name}={value}"
        for name, value in _PROXY_PACKAGE_MANAGER_ENVIRONMENT
        if f"{name}=" not in effective_prefix
    ]
    if assignments:
        environment = "ENV " + " \\\n    ".join(assignments) + "\n\n"
        updated = updated[:insertion_index] + environment + updated[insertion_index:]
    if updated == text:
        return False
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
    rewrite_ubuntu_mirrors(task_dir / "environment" / "Dockerfile")
    _ensure_proxy_ca_runtime_environment(task_dir)
    _inject_github_credential(task_dir, _task_github_token(task))
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


def _task_repair_action_without_image_cleanup(
    task: PipelineTask,
    workspace: Path,
    *,
    reward_rejection_reason: str | None,
) -> StageExecution:
    task_dir = workspace / "tasks" / task.task_id
    if not task_dir.is_dir():
        raise RuntimeError(f"materialized task directory is missing: {task.task_id}")
    rewrite_ubuntu_mirrors(task_dir / "environment" / "Dockerfile")
    _ensure_proxy_ca_runtime_environment(task_dir)
    _inject_github_credential(task_dir, _task_github_token(task))
    original_files = capture_task_files(task_dir)
    if not original_files:
        raise RuntimeError(f"repair input task directory is empty: {task.task_id}")
    tests_dir = task_dir / "tests"
    test_files = (
        sorted(
            path.relative_to(task_dir).as_posix()
            for path in tests_dir.rglob("*")
            if path.is_file() and path.name != "test.sh"
        )
        if tests_dir.is_dir()
        else []
    )
    result = run_claude_code_session(
        repo=task.repo,
        pr_number=task.pr,
        repo_path=task_dir,
        task_dir=task_dir,
        task_id=task.task_id,
        dataset_path=workspace / "tasks",
        test_files=test_files,
        timeout=_environment_positive_integer(
            (
                "SWEGEN_REWARD_REPAIR_TIMEOUT_SECONDS"
                if reward_rejection_reason is not None
                else "SWEGEN_REPAIR_TIMEOUT_SECONDS"
            ),
            (
                DEFAULT_REWARD_REPAIR_TIMEOUT_SECONDS
                if reward_rejection_reason is not None
                else DEFAULT_REPAIR_TIMEOUT_SECONDS
            ),
        ),
        verbose=False,
        jobs_dir=(
            workspace
            / ".swegen"
            / (
                "reward-repair-harbor-jobs"
                if reward_rejection_reason is not None
                else "repair-harbor-jobs"
            )
        ),
        validate=True,
        repair=True,
        repair_reason=reward_rejection_reason,
    )
    rewrite_ubuntu_mirrors(task_dir / "environment" / "Dockerfile")
    _ensure_proxy_ca_runtime_environment(task_dir)
    _inject_github_credential(task_dir, _task_github_token(task))
    files = capture_task_files(task_dir)
    if not files:
        raise RuntimeError(f"repaired task directory is empty: {task.task_id}")
    agent_error = (
        _compact_safe_error_detail(result.error_message, 1000) if result.error_message else None
    )
    files_changed = files != original_files
    execution_result = {
        "agent_reported_success": result.success,
        "agent_nop_passed": result.nop_passed,
        "agent_oracle_passed": result.oracle_passed,
        "agent_changed_files": files_changed,
        "agent_error": agent_error,
        "file_count": len(files),
        "total_bytes": sum(task_file.size_bytes or 0 for task_file in files),
    }
    if not (result.nop_passed and result.oracle_passed) and not files_changed:
        return StageExecution.failed(
            {
                **execution_result,
                "error": agent_error
                or "repair agent failed validation without changing task artifacts",
            }
        )
    return StageExecution.succeeded(execution_result, files)


def _task_repair_action(
    task: PipelineTask,
    workspace: Path,
    *,
    reward_rejection_reason: str | None,
) -> StageExecution:
    local_tag = local_image_tag(task.task_id)
    try:
        return _task_repair_action_without_image_cleanup(
            task,
            workspace,
            reward_rejection_reason=reward_rejection_reason,
        )
    finally:
        try:
            remove_local_image(local_tag)
        except Exception as error:
            LOGGER.warning(
                "failed to remove repair image %s: %s",
                local_tag,
                _compact_safe_text(error),
            )


def repair_action(task: PipelineTask, workspace: Path) -> StageExecution:
    """Let Claude Code repair task packaging, then return it to validation."""

    return _task_repair_action(task, workspace, reward_rejection_reason=None)


def reward_repair_action(task: PipelineTask, workspace: Path) -> StageExecution:
    """Repair a Reward-rejected verifier, then return it to NOP/Oracle."""

    # The detector states its actual verdict at the end of a long explanation,
    # so a head slice hands the repair model the preamble without the finding.
    reason = _compact_safe_error_detail(
        task.last_reason or "Reward-hacking detector rejected the verifier without a reason.",
        4000,
    )
    return _task_repair_action(
        task,
        workspace,
        reward_rejection_reason=reason,
    )


def reward_action(task: PipelineTask, workspace: Path) -> StageExecution:
    """Reject test bundles that the reward-hacking detector flags."""

    task_dir = workspace / "tasks" / task.task_id
    test_bundle = build_test_bundle(task_dir)
    if not test_bundle.strip():
        raise RuntimeError("reward test bundle is empty or unreadable")
    api_key = os.environ.get("SWEGEN_REWARD_API_KEY", "").strip()
    if not api_key and _environment_boolean("SWEGEN_REWARD_ALLOW_PROVIDER_KEY_FALLBACK"):
        api_key = (
            os.environ.get("OPENAI_API_KEY", "").strip()
            or os.environ.get("ANTHROPIC_API_KEY", "").strip()
        )
    if not api_key:
        raise RuntimeError(
            "SWEGEN_REWARD_API_KEY is required; provider-key fallback requires "
            "SWEGEN_REWARD_ALLOW_PROVIDER_KEY_FALLBACK=true"
        )
    endpoint = _environment_value(
        "SWEGEN_REWARD_ENDPOINT",
        supplied_by=_REWARD_SETTINGS_SUPPLIED_BY,
    )
    primary_model = _environment_value(
        "SWEGEN_REWARD_PRIMARY_MODEL",
        supplied_by=_REWARD_SETTINGS_SUPPLIED_BY,
    )
    # SWEGEN_REWARD_FALLBACK_MODEL is a separate required key rather than
    # defaulting to the primary: the ConfigMap currently sets both to the same
    # model, which makes the "fallback" attempt a retry of the primary.
    fallback_model = _environment_value(
        "SWEGEN_REWARD_FALLBACK_MODEL",
        supplied_by=_REWARD_SETTINGS_SUPPLIED_BY,
    )
    primary = LLMConfig(
        name="primary",
        endpoint=endpoint,
        model=primary_model,
        api_key=api_key,
    )
    fallback = LLMConfig(
        name="fallback",
        endpoint=endpoint,
        model=fallback_model,
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
        raise RuntimeError(
            "reward-hacking checker infrastructure error "
            f"(model={_compact_safe_text(selected.model, 200)}): "
            f"{_compact_safe_text(verdict.error, 500)}"
        )
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
    """Build and publish a task image to both platform and trajectory SWR registries."""

    # Primary registry (platform by default)
    host = _environment_value("SWEGEN_SWR_HOST", DEFAULT_SWR_HOST).strip("/")
    repository = _environment_value(
        "SWEGEN_SWR_REPOSITORY",
        DEFAULT_SWR_REPOSITORY,
    ).strip("/")
    registry = _environment_value("SWEGEN_SWR_REGISTRY", DEFAULT_SWR_REGISTRY)
    suffix = _environment_value("SWEGEN_SWR_SUFFIX", DEFAULT_SWR_SUFFIX)

    # Secondary registry (trajectory) for sync
    trajectory_host = TRAJECTORY_SWR_HOST.strip("/")
    trajectory_repository = TRAJECTORY_SWR_REPOSITORY.strip("/")
    trajectory_registry = TRAJECTORY_SWR_REGISTRY
    trajectory_suffix = TRAJECTORY_SWR_SUFFIX

    remote_tag = f"{host}/{repository}:{task.task_id}"
    trajectory_remote_tag = f"{trajectory_host}/{trajectory_repository}:{task.task_id}"
    expected_local_tag = local_image_tag(task.task_id)
    cleanup_tags = [expected_local_tag, remote_tag, trajectory_remote_tag]
    try:
        task_dir = workspace / "tasks" / task.task_id
        if not task_dir.is_dir():
            raise RuntimeError(f"materialized task directory is missing: {task.task_id}")
        # Validate/Repair normalize the build context before Harbor submits it
        # to the remote farm.  Reproduce those deterministic edits before
        # hashing here; hashing the raw Postgres materialization cannot match
        # the successful remote-build row and incorrectly falls through to a
        # fresh node-local build.
        rewrite_ubuntu_mirrors(task_dir / "environment" / "Dockerfile")
        _ensure_proxy_ca_runtime_environment(task_dir)
        remote_build_tag = _remote_buildkit_image_for_push(
            task.task_id,
            task_dir,
            expected_repository=f"{host}/{repository}",
        )
        if remote_build_tag is not None:
            if remote_build_tag not in cleanup_tags:
                cleanup_tags.insert(1, remote_build_tag)
            # Remote buildkit pushed to primary; sync that image to trajectory too.
            trajectory_success = False
            if not image_exists_in_registry(trajectory_remote_tag):
                try:
                    trajectory_success = push_to_registry(
                        remote_build_tag, trajectory_remote_tag, log=LOGGER.info
                    )
                    if trajectory_success:
                        LOGGER.info(
                            "Synced remote-built %s to trajectory registry", task.task_id
                        )
                    else:
                        LOGGER.warning(
                            "Failed to sync remote-built %s to trajectory (non-fatal)",
                            task.task_id,
                        )
                except Exception as error:
                    LOGGER.warning(
                        "trajectory sync of remote-built %s failed (non-fatal): %s",
                        task.task_id,
                        _compact_safe_text(error),
                    )
            else:
                trajectory_success = True
            return StageExecution.succeeded(
                {
                    "remote_tag": remote_build_tag,
                    "trajectory_remote_tag": trajectory_remote_tag,
                    "registry": registry,
                    "suffix": suffix,
                    "trajectory_registry": trajectory_registry,
                    "trajectory_suffix": trajectory_suffix,
                    "skipped": True,
                    "already_present": True,
                    "remote_buildkit": True,
                    "synced_to_trajectory": trajectory_success,
                }
            )
        if image_exists_in_registry(remote_tag):
            # Primary exists, check if trajectory also exists
            trajectory_exists = image_exists_in_registry(trajectory_remote_tag)
            if trajectory_exists:
                return StageExecution.succeeded(
                    {
                        "remote_tag": remote_tag,
                        "trajectory_remote_tag": trajectory_remote_tag,
                        "registry": registry,
                        "suffix": suffix,
                        "trajectory_registry": trajectory_registry,
                        "trajectory_suffix": trajectory_suffix,
                        "skipped": True,
                        "already_present": True,
                        "synced_to_trajectory": True,
                    }
                )
            # Primary exists but trajectory missing - still need to build and push to trajectory
            LOGGER.info("Primary registry has %s, but trajectory missing - will sync", remote_tag)

        proxy_environment = {
            name: value for name in _PROXY_ENVIRONMENT_NAMES if (value := os.environ.get(name, ""))
        }
        built_tag = build_image_direct(
            task.task_id,
            task_dir,
            proxy_env=proxy_environment,
            log=LOGGER.info,
        )
        if not isinstance(built_tag, str) or not built_tag.strip():
            raise RuntimeError(f"image build failed for {task.task_id}")
        if built_tag not in cleanup_tags:
            cleanup_tags.insert(1, built_tag)

        # Push to primary registry
        if not push_to_registry(built_tag, remote_tag, log=LOGGER.info):
            raise RuntimeError(f"image push to primary registry failed for {task.task_id}")

        # Push to trajectory registry for sync
        trajectory_success = False
        try:
            trajectory_success = push_to_registry(built_tag, trajectory_remote_tag, log=LOGGER.info)
            if trajectory_success:
                LOGGER.info("Successfully synced %s to trajectory registry", task.task_id)
            else:
                LOGGER.warning("Failed to sync %s to trajectory registry (non-fatal)", task.task_id)
        except Exception as error:
            LOGGER.warning(
                "trajectory registry sync failed for %s (non-fatal): %s",
                task.task_id,
                _compact_safe_text(error),
            )

        return StageExecution.succeeded(
            {
                "remote_tag": remote_tag,
                "trajectory_remote_tag": trajectory_remote_tag,
                "registry": registry,
                "suffix": suffix,
                "trajectory_registry": trajectory_registry,
                "trajectory_suffix": trajectory_suffix,
                "skipped": False,
                "already_present": False,
                "synced_to_trajectory": trajectory_success,
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


def _remote_buildkit_image_for_push(
    task_id: str,
    task_dir: Path,
    *,
    expected_repository: str,
) -> str | None:
    try:
        config = RemoteBuildkitConfig.from_env()
    except ValueError as error:
        LOGGER.warning("remote BuildKit push lookup is disabled: %s", _compact_safe_text(error))
        return None
    if config is None:
        return None
    environment_dir = task_dir / "environment"
    if not environment_dir.is_dir():
        return None
    digest = context_digest(
        environment_dir,
        dockerfile_registry_rewrites=config.dockerfile_registry_rewrites,
    )
    # The exact successful build row is authoritative.  In local_overflow mode
    # route selection happens dynamically after checking node-local slots, so
    # select_build_route() cannot reconstruct whether this particular build
    # overflowed to the farm.  Requiring a static "remote" route here caused
    # Push to rebuild (and often fail) even though SWR already held the exact
    # content-addressed image.
    image_ref = find_successful_remote_image(
        environment_name=task_id,
        context_digest=digest,
    )
    if image_ref is None:
        return None
    image_repository = image_ref.rsplit("@", 1)[0].rsplit(":", 1)[0]
    if image_repository != expected_repository:
        LOGGER.warning(
            "remote BuildKit image repository %s does not match push repository %s; "
            "falling back to the local push path",
            image_repository,
            expected_repository,
        )
        return None
    return image_ref


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
        PipelineStage.REPAIR: repair_action,
        PipelineStage.REWARD: reward_action,
        PipelineStage.REWARD_REPAIR: reward_repair_action,
        PipelineStage.PUSH: push_action,
    }
    return actions[normalized_stage]


__all__ = [
    "action_for_stage",
    "build_generate_command",
    "generate_action",
    "push_action",
    "repair_action",
    "reward_repair_action",
    "reward_action",
    "validate_action",
]
