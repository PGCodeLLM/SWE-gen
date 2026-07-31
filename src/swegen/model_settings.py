from __future__ import annotations

import hashlib
import os
import random
import re
import tomllib
from dataclasses import dataclass, field
from math import ceil, isfinite
from pathlib import Path
from typing import Any

CONFIG_PATH_ENV = "SWEGEN_CONFIG"
DEFAULT_CONFIG_FILE = Path("swegen.toml")

# These variables are consumed by third-party SDKs and child processes.  SWE-gen
# deliberately replaces inherited values with the values in swegen.toml so a
# shell, dotenv file, or Kubernetes Secret cannot silently change model routing.
MANAGED_RUNTIME_ENV_VARS = (
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_MODEL",
    "ANTHROPIC_DEFAULT_OPUS_MODEL",
    "ANTHROPIC_DEFAULT_SONNET_MODEL",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL",
    "ANTHROPIC_SMALL_FAST_MODEL",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "GITHUB_TOKEN",
    "GH_TOKEN",
    "OPENAI_API_KEY",
    "OPENAI_BASE_URL",
    "OPENAI_MODEL",
    "REPO_CREATION_TOKEN",
    "SWEGEN_CLAUDE_FAST_MODEL",
    "SWEGEN_AGENT_REASONING_EFFORT",
    "SWEGEN_REWARD_API_KEY",
    "SWEGEN_REWARD_ENDPOINT",
    "SWEGEN_REWARD_PRIMARY_MODEL",
    "SWEGEN_REWARD_FALLBACK_MODEL",
)


@dataclass(frozen=True, slots=True)
class ModelSettings:
    model: str
    base_url: str | None = None
    api_key: str | None = None
    auth_token: str | None = None
    oauth_token: str | None = None
    fast_model: str | None = None
    opus_model: str | None = None
    sonnet_model: str | None = None
    reasoning_effort: str = "high"


@dataclass(frozen=True, slots=True)
class OpenAISettings:
    api_key: str
    base_url: str | None
    task_instruction_model: str
    verdict_model: str


@dataclass(frozen=True, slots=True)
class AnalysisSettings:
    classifier_model: str
    agent_model: str


@dataclass(frozen=True, slots=True)
class HackingLLMSettings:
    name: str
    endpoint: str
    model: str
    api_key: str


@dataclass(frozen=True, slots=True)
class SWRTargetSettings:
    host: str
    repository: str
    registry: str
    suffix: str
    username: str | None = field(default=None, repr=False)
    password: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not self.host.strip().strip("/"):
            raise ValueError("SWR host must be non-empty")
        if not self.repository.strip().strip("/"):
            raise ValueError("SWR repository must be non-empty")
        if not self.registry.strip():
            raise ValueError("SWR registry label must be non-empty")
        if (self.username is None) != (self.password is None):
            raise ValueError("SWR username and password must be configured together")

    @property
    def repository_prefix(self) -> str:
        return f"{self.host.rstrip('/')}/{self.repository.strip('/')}"

    def image_reference(self, instance_id: str) -> str:
        if not instance_id.strip():
            raise ValueError("instance_id must be non-empty")
        return f"{self.repository_prefix}:{instance_id}"


@dataclass(frozen=True, slots=True)
class AutoqueueSettings:
    source_table: str = "public.pr_tasks"
    max_queued: int | None = None
    max_queued_per_generate_worker: float = 1.5
    generate_workers: int = 1
    poll_seconds: float = 30.0
    task_version: int = 1
    max_retries: int = 3
    pr_categories: tuple[str, ...] = ("feature",)
    exclude_languages: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.max_queued is not None and self.max_queued < 1:
            raise ValueError("[autoqueue].max_queued must be >= 1")
        if (
            not isfinite(self.max_queued_per_generate_worker)
            or self.max_queued_per_generate_worker <= 0
        ):
            raise ValueError("[autoqueue].max_queued_per_generate_worker must be positive")
        if self.generate_workers < 1:
            raise ValueError("[autoqueue].generate_workers must be >= 1")
        if self.poll_seconds <= 0:
            raise ValueError("[autoqueue].poll_seconds must be positive")
        if self.task_version < 1:
            raise ValueError("[autoqueue].task_version must be >= 1")
        if self.max_retries < 1:
            raise ValueError("[autoqueue].max_retries must be >= 1")
        if "." not in self.source_table:
            raise ValueError("[autoqueue].source_table must be schema-qualified")

    @property
    def queue_limit(self) -> int:
        """Maximum generate-stage backlog, with explicit max_queued taking priority."""

        if self.max_queued is not None:
            return self.max_queued
        # Round up so fractional per-worker limits never under-provision a worker.
        return max(1, ceil(self.max_queued_per_generate_worker * self.generate_workers))


@dataclass(frozen=True, slots=True)
class PipelineSettings:
    namespace: str
    secret_source_namespace: str
    worker_image: str
    workspace_host_path: Path
    repo_cache_host_path: Path
    successful_tasks_host_path: Path
    k3s_nodes: tuple[str, ...]
    k3s_ssh_user: str
    build_ca_path: Path
    build_worker_image_on_start: bool
    autoqueue_workers: int
    generate_workers: int
    validate_workers: int
    reward_workers: int
    push_workers: int
    rollout_timeout_seconds: int

    def __post_init__(self) -> None:
        for field_name in (
            "autoqueue_workers",
            "generate_workers",
            "validate_workers",
            "reward_workers",
            "push_workers",
            "rollout_timeout_seconds",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"[pipeline].{field_name} must be an integer >= 1")
        if not re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", self.namespace):
            raise ValueError("[pipeline].namespace must be a Kubernetes DNS label")
        if not re.fullmatch(
            r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?",
            self.secret_source_namespace,
        ):
            raise ValueError(
                "[pipeline].secret_source_namespace must be a Kubernetes DNS label"
            )
        if self.namespace == self.secret_source_namespace:
            raise ValueError(
                "[pipeline].namespace must differ from secret_source_namespace"
            )
        if not self.worker_image.strip() or any(character.isspace() for character in self.worker_image):
            raise ValueError("[pipeline].worker_image must be a non-empty image reference")
        for field_name in (
            "workspace_host_path",
            "repo_cache_host_path",
            "successful_tasks_host_path",
            "build_ca_path",
        ):
            if not getattr(self, field_name).is_absolute():
                raise ValueError(f"[pipeline].{field_name} must be an absolute path")
        if len(
            {
                self.workspace_host_path,
                self.repo_cache_host_path,
                self.successful_tasks_host_path,
            }
        ) != 3:
            raise ValueError("[pipeline] host storage paths must be distinct")
        if not self.k3s_nodes:
            raise ValueError("[pipeline].k3s_nodes must contain at least one node")
        if not self.k3s_ssh_user.strip():
            raise ValueError("[pipeline].k3s_ssh_user must be non-empty")


@dataclass(frozen=True, slots=True)
class CompletedTaskSettings:
    output_dir: Path = Path("data_cache/successful_harbor_tasks")


def config_path() -> Path:
    override = os.environ.get(CONFIG_PATH_ENV, "").strip()
    return Path(override) if override else DEFAULT_CONFIG_FILE


def load_config(*, required: bool = False) -> dict[str, Any]:
    path = config_path()
    if not path.is_file():
        if required:
            raise FileNotFoundError(
                f"SWE-gen configuration not found: {path}. "
                "Copy swegen.toml.example to swegen.toml and fill in its values."
            )
        return {}
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise RuntimeError(f"Could not load {path}: {error}") from error
    if not isinstance(data, dict):
        raise RuntimeError(f"Invalid configuration in {path}: expected a TOML document")
    return data


def _table(name: str, *, required_config: bool = False) -> dict[str, Any]:
    value = load_config(required=required_config).get(name, {})
    if not isinstance(value, dict):
        raise ValueError(f"[{name}] in {config_path()} must be a TOML table")
    return value


def _nested_table(section: str, name: str, *, required: bool = False) -> dict[str, Any]:
    parent = _table(section, required_config=required)
    value = parent.get(name, {})
    if not isinstance(value, dict):
        raise ValueError(f"[{section}.{name}] in {config_path()} must be a TOML table")
    return value


def _required_string(table: dict[str, Any], section: str, key: str) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"[{section}].{key} must be set in {config_path()}")
    return value.strip()


def _optional_string(table: dict[str, Any], key: str) -> str | None:
    value = table.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string when configured")
    return value.strip()


def _optional_secret(table: dict[str, Any], key: str) -> str | None:
    value = table.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string when configured")
    return value


def _required_integer(table: dict[str, Any], section: str, key: str) -> int:
    value = table.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"[{section}].{key} must be an integer >= 1")
    return value


def _required_boolean(table: dict[str, Any], section: str, key: str) -> bool:
    value = table.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"[{section}].{key} must be a boolean")
    return value


def _required_path(table: dict[str, Any], section: str, key: str) -> Path:
    return Path(_required_string(table, section, key)).expanduser()


def _string_tuple(value: object, field: str, *, default: tuple[str, ...]) -> tuple[str, ...]:
    if value is None:
        return default
    if not isinstance(value, list):
        raise ValueError(f"{field} must be a TOML array of strings")
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{field} must contain only non-empty strings")
        lowered = item.strip().lower()
        if lowered not in normalized:
            normalized.append(lowered)
    return tuple(normalized)


def load_model_settings(*, required: bool = True) -> ModelSettings:
    table = _table("model", required_config=required)
    model = (
        _required_string(table, "model", "model")
        if required
        else str(table.get("model") or "").strip()
    )
    if required and not (
        _optional_string(table, "api_key")
        or _optional_string(table, "auth_token")
        or _optional_string(table, "oauth_token")
    ):
        raise ValueError(
            f"[model] must define api_key, auth_token, or oauth_token in {config_path()}"
        )
    effort = str(table.get("reasoning_effort") or "high").strip().lower()
    if effort not in {"low", "medium", "high", "xhigh", "max"}:
        raise ValueError("[model].reasoning_effort must be low, medium, high, xhigh, or max")
    return ModelSettings(
        model=model,
        base_url=_optional_string(table, "base_url"),
        api_key=_optional_string(table, "api_key"),
        auth_token=_optional_string(table, "auth_token"),
        oauth_token=_optional_string(table, "oauth_token"),
        fast_model=_optional_string(table, "fast_model"),
        opus_model=_optional_string(table, "opus_model"),
        sonnet_model=_optional_string(table, "sonnet_model"),
        reasoning_effort=effort,
    )


def load_openai_settings(*, required: bool = True) -> OpenAISettings:
    table = _table("openai", required_config=required)
    api_key = (
        _required_string(table, "openai", "api_key")
        if required
        else str(table.get("api_key") or "").strip()
    )
    task_model = (
        _required_string(table, "openai", "task_instruction_model")
        if required
        else str(table.get("task_instruction_model") or "").strip()
    )
    verdict_model = (
        _required_string(table, "openai", "verdict_model")
        if required
        else str(table.get("verdict_model") or "").strip()
    )
    return OpenAISettings(
        api_key=api_key,
        base_url=_optional_string(table, "base_url"),
        task_instruction_model=task_model,
        verdict_model=verdict_model,
    )


def load_analysis_settings(*, required: bool = True) -> AnalysisSettings:
    table = _table("analysis", required_config=required)
    return AnalysisSettings(
        classifier_model=(
            _required_string(table, "analysis", "classifier_model")
            if required
            else str(table.get("classifier_model") or "").strip()
        ),
        agent_model=(
            _required_string(table, "analysis", "agent_model")
            if required
            else str(table.get("agent_model") or "").strip()
        ),
    )


def load_hacking_settings() -> tuple[HackingLLMSettings, ...]:
    entries = _table("hacking", required_config=True).get("llm")
    if not isinstance(entries, list) or not entries:
        raise ValueError(f"[[hacking.llm]] must contain at least one checker in {config_path()}")
    configs: list[HackingLLMSettings] = []
    for index, entry in enumerate(entries, 1):
        if not isinstance(entry, dict):
            raise ValueError(f"[[hacking.llm]] entry #{index} must be a table")
        configs.append(
            HackingLLMSettings(
                name=str(entry.get("name") or f"llm{index}").strip(),
                endpoint=_required_string(entry, f"hacking.llm #{index}", "endpoint"),
                model=_required_string(entry, f"hacking.llm #{index}", "model"),
                api_key=_required_string(entry, f"hacking.llm #{index}", "api_key"),
            )
        )
    return tuple(configs)


def load_swr_target(name: str) -> SWRTargetSettings:
    table = _nested_table("swr", name, required=True)
    registry = table.get("registry", name)
    suffix = table.get("suffix", "")
    username = _optional_string(table, "username")
    password = _optional_secret(table, "password")
    if name == "minddistiller" and (username is None or password is None):
        raise ValueError(
            f"[swr.minddistiller] must define username and password in {config_path()}"
        )
    if not isinstance(registry, str) or not registry.strip():
        raise ValueError(f"[swr.{name}].registry must be a non-empty string")
    if not isinstance(suffix, str):
        raise ValueError(f"[swr.{name}].suffix must be a string")
    return SWRTargetSettings(
        host=_required_string(table, f"swr.{name}", "host").rstrip("/"),
        repository=_required_string(table, f"swr.{name}", "repository").strip("/"),
        registry=registry.strip(),
        suffix=suffix.strip(),
        username=username,
        password=password,
    )


def load_autoqueue_settings() -> AutoqueueSettings:
    table = _table("autoqueue", required_config=True)
    pipeline = load_pipeline_settings()
    raw_max = table.get("max_queued")
    max_queued: int | None
    if raw_max is None:
        max_queued = None
    elif isinstance(raw_max, bool) or not isinstance(raw_max, int):
        raise ValueError("[autoqueue].max_queued must be an integer")
    else:
        max_queued = raw_max
    return AutoqueueSettings(
        source_table=str(table.get("source_table") or "public.pr_tasks").strip(),
        max_queued=max_queued,
        max_queued_per_generate_worker=float(table.get("max_queued_per_generate_worker", 1.5)),
        generate_workers=pipeline.generate_workers,
        poll_seconds=float(table.get("poll_seconds", 30.0)),
        task_version=int(table.get("task_version", 1)),
        max_retries=int(table.get("max_retries", 3)),
        pr_categories=_string_tuple(
            table.get("pr_categories"),
            "[autoqueue].pr_categories",
            default=("feature",),
        ),
        exclude_languages=_string_tuple(
            table.get("exclude_languages"),
            "[autoqueue].exclude_languages",
            default=(),
        ),
    )


def load_pipeline_settings() -> PipelineSettings:
    table = _table("pipeline", required_config=True)
    return PipelineSettings(
        namespace=_required_string(table, "pipeline", "namespace"),
        secret_source_namespace=_required_string(
            table,
            "pipeline",
            "secret_source_namespace",
        ),
        worker_image=_required_string(table, "pipeline", "worker_image"),
        workspace_host_path=_required_path(table, "pipeline", "workspace_host_path"),
        repo_cache_host_path=_required_path(table, "pipeline", "repo_cache_host_path"),
        successful_tasks_host_path=_required_path(
            table,
            "pipeline",
            "successful_tasks_host_path",
        ),
        k3s_nodes=_string_tuple(
            table.get("k3s_nodes"),
            "[pipeline].k3s_nodes",
            default=(),
        ),
        k3s_ssh_user=_required_string(table, "pipeline", "k3s_ssh_user"),
        build_ca_path=_required_path(table, "pipeline", "build_ca_path"),
        build_worker_image_on_start=_required_boolean(
            table,
            "pipeline",
            "build_worker_image_on_start",
        ),
        autoqueue_workers=_required_integer(table, "pipeline", "autoqueue_workers"),
        generate_workers=_required_integer(table, "pipeline", "generate_workers"),
        validate_workers=_required_integer(table, "pipeline", "validate_workers"),
        reward_workers=_required_integer(table, "pipeline", "reward_workers"),
        push_workers=_required_integer(table, "pipeline", "push_workers"),
        rollout_timeout_seconds=_required_integer(
            table,
            "pipeline",
            "rollout_timeout_seconds",
        ),
    )


def load_completed_task_settings() -> CompletedTaskSettings:
    table = _table("completed_tasks")
    raw = table.get("output_dir", "data_cache/successful_harbor_tasks")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("[completed_tasks].output_dir must be a non-empty path")
    return CompletedTaskSettings(_config_relative_path(raw.strip()))


def _config_relative_path(raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path
    return config_path().expanduser().resolve().parent / path


def load_github_token() -> str | None:
    tokens = load_github_tokens()
    return random.choice(tokens) if tokens else None


def load_github_tokens() -> list[str]:
    table = _table("github")
    raw = table.get("gh_tokens")
    if isinstance(raw, list):
        tokens = [str(token).strip() for token in raw if str(token).strip()]
        if tokens:
            return tokens
    single = str(table.get("token") or "").strip()
    return [single] if single else []


def session_header_env(instance_id: str, header: str = "X-Session-ID") -> dict[str, str]:
    digest = hashlib.sha256(instance_id.encode("utf-8")).hexdigest()
    return {"ANTHROPIC_CUSTOM_HEADERS": f"{header}: {digest}"}


def claude_runtime_env(instance_id: str) -> dict[str, str]:
    settings = load_model_settings()
    env = session_header_env(instance_id)
    if settings.api_key:
        env["ANTHROPIC_API_KEY"] = settings.api_key
    if settings.auth_token:
        env["ANTHROPIC_AUTH_TOKEN"] = settings.auth_token
    if settings.oauth_token:
        env["CLAUDE_CODE_OAUTH_TOKEN"] = settings.oauth_token
    if settings.base_url:
        env["ANTHROPIC_BASE_URL"] = settings.base_url
    env["ANTHROPIC_MODEL"] = settings.model
    if settings.fast_model:
        env["SWEGEN_CLAUDE_FAST_MODEL"] = settings.fast_model
        env["ANTHROPIC_DEFAULT_HAIKU_MODEL"] = settings.fast_model
        env["ANTHROPIC_SMALL_FAST_MODEL"] = settings.fast_model
    if settings.opus_model:
        env["ANTHROPIC_DEFAULT_OPUS_MODEL"] = settings.opus_model
    if settings.sonnet_model:
        env["ANTHROPIC_DEFAULT_SONNET_MODEL"] = settings.sonnet_model
    env["SWEGEN_AGENT_REASONING_EFFORT"] = settings.reasoning_effort
    return env


def claude_session_env(instance_id: str, header: str = "X-Session-ID") -> dict[str, str]:
    env = claude_runtime_env(instance_id)
    if header != "X-Session-ID":
        env.update(session_header_env(instance_id, header))
    return env


def configured_subprocess_env(instance_id: str = "swegen") -> dict[str, str]:
    env = os.environ.copy()
    for name in MANAGED_RUNTIME_ENV_VARS:
        env.pop(name, None)
    env.update(claude_runtime_env(instance_id))
    github_token = load_github_token()
    if github_token:
        env["GITHUB_TOKEN"] = github_token
    openai = load_openai_settings(required=False)
    if openai.api_key:
        env["OPENAI_API_KEY"] = openai.api_key
    if openai.base_url:
        env["OPENAI_BASE_URL"] = openai.base_url
    if openai.task_instruction_model:
        env["OPENAI_MODEL"] = openai.task_instruction_model
    return env


def configure_current_process(instance_id: str = "swegen") -> None:
    env = configured_subprocess_env(instance_id)
    for name in MANAGED_RUNTIME_ENV_VARS:
        os.environ.pop(name, None)
    for name in MANAGED_RUNTIME_ENV_VARS:
        if name in env:
            os.environ[name] = env[name]
