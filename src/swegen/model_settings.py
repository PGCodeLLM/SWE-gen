from __future__ import annotations

import hashlib
import logging
import os
import random
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger("swegen")

DEFAULT_CONFIG_FILE = Path("swegen.toml")
# These values may be required by third-party SDK subprocesses, but users must
# configure them in swegen.toml.  We deliberately discard inherited values so
# shell/.env state cannot silently change a run.
MANAGED_RUNTIME_ENV_VARS = (
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_MODEL",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "GITHUB_TOKEN",
    "GH_TOKEN",
    "OPENAI_API_KEY",
    "OPENAI_BASE_URL",
    "REPO_CREATION_TOKEN",
)


@dataclass(frozen=True)
class ModelSettings:
    model: str
    base_url: str | None = None
    api_key: str | None = None
    auth_token: str | None = None
    oauth_token: str | None = None


@dataclass(frozen=True)
class OpenAISettings:
    api_key: str
    base_url: str | None
    task_instruction_model: str
    verdict_model: str


@dataclass(frozen=True)
class AnalysisSettings:
    classifier_model: str
    agent_model: str


@dataclass(frozen=True)
class OrchestratorSettings:
    produce_count: int | None = None

    def __post_init__(self) -> None:
        if self.produce_count is not None and self.produce_count < 1:
            raise ValueError("[orchestrator].produce_count must be >= 1")


@dataclass(frozen=True)
class DatabaseSettings:
    host: str
    port: int
    database: str
    user: str
    password: str
    table: str
    max_retries: int = 3
    exclude_languages: tuple[str, ...] = ()
    pr_categories: tuple[str, ...] = ("feature",)
    connect_timeout: int = 10

    def __post_init__(self) -> None:
        if self.max_retries < 1:
            raise ValueError("[database].max-retries must be >= 1")
        object.__setattr__(
            self,
            "exclude_languages",
            _normalize_string_values(
                self.exclude_languages,
                "[database].exclude_languages",
                allow_empty=True,
            ),
        )
        object.__setattr__(
            self,
            "pr_categories",
            _normalize_string_values(
                self.pr_categories,
                "[database].pr_category",
                allow_empty=False,
            ),
        )


@dataclass(frozen=True)
class TimeoutSettings:
    task_instruction: int = 90
    docker_build: int = 600
    claude_code: int = 3200
    harbor_nop: int = 600
    harbor_oracle: int = 600
    hacking_check: int = 600
    swr_upload: int = 1800
    lease_fraction: float = 0.1

    def total_seconds_per_task(self, claude_code_override: int | None = None) -> int:
        return (
            self.task_instruction
            + self.docker_build
            + (claude_code_override or self.claude_code)
            + self.harbor_nop
            + self.harbor_oracle
            + self.hacking_check
            + self.swr_upload
        )

    def lease_seconds_per_task(self, claude_code_override: int | None = None) -> int:
        total = self.total_seconds_per_task(claude_code_override)
        return max(1, round(total * self.lease_fraction))


@dataclass(frozen=True)
class SWRSettings:
    enabled: bool
    registry: str
    repository: str
    username: str
    password: str
    tag_prefix: str = ""
    retries: int = 3
    push_timeout: int = 1800


def config_path() -> Path:
    """Return the single supported configuration location."""
    return DEFAULT_CONFIG_FILE


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
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise RuntimeError(f"Could not load {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"Invalid configuration in {path}: expected a TOML document")
    return data


def _table(name: str, *, required_config: bool = False) -> dict[str, Any]:
    value = load_config(required=required_config).get(name, {})
    if not isinstance(value, dict):
        raise ValueError(f"[{name}] in {config_path()} must be a TOML table")
    return value


def _required_string(table: dict[str, Any], section: str, key: str) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"[{section}].{key} must be set in {config_path()}")
    return value.strip()


def _normalize_string_values(
    values: object,
    field_name: str,
    *,
    allow_empty: bool,
) -> tuple[str, ...]:
    if not isinstance(values, (list, tuple)):
        raise ValueError(f"{field_name} must be a TOML array of strings")
    normalized: list[str] = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field_name} must contain only non-empty strings")
        item = value.strip().lower()
        if item not in normalized:
            normalized.append(item)
    if not normalized and not allow_empty:
        raise ValueError(f"{field_name} must contain at least one value")
    return tuple(normalized)


def load_model_settings() -> ModelSettings:
    table = _table("model")
    return ModelSettings(
        model=str(table.get("model") or "").strip(),
        base_url=str(table["base_url"]).strip() if table.get("base_url") else None,
        api_key=str(table["api_key"]).strip() if table.get("api_key") else None,
        auth_token=(str(table["auth_token"]).strip() if table.get("auth_token") else None),
        oauth_token=(str(table["oauth_token"]).strip() if table.get("oauth_token") else None),
    )


def load_openai_settings(*, require_api_key: bool = True) -> OpenAISettings:
    table = _table("openai", required_config=require_api_key)
    api_key = str(table.get("api_key") or "").strip()
    if require_api_key and not api_key:
        raise ValueError(f"[openai].api_key must be set in {config_path()}")
    task_instruction_model = str(table.get("task_instruction_model") or "").strip()
    verdict_model = str(table.get("verdict_model") or "").strip()
    if require_api_key and not task_instruction_model:
        raise ValueError(f"[openai].task_instruction_model must be set in {config_path()}")
    if require_api_key and not verdict_model:
        raise ValueError(f"[openai].verdict_model must be set in {config_path()}")
    return OpenAISettings(
        api_key=api_key,
        base_url=str(table["base_url"]).strip() if table.get("base_url") else None,
        task_instruction_model=task_instruction_model,
        verdict_model=verdict_model,
    )


def load_analysis_settings() -> AnalysisSettings:
    table = _table("analysis")
    return AnalysisSettings(
        classifier_model=str(table.get("classifier_model") or "").strip(),
        agent_model=str(table.get("agent_model") or "").strip(),
    )


def load_orchestrator_settings() -> OrchestratorSettings:
    table = _table("orchestrator")
    raw_produce_count = table.get("produce_count")
    if raw_produce_count is None:
        return OrchestratorSettings()
    if isinstance(raw_produce_count, bool) or not isinstance(raw_produce_count, int):
        raise ValueError("[orchestrator].produce_count must be an integer")
    return OrchestratorSettings(produce_count=raw_produce_count)


def load_database_settings() -> DatabaseSettings:
    table = _table("database", required_config=True)
    relation = _required_string(table, "database", "table")
    if relation.count(".") != 1:
        raise ValueError("[database].table must be schema-qualified (for example swegen.pr_tasks)")
    return DatabaseSettings(
        host=_required_string(table, "database", "host"),
        port=int(table.get("port", 5432)),
        database=_required_string(table, "database", "database"),
        user=_required_string(table, "database", "user"),
        password=_required_string(table, "database", "password"),
        table=relation,
        max_retries=int(table.get("max-retries", 3)),
        exclude_languages=_normalize_string_values(
            table.get("exclude_languages", []),
            "[database].exclude_languages",
            allow_empty=True,
        ),
        pr_categories=_normalize_string_values(
            table.get("pr_category", ["feature"]),
            "[database].pr_category",
            allow_empty=False,
        ),
        connect_timeout=int(table.get("connect_timeout", 10)),
    )


def load_timeout_settings() -> TimeoutSettings:
    table = _table("timeouts")
    return TimeoutSettings(
        task_instruction=int(table.get("task_instruction", 90)),
        docker_build=int(table.get("docker_build", 600)),
        claude_code=int(table.get("claude_code", 3200)),
        harbor_nop=int(table.get("harbor_nop", 600)),
        harbor_oracle=int(table.get("harbor_oracle", 600)),
        hacking_check=int(table.get("hacking_check", 600)),
        swr_upload=int(table.get("swr_upload", 1800)),
        lease_fraction=float(table.get("lease_fraction", 0.1)),
    )


def load_swr_settings() -> SWRSettings:
    table = _table("swr")
    enabled = bool(table.get("enabled", False))
    settings = SWRSettings(
        enabled=enabled,
        registry=str(table.get("registry") or "").strip().rstrip("/"),
        repository=str(table.get("repository") or "").strip().strip("/"),
        username=str(table.get("username") or "").strip(),
        password=str(table.get("password") or ""),
        tag_prefix=str(table.get("tag_prefix") or ""),
        retries=max(1, int(table.get("retries", 3))),
        push_timeout=max(1, int(table.get("push_timeout", 1800))),
    )
    if enabled:
        missing = [
            name
            for name in ("registry", "repository", "username", "password")
            if not getattr(settings, name)
        ]
        if missing:
            raise ValueError(f"[swr] is enabled but these values are missing: {', '.join(missing)}")
    return settings


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
    """Build the SDK environment exclusively from swegen.toml."""
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
    return env


def configured_subprocess_env(instance_id: str = "swegen") -> dict[str, str]:
    """Return a child environment with managed secrets replaced from TOML."""
    env = os.environ.copy()
    for name in MANAGED_RUNTIME_ENV_VARS:
        env.pop(name, None)
    env.update(claude_runtime_env(instance_id))
    github_token = load_github_token()
    if github_token:
        env["GITHUB_TOKEN"] = github_token
    openai = load_openai_settings(require_api_key=False)
    if openai.api_key:
        env["OPENAI_API_KEY"] = openai.api_key
    if openai.base_url:
        env["OPENAI_BASE_URL"] = openai.base_url
    return env


def configure_current_process(instance_id: str = "swegen") -> None:
    """Replace any inherited API/model variables with central configuration."""
    env = configured_subprocess_env(instance_id)
    for name in MANAGED_RUNTIME_ENV_VARS:
        os.environ.pop(name, None)
    for name in MANAGED_RUNTIME_ENV_VARS:
        if name in env:
            os.environ[name] = env[name]
