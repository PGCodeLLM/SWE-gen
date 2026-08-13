from __future__ import annotations

import hashlib
import logging
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger("swegen")

# There is deliberately no default model. A hardcoded real-looking model name
# made a missing or stale model-credential Secret indistinguishable from a
# working one: every session kept starting, then died at the gateway with
# "Invalid model name". The model must now be configured explicitly.
MODEL_ENV = "ANTHROPIC_MODEL"
MODEL_SUPPLIED_BY = (
    "the stage's model-credential Secret (e.g. swegen-repair-model-credentials "
    "or swegen-model-credentials-*, key ANTHROPIC_MODEL) in namespace "
    "swegen-pipeline, or [model].model in swegen.toml"
)

# Claude Code uses its Haiku tier for internal lightweight work such as
# built-in Explore subagents and Bash command-path extraction.  Our compatible
# endpoint serves that role under this model name too, so the fast model
# resolves to the primary model rather than to a separate literal: the previous
# hardcoded value contradicted what deploy/k3s/create-secrets.sh actually
# writes into SWEGEN_CLAUDE_FAST_MODEL.
CLAUDE_FAST_MODEL_ENV = "SWEGEN_CLAUDE_FAST_MODEL"


class MissingRequiredSetting(RuntimeError):
    """A setting with no safe default was not supplied by the environment.

    Raised instead of substituting a hardcoded value, so a deleted, renamed or
    stale Secret/ConfigMap key fails at startup with the variable named rather
    than silently running against the wrong model or endpoint.
    """

    def __init__(self, name: str, *, supplied_by: str) -> None:
        self.name = name
        self.supplied_by = supplied_by
        super().__init__(
            f"{name} is not set and has no default. It is supplied by "
            f"{supplied_by}; verify that source still defines it and that the "
            f"workload mounts it."
        )


def required_environment_value(name: str, *, supplied_by: str) -> str:
    """Return a required environment value, or raise naming what supplies it.

    Blank counts as unset: an empty ConfigMap/Secret value is a configuration
    mistake, not an instruction to fall back.
    """

    value = os.environ.get(name, "").strip()
    if not value:
        raise MissingRequiredSetting(name, supplied_by=supplied_by)
    return value

# Env var pointing at an alternate config file location (otherwise swegen.toml in
# the current working directory is used).
CONFIG_PATH_ENV = "SWEGEN_CONFIG"
DEFAULT_CONFIG_FILE = "swegen.toml"


@dataclass(frozen=True)
class ModelSettings:
    """Resolved model + endpoint for the Claude Code SDK session.

    Attributes:
        model: Model name to pass to the SDK.
        base_url: Inference endpoint (ANTHROPIC_BASE_URL), or None to use the
            SDK/CLI default.
    """

    model: str
    base_url: str | None = None


def _config_path() -> Path:
    override = os.environ.get(CONFIG_PATH_ENV)
    return Path(override) if override else Path(DEFAULT_CONFIG_FILE)


def _read_table(section: str) -> dict:
    """Read a top-level table (e.g. ``[model]``) from the config file.

    Returns an empty dict if the file is missing or malformed (a malformed file
    is logged as a warning rather than crashing the pipeline).
    """
    path = _config_path()
    if not path.exists():
        return {}
    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError) as e:
        logger.warning("Ignoring malformed config file %s: %s", path, e)
        return {}
    table = data.get(section, {})
    if not isinstance(table, dict):
        logger.warning("Ignoring [%s] in %s: expected a table", section, path)
        return {}
    return table


def load_model_settings() -> ModelSettings:
    """Resolve model + endpoint with precedence: env var > swegen.toml.

    - model:    ANTHROPIC_MODEL    > [model].model    (required, no default)
    - base_url: ANTHROPIC_BASE_URL > [model].base_url > None

    Raises:
        MissingRequiredSetting: when neither ``ANTHROPIC_MODEL`` nor
            ``[model].model`` names a model.
    """
    table = _read_table("model")

    model = (os.environ.get(MODEL_ENV, "").strip() or str(table.get("model") or "").strip()).strip()
    if not model:
        raise MissingRequiredSetting(MODEL_ENV, supplied_by=MODEL_SUPPLIED_BY)
    base_url = os.environ.get("ANTHROPIC_BASE_URL") or table.get("base_url") or None

    return ModelSettings(model=model, base_url=base_url)


def session_header_env(instance_id: str, header: str = "X-Session-ID") -> dict[str, str]:
    """Env mapping that pins a stable per-instance routing header for the SDK.

    The header value is a deterministic SHA-256 hash of ``instance_id``, so every
    Claude SDK round for the same instance carries the same ``X-Session-ID``. When
    the endpoint is a router fronting multiple models, this keeps one instance
    pinned to one model, maximizing KV cache reuse. The hash is stable across
    processes and retries (unlike Python's salted ``hash()``).

    Any ``ANTHROPIC_CUSTOM_HEADERS`` already in the environment is preserved; the
    session header is appended on its own line.
    """
    digest = hashlib.sha256(instance_id.encode("utf-8")).hexdigest()
    line = f"{header}: {digest}"
    existing = os.environ.get("ANTHROPIC_CUSTOM_HEADERS", "").strip()
    value = f"{existing}\n{line}" if existing else line
    return {"ANTHROPIC_CUSTOM_HEADERS": value}


def claude_session_env(instance_id: str, header: str = "X-Session-ID") -> dict[str, str]:
    """Return environment overrides shared by every Claude SDK session.

    Claude Code has two names for its lightweight internal model.  The legacy
    ``ANTHROPIC_SMALL_FAST_MODEL`` takes precedence for fast helpers such as
    Bash command-path extraction, while the built-in ``haiku`` tier used by
    Explore subagents resolves through ``ANTHROPIC_DEFAULT_HAIKU_MODEL``.
    Setting both prevents either path from falling back to Claude Haiku.

    ``SWEGEN_CLAUDE_FAST_MODEL`` is the preferred SWE-Gen override.  Existing
    Claude Code model variables remain valid fallbacks for direct invocations.
    The last resort is the resolved primary model, not a separate literal: one
    endpoint serves both tiers, and a distinct hardcoded fast model silently
    pointed internal calls at a model the gateway may not list.

    Raises:
        MissingRequiredSetting: when no fast-model variable is set and no
            primary model is configured either.
    """
    env = session_header_env(instance_id, header)
    fast_model = (
        os.environ.get(CLAUDE_FAST_MODEL_ENV)
        or os.environ.get("ANTHROPIC_SMALL_FAST_MODEL")
        or os.environ.get("ANTHROPIC_DEFAULT_HAIKU_MODEL")
        or load_model_settings().model
    ).strip()
    if fast_model:
        env.update(
            {
                "ANTHROPIC_SMALL_FAST_MODEL": fast_model,
                "ANTHROPIC_DEFAULT_HAIKU_MODEL": fast_model,
            }
        )
    return env


def load_github_token() -> str | None:
    """Return the GitHub token from ``[github].token`` in swegen.toml, if set.

    This is only the config-file value; callers are expected to give precedence
    to an explicit flag or the GITHUB_TOKEN environment variable.
    """
    token = _read_table("github").get("token")
    return token or None


def load_github_tokens() -> list[str]:
    """Return the pool of GitHub tokens configured in swegen.toml.

    Reads ``[github].gh_tokens`` (a list of strings) and falls back to the
    single ``[github].token`` when no list is set. Non-string and empty entries
    are dropped. Returns an empty list when nothing is configured.

    Callers are expected to give precedence to an explicit flag or the
    GITHUB_TOKEN environment variable over this pool.
    """
    table = _read_table("github")
    raw = table.get("gh_tokens")
    if isinstance(raw, list):
        tokens = [t.strip() for t in raw if isinstance(t, str) and t.strip()]
        if tokens:
            return tokens
        if raw:
            logger.warning("Ignoring [github].gh_tokens: no usable string entries")
    elif raw is not None:
        logger.warning("Ignoring [github].gh_tokens: expected a list of strings")
    single = table.get("token")
    return [single] if isinstance(single, str) and single.strip() else []
