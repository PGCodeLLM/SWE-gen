from __future__ import annotations

import hashlib
import logging
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger("swegen")

# Model used when nothing is configured via env var or swegen.toml. Kept here so
# the out-of-the-box behavior matches what the runner previously hardcoded.
DEFAULT_MODEL = "qwen3.5-397b-a17b-alex-swe-gen"

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
    """Resolve model + endpoint with precedence: env var > swegen.toml > default.

    - model:    ANTHROPIC_MODEL    > [model].model    > DEFAULT_MODEL
    - base_url: ANTHROPIC_BASE_URL > [model].base_url > None
    """
    table = _read_table("model")

    model = os.environ.get("ANTHROPIC_MODEL") or table.get("model") or DEFAULT_MODEL
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
