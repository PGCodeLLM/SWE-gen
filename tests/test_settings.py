from __future__ import annotations

from pathlib import Path

import pytest

from swegen import model_settings


def _write_config(path: Path) -> None:
    path.write_text(
        """
[model]
model = "configured-claude"
base_url = "https://claude.example"
api_key = "configured-anthropic-key"

[openai]
api_key = "configured-openai-key"
base_url = "https://openai.example/v1"
task_instruction_model = "instruction-model"
verdict_model = "verdict-model"

[analysis]
classifier_model = "classifier-model"
agent_model = "agent-model"

[github]
token = "configured-github-key"
""".strip()
        + "\n"
    )


def test_api_and_model_environment_variables_do_not_override_toml(tmp_path, monkeypatch):
    config = tmp_path / "swegen.toml"
    _write_config(config)
    monkeypatch.setattr(model_settings, "DEFAULT_CONFIG_FILE", config)
    monkeypatch.setenv("ANTHROPIC_MODEL", "environment-model")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "environment-key")
    monkeypatch.setenv("OPENAI_API_KEY", "environment-openai-key")
    monkeypatch.setenv("GITHUB_TOKEN", "environment-github-key")

    claude = model_settings.load_model_settings()
    openai = model_settings.load_openai_settings()
    child_env = model_settings.configured_subprocess_env("instance")

    assert claude.model == "configured-claude"
    assert claude.api_key == "configured-anthropic-key"
    assert openai.api_key == "configured-openai-key"
    assert child_env["ANTHROPIC_API_KEY"] == "configured-anthropic-key"
    assert child_env["OPENAI_API_KEY"] == "configured-openai-key"
    assert child_env["GITHUB_TOKEN"] == "configured-github-key"


def test_timeout_lease_uses_all_configured_phases():
    settings = model_settings.TimeoutSettings(
        task_instruction=70,
        docker_build=10,
        claude_code=20,
        harbor_nop=30,
        harbor_oracle=40,
        hacking_check=50,
        swr_upload=60,
        lease_fraction=0.1,
    )
    assert settings.lease_seconds_per_task() == 28


def test_database_max_retries_is_loaded_from_hyphenated_toml_key(tmp_path, monkeypatch):
    config = tmp_path / "swegen.toml"
    config.write_text(
        """
[database]
host = "localhost"
database = "mindforge"
user = "postgres"
password = "secret"
table = "swegen.pr_tasks"
max-retries = 7
""".strip()
        + "\n"
    )
    monkeypatch.setattr(model_settings, "DEFAULT_CONFIG_FILE", config)

    assert model_settings.load_database_settings().max_retries == 7


@pytest.mark.parametrize("max_retries", [0, -1])
def test_database_max_retries_must_be_positive(max_retries):
    with pytest.raises(ValueError, match=r"max-retries must be >= 1"):
        model_settings.DatabaseSettings(
            host="localhost",
            port=5432,
            database="mindforge",
            user="postgres",
            password="secret",
            table="swegen.pr_tasks",
            max_retries=max_retries,
        )
