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
    assert settings.total_seconds_per_task() == 280
    assert settings.lease_seconds_per_task() == 28


def test_orchestrator_produce_count_defaults_to_unbounded(tmp_path, monkeypatch):
    config = tmp_path / "swegen.toml"
    config.write_text("[orchestrator]\n")
    monkeypatch.setattr(model_settings, "DEFAULT_CONFIG_FILE", config)

    assert model_settings.load_orchestrator_settings().produce_count is None


def test_orchestrator_produce_count_is_loaded(tmp_path, monkeypatch):
    config = tmp_path / "swegen.toml"
    config.write_text("[orchestrator]\nproduce_count = 25\n")
    monkeypatch.setattr(model_settings, "DEFAULT_CONFIG_FILE", config)

    assert model_settings.load_orchestrator_settings().produce_count == 25


@pytest.mark.parametrize("value", ["0", "-1", '"10"', "true"])
def test_orchestrator_produce_count_must_be_a_positive_integer(value, tmp_path, monkeypatch):
    config = tmp_path / "swegen.toml"
    config.write_text(f"[orchestrator]\nproduce_count = {value}\n")
    monkeypatch.setattr(model_settings, "DEFAULT_CONFIG_FILE", config)

    with pytest.raises(ValueError, match=r"produce_count"):
        model_settings.load_orchestrator_settings()


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

    settings = model_settings.load_database_settings()
    assert settings.max_retries == 7
    assert settings.exclude_languages == ()
    assert settings.pr_categories == ("feature",)


def test_database_language_and_category_filters_are_normalized(tmp_path, monkeypatch):
    config = tmp_path / "swegen.toml"
    config.write_text(
        """
[database]
host = "localhost"
database = "mindforge"
user = "postgres"
password = "secret"
table = "swegen.pr_tasks"
exclude_languages = ["Python", " rust ", "PYTHON"]
pr_category = ["Feature", "bugfix", "FEATURE"]
""".strip()
        + "\n"
    )
    monkeypatch.setattr(model_settings, "DEFAULT_CONFIG_FILE", config)

    settings = model_settings.load_database_settings()
    assert settings.exclude_languages == ("python", "rust")
    assert settings.pr_categories == ("feature", "bugfix")


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


@pytest.mark.parametrize("pr_categories", [(), [], "feature"])
def test_database_pr_category_must_be_a_nonempty_array(pr_categories):
    with pytest.raises(ValueError, match=r"pr_category"):
        model_settings.DatabaseSettings(
            host="localhost",
            port=5432,
            database="mindforge",
            user="postgres",
            password="secret",
            table="swegen.pr_tasks",
            pr_categories=pr_categories,
        )


def test_database_exclude_languages_must_be_an_array():
    with pytest.raises(ValueError, match=r"exclude_languages"):
        model_settings.DatabaseSettings(
            host="localhost",
            port=5432,
            database="mindforge",
            user="postgres",
            password="secret",
            table="swegen.pr_tasks",
            exclude_languages="python",
        )
