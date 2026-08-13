from __future__ import annotations

import pytest

from swegen.model_settings import (
    MissingRequiredSetting,
    claude_session_env,
    load_model_settings,
    required_environment_value,
)

_MODEL_ENVIRONMENT_NAMES = (
    "ANTHROPIC_MODEL",
    "ANTHROPIC_BASE_URL",
    "SWEGEN_CLAUDE_FAST_MODEL",
    "ANTHROPIC_SMALL_FAST_MODEL",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL",
    "SWEGEN_CONFIG",
)


@pytest.fixture(autouse=True)
def clear_model_environment(monkeypatch, tmp_path) -> None:
    """Resolve models from the environment alone, never a stray swegen.toml.

    ``load_model_settings`` reads ``swegen.toml`` from the working directory, so
    point the config path at an empty tmp dir to keep these tests hermetic.
    """

    for name in _MODEL_ENVIRONMENT_NAMES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SWEGEN_CONFIG", str(tmp_path / "absent.toml"))


def test_claude_session_env_replaces_both_internal_haiku_paths(monkeypatch) -> None:
    """With no fast-model override, both internal paths use the primary model.

    The fast model used to fall back to a hardcoded literal that contradicted
    what deploy/k3s/create-secrets.sh writes, pointing Claude Code's internal
    calls at a model the endpoint need not serve.
    """

    monkeypatch.setenv("ANTHROPIC_MODEL", "primary-model")

    env = claude_session_env("owner__repo-1")

    assert env["ANTHROPIC_SMALL_FAST_MODEL"] == "primary-model"
    assert env["ANTHROPIC_DEFAULT_HAIKU_MODEL"] == "primary-model"


def test_claude_session_env_honors_swegen_fast_model_override(monkeypatch) -> None:
    monkeypatch.setenv("SWEGEN_CLAUDE_FAST_MODEL", "custom-fast-model")

    env = claude_session_env("owner__repo-2")

    assert env["ANTHROPIC_SMALL_FAST_MODEL"] == "custom-fast-model"
    assert env["ANTHROPIC_DEFAULT_HAIKU_MODEL"] == "custom-fast-model"


def test_claude_session_env_preserves_the_claude_code_fallback_chain(monkeypatch) -> None:
    """SWEGEN_CLAUDE_FAST_MODEL > SMALL_FAST > DEFAULT_HAIKU > primary model."""

    monkeypatch.setenv("ANTHROPIC_MODEL", "primary-model")
    monkeypatch.setenv("ANTHROPIC_DEFAULT_HAIKU_MODEL", "haiku-model")

    assert claude_session_env("id")["ANTHROPIC_SMALL_FAST_MODEL"] == "haiku-model"

    monkeypatch.setenv("ANTHROPIC_SMALL_FAST_MODEL", "small-fast-model")
    assert claude_session_env("id")["ANTHROPIC_DEFAULT_HAIKU_MODEL"] == "small-fast-model"

    monkeypatch.setenv("SWEGEN_CLAUDE_FAST_MODEL", "swegen-fast-model")
    assert claude_session_env("id")["ANTHROPIC_SMALL_FAST_MODEL"] == "swegen-fast-model"


def test_claude_session_env_requires_a_model_when_nothing_is_configured() -> None:
    """No model anywhere must fail by name, not silently pick a literal."""

    with pytest.raises(MissingRequiredSetting, match="ANTHROPIC_MODEL"):
        claude_session_env("owner__repo-3")


def test_load_model_settings_requires_an_explicitly_configured_model() -> None:
    with pytest.raises(MissingRequiredSetting) as raised:
        load_model_settings()

    message = str(raised.value)
    assert "ANTHROPIC_MODEL" in message
    # Actionable: names the Secret family that supplies it in the k3s deploy.
    assert "model-credential" in message


def test_load_model_settings_reads_the_environment_and_config_file(monkeypatch, tmp_path) -> None:
    config = tmp_path / "swegen.toml"
    config.write_text('[model]\nmodel = "toml-model"\nbase_url = "https://toml.example"\n')
    monkeypatch.setenv("SWEGEN_CONFIG", str(config))

    from_file = load_model_settings()
    assert from_file.model == "toml-model"
    assert from_file.base_url == "https://toml.example"

    monkeypatch.setenv("ANTHROPIC_MODEL", "env-model")
    assert load_model_settings().model == "env-model"


def test_load_model_settings_treats_a_blank_model_as_unset(monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_MODEL", "   ")

    with pytest.raises(MissingRequiredSetting, match="ANTHROPIC_MODEL"):
        load_model_settings()


def test_required_environment_value_names_the_variable_and_its_source(monkeypatch) -> None:
    monkeypatch.delenv("SWEGEN_EXAMPLE_SETTING", raising=False)

    with pytest.raises(MissingRequiredSetting) as raised:
        required_environment_value("SWEGEN_EXAMPLE_SETTING", supplied_by="ConfigMap example-config")

    message = str(raised.value)
    assert "SWEGEN_EXAMPLE_SETTING" in message
    assert "ConfigMap example-config" in message
    assert raised.value.name == "SWEGEN_EXAMPLE_SETTING"

    monkeypatch.setenv("SWEGEN_EXAMPLE_SETTING", " configured ")
    assert (
        required_environment_value("SWEGEN_EXAMPLE_SETTING", supplied_by="ConfigMap example-config")
        == "configured"
    )
