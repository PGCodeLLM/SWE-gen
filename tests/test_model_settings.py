from __future__ import annotations

from pathlib import Path

from swegen.model_settings import (
    claude_session_env,
    configured_subprocess_env,
    load_autoqueue_settings,
    load_hacking_settings,
)


def write_config(path: Path, *, max_queued: int | None = None) -> None:
    explicit_limit = f"max_queued = {max_queued}\n" if max_queued is not None else ""
    path.write_text(
        "[model]\n"
        'model = "configured-model"\n'
        'base_url = "https://configured.example"\n'
        'api_key = "configured-anthropic-key"\n'
        'fast_model = "configured-fast-model"\n'
        "\n[openai]\n"
        'api_key = "configured-openai-key"\n'
        'base_url = "https://openai.example/v1"\n'
        'task_instruction_model = "instruction-model"\n'
        'verdict_model = "verdict-model"\n'
        "\n[github]\n"
        'gh_tokens = ["configured-github-token"]\n'
        "\n[[hacking.llm]]\n"
        'name = "checker"\n'
        'endpoint = "https://checker.example"\n'
        'model = "checker-model"\n'
        'api_key = "checker-key"\n'
        "\n[autoqueue]\n"
        f"{explicit_limit}"
        "max_queued_per_generate_worker = 1.5\n"
        "generate_workers = 3\n"
    )


def test_claude_session_env_uses_toml_and_ignores_inherited_model_values(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = tmp_path / "swegen.toml"
    write_config(config)
    monkeypatch.setenv("SWEGEN_CONFIG", str(config))
    monkeypatch.setenv("ANTHROPIC_MODEL", "inherited-model")
    monkeypatch.setenv("SWEGEN_CLAUDE_FAST_MODEL", "inherited-fast-model")

    env = claude_session_env("owner__repo-1")

    assert env["ANTHROPIC_MODEL"] == "configured-model"
    assert env["ANTHROPIC_BASE_URL"] == "https://configured.example"
    assert env["ANTHROPIC_API_KEY"] == "configured-anthropic-key"
    assert env["ANTHROPIC_SMALL_FAST_MODEL"] == "configured-fast-model"
    assert env["ANTHROPIC_DEFAULT_HAIKU_MODEL"] == "configured-fast-model"


def test_configured_subprocess_env_replaces_inherited_openai_and_github_values(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = tmp_path / "swegen.toml"
    write_config(config)
    monkeypatch.setenv("SWEGEN_CONFIG", str(config))
    monkeypatch.setenv("OPENAI_API_KEY", "inherited-openai-key")
    monkeypatch.setenv("GITHUB_TOKEN", "inherited-github-token")

    env = configured_subprocess_env("owner__repo-2")

    assert env["OPENAI_API_KEY"] == "configured-openai-key"
    assert env["OPENAI_BASE_URL"] == "https://openai.example/v1"
    assert env["OPENAI_MODEL"] == "instruction-model"
    assert env["GITHUB_TOKEN"] == "configured-github-token"


def test_hacking_checker_endpoint_model_and_key_come_from_toml(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = tmp_path / "swegen.toml"
    write_config(config)
    monkeypatch.setenv("SWEGEN_CONFIG", str(config))
    monkeypatch.setenv("SWEGEN_REWARD_ENDPOINT", "https://inherited.example")

    (checker,) = load_hacking_settings()

    assert checker.endpoint == "https://checker.example"
    assert checker.model == "checker-model"
    assert checker.api_key == "checker-key"


def test_autoqueue_explicit_limit_takes_priority(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "swegen.toml"
    write_config(config, max_queued=2)
    monkeypatch.setenv("SWEGEN_CONFIG", str(config))

    assert load_autoqueue_settings().queue_limit == 2


def test_autoqueue_defaults_to_one_point_five_per_generate_worker(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = tmp_path / "swegen.toml"
    write_config(config)
    monkeypatch.setenv("SWEGEN_CONFIG", str(config))

    assert load_autoqueue_settings().queue_limit == 5
