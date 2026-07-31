from __future__ import annotations

from pathlib import Path

import pytest

from swegen.model_settings import (
    claude_session_env,
    configured_subprocess_env,
    load_autoqueue_settings,
    load_hacking_settings,
    load_pipeline_settings,
    load_swr_target,
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
        "\n[pipeline]\n"
        'namespace = "swegen-pipeline-tester"\n'
        'secret_source_namespace = "swegen-pipeline"\n'
        'worker_image = "swegen-worker:test"\n'
        'workspace_host_path = "/data/swegen-test/workspaces"\n'
        'repo_cache_host_path = "/data/swegen-test/cache"\n'
        'successful_tasks_host_path = "/data/swegen-test/successful"\n'
        'k3s_nodes = ["192.0.2.10"]\n'
        'k3s_ssh_user = "root"\n'
        'build_ca_path = "/etc/ssl/certs/ca-certificates.crt"\n'
        "build_worker_image_on_start = false\n"
        "autoqueue_workers = 1\n"
        "generate_workers = 3\n"
        "validate_workers = 4\n"
        "reward_workers = 2\n"
        "push_workers = 1\n"
        "rollout_timeout_seconds = 900\n"
        "\n[autoqueue]\n"
        f"{explicit_limit}"
        "max_queued_per_generate_worker = 1.5\n"
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


def test_pipeline_worker_counts_come_from_toml(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "swegen.toml"
    write_config(config)
    monkeypatch.setenv("SWEGEN_CONFIG", str(config))

    settings = load_pipeline_settings()

    assert settings.autoqueue_workers == 1
    assert settings.namespace == "swegen-pipeline-tester"
    assert settings.secret_source_namespace == "swegen-pipeline"
    assert settings.worker_image == "swegen-worker:test"
    assert settings.workspace_host_path == Path("/data/swegen-test/workspaces")
    assert settings.repo_cache_host_path == Path("/data/swegen-test/cache")
    assert settings.successful_tasks_host_path == Path("/data/swegen-test/successful")
    assert settings.k3s_nodes == ("192.0.2.10",)
    assert settings.build_worker_image_on_start is False
    assert settings.generate_workers == 3
    assert settings.validate_workers == 4
    assert settings.reward_workers == 2
    assert settings.push_workers == 1
    assert settings.rollout_timeout_seconds == 900


def test_pipeline_worker_counts_are_required_in_toml(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "swegen.toml"
    config.write_text(
        "[pipeline]\n"
        'namespace = "swegen-pipeline-tester"\n'
        'secret_source_namespace = "swegen-pipeline"\n'
        'worker_image = "swegen-worker:test"\n'
        'workspace_host_path = "/data/swegen-test/workspaces"\n'
        'repo_cache_host_path = "/data/swegen-test/cache"\n'
        'successful_tasks_host_path = "/data/swegen-test/successful"\n'
        'k3s_nodes = ["192.0.2.10"]\n'
        'k3s_ssh_user = "root"\n'
        'build_ca_path = "/etc/ssl/certs/ca-certificates.crt"\n'
        "build_worker_image_on_start = false\n"
        "autoqueue_workers = 1\n"
    )
    monkeypatch.setenv("SWEGEN_CONFIG", str(config))

    with pytest.raises(ValueError, match=r"\[pipeline].generate_workers"):
        load_pipeline_settings()


def test_minddistiller_credentials_come_directly_from_toml(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = tmp_path / "swegen.toml"
    config.write_text(
        "[swr.minddistiller]\n"
        'host = "mind.example"\n'
        'repository = "team/generated"\n'
        'registry = "trajectory"\n'
        'suffix = ""\n'
        'username = "configured-user"\n'
        'password = "configured-password"\n'
    )
    monkeypatch.setenv("SWEGEN_CONFIG", str(config))

    target = load_swr_target("minddistiller")

    assert target.username == "configured-user"
    assert target.password == "configured-password"
    assert "configured-password" not in repr(target)


def test_minddistiller_requires_username_and_password(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "swegen.toml"
    config.write_text(
        "[swr.minddistiller]\n"
        'host = "mind.example"\n'
        'repository = "team/generated"\n'
        'username = "configured-user"\n'
    )
    monkeypatch.setenv("SWEGEN_CONFIG", str(config))

    with pytest.raises(ValueError, match="must define username and password"):
        load_swr_target("minddistiller")
