from __future__ import annotations

from swegen.model_settings import claude_session_env


def test_claude_session_env_replaces_both_internal_haiku_paths(monkeypatch) -> None:
    monkeypatch.delenv("SWEGEN_CLAUDE_FAST_MODEL", raising=False)
    monkeypatch.delenv("ANTHROPIC_SMALL_FAST_MODEL", raising=False)
    monkeypatch.delenv("ANTHROPIC_DEFAULT_HAIKU_MODEL", raising=False)

    env = claude_session_env("owner__repo-1")

    assert env["ANTHROPIC_SMALL_FAST_MODEL"] == "gpt-5.3-codex-spark"
    assert env["ANTHROPIC_DEFAULT_HAIKU_MODEL"] == "gpt-5.3-codex-spark"


def test_claude_session_env_honors_swegen_fast_model_override(monkeypatch) -> None:
    monkeypatch.setenv("SWEGEN_CLAUDE_FAST_MODEL", "custom-fast-model")

    env = claude_session_env("owner__repo-2")

    assert env["ANTHROPIC_SMALL_FAST_MODEL"] == "custom-fast-model"
    assert env["ANTHROPIC_DEFAULT_HAIKU_MODEL"] == "custom-fast-model"
