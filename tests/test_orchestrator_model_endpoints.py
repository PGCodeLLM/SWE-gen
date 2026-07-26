from __future__ import annotations

import json
import os
import queue
from pathlib import Path

import pytest

import orchestrator


def profile_values(label: str) -> dict[str, str]:
    return {
        "OPENAI_API_KEY": f"openai-secret-{label}",
        "ANTHROPIC_API_KEY": f"anthropic-secret-{label}",
        "ANTHROPIC_AUTH_TOKEN": f"anthropic-secret-{label}",
        "OPENAI_BASE_URL": f"https://{label}.example/v1",
        "ANTHROPIC_BASE_URL": f"https://{label}.example",
        "OPENAI_MODEL": f"opus-{label}",
        "ANTHROPIC_MODEL": f"opus-{label}",
        "ANTHROPIC_DEFAULT_OPUS_MODEL": f"opus-{label}",
        "ANTHROPIC_DEFAULT_SONNET_MODEL": f"sonnet-{label}",
    }


def write_profile(
    path: Path,
    label: str,
    *,
    values: dict[str, str] | None = None,
    extra_lines: tuple[str, ...] = (),
    mode: int = 0o600,
) -> Path:
    configured = values or profile_values(label)
    lines = [f"export {name}={value}" for name, value in configured.items()]
    lines.extend(extra_lines)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    path.chmod(mode)
    return path


def make_profiles() -> tuple[orchestrator.ModelProfile, orchestrator.ModelProfile]:
    return (
        orchestrator.ModelProfile("backend-000", profile_values("first")),
        orchestrator.ModelProfile("backend-001", profile_values("second")),
    )


def test_load_model_profiles_is_private_strict_and_stably_ordered(tmp_path, monkeypatch) -> None:
    profile_dir = tmp_path / "profiles"
    profile_dir.mkdir()
    write_profile(profile_dir / "backend-010.env", "ten")
    write_profile(profile_dir / "backend-002.env", "two")
    manifest = profile_dir / orchestrator.MODEL_PROFILE_MANIFEST
    manifest.write_text("backend-010.env\nbackend-002.env\n", encoding="utf-8")
    manifest.chmod(0o600)
    monkeypatch.setenv(orchestrator.MODEL_PROFILE_DIR_ENV, str(profile_dir))

    profiles = orchestrator.load_model_profiles()

    assert [profile.profile_id for profile in profiles] == ["backend-010", "backend-002"]
    assert profiles[0].env == profile_values("ten")
    assert "secret" not in repr(profiles[0])


def test_load_model_profiles_uses_filename_order_without_manifest(tmp_path) -> None:
    write_profile(tmp_path / "backend-010.env", "ten")
    write_profile(tmp_path / "backend-002.env", "two")

    profiles = orchestrator.load_model_profiles(tmp_path)

    assert [profile.profile_id for profile in profiles] == ["backend-002", "backend-010"]


def test_load_model_profiles_preserves_legacy_env_when_not_configured(monkeypatch) -> None:
    monkeypatch.delenv(orchestrator.MODEL_PROFILE_DIR_ENV, raising=False)

    assert orchestrator.load_model_profiles() == ()


@pytest.mark.parametrize(
    "case", ["missing-dir", "empty-dir", "insecure", "missing", "unknown", "invalid-url"]
)
def test_load_model_profiles_fails_closed_on_unsafe_configuration(
    tmp_path, monkeypatch, case
) -> None:
    profile_dir = tmp_path / case
    if case != "missing-dir":
        profile_dir.mkdir()
    if case == "insecure":
        write_profile(profile_dir / "backend-000.env", "bad", mode=0o644)
    elif case == "missing":
        values = profile_values("bad")
        del values["ANTHROPIC_BASE_URL"]
        write_profile(profile_dir / "backend-000.env", "bad", values=values)
    elif case == "unknown":
        write_profile(
            profile_dir / "backend-000.env",
            "bad",
            extra_lines=("export PATH=/untrusted",),
        )
    elif case == "invalid-url":
        values = profile_values("bad")
        values["OPENAI_BASE_URL"] = "https://bad.example:not-a-port/v1"
        write_profile(profile_dir / "backend-000.env", "bad", values=values)
    monkeypatch.setenv(orchestrator.MODEL_PROFILE_DIR_ENV, str(profile_dir))

    with pytest.raises(orchestrator.ModelProfileError) as caught:
        orchestrator.load_model_profiles()

    assert "secret" not in str(caught.value)


def test_load_model_profiles_rejects_malformed_or_symlinked_files(tmp_path) -> None:
    malformed_dir = tmp_path / "malformed"
    malformed_dir.mkdir()
    write_profile(
        malformed_dir / "backend-000.env",
        "bad",
        extra_lines=('export BROKEN="unterminated',),
    )
    with pytest.raises(orchestrator.ModelProfileError, match="malformed syntax"):
        orchestrator.load_model_profiles(malformed_dir)

    target = write_profile(tmp_path / "target.env", "target")
    symlink_dir = tmp_path / "symlink"
    symlink_dir.mkdir()
    (symlink_dir / "backend-000.env").symlink_to(target)
    with pytest.raises(orchestrator.ModelProfileError, match="regular file"):
        orchestrator.load_model_profiles(symlink_dir)


def test_profile_selection_and_application_are_stable_and_isolated() -> None:
    profiles = make_profiles()
    entry = orchestrator.Entry("Owner/Repo", "42")

    selected = orchestrator.select_model_profile(entry, profiles)
    assert selected is orchestrator.select_model_profile(entry, profiles)
    assert (
        orchestrator.select_model_profile(entry, profiles, 1)
        is profiles[(profiles.index(selected) + 1) % len(profiles)]
    )

    base_env = {
        **dict.fromkeys(orchestrator.MODEL_PROFILE_ENV_VARS, "stale"),
        orchestrator.MODEL_PROFILE_DIR_ENV: "/private/profiles",
        orchestrator.MODEL_PROFILE_ID_ENV: "stale-profile",
        "CLAUDE_CODE_OAUTH_TOKEN": "stale-oauth",
        **dict.fromkeys(orchestrator.MODEL_PROFILE_FAST_MODEL_VARS, "stale-fast-model"),
        "UNRELATED": "preserved",
    }
    original = dict(base_env)
    child_env = orchestrator.build_model_profile_env(base_env, selected)
    legacy_env = orchestrator.build_model_profile_env(base_env, None)

    assert base_env == original
    assert child_env is not base_env
    assert legacy_env == base_env
    assert legacy_env is not base_env
    assert {name: child_env[name] for name in orchestrator.MODEL_PROFILE_ENV_VARS} == selected.env
    assert child_env[orchestrator.MODEL_PROFILE_ID_ENV] == selected.profile_id
    assert orchestrator.MODEL_PROFILE_DIR_ENV not in child_env
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in child_env
    assert {
        child_env[name] for name in orchestrator.MODEL_PROFILE_FAST_MODEL_VARS
    } == {selected.env["ANTHROPIC_DEFAULT_SONNET_MODEL"]}
    assert child_env["UNRELATED"] == "preserved"


def test_process_entry_pins_then_rotates_profiles_without_mutating_shared_env(
    tmp_path, monkeypatch
) -> None:
    profiles = make_profiles()
    entry = orchestrator.Entry("owner/repo", "7")
    initial = orchestrator.select_model_profile(entry, profiles)
    failover = orchestrator.select_model_profile(entry, profiles, 1)
    base_env = {
        **dict.fromkeys(orchestrator.MODEL_PROFILE_ENV_VARS, "stale"),
        orchestrator.MODEL_PROFILE_DIR_ENV: "/private/profiles",
        "CLAUDE_CODE_OAUTH_TOKEN": "stale-oauth",
        "UNRELATED": "preserved",
    }
    original_env = dict(base_env)
    process_environment = dict(os.environ)
    attempts: list[dict[str, str]] = []

    def fake_run(_cmd, env, log):
        attempts.append(env)
        if len(attempts) == 1:
            log.write("Connection reset by peer\n")
            log.flush()
            return 1
        return 0

    monkeypatch.setattr(orchestrator, "run_command_to_log", fake_run)
    monkeypatch.setattr(orchestrator.time, "sleep", lambda _seconds: None)
    log_path = tmp_path / "worker.log"
    with log_path.open("w", encoding="utf-8") as raw_log:
        result = orchestrator.process_entry(
            0,
            entry,
            "[task]",
            base_env,
            "swegen",
            orchestrator.TimestampedLog(raw_log),
            log_path,
            None,
            None,
            tmp_path,
            None,
            2,
            [],
            False,
            profiles,
        )

    assert len(attempts) == 2
    assert attempts[0] is not attempts[1]
    assert attempts[0][orchestrator.MODEL_PROFILE_ID_ENV] == initial.profile_id
    assert attempts[1][orchestrator.MODEL_PROFILE_ID_ENV] == failover.profile_id
    assert {name: attempts[0][name] for name in orchestrator.MODEL_PROFILE_ENV_VARS} == initial.env
    assert {name: attempts[1][name] for name in orchestrator.MODEL_PROFILE_ENV_VARS} == failover.env
    assert all(
        {
            env[name]
            for name in orchestrator.MODEL_PROFILE_FAST_MODEL_VARS
        }
        == {profile.env["ANTHROPIC_DEFAULT_SONNET_MODEL"]}
        for env, profile in zip(attempts, (initial, failover), strict=True)
    )
    assert all(orchestrator.MODEL_PROFILE_DIR_ENV not in env for env in attempts)
    assert all("CLAUDE_CODE_OAUTH_TOKEN" not in env for env in attempts)
    assert all(env["UNRELATED"] == "preserved" for env in attempts)
    assert result.model_profile_id == failover.profile_id
    assert base_env == original_env
    assert os.environ == process_environment
    log_contents = log_path.read_text(encoding="utf-8")
    assert initial.profile_id in log_contents
    assert failover.profile_id in log_contents
    assert all(secret not in log_contents for secret in ("openai-secret", "anthropic-secret"))


def test_process_entry_keeps_profile_for_github_only_retry(tmp_path, monkeypatch) -> None:
    profiles = make_profiles()
    entry = orchestrator.Entry("owner/repo", "8")
    attempts: list[dict[str, str]] = []

    def fake_run(_cmd, env, log):
        attempts.append(env)
        if len(attempts) == 1:
            log.write("API rate limit exceeded\n")
            log.flush()
            return 1
        return 0

    monkeypatch.setattr(orchestrator, "run_command_to_log", fake_run)
    monkeypatch.setattr(orchestrator.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        orchestrator,
        "pick_github_token",
        lambda pool, excluded: next(token for token in pool if token not in excluded),
    )
    log_path = tmp_path / "worker.log"
    with log_path.open("w", encoding="utf-8") as raw_log:
        result = orchestrator.process_entry(
            0,
            entry,
            "[task]",
            {},
            "swegen",
            orchestrator.TimestampedLog(raw_log),
            log_path,
            None,
            None,
            tmp_path,
            None,
            2,
            ["github-one", "github-two"],
            False,
            profiles,
        )

    assert len(attempts) == 2
    assert (
        attempts[0][orchestrator.MODEL_PROFILE_ID_ENV]
        == attempts[1][orchestrator.MODEL_PROFILE_ID_ENV]
    )
    assert attempts[0]["GITHUB_TOKEN"] != attempts[1]["GITHUB_TOKEN"]
    assert result.model_profile_id == attempts[1][orchestrator.MODEL_PROFILE_ID_ENV]


def test_run_consumer_propagates_profile_id_to_outcome(tmp_path, monkeypatch) -> None:
    entry = orchestrator.Entry("owner/repo", "10")
    work_queue: queue.Queue[list[orchestrator.Entry] | None] = queue.Queue()
    work_queue.put([entry])
    work_queue.put(None)
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    monkeypatch.setattr(
        orchestrator,
        "process_entry",
        lambda *_args, **_kwargs: orchestrator.ProcessResult(
            returncode=0, model_profile_id="backend-001"
        ),
    )

    outcomes = orchestrator.run_consumer(
        0,
        work_queue,
        {},
        "swegen",
        log_dir,
        model_profiles=make_profiles(),
    )

    assert len(outcomes) == 1
    assert outcomes[0].model_profile_id == "backend-001"


def test_progress_records_only_safe_profile_id(tmp_path) -> None:
    progress_queue: queue.Queue[orchestrator.Outcome | None] = queue.Queue()
    progress_path = tmp_path / "progress.jsonl"
    status_path = tmp_path / "status.jsonl"
    progress_queue.put(
        orchestrator.Outcome(
            worker_id=3,
            entry=orchestrator.Entry("owner/repo", "9"),
            returncode=0,
            model_profile_id="backend-001",
        )
    )
    progress_queue.put(None)

    orchestrator.write_progress_jsonl(progress_queue, progress_path, status_path)

    progress = json.loads(progress_path.read_text(encoding="utf-8"))
    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert progress["model_profile_id"] == "backend-001"
    assert status["model_profile_id"] == "backend-001"
    assert "API_KEY" not in json.dumps((progress, status))
