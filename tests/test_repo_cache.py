from __future__ import annotations

import base64
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from swegen.create.repo_cache import RepoCache


def test_git_environment_bridges_github_token_without_persisting_it(monkeypatch) -> None:
    token = "ghp_test_token"
    monkeypatch.setenv("GITHUB_TOKEN", token)

    environment = RepoCache._git_environment()

    assert environment["GIT_TERMINAL_PROMPT"] == "0"
    assert environment["GIT_CONFIG_COUNT"] == "1"
    assert environment["GIT_CONFIG_KEY_0"] == "http.https://github.com/.extraheader"
    assert environment["GIT_CONFIG_VALUE_0"] == (
        "Authorization: Basic " + base64.b64encode(f"x-access-token:{token}".encode()).decode()
    )


def test_clone_passes_github_auth_only_through_environment(monkeypatch, tmp_path: Path) -> None:
    token = "ghp_test_token"
    monkeypatch.setenv("GITHUB_TOKEN", token)
    cache = RepoCache(tmp_path / "cache")

    with (
        patch.object(cache, "_checkout"),
        patch("swegen.create.repo_cache.subprocess.run") as run,
    ):
        cache._clone(
            "https://github.com/example/repo.git",
            tmp_path / "repo",
            "deadbeef",
        )

    command = run.call_args.args[0]
    environment = run.call_args.kwargs["env"]
    assert token not in " ".join(command)
    assert token not in command[2]
    assert environment["GIT_CONFIG_VALUE_0"].startswith("Authorization: Basic ")


@pytest.mark.parametrize(
    ("method", "args", "command"),
    [
        ("_clone", ("https://github.com/example/repo.git", Path("repo"), "deadbeef"), "git clone"),
        ("_fetch_and_checkout", (Path("repo"), "deadbeef"), "git fetch --all"),
    ],
)
def test_git_failures_preserve_stderr(
    tmp_path: Path, method: str, args: tuple[object, ...], command: str
) -> None:
    cache = RepoCache(tmp_path / "cache")
    error = subprocess.CalledProcessError(
        128,
        ["git"],
        stderr=b"fatal: proxy connection refused\xff",
    )
    resolved_args = tuple(tmp_path / arg if isinstance(arg, Path) else arg for arg in args)

    with patch("swegen.create.repo_cache.subprocess.run", side_effect=error):
        with pytest.raises(RuntimeError) as raised:
            getattr(cache, method)(*resolved_args)

    message = str(raised.value)
    assert f"{command} failed with exit code 128" in message
    assert "Git stderr:" in message
    assert "fatal: proxy connection refused" in message
