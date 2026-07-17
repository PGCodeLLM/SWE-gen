from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from swegen.create.repo_cache import RepoCache


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
    resolved_args = tuple(
        tmp_path / arg if isinstance(arg, Path) else arg for arg in args
    )

    with patch("swegen.create.repo_cache.subprocess.run", side_effect=error):
        with pytest.raises(RuntimeError) as raised:
            getattr(cache, method)(*resolved_args)

    message = str(raised.value)
    assert f"{command} failed with exit code 128" in message
    assert "Git stderr:" in message
    assert "fatal: proxy connection refused" in message
