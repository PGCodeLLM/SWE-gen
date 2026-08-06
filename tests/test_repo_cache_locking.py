from __future__ import annotations

import os
import threading
import time
from pathlib import Path

import pytest

from swegen.create.repo_cache import RepoCache


def _make_git_repo(repo_path: Path) -> None:
    """Create a bare-minimum on-disk repo with a .git dir so it looks cached."""
    (repo_path / ".git").mkdir(parents=True, exist_ok=True)


def test_same_repo_concurrent_calls_are_serialized(tmp_path: Path) -> None:
    """Two concurrent get_or_clone for the SAME repo must not run git at once."""
    cache = RepoCache(tmp_path / "cache")
    repo_path = cache.cache_dir / "example" / "repo"
    _make_git_repo(repo_path)

    active = 0
    max_concurrent = 0
    overlap_detected = False
    lock = threading.Lock()

    def fake_fetch_and_checkout(path: Path, sha: str) -> None:
        nonlocal active, max_concurrent, overlap_detected
        with lock:
            active += 1
            max_concurrent = max(max_concurrent, active)
            if active > 1:
                overlap_detected = True
        # Hold the critical section long enough for a real overlap to show.
        time.sleep(0.2)
        with lock:
            active -= 1

    cache._fetch_and_checkout = fake_fetch_and_checkout  # type: ignore[assignment]

    barrier = threading.Barrier(2)

    def worker(sha: str) -> None:
        barrier.wait()
        cache.get_or_clone("example/repo", sha)

    threads = [
        threading.Thread(target=worker, args=("a" * 40,)),
        threading.Thread(target=worker, args=("b" * 40,)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not overlap_detected
    assert max_concurrent == 1


def test_lock_files_are_per_repo(tmp_path: Path) -> None:
    """Different repos get different lock files and do not block each other."""
    cache = RepoCache(tmp_path / "cache")

    lock_a = cache._lock_path("example", "repo-a")
    lock_b = cache._lock_path("example", "repo-b")
    assert lock_a != lock_b

    for repo in ("example/repo-a", "example/repo-b"):
        owner, name = repo.split("/")
        _make_git_repo(cache.cache_dir / owner / name)

    # If the locks were shared, the second call would block behind the first's
    # held lock. Acquire repo-a's lock and confirm repo-b still proceeds.
    started_b = threading.Event()

    def fetch_a(path: Path, sha: str) -> None:
        started_b.wait(timeout=2.0)

    def fetch_b(path: Path, sha: str) -> None:
        started_b.set()

    def dispatch(path: Path, sha: str) -> None:
        if "repo-a" in str(path):
            fetch_a(path, sha)
        else:
            fetch_b(path, sha)

    cache._fetch_and_checkout = dispatch  # type: ignore[assignment]

    ta = threading.Thread(target=cache.get_or_clone, args=("example/repo-a", "a" * 40))
    tb = threading.Thread(target=cache.get_or_clone, args=("example/repo-b", "b" * 40))
    ta.start()
    tb.start()
    ta.join(timeout=5.0)
    tb.join(timeout=5.0)

    assert not ta.is_alive()
    assert not tb.is_alive()
    assert started_b.is_set()


def test_stale_git_index_lock_is_removed(tmp_path: Path) -> None:
    """A stale .git/index.lock older than threshold is cleared before a git op."""
    cache = RepoCache(tmp_path / "cache")
    repo_path = cache.cache_dir / "example" / "repo"
    _make_git_repo(repo_path)

    lock_file = repo_path / ".git" / "index.lock"
    lock_file.write_text("stale")

    # Age it well beyond the default 300s threshold.
    old = time.time() - 10_000
    os.utime(lock_file, (old, old))

    cache._clear_stale_git_locks(repo_path)

    assert not lock_file.exists()


def test_fresh_git_index_lock_is_preserved(tmp_path: Path) -> None:
    """A fresh .git/index.lock (peer mid-operation) must NOT be removed."""
    cache = RepoCache(tmp_path / "cache")
    repo_path = cache.cache_dir / "example" / "repo"
    _make_git_repo(repo_path)

    lock_file = repo_path / ".git" / "index.lock"
    lock_file.write_text("fresh")

    cache._clear_stale_git_locks(repo_path)

    assert lock_file.exists()


def test_lock_acquisition_timeout_raises_clear_error(tmp_path: Path, monkeypatch) -> None:
    """Exceeding the acquisition timeout raises a clear RuntimeError."""
    monkeypatch.setenv("SWEGEN_REPO_CACHE_LOCK_TIMEOUT_SECONDS", "0.3")
    cache = RepoCache(tmp_path / "cache")
    repo_path = cache.cache_dir / "example" / "repo"
    _make_git_repo(repo_path)

    holder_acquired = threading.Event()
    release_holder = threading.Event()

    def hold_lock() -> None:
        with cache._repo_lock("example", "repo"):
            holder_acquired.set()
            release_holder.wait(timeout=5.0)

    holder = threading.Thread(target=hold_lock)
    holder.start()
    assert holder_acquired.wait(timeout=2.0)

    try:
        with pytest.raises(RuntimeError) as raised:
            cache.get_or_clone("example/repo", "a" * 40)
    finally:
        release_holder.set()
        holder.join(timeout=5.0)

    message = str(raised.value)
    assert "Timed out" in message
    assert "example/repo" in message


def test_first_caller_clones_under_the_lock(tmp_path: Path) -> None:
    """The initial-clone path still works while holding the per-repo lock."""
    cache = RepoCache(tmp_path / "cache")

    clone_calls: list[tuple[str, Path, str]] = []

    def fake_clone(repo_url: str, repo_path: Path, head_sha: str) -> None:
        # The repo does not exist yet: this is the initial-clone branch.
        assert not (repo_path / ".git").exists()
        clone_calls.append((repo_url, repo_path, head_sha))

    cache._clone = fake_clone  # type: ignore[assignment]

    result = cache.get_or_clone("example/repo", "deadbeef")

    assert result == cache.cache_dir / "example" / "repo"
    assert len(clone_calls) == 1
    assert clone_calls[0][0] == "https://github.com/example/repo.git"
    assert clone_calls[0][2] == "deadbeef"
