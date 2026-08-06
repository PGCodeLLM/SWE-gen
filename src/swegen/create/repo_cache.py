from __future__ import annotations

import base64
import contextlib
import fcntl
import logging
import os
import subprocess
import time
from pathlib import Path


def _env_float(name: str, default: float) -> float:
    """Read a float from the environment, falling back to ``default``."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


# Maximum time to wait to acquire the per-repo lock before giving up. A crashed
# lock holder must not deadlock a worker forever, so we bound the wait.
DEFAULT_LOCK_TIMEOUT_SECONDS = 600.0

# Age above which a leftover ``.git/*.lock`` is treated as stale (left by a
# crashed/killed pod) and removed before running a git mutation.
DEFAULT_STALE_LOCK_SECONDS = 300.0


class RepoCache:
    """Manages local clones of repositories for CC analysis."""

    def __init__(self, cache_dir: Path | None = None):
        """
        Initialize the repo cache.

        Args:
            cache_dir: Directory to store clones. Defaults to .cache/repos
        """
        self.cache_dir = cache_dir or Path(".cache/repos")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.locks_dir = self.cache_dir / "locks"
        self.locks_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger("swegen")

    @property
    def lock_timeout_seconds(self) -> float:
        """Bounded wait for acquiring the per-repo lock (env-overridable)."""
        return _env_float(
            "SWEGEN_REPO_CACHE_LOCK_TIMEOUT_SECONDS", DEFAULT_LOCK_TIMEOUT_SECONDS
        )

    @property
    def stale_lock_seconds(self) -> float:
        """Age above which a leftover git ``*.lock`` is removed (env-overridable)."""
        return _env_float(
            "SWEGEN_REPO_CACHE_STALE_LOCK_SECONDS", DEFAULT_STALE_LOCK_SECONDS
        )

    def _lock_path(self, owner: str, name: str) -> Path:
        """Path to the swegen-owned per-repo lock file.

        This is a SEPARATE file from git's own ``.git/index.lock``: it serializes
        the whole clone/fetch/checkout sequence across pods on a shared node.
        """
        return self.locks_dir / f"{owner}__{name}.lock"

    @contextlib.contextmanager
    def _repo_lock(self, owner: str, name: str):
        """Serialize per-repo git operations with an inter-process ``flock``.

        The FIRST pod acquires the lock, sees the repo absent, and clones it;
        subsequent pods block here, then find the repo present and just
        fetch+checkout their SHA. Because different pods need different commits
        of the same on-disk clone, serializing is the correct minimal fix.
        """
        self.locks_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self._lock_path(owner, name)
        timeout = self.lock_timeout_seconds
        deadline = time.monotonic() + timeout

        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            backoff = 0.05
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(
                            f"Timed out after {timeout:.0f}s waiting for repo cache "
                            f"lock {lock_path} ({owner}/{name}). Another pod may be "
                            "holding it, or a previous holder crashed without "
                            "releasing it."
                        ) from None
                    remaining = deadline - time.monotonic()
                    time.sleep(min(backoff, max(0.0, remaining)))
                    backoff = min(backoff * 2, 5.0)
            self.logger.debug("Acquired repo cache lock: %s", lock_path)
            yield
        finally:
            with contextlib.suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _clear_stale_git_locks(self, repo_path: Path) -> None:
        """Remove clearly-stale git ``*.lock`` files left by a crashed pod.

        We cannot check cross-pod whether a git process is actually running, so
        we gate strictly on mtime: only locks older than ``stale_lock_seconds``
        are removed. Fresh locks (a peer mid-operation) are left untouched.
        """
        git_dir = repo_path / ".git"
        if not git_dir.is_dir():
            return
        threshold = self.stale_lock_seconds
        now = time.time()
        for lock_file in git_dir.glob("*.lock"):
            try:
                age = now - lock_file.stat().st_mtime
            except OSError:
                continue
            if age <= threshold:
                continue
            try:
                lock_file.unlink()
            except OSError as exc:
                self.logger.warning(
                    "Failed to remove stale git lock %s: %s", lock_file, exc
                )
                continue
            self.logger.warning(
                "Removed stale git lock %s (age %.0fs > %.0fs threshold)",
                lock_file,
                age,
                threshold,
            )

    def get_or_clone(
        self,
        repo: str,
        head_sha: str,
        repo_url: str | None = None,
    ) -> Path:
        """
        Get cached repo or clone it. Checkout the specified commit.

        Args:
            repo: Repository in "owner/repo" format
            head_sha: Commit SHA to checkout
            repo_url: Optional clone URL (defaults to https://github.com/{repo}.git)

        Returns:
            Path to the repository root
        """
        owner, name = self._parse_repo(repo)
        repo_path = self.cache_dir / owner / name

        if repo_url is None:
            repo_url = f"https://github.com/{repo}.git"

        # Serialize the whole clone/fetch/checkout sequence per repo so that
        # concurrent pods on a shared node do not collide on git's index.lock.
        with self._repo_lock(owner, name):
            if repo_path.exists() and (repo_path / ".git").exists():
                self.logger.debug("Using cached repo: %s", repo_path)
                self._clear_stale_git_locks(repo_path)
                self._fetch_and_checkout(repo_path, head_sha)
            else:
                self.logger.info("Cloning repo to cache: %s -> %s", repo, repo_path)
                self._clone(repo_url, repo_path, head_sha)

        return repo_path

    def _parse_repo(self, repo: str) -> tuple[str, str]:
        """Parse 'owner/repo' into (owner, repo) tuple."""
        # Handle full URLs
        if repo.startswith("https://"):
            repo = repo.replace("https://github.com/", "").rstrip(".git")
        if repo.startswith("git@"):
            repo = repo.replace("git@github.com:", "").rstrip(".git")

        parts = repo.split("/")
        if len(parts) != 2:
            raise ValueError(f"Invalid repo format: {repo}. Expected 'owner/repo'")
        return parts[0], parts[1]

    @staticmethod
    def _decode_subprocess_output(output: bytes | str | None) -> str:
        """Return captured subprocess output without hiding decoding failures."""
        if output is None:
            return ""
        if isinstance(output, bytes):
            return output.decode(errors="replace").strip()
        return output.strip()

    def _raise_git_command_error(self, command: str, error: subprocess.CalledProcessError) -> None:
        """Log and raise a Git failure while preserving its diagnostic stderr."""
        stderr = self._decode_subprocess_output(error.stderr)
        message = f"{command} failed with exit code {error.returncode}"
        if stderr:
            message += f"\nGit stderr:\n{stderr}"
        else:
            message += "\nGit produced no stderr."
        self.logger.error("%s", message)
        raise RuntimeError(message) from error

    @staticmethod
    def _git_environment() -> dict[str, str]:
        """Return a non-interactive Git environment with GitHub token auth.

        Git does not consume ``GITHUB_TOKEN`` by itself.  Supply the token as
        an in-memory Git config header so clone/fetch/submodule commands can
        authenticate without putting the credential in a URL, command line,
        repository config, or credential store.
        """

        environment = dict(os.environ)
        environment["GIT_TERMINAL_PROMPT"] = "0"
        token = environment.get("GITHUB_TOKEN", "").strip()
        if not token:
            return environment

        try:
            config_count = int(environment.get("GIT_CONFIG_COUNT", "0"))
        except ValueError:
            config_count = 0
        if config_count < 0:
            config_count = 0
        credential = base64.b64encode(f"x-access-token:{token}".encode()).decode()
        environment[f"GIT_CONFIG_KEY_{config_count}"] = "http.https://github.com/.extraheader"
        environment[f"GIT_CONFIG_VALUE_{config_count}"] = f"Authorization: Basic {credential}"
        environment["GIT_CONFIG_COUNT"] = str(config_count + 1)
        return environment

    def _clone(self, repo_url: str, repo_path: Path, head_sha: str) -> None:
        """Clone a repository and checkout the specified commit."""
        repo_path.parent.mkdir(parents=True, exist_ok=True)

        # Full clone for maximum CC context
        self.logger.debug("Cloning %s...", repo_url)
        try:
            subprocess.run(
                ["git", "clone", repo_url, str(repo_path)],
                check=True,
                capture_output=True,
                env=self._git_environment(),
            )
        except subprocess.CalledProcessError as error:
            self._raise_git_command_error("git clone", error)

        # Checkout the target commit
        self._checkout(repo_path, head_sha)

    def _fetch_and_checkout(self, repo_path: Path, head_sha: str) -> None:
        """Fetch latest and checkout the specified commit."""
        self.logger.debug("Fetching updates for %s...", repo_path)

        # Fetch all refs
        try:
            subprocess.run(
                ["git", "fetch", "--all"],
                cwd=str(repo_path),
                check=True,
                capture_output=True,
                env=self._git_environment(),
            )
        except subprocess.CalledProcessError as error:
            self._raise_git_command_error("git fetch --all", error)

        # Try to checkout the commit
        self._checkout(repo_path, head_sha)

    def _clean_repo(self, repo_path: Path) -> None:
        """Thoroughly clean the repository, including submodules."""
        # Deinit all submodules to remove their contents
        subprocess.run(
            ["git", "submodule", "deinit", "--all", "-f"],
            cwd=str(repo_path),
            capture_output=True,  # Don't check - might fail if no submodules
            env=self._git_environment(),
        )
        # Reset any tracked changes
        subprocess.run(
            ["git", "reset", "--hard"],
            cwd=str(repo_path),
            check=True,
            capture_output=True,
            env=self._git_environment(),
        )
        # Clean untracked files, including nested git repos (-ff) and ignored files (-x)
        subprocess.run(
            ["git", "clean", "-ffdx"],
            cwd=str(repo_path),
            check=True,
            capture_output=True,
            env=self._git_environment(),
        )

    def _checkout(self, repo_path: Path, sha: str) -> None:
        """Checkout a specific commit, fetching if needed."""
        try:
            # First, thoroughly clean the repo
            self._clean_repo(repo_path)

            # Try direct checkout
            subprocess.run(
                ["git", "checkout", sha],
                cwd=str(repo_path),
                check=True,
                capture_output=True,
                env=self._git_environment(),
            )
            self.logger.debug("Checked out %s", sha[:8])
        except subprocess.CalledProcessError as e:
            # Commit not available, fetch it specifically
            self.logger.debug(
                "Commit %s not found, fetching... (stderr: %s)",
                sha[:8],
                e.stderr.decode() if e.stderr else "",
            )
            try:
                subprocess.run(
                    ["git", "fetch", "origin", sha],
                    cwd=str(repo_path),
                    check=True,
                    capture_output=True,
                    env=self._git_environment(),
                )
                # Clean again before checkout to ensure no untracked files
                self._clean_repo(repo_path)
                subprocess.run(
                    ["git", "checkout", sha],
                    cwd=str(repo_path),
                    check=True,
                    capture_output=True,
                    env=self._git_environment(),
                )
                self.logger.debug("Fetched and checked out %s", sha[:8])
            except subprocess.CalledProcessError as fetch_err:
                # Provide more context in the error
                stderr = fetch_err.stderr.decode() if fetch_err.stderr else ""
                self.logger.error("Failed to checkout %s: %s", sha[:8], stderr)
                raise RuntimeError(
                    f"Cannot checkout commit {sha[:8]}. It may have been force-pushed or deleted. Error: {stderr}"
                ) from fetch_err

        # Update submodules if any
        try:
            subprocess.run(
                ["git", "submodule", "update", "--init", "--recursive"],
                cwd=str(repo_path),
                check=True,
                capture_output=True,
                timeout=120,
                env=self._git_environment(),
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            self.logger.debug("Submodule update skipped or failed (non-fatal)")
