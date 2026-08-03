#!/usr/bin/env python3
"""Node-local Docker CLI wrapper: admit concurrent build-producing commands.

Only build forms take a slot (shared across all pods on the host via flock):
  docker build ...
  docker buildx build|bake ...
  docker compose ... build ...

Non-build commands pass through immediately. The slot FD is held for the
lifetime of the real docker child so crash/exit releases the lock.
"""

from __future__ import annotations

import fcntl
import os
import sys
import time
from pathlib import Path

REAL_DOCKER = os.environ.get("SWEGEN_REAL_DOCKER", "/usr/bin/docker")
SLOT_DIR = Path(os.environ.get("SWEGEN_BUILD_SLOT_DIR", "/run/swegen-build-slots"))
GC_LOCK_NAME = os.environ.get("SWEGEN_BUILD_GC_LOCK_NAME", "gc.lock")
DEFAULT_SLOTS = int(os.environ.get("SWEGEN_BUILD_SLOTS", "32"))
POLL_SECONDS = float(os.environ.get("SWEGEN_BUILD_SLOT_POLL_SECONDS", "0.25"))
LOG = os.environ.get("SWEGEN_BUILD_SLOT_LOG", "")
SLOTS_FULL_EXIT_CODE = 75
SLOTS_FULL_MARKER = "SWEGEN_BUILD_SLOTS_FULL"


def _env_bool(name: str, *, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _log(msg: str) -> None:
    if not LOG:
        return
    try:
        with open(LOG, "a", encoding="utf-8") as fh:
            fh.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} pid={os.getpid()} {msg}\n")
    except OSError:
        pass


def needs_build_slot(argv: list[str]) -> bool:
    """Return True when argv (docker args, no binary) produces a BuildKit solve."""

    if not argv:
        return False
    if argv[0] == "build":
        return True
    if argv[0] == "buildx":
        # docker buildx build|bake ...
        return len(argv) >= 2 and argv[1] in {"build", "bake"}
    if argv[0] in {"compose", "compose-plugin"}:
        # docker compose [global options] build ...
        # skip compose global flags that take a value
        value_flags = {
            "-f",
            "--file",
            "-p",
            "--project-name",
            "--profile",
            "--project-directory",
            "--env-file",
            "--ansi",
            "--parallel",
            "--progress",
            "-t",
            "--timeout",
        }
        i = 1
        while i < len(argv):
            arg = argv[i]
            if arg == "build":
                return True
            if arg in value_flags:
                i += 2
                continue
            if arg.startswith("--") and "=" in arg:
                # e.g. --progress=plain
                i += 1
                continue
            if arg.startswith("-"):
                i += 1
                continue
            # first positional subcommand
            return arg == "build"
        return False
    return False


def slot_count() -> int:
    count_path = SLOT_DIR / "count"
    try:
        raw = count_path.read_text(encoding="utf-8").strip()
        n = int(raw)
        if n > 0:
            return n
    except (OSError, ValueError):
        pass
    return max(DEFAULT_SLOTS, 1)


def ensure_slot_files(n: int) -> None:
    SLOT_DIR.mkdir(parents=True, exist_ok=True)
    for i in range(n):
        path = SLOT_DIR / str(i)
        if not path.exists():
            path.touch()


def try_acquire_slot(n: int) -> tuple[int, int] | None:
    """Acquire one slot without waiting, returning ``None`` when all are busy."""

    ensure_slot_files(n)
    for i in range(n):
        path = SLOT_DIR / str(i)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            continue
        return i, fd
    return None


def acquire_gc_guard(*, nonblocking: bool) -> int | None:
    """Hold a shared guard so BuildKit GC cannot overlap this build."""

    SLOT_DIR.mkdir(parents=True, exist_ok=True)
    fd = os.open(SLOT_DIR / GC_LOCK_NAME, os.O_RDWR | os.O_CREAT, 0o644)
    operation = fcntl.LOCK_SH | (fcntl.LOCK_NB if nonblocking else 0)
    try:
        fcntl.flock(fd, operation)
    except BlockingIOError:
        os.close(fd)
        return None
    return fd


def keep_across_exec(fd: int) -> None:
    """Clear close-on-exec so an admission lock lives with the Docker child."""

    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    fcntl.fcntl(fd, fcntl.F_SETFD, flags & ~fcntl.FD_CLOEXEC)


def acquire_slot(n: int) -> tuple[int, int]:
    """Block until a slot is held; return (slot_index, fd)."""

    started = time.monotonic()
    logged_wait = False
    while True:
        acquired = try_acquire_slot(n)
        if acquired is not None:
            slot, fd = acquired
            waited = time.monotonic() - started
            _log(f"acquired slot={slot} waited_s={waited:.3f} n={n}")
            return slot, fd
        if not logged_wait and time.monotonic() - started > 1.0:
            _log(f"waiting for build slot n={n}")
            logged_wait = True
        time.sleep(POLL_SECONDS)


def main(argv: list[str]) -> int:
    docker_argv = argv[1:]
    if not needs_build_slot(docker_argv):
        os.execv(REAL_DOCKER, [REAL_DOCKER, *docker_argv])

    n = slot_count()
    nonblocking = _env_bool("SWEGEN_BUILD_SLOT_NONBLOCKING")
    if nonblocking:
        acquired = try_acquire_slot(n)
        if acquired is None:
            print(f"{SLOTS_FULL_MARKER} slots={n}", file=sys.stderr)
            return SLOTS_FULL_EXIT_CODE
        _slot, fd = acquired
    else:
        _slot, fd = acquire_slot(n)
    gc_fd = acquire_gc_guard(nonblocking=nonblocking)
    if gc_fd is None:
        os.close(fd)
        print(f"{SLOTS_FULL_MARKER} buildkit_gc_active=1 slots={n}", file=sys.stderr)
        return SLOTS_FULL_EXIT_CODE

    # Keep both locks open across exec so they live for the Docker process.
    keep_across_exec(fd)
    keep_across_exec(gc_fd)
    os.execv(REAL_DOCKER, [REAL_DOCKER, *docker_argv])
    return 127


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv))
    except OSError as exc:
        print(f"swegen-docker-slot-wrapper: {exc}", file=sys.stderr)
        raise SystemExit(126) from exc
