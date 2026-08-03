from __future__ import annotations

import fcntl
import importlib.util
import os
from pathlib import Path

WRAPPER = Path(__file__).resolve().parents[1] / "deploy" / "k3s" / "docker-build-slot-wrapper.py"


def _load():
    spec = importlib.util.spec_from_file_location("docker_build_slot_wrapper", WRAPPER)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_default_slot_count_is_32() -> None:
    assert _load().DEFAULT_SLOTS == 32


def test_needs_slot_for_build_forms() -> None:
    m = _load()
    assert m.needs_build_slot(["build", "-t", "x", "."])
    assert m.needs_build_slot(["buildx", "build", "-t", "x", "."])
    assert m.needs_build_slot(["buildx", "bake", "-f", "file"])
    assert m.needs_build_slot(["compose", "build"])
    assert m.needs_build_slot(["compose", "-f", "a.yml", "-p", "proj", "build"])
    assert m.needs_build_slot(["compose", "--progress", "plain", "build", "svc"])


def test_no_slot_for_non_build_forms() -> None:
    m = _load()
    assert not m.needs_build_slot(["ps"])
    assert not m.needs_build_slot(["info"])
    assert not m.needs_build_slot(["compose", "up", "-d"])
    assert not m.needs_build_slot(["compose", "down"])
    assert not m.needs_build_slot(["compose", "run", "svc", "sh"])
    assert not m.needs_build_slot(["buildx", "ls"])
    assert not m.needs_build_slot(["pull", "alpine"])
    assert not m.needs_build_slot(["image", "ls"])


def test_nonblocking_probe_reports_full_slots(tmp_path: Path) -> None:
    m = _load()
    m.SLOT_DIR = tmp_path
    m.ensure_slot_files(1)
    held_fd = os.open(tmp_path / "0", os.O_RDWR)
    fcntl.flock(held_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert m.try_acquire_slot(1) is None
    finally:
        os.close(held_fd)


def test_gc_guard_blocks_new_nonblocking_builds(tmp_path: Path) -> None:
    m = _load()
    m.SLOT_DIR = tmp_path
    exclusive_fd = os.open(tmp_path / m.GC_LOCK_NAME, os.O_RDWR | os.O_CREAT, 0o644)
    fcntl.flock(exclusive_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert m.acquire_gc_guard(nonblocking=True) is None
    finally:
        os.close(exclusive_fd)


def test_gc_guard_is_shared_between_builds(tmp_path: Path) -> None:
    m = _load()
    m.SLOT_DIR = tmp_path
    first = m.acquire_gc_guard(nonblocking=True)
    second = m.acquire_gc_guard(nonblocking=True)
    try:
        assert first is not None
        assert second is not None
    finally:
        if first is not None:
            os.close(first)
        if second is not None:
            os.close(second)
