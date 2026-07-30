from __future__ import annotations

import importlib.util
from pathlib import Path

WRAPPER = Path(__file__).resolve().parents[1] / "deploy" / "k3s" / "docker-build-slot-wrapper.py"


def _load():
    spec = importlib.util.spec_from_file_location("docker_build_slot_wrapper", WRAPPER)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_default_slot_count_is_48() -> None:
    assert _load().DEFAULT_SLOTS == 48


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
