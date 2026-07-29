from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import push_all_verified as push


def test_build_image_merges_operator_no_proxy_with_required_suffixes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    environment = tmp_path / "environment"
    environment.mkdir()
    (environment / "Dockerfile").write_text("FROM scratch\n")
    calls: list[tuple[list[str], float, int]] = []

    def fake_run(command, **kwargs):
        calls.append((list(command), kwargs["timeout_seconds"], kwargs["max_output_bytes"]))
        return SimpleNamespace(returncode=0, tail="", output_truncated=False)

    monkeypatch.setattr(push, "run_bounded_command", fake_run, raising=False)
    monkeypatch.setattr(push, "image_exists_locally", lambda tag: True)

    tag = push.build_image_direct(
        "owner__repo-1",
        tmp_path,
        proxy_env={
            "HTTPS_PROXY": "http://proxy.example:8080",
            "NO_PROXY": "localhost,.operator.internal",
            "no_proxy": "127.0.0.1,.lower.internal",
        },
    )

    assert tag == push.local_image_tag("owner__repo-1")
    command, timeout_seconds, max_output_bytes = calls[0]
    assert timeout_seconds == 3600
    assert max_output_bytes == push.MAX_DOCKER_OUTPUT_BYTES
    build_args = [
        command[index + 1] for index, value in enumerate(command) if value == "--build-arg"
    ]
    upper = next(value for value in build_args if value.startswith("NO_PROXY="))
    lower = next(value for value in build_args if value.startswith("no_proxy="))
    for value in (upper, lower):
        assert ".myhuaweicloud.com" in value
        assert ".huaweicloud.com" in value
        assert "localhost" in value
        assert "127.0.0.1" in value
    assert ".operator.internal" in upper
    assert ".lower.internal" in lower


def test_build_image_logs_only_bounded_failure_tail(tmp_path: Path, monkeypatch) -> None:
    environment = tmp_path / "environment"
    environment.mkdir()
    (environment / "Dockerfile").write_text("FROM scratch\n")
    tail = "x" * 200
    logged: list[str] = []

    monkeypatch.setattr(
        push,
        "run_bounded_command",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1,
            tail=tail,
            output_truncated=True,
        ),
        raising=False,
    )

    assert push.build_image_direct("owner__repo-1", tmp_path, log=logged.append) is None
    assert tail in logged[-1]
    assert "truncated" in logged[-1]


def test_push_uses_bounded_commands_and_cleans_remote_alias(monkeypatch) -> None:
    calls: list[tuple[list[str], float, int]] = []
    removed: list[tuple[str, bool]] = []

    def fake_run(command, **kwargs):
        calls.append((list(command), kwargs["timeout_seconds"], kwargs["max_output_bytes"]))
        return SimpleNamespace(returncode=0, tail="", output_truncated=False)

    monkeypatch.setattr(push, "run_bounded_command", fake_run, raising=False)
    monkeypatch.setattr(
        push,
        "_safe_rmi",
        lambda tag, force=True, timeout=120: removed.append((tag, force)),
    )

    assert push.push_to_registry("source:latest", "registry.example/task:1") is True

    assert calls == [
        (
            ["docker", "tag", "source:latest", "registry.example/task:1"],
            60,
            push.MAX_DOCKER_OUTPUT_BYTES,
        ),
        (["docker", "push", "registry.example/task:1"], 1800, push.MAX_DOCKER_OUTPUT_BYTES),
    ]
    assert removed == [("registry.example/task:1", False)]


def test_push_timeout_cleans_remote_alias(monkeypatch) -> None:
    removed: list[str] = []
    logged: list[str] = []

    def fake_run(command, **kwargs):
        if command[1] == "push":
            raise TimeoutError("docker push timed out; tail: safe-tail")
        return SimpleNamespace(returncode=0, tail="", output_truncated=False)

    monkeypatch.setattr(push, "run_bounded_command", fake_run, raising=False)
    monkeypatch.setattr(push, "_safe_rmi", lambda tag, **kwargs: removed.append(tag))

    assert (
        push.push_to_registry("source:latest", "registry.example/task:1", log=logged.append)
        is False
    )
    assert removed == ["registry.example/task:1"]
    assert "safe-tail" in logged[-1]
