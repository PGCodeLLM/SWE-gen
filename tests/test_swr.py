from __future__ import annotations

import subprocess

from swegen.model_settings import SWRSettings
from swegen.swr import PROXY_ENVIRONMENT_VARIABLES, upload_image_to_swr


def test_swr_docker_commands_run_without_proxy_environment(monkeypatch):
    for name in PROXY_ENVIRONMENT_VARIABLES:
        monkeypatch.setenv(name, f"proxy-value-for-{name.lower()}")
    monkeypatch.setenv("SWEGEN_TEST_MARKER", "preserved")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/tmp/combined-ca.pem")

    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    settings = SWRSettings(
        enabled=True,
        registry="registry.example.com",
        repository="aifm.coder.exp/swegen/generated",
        username="user",
        password="secret",
        retries=1,
        push_timeout=30,
    )

    result = upload_image_to_swr("Owner__Repo-1", ("local:latest",), settings)

    assert result.success
    assert [call[0][:2] for call in calls] == [
        ["docker", "image"],
        ["docker", "login"],
        ["docker", "tag"],
        ["docker", "push"],
    ]
    remote_ref = "registry.example.com/aifm.coder.exp/swegen/generated:owner__repo-1"
    assert calls[2][0] == ["docker", "tag", "local:latest", remote_ref]
    assert calls[3][0] == ["docker", "push", remote_ref]
    assert result.remote_ref == remote_ref
    for _command, kwargs in calls:
        command_env = kwargs["env"]
        assert command_env["SWEGEN_TEST_MARKER"] == "preserved"
        assert command_env["REQUESTS_CA_BUNDLE"] == "/tmp/combined-ca.pem"
        assert all(name not in command_env for name in PROXY_ENVIRONMENT_VARIABLES)
