from __future__ import annotations

import asyncio

from swegen.create import claude_code_runner
from swegen.create.claude_code_utils import (
    DISALLOWED_AUTOMATION_TOOLS,
    TASK_GENERATION_TOOLS,
)
from swegen.model_settings import ModelSettings


class FakeClaudeClient:
    instances = []
    block_response = False

    def __init__(self, options):
        self.options = options
        self.events = []
        type(self).instances.append(self)

    async def connect(self):
        self.events.append("connect")

    async def query(self, prompt):
        self.events.append("query")

    async def receive_response(self):
        self.events.append("receive")
        if self.block_response:
            await asyncio.sleep(60)
        if False:
            yield None

    async def interrupt(self):
        self.events.append("interrupt")

    async def disconnect(self):
        self.events.append("disconnect")


def _patch_runner(monkeypatch, *, timed_out_calls):
    FakeClaudeClient.instances.clear()
    monkeypatch.setattr(claude_code_runner, "ClaudeSDKClient", FakeClaudeClient)
    monkeypatch.setattr(
        claude_code_runner,
        "load_model_settings",
        lambda: ModelSettings(model="configured-model"),
    )
    monkeypatch.setattr(claude_code_runner, "claude_runtime_env", lambda _task_id: {})
    monkeypatch.setattr(claude_code_runner, "suffixed_docker_config_args", lambda *_args: [])

    expected = claude_code_runner.ClaudeCodeResult(True, True, True)

    def fake_check(_jobs_dir, _task_id, _logger, timed_out=False):
        timed_out_calls.append(timed_out)
        return expected

    monkeypatch.setattr(claude_code_runner, "_check_validation_state", fake_check)
    return expected


def _run_session(tmp_path, *, timeout):
    return asyncio.run(
        claude_code_runner._run_claude_code_session_async(
            repo="owner/repo",
            pr_number=1,
            repo_path=tmp_path / "repo",
            task_dir=tmp_path / "tasks" / "owner__repo-1",
            task_id="owner__repo-1",
            dataset_path=tmp_path / "tasks",
            test_files=[],
            timeout=timeout,
            jobs_dir=tmp_path / "jobs",
        )
    )


def test_runner_restricts_tools_and_disconnects_client(monkeypatch, tmp_path):
    timed_out_calls = []
    expected = _patch_runner(monkeypatch, timed_out_calls=timed_out_calls)
    FakeClaudeClient.block_response = False

    result = _run_session(tmp_path, timeout=5)

    assert result == expected
    assert timed_out_calls == [False]
    client = FakeClaudeClient.instances[0]
    assert client.events == ["connect", "query", "receive", "disconnect"]
    assert client.options.tools == list(TASK_GENERATION_TOOLS)
    assert client.options.allowed_tools == list(TASK_GENERATION_TOOLS)
    assert client.options.disallowed_tools == list(DISALLOWED_AUTOMATION_TOOLS)


def test_runner_interrupts_and_disconnects_on_timeout(monkeypatch, tmp_path):
    timed_out_calls = []
    expected = _patch_runner(monkeypatch, timed_out_calls=timed_out_calls)
    FakeClaudeClient.block_response = True

    result = _run_session(tmp_path, timeout=0.01)

    assert result == expected
    assert timed_out_calls == [True]
    client = FakeClaudeClient.instances[0]
    assert client.events == [
        "connect",
        "query",
        "receive",
        "interrupt",
        "disconnect",
    ]
