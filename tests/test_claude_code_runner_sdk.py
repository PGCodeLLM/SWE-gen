import asyncio
from types import SimpleNamespace

from swegen.create import claude_code_runner as runner
from swegen.model_settings import ModelSettings


def test_incomplete_sdk_turn_is_continued_in_same_client(tmp_path, monkeypatch) -> None:
    prompts: list[str] = []
    options_seen = []

    class FakeClient:
        def __init__(self, options):
            options_seen.append(options)

        async def __aenter__(self):
            assert "GITHUB_TOKEN" not in runner.os.environ
            assert "SWEGEN_CONFIG" not in runner.os.environ
            return self

        async def __aexit__(self, *_args):
            return False

        async def query(self, prompt):
            prompts.append(prompt)

        async def receive_response(self):
            if False:
                yield None

    states = iter(
        [
            runner.ClaudeCodeResult(False, False, False, "incomplete"),
            runner.ClaudeCodeResult(True, True, True),
        ]
    )
    monkeypatch.setattr(runner, "ClaudeSDKClient", FakeClient)
    monkeypatch.setattr(runner, "load_model_settings", lambda: ModelSettings(model="test-model"))
    monkeypatch.setattr(runner, "_check_validation_state", lambda *_args, **_kwargs: next(states))
    monkeypatch.setenv("GITHUB_TOKEN", "test-github-token")
    monkeypatch.setenv("SWEGEN_CONFIG", "/private/swegen.toml")

    repo_path = tmp_path / "repo"
    task_dir = tmp_path / "tasks" / "owner__repo-1"
    repo_path.mkdir()
    task_dir.mkdir(parents=True)

    result = asyncio.run(
        runner._run_claude_code_session_async(
            repo="owner/repo",
            pr_number=1,
            repo_path=repo_path,
            task_dir=task_dir,
            task_id="owner__repo-1",
            dataset_path=task_dir.parent,
            test_files=[],
            timeout=5,
            jobs_dir=tmp_path / "jobs",
        )
    )

    assert result.success is True
    assert len(prompts) == 2
    assert "Work synchronously" in prompts[0]
    assert prompts[1] == runner.CC_CONTINUATION_PROMPT
    assert options_seen[0].disallowed_tools == ["Task"]
    assert runner.os.environ["GITHUB_TOKEN"] == "test-github-token"
    assert runner.os.environ["SWEGEN_CONFIG"] == "/private/swegen.toml"


def test_preexisting_harbor_results_are_not_accepted(tmp_path, monkeypatch) -> None:
    jobs_dir = tmp_path / "jobs"
    result = jobs_dir / "task-nop-1" / "trial" / "result.json"
    result.parent.mkdir(parents=True)
    result.write_text("old")
    baseline = runner._snapshot_job_results(jobs_dir, "task")
    monkeypatch.setattr(
        runner,
        "parse_harbor_outcome",
        lambda _path: SimpleNamespace(reward=0),
    )

    assert runner._check_job_results(jobs_dir, "task", baseline=baseline) == (
        False,
        False,
    )

    result.write_text("new-result")
    assert runner._check_job_results(jobs_dir, "task", baseline=baseline) == (
        True,
        False,
    )
