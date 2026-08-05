"""The generate entrypoint must authenticate GitHub calls from the token pool.

Regression guard: run_reversal once built PRToHarborPipeline with no token, so
the PR/issue fetcher fell back to unauthenticated GitHub (60/hr per egress IP).
A few hundred worker pods exhausted that instantly, surfacing as
api.github.com read timeouts and a runaway generate failure rate.
"""
from __future__ import annotations

import importlib

import swegen.create.create as create_mod


def test_pick_github_token_prefers_env(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "env-token-xyz")
    assert create_mod._pick_github_token() == "env-token-xyz"


def test_pick_github_token_samples_pool_when_no_env(monkeypatch) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    pool = ["ghp_a", "ghp_b", "ghp_c", "ghp_d", "ghp_e"]
    monkeypatch.setattr(
        "swegen.model_settings.load_github_tokens", lambda: pool
    )
    # Every pick must come from the pool, and across many picks it must not be
    # pinned to a single token (random.choice spreads pods across the pool).
    picks = {create_mod._pick_github_token() for _ in range(200)}
    assert picks, "expected at least one token"
    assert picks <= set(pool)
    assert len(picks) > 1


def test_pick_github_token_none_when_pool_empty(monkeypatch) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.setattr("swegen.model_settings.load_github_tokens", lambda: [])
    assert create_mod._pick_github_token() is None


def test_run_reversal_passes_token_into_pipeline(monkeypatch) -> None:
    # run_reversal must hand the process token to PRToHarborPipeline, not
    # construct it token-less.
    captured = {}

    class _FakePipeline:
        def __init__(self, repo, pr_number, github_token=None):
            captured["github_token"] = github_token
            self.task_id = "owner__repo-1"
            raise RuntimeError("stop after construction")

    monkeypatch.setattr(create_mod, "PRToHarborPipeline", _FakePipeline)
    monkeypatch.setattr(create_mod, "_PROCESS_GITHUB_TOKEN", "ghp_sentinel")

    class _Cfg:
        repo = "owner/repo"
        pr = 1
        state_dir = "/tmp"

    try:
        create_mod.run_reversal(_Cfg())
    except RuntimeError:
        pass
    assert captured.get("github_token") == "ghp_sentinel"
