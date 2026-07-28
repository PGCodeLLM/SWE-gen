from __future__ import annotations

from swegen.create import claude_code_utils


def test_claude_permission_mode_uses_default_for_root(monkeypatch):
    monkeypatch.setattr(claude_code_utils.os, "geteuid", lambda: 0)

    assert claude_code_utils.claude_permission_mode() == "default"


def test_claude_permission_mode_preserves_bypass_for_non_root(monkeypatch):
    monkeypatch.setattr(claude_code_utils.os, "geteuid", lambda: 1000)

    assert claude_code_utils.claude_permission_mode() == "bypassPermissions"


def test_automation_tool_sets_exclude_subagents_and_plan_mode():
    forbidden = set(claude_code_utils.DISALLOWED_AUTOMATION_TOOLS)

    assert forbidden.isdisjoint(claude_code_utils.TASK_GENERATION_TOOLS)
    assert forbidden.isdisjoint(claude_code_utils.CLASSIFIER_TOOLS)
    assert "Task" not in claude_code_utils.TASK_GENERATION_TOOLS
    assert "EnterPlanMode" not in claude_code_utils.TASK_GENERATION_TOOLS
