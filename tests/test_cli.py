from __future__ import annotations

from pathlib import Path

import pytest
import typer

import orchestrator
from swegen.cli import app


def test_repository_farm_command_is_removed():
    command = typer.main.get_command(app)

    assert "farm" not in command.commands


def test_database_retry_ceiling_is_not_a_transient_retry_cli_option():
    args = orchestrator.parse_args(["--workers", "1", "--transient-attempts", "4"])

    assert args.transient_attempts == 4
    with pytest.raises(SystemExit):
        orchestrator.parse_args(["--workers", "1", "--max-retries", "4"])


def test_orchestrator_run_layout_separates_successful_and_postprocessed_tasks(
    tmp_path: Path,
):
    args = orchestrator.parse_args(
        ["--workers", "1", "--runs-dir", str(tmp_path), "--run-name", "run-1"]
    )

    orchestrator.resolve_run_layout(args)

    assert args.tasks_dir == tmp_path / "run-1" / "tasks"
    assert args.successful_dir == tmp_path / "run-1" / "tasks_bz"
    assert args.postprocessed_dir == tmp_path / "run-1" / "tasks_postprocessed"


def test_slurm_child_command_forwards_both_successful_output_trees(tmp_path: Path):
    args = orchestrator.parse_args(
        ["--workers", "1", "--runs-dir", str(tmp_path), "--run-name", "run-1"]
    )
    orchestrator.resolve_run_layout(args)

    command = orchestrator.build_child_command(args, tmp_path / "node-logs")

    successful_index = command.index("--successful-output")
    postprocessed_index = command.index("--postprocessed-output")
    assert command[successful_index + 1] == str(tmp_path / "run-1" / "tasks_bz")
    assert command[postprocessed_index + 1] == str(tmp_path / "run-1" / "tasks_postprocessed")
