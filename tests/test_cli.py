from __future__ import annotations

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
