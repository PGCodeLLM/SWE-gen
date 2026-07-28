from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from harbor.models.environment_type import EnvironmentType


@dataclass(frozen=True)
class CreateConfig:
    """Configuration for the create command (PR → Harbor task).

    The create command uses a language-agnostic pipeline that works
    for any repository. Claude Code analyzes the repo to detect language, runtime,
    build system, and test framework automatically.

    Attributes:
        repo: GitHub repository in "owner/repo" format or full URL
        pr: Pull request number
        output: Output directory for generated tasks (default: tasks/)
        cc_timeout: Timeout for Claude Code session in seconds
        validate: Run Harbor validations (NOP + Oracle)
        force: Bypass local dedupe and regenerate existing tasks
        state_dir: Directory for run-local state/logs/jobs
        repo_cache_dir: Directory for shared git repository cache
        use_cache: Reuse successful Dockerfiles from previous tasks as hints
        require_minimum_difficulty: Require 3+ source files for task
        min_source_files: Minimum number of source files required (default: 3)
        max_source_files: Maximum number of source files allowed to avoid large refactors (default: 10)
        require_issue: Require PR to have a linked issue (higher quality instructions)
        allow_unmerged: Allow processing unmerged PRs (for testing/preview, default: False)
        environment: Environment type for Harbor runs (docker, daytona, e2b, modal, runloop, gke)
        generate_name: Generate semantic task name instead of PR number
        verbose: Increase output verbosity
        quiet: Reduce output verbosity
    """

    repo: str
    pr: int
    output: Path = field(default_factory=lambda: Path("tasks"))
    cc_timeout: int = 3200
    validate: bool = True
    force: bool = False
    state_dir: Path = field(default_factory=lambda: Path(".swegen"))
    repo_cache_dir: Path = field(default_factory=lambda: Path("data_cache/repos"))
    use_cache: bool = True
    require_minimum_difficulty: bool = True
    min_source_files: int = 3
    max_source_files: int = 10
    require_issue: bool = True
    allow_unmerged: bool = False
    environment: EnvironmentType = EnvironmentType.DOCKER
    generate_name: bool = False
    verbose: bool = False
    quiet: bool = False
    keep_image: bool = False

    # Computed property for backward compatibility with old code
    @property
    def no_validate(self) -> bool:
        """Inverse of validate for backward compatibility."""
        return not self.validate


@dataclass(frozen=True)
class ValidateConfig:
    """Configuration for the validate command.

    Attributes:
        path: Path to Harbor dataset root or specific task directory
        task: Task ID when path points to dataset root
        agent: Agent to run: both, nop, or oracle
        jobs_dir: Directory to store Harbor job artifacts
        timeout_multiplier: Multiply default timeouts
        environment: Environment type for Harbor runs (docker, daytona, e2b, modal, runloop, gke)
        verbose: Increase output verbosity
        quiet: Reduce output verbosity
        max_parallel: Maximum number of parallel validations (batch mode)
        show_passed: Show passed tasks in output (batch mode)
    """

    path: Path
    task: str | None = None
    agent: Literal["both", "nop", "oracle"] = "both"
    jobs_dir: Path = field(default_factory=lambda: Path(".swegen/harbor-jobs"))
    timeout_multiplier: float | None = None
    environment: EnvironmentType = EnvironmentType.DOCKER
    verbose: bool = False
    quiet: bool = False
    max_parallel: int = 8
    show_passed: bool = False
