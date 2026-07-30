from __future__ import annotations

from importlib.metadata import PackageNotFoundError as _PkgNotFound
from importlib.metadata import version as _pkg_version
from pathlib import Path

import typer
from harbor.models.environment_type import EnvironmentType

from swegen.analyze import AnalyzeArgs, run_analyze
from swegen.config import CreateConfig
from swegen.create import MissingIssueError, TrivialPRError
from swegen.create.create import run_reversal
from swegen.model_settings import configure_current_process
from swegen.tools.validate import ValidateArgs, run_validate
from swegen.tools.validate_utils import ValidationError

app = typer.Typer(no_args_is_help=True, add_completion=False, help="Task generation CLI")


@app.callback(invoke_without_command=True)
def _root(
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        help="Show swegen version and exit",
        is_eager=True,
    ),
) -> None:
    if version:
        try:
            typer.echo(f"swegen {_pkg_version('swe-gen')}")
        except _PkgNotFound:
            typer.echo("swegen (version unknown)")
        raise typer.Exit()


create_app = typer.Typer(
    no_args_is_help=True,
    invoke_without_command=True,
    add_completion=False,
    help="Create a Harbor task from a merged PR and validate",
)


@create_app.callback()
def create_cmd(
    repo: str = typer.Option(..., help="GitHub repository (owner/repo or URL)"),
    pr: int = typer.Option(..., help="PR number"),
    output: Path = typer.Option(Path("tasks"), help="Output root", show_default=True),
    cc_timeout: int = typer.Option(
        3200, help="Timeout for CC session in seconds (~53 min default)", show_default=True
    ),
    validate: bool = typer.Option(
        True, help="Run Harbor validations; --no-validate skips validation"
    ),
    force: bool = typer.Option(False, help="Bypass local dedupe and regenerate"),
    state_dir: Path = typer.Option(
        Path(".swegen"), help="Local run state/logs/jobs dir", show_default=True
    ),
    repo_cache_dir: Path = typer.Option(
        Path("data_cache/repos"), help="Shared git repo cache dir", show_default=True
    ),
    no_cache: bool = typer.Option(
        False,
        "--no-cache",
        help="Disable using successful Dockerfiles from previous tasks as hints",
    ),
    require_minimum_difficulty: bool = typer.Option(
        True,
        help="Require minimum difficulty (3+ source files); --no-require-minimum-difficulty to skip this check",
    ),
    min_source_files: int = typer.Option(
        3, help="Minimum number of source files required (tests excluded)", show_default=True
    ),
    max_source_files: int = typer.Option(
        10,
        help="Maximum number of source files to avoid large refactors (tests excluded)",
        show_default=True,
    ),
    generate_name: bool = typer.Option(
        False,
        "--generate-name",
        help="Generate a semantic task name (owner__repo-name) instead of using PR number",
    ),
    require_issue: bool = typer.Option(
        True,
        help="Require PR to have a linked issue (higher quality instructions); --no-require-issue uses PR body/title instead",
    ),
    allow_unmerged: bool = typer.Option(
        False,
        help="Allow processing unmerged PRs (for testing/preview); --allow-unmerged to enable",
    ),
    environment: str = typer.Option(
        "docker",
        "-e",
        "--env",
        help="Environment type for Harbor runs (docker|daytona|e2b|modal|runloop|gke)",
        show_default=True,
    ),
    verbose: bool = typer.Option(False, "-v", "--verbose", help="Increase output verbosity"),
    quiet: bool = typer.Option(False, "-q", "--quiet", help="Reduce output verbosity"),
) -> None:
    configure_current_process("swegen-create")
    config = CreateConfig(
        repo=repo,
        pr=pr,
        output=output,
        cc_timeout=cc_timeout,
        validate=validate,
        force=force,
        state_dir=state_dir,
        repo_cache_dir=repo_cache_dir,
        use_cache=not no_cache,
        require_minimum_difficulty=require_minimum_difficulty,
        min_source_files=min_source_files,
        max_source_files=max_source_files,
        require_issue=require_issue,
        allow_unmerged=allow_unmerged,
        environment=EnvironmentType(environment),
        generate_name=generate_name,
        verbose=verbose,
        quiet=quiet,
    )
    try:
        run_reversal(config)
    except (TrivialPRError, MissingIssueError, ValidationError, FileExistsError) as err:
        # These exceptions have already displayed user-friendly messages
        # Exit with error code but don't show traceback
        raise SystemExit(1) from err


app.add_typer(create_app, name="create")


@app.command(help="Validate an existing Harbor task by running NOP and ORACLE")
def validate(
    path: Path = typer.Argument(
        ...,
        help="Path to Harbor dataset root, specific task directory, or task ID when used with dataset root",
    ),
    task: str
    | None = typer.Option(None, "--task", "-t", help="Task ID when --path points to dataset root"),
    agent: str = typer.Option("both", help="Agent to run: both|nop|oracle", show_default=True),
    jobs_dir: Path = typer.Option(
        Path(".swegen/harbor-jobs"),
        help="Directory to store Harbor job artifacts",
        show_default=True,
    ),
    timeout_multiplier: float
    | None = typer.Option(None, help="Multiply default timeouts (e.g., 3.0)"),
    environment: str = typer.Option(
        "docker",
        "-e",
        "--env",
        help="Environment type for Harbor runs (docker|daytona|e2b|modal|runloop|gke)",
        show_default=True,
    ),
    verbose: bool = typer.Option(False, "-v", "--verbose", help="Increase output verbosity"),
    quiet: bool = typer.Option(False, "-q", "--quiet", help="Reduce output verbosity"),
    max_parallel: int = typer.Option(
        8, help="Maximum number of parallel validations (batch mode only)", show_default=True
    ),
    show_passed: bool = typer.Option(
        False,
        "--show-passed",
        help="Show passed tasks in output (batch mode: default shows only failures)",
    ),
    output: Path
    | None = typer.Option(
        None, "-o", "--output", help="Write results to file as they complete (batch mode only)"
    ),
    docker_prune_batch: int = typer.Option(
        5,
        help="Run docker cleanup after every N tasks (0 to disable, local docker only)",
        show_default=True,
    ),
) -> None:
    if agent not in ("both", "nop", "oracle"):
        raise typer.BadParameter("agent must be one of: both, nop, oracle")
    run_validate(
        ValidateArgs(
            path=path,
            task=task,
            jobs_dir=jobs_dir,
            agent=agent,
            timeout_multiplier=timeout_multiplier,
            verbose=verbose,
            quiet=quiet,
            environment=EnvironmentType(environment),
            max_parallel=max_parallel,
            show_passed=show_passed,
            output_file=output,
            docker_prune_batch=docker_prune_batch,
        )
    )


@app.command(help="Analyze a task by running agent trials and classifying outcomes")
def analyze(
    path: Path = typer.Argument(..., help="Path to the task directory to analyze"),
    agent: str = typer.Option(
        "claude-code", "-a", "--agent", help="Agent to run trials with", show_default=True
    ),
    n_trials: int = typer.Option(
        3, "-k", "--n-trials", help="Number of trials to run", show_default=True
    ),
    n_concurrent: int = typer.Option(
        3, "-n", "--n-concurrent", help="Number of concurrent trials (1=sequential, 3-5 recommended)", show_default=True
    ),
    jobs_dir: Path = typer.Option(
        Path(".swegen/analyze-jobs"),
        "--jobs-dir",
        help="Directory to store job artifacts",
        show_default=True,
    ),
    skip_quality_check: bool = typer.Option(
        False, "--skip-quality-check", help="Skip static quality check"
    ),
    skip_baseline: bool = typer.Option(
        False, "--skip-baseline", help="Skip baseline validation (nop/oracle)"
    ),
    skip_classify: bool = typer.Option(
        False, "--skip-classify", help="Skip LLM classification of trial outcomes"
    ),
    timeout_multiplier: float = typer.Option(
        1.0, "--timeout-multiplier", help="Multiply default timeouts", show_default=True
    ),
    environment: str = typer.Option(
        "docker",
        "-e",
        "--env",
        help="Environment type for Harbor runs (docker|daytona|e2b|modal|runloop|gke)",
        show_default=True,
    ),
    verbose: bool = typer.Option(False, "-v", "--verbose", help="Increase output verbosity"),
    classification_timeout: int = typer.Option(
        300,
        "--classification-timeout",
        help="Timeout per trial classification in seconds",
        show_default=True,
    ),
    verdict_timeout: int = typer.Option(
        180,
        "--verdict-timeout",
        help="Timeout for verdict synthesis in seconds",
        show_default=True,
    ),
) -> None:
    """
    Analyze a Harbor task to determine if it's well-specified.

    This command classifies trial outcomes to identify TASK PROBLEMS vs AGENT PROBLEMS:

    1. Static quality check (Harbor's tasks check)
    2. Baseline validation (nop should fail, oracle should pass)
    3. Run N agent trials (default: 3 with Claude Code)
    4. Classify each trial outcome:
       - GOOD_SUCCESS: Agent solved it correctly
       - BAD_SUCCESS: Agent cheated or tests too permissive
       - GOOD_FAILURE: Agent failed due to its own limitations
       - BAD_FAILURE: Agent failed due to task issues
       - HARNESS_ERROR: Infrastructure problem
    5. Compute task verdict with recommendations

    The goal is to identify tasks that need fixing before release.

    Flags match Harbor CLI conventions:
        -k / --n-trials: Total number of trials to run
        -n / --n-concurrent: Number of trials to run concurrently (parallelism)

    Examples:
        # Sequential (default)
        swegen analyze tasks/my-task -k 5

        # Parallel (3 trials at once)
        swegen analyze tasks/my-task -k 10 -n 3
    """
    run_analyze(
        AnalyzeArgs(
            task_path=path,
            agent=agent,
            n_trials=n_trials,
            n_concurrent=n_concurrent,
            jobs_dir=jobs_dir,
            skip_quality_check=skip_quality_check,
            skip_baseline=skip_baseline,
            skip_classify=skip_classify,
            environment=environment,
            timeout_multiplier=timeout_multiplier,
            verbose=verbose,
            classification_timeout=classification_timeout,
            verdict_timeout=verdict_timeout,
        )
    )
