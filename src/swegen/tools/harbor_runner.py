from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from harbor.models.environment_type import EnvironmentType
from harbor.models.job.result import JobResult
from harbor.models.trial.paths import TrialPaths
from harbor.models.trial.result import TrialResult

SUFFIXED_DOCKER_ENV_IMPORT_PATH = "swegen.tools.suffixed_docker:SwegenDockerEnvironment"


def harbor_cmd_base() -> list[str]:
    """Get the base command to invoke Harbor.

    Prefers direct `harbor` binary, falls back to `uv run harbor`.
    """
    if shutil.which("harbor"):
        return ["harbor"]
    sibling = Path(sys.executable).with_name("harbor")
    if sibling.is_file() and os.access(sibling, os.X_OK):
        return [str(sibling)]
    if shutil.which("uv"):
        return ["uv", "run", "harbor"]
    return [sys.executable, "-c", "from harbor.cli.main import app; app()"]


def _is_docker_environment(environment: EnvironmentType | str) -> bool:
    value = environment.value if isinstance(environment, EnvironmentType) else environment
    return value == EnvironmentType.DOCKER.value


def write_suffixed_docker_config(config_dir: Path) -> Path:
    """Write a Harbor config fragment selecting swegen's suffixed Docker environment."""
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "swegen-harbor-docker-suffix.json"
    config = {
        "environment": {
            "type": EnvironmentType.DOCKER.value,
            "import_path": SUFFIXED_DOCKER_ENV_IMPORT_PATH,
        }
    }
    config_path.write_text(json.dumps(config, indent=2))
    return config_path


def suffixed_docker_config_args(config_dir: Path, environment: EnvironmentType | str) -> list[str]:
    """Return Harbor CLI args that apply the swegen Docker suffix for Docker runs."""
    if not _is_docker_environment(environment):
        return []
    return ["--config", str(write_suffixed_docker_config(config_dir))]


def _reap_harbor_containers(task_id: str, environment: EnvironmentType | str) -> None:
    """Force-remove any Docker containers Harbor left behind for ``task_id``.

    Harbor tears down its ``docker compose`` project only if its ``stop()`` runs
    to completion. When a run hits the wall-timeout and we SIGKILL the Harbor
    process group (below), or ``compose down`` fails under daemon load, the
    ``<project>-main-1`` container is orphaned and stays ``Up`` forever. Dozens
    accumulate per node over days and exhaust the Docker daemon, freezing all
    subsequent builds. Harbor names the compose project ``<task_id>__<suffix>``,
    so remove every container whose ``com.docker.compose.project`` label starts
    with ``<task_id>__``. This removes CONTAINERS ONLY (no ``--rmi``), so the
    built images are preserved for the SWR push / faster subsequent runs.
    """
    if not _is_docker_environment(environment):
        return
    # Docker's --filter label match does not support prefix globs, so list all
    # containers with their compose-project label and prefix-match in Python.
    try:
        described = subprocess.run(
            [
                "docker",
                "ps",
                "-a",
                "--format",
                "{{.ID}} {{.Label \"com.docker.compose.project\"}}",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except Exception:  # best-effort cleanup; never let it break the caller
        return
    ids = [
        parts[0]
        for line in described.stdout.splitlines()
        if len(parts := line.split(" ", 1)) == 2 and parts[1].startswith(f"{task_id}__")
    ]
    for container_id in ids:
        try:
            subprocess.run(
                ["docker", "rm", "-f", container_id],
                capture_output=True,
                text=True,
                timeout=60,
            )
        except Exception:
            pass


def run_harbor_agent(
    task_id: str,
    dataset_path: Path,
    jobs_dir: Path,
    agent: str,
    timeout_multiplier: float | None = None,
    capture_output: bool = False,
    delete_after: bool = True,
    environment: EnvironmentType = EnvironmentType.DOCKER,
    wall_timeout_seconds: float | None = None,
) -> tuple[int, Path | None]:
    """Run a Harbor agent and return (exit_code, job_result_path).

    Args:
        task_id: The task identifier
        dataset_path: Path to the Harbor dataset root
        jobs_dir: Parent directory for job artifacts
        agent: Agent type ("nop" or "oracle")
        timeout_multiplier: Optional timeout multiplier for long tasks
        capture_output: If True, suppress stdout/stderr (for rich console usage)
        delete_after: If True, delete Docker images after run (default: True)
                     Set to False to keep images for faster subsequent runs
        environment: Environment type (docker, daytona, e2b, modal, runloop, gke)
        wall_timeout_seconds: Optional outer wall-clock limit. When exceeded,
            terminate the complete Harbor/Compose client process group so a
            timed-out Docker build cannot orphan clients and block a worker.

    Returns:
        Tuple of (exit_code, path_to_result_json or None)
    """
    # Create unique job directory to avoid race conditions
    unique_parent = jobs_dir / f"{task_id}.{agent}.{int(time.time())}"
    unique_parent.mkdir(parents=True, exist_ok=True)
    before = set(unique_parent.iterdir())

    cmd = harbor_cmd_base() + [
        "run",
        *suffixed_docker_config_args(unique_parent, environment),
        "--agent",
        agent,
        "-p",
        str(dataset_path),
        "-t",
        task_id,
        "--jobs-dir",
        str(unique_parent),
        "--env",
        environment.value,
    ]
    if timeout_multiplier is not None:
        cmd += ["--timeout-multiplier", str(timeout_multiplier)]

    # Control image deletion: --no-delete keeps images for faster subsequent runs
    if not delete_after:
        cmd.append("--no-delete")

    # Force Compose's build onto the legacy per-service build path instead of
    # delegating to `docker buildx bake`. Compose v2 (v5.3.1 here) defaults to
    # bake, whose `docker-buildx bake` procs deadlock on futex_wait_queue under
    # Stage-2 concurrency (builds wedge at ~0% CPU, never harvested, slots never
    # free — the whole baseline queue stalls). COMPOSE_BAKE=0 disables that path.
    # Passed explicitly in the child env (not just inherited) so it survives any
    # sg/newgrp/login-shell hop Harbor makes when it shells out to Compose.
    child_env = {
        **os.environ,
        "COMPOSE_BAKE": "0",
        "DOCKER_BUILDKIT": "1",
    }
    child = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE if capture_output else None,
        stderr=subprocess.PIPE if capture_output else None,
        text=True,
        start_new_session=True,
        env=child_env,
    )
    try:
        stdout, stderr = child.communicate(timeout=wall_timeout_seconds)
    except subprocess.TimeoutExpired as error:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            child.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.communicate()
        # Killing the Harbor process group skips its compose teardown, so the
        # container is orphaned. Reap it explicitly (images preserved) before
        # surfacing the timeout, or leaked containers pile up and wedge Docker.
        _reap_harbor_containers(task_id, environment)
        raise TimeoutError(
            f"Harbor {agent} timed out after {wall_timeout_seconds:g} seconds"
        ) from error

    # Normal completion: Harbor's own teardown may still have failed silently
    # under daemon load (it swallows compose-down errors), so reap defensively.
    # Containers only — images are kept for the SWR push.
    _reap_harbor_containers(task_id, environment)

    proc = subprocess.CompletedProcess(
        args=cmd,
        returncode=child.returncode,
        stdout=stdout or "",
        stderr=stderr or "",
    )

    # Check if directory still exists after subprocess
    if not unique_parent.exists():
        return proc.returncode, None

    after = set(unique_parent.iterdir())
    new_dirs = [p for p in (after - before) if p.is_dir()]
    job_dir = (
        sorted(new_dirs, key=lambda p: p.stat().st_mtime, reverse=True)[0] if new_dirs else None
    )
    job_result = (job_dir / "result.json").resolve() if job_dir else None

    return proc.returncode, job_result


@dataclass(frozen=True)
class HarborOutcome:
    reward: int | None
    error: str | None


def parse_harbor_outcome(job_result_path: Path | None) -> HarborOutcome:
    """Parse Harbor job result and return both reward and error (best-effort).

    Uses Harbor's JobResult and TrialResult Pydantic models for type-safe parsing.
    This automatically handles schema changes and provides better error messages.

    Args:
        job_result_path: Path to the job-level result.json

    Returns:
        HarborOutcome with:
        - reward: 0 or 1 (or None if unavailable)
        - error: best-effort exception message (or None)
    """
    if not job_result_path or not job_result_path.exists():
        return HarborOutcome(reward=None, error=None)

    try:
        # Use Harbor's JobResult model for type-safe parsing
        job_result = JobResult.model_validate_json(job_result_path.read_text())

        # Prefer structured exception info from typed trial results.
        error: str | None = None
        for trial_result in job_result.trial_results:
            if getattr(trial_result, "exception_info", None):
                exc = trial_result.exception_info
                msg = getattr(exc, "exception_message", None) or getattr(
                    exc, "exception_type", None
                )
                if msg:
                    error = str(msg)
                    break

        # Method 1: Check reward_stats in job stats (fastest)
        if job_result.stats.evals:
            # Get first eval (typically only one for single-task runs)
            first_eval = next(iter(job_result.stats.evals.values()))

            # Check reward_stats for "reward" key
            if first_eval.reward_stats and "reward" in first_eval.reward_stats:
                reward_map = first_eval.reward_stats["reward"]

                # Check for reward=1 first (oracle success)
                if 1 in reward_map or 1.0 in reward_map:
                    return HarborOutcome(reward=1, error=error)
                # Then check for reward=0 (nop success)
                if 0 in reward_map or 0.0 in reward_map:
                    return HarborOutcome(reward=0, error=error)

        # Method 2: Check trial results directly
        for trial_result in job_result.trial_results:
            if trial_result.verifier_result and trial_result.verifier_result.rewards:
                reward_value = trial_result.verifier_result.rewards.get("reward")
                if reward_value is not None:
                    return HarborOutcome(reward=int(float(reward_value)), error=error)

        # Method 3: Fallback - scan trial directories using TrialPaths
        job_root = job_result_path.parent
        for trial_dir in (p for p in job_root.iterdir() if p.is_dir()):
            try:
                trial_paths = TrialPaths(trial_dir)
                if not trial_paths.result_path.exists():
                    continue
                trial_result = TrialResult.model_validate_json(trial_paths.result_path.read_text())

                if error is None and getattr(trial_result, "exception_info", None):
                    exc = trial_result.exception_info
                    msg = getattr(exc, "exception_message", None) or getattr(
                        exc, "exception_type", None
                    )
                    if msg:
                        error = str(msg)

                if trial_result.verifier_result and trial_result.verifier_result.rewards:
                    reward_value = trial_result.verifier_result.rewards.get("reward")
                    if reward_value is not None:
                        return HarborOutcome(reward=int(float(reward_value)), error=error)
            except Exception:
                # Not a valid trial directory, continue searching
                continue

    except Exception:
        return HarborOutcome(reward=None, error=None)

    return HarborOutcome(reward=None, error=error)
