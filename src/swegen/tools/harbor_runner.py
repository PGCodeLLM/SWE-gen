from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from math import isfinite
from pathlib import Path
from threading import Event

from harbor.models.environment_type import EnvironmentType
from harbor.models.job.result import JobResult
from harbor.models.trial.paths import TrialPaths
from harbor.models.trial.result import TrialResult

SUFFIXED_DOCKER_ENV_IMPORT_PATH = "swegen.tools.suffixed_docker:SwegenDockerEnvironment"
HARBOR_CANCEL_POLL_SECONDS = 1.0
HARBOR_STOP_GRACE_SECONDS = 10.0
COMPOSE_REAP_GRACE_SECONDS = 5.0


class HarborRunCancelled(RuntimeError):
    """Raised when a worker shutdown interrupts an active Harbor run."""


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
                '{{.ID}} {{.Label "com.docker.compose.project"}}',
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


def _stop_harbor_process_group(child: subprocess.Popen[str]) -> None:
    """Boundedly terminate Harbor and every Compose/Buildx client it spawned."""

    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        child.communicate(timeout=HARBOR_STOP_GRACE_SECONDS)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    child.communicate()


@dataclass(frozen=True, slots=True)
class _ComposeBuildProcess:
    pid: int
    started_at_ticks: int
    project: str


def _process_started_at_ticks(pid: int) -> int:
    """Read Linux's monotonic process start tick from ``/proc/<pid>/stat``."""

    stat = (Path("/proc") / str(pid) / "stat").read_text()
    closing_parenthesis = stat.rfind(")")
    if closing_parenthesis < 0:
        raise ValueError(f"invalid /proc stat for pid {pid}")
    # Fields after the command name begin at field 3 (state); starttime is
    # field 22, hence offset 19 in this suffix.
    return int(stat[closing_parenthesis + 2 :].split()[19])


def _compose_build_processes(process_group_id: int) -> tuple[_ComposeBuildProcess, ...]:
    """Return Compose build clients that belong to one Harbor process group."""

    processes: list[_ComposeBuildProcess] = []
    try:
        entries = tuple(Path("/proc").iterdir())
    except OSError:
        return ()
    for entry in entries:
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            if os.getpgid(pid) != process_group_id:
                continue
            argv = tuple(
                value.decode(errors="replace")
                for value in (entry / "cmdline").read_bytes().split(b"\0")
                if value
            )
            if (
                len(argv) < 3
                or Path(argv[0]).name != "docker"
                or argv[1] != "compose"
                or "build" not in argv[2:]
            ):
                continue
            project_index = argv.index("-p")
            project = argv[project_index + 1]
            if not project:
                continue
            processes.append(
                _ComposeBuildProcess(
                    pid=pid,
                    started_at_ticks=_process_started_at_ticks(pid),
                    project=project,
                )
            )
        except (IndexError, OSError, ProcessLookupError, ValueError):
            # Processes can exit or change between /proc enumeration and read.
            continue
    return tuple(processes)


def _duplicate_compose_build_pids(
    processes: tuple[_ComposeBuildProcess, ...],
) -> tuple[int, ...]:
    """Choose every stale retry client while keeping the newest per project."""

    by_project: dict[str, list[_ComposeBuildProcess]] = {}
    for process in processes:
        by_project.setdefault(process.project, []).append(process)
    stale: list[int] = []
    for project_processes in by_project.values():
        ordered = sorted(
            project_processes,
            key=lambda process: (process.started_at_ticks, process.pid),
        )
        stale.extend(process.pid for process in ordered[:-1])
    return tuple(stale)


def _reap_duplicate_compose_builds(
    process_group_id: int,
    terminating_since: dict[int, float],
) -> int:
    """Cancel Harbor's abandoned first build when its built-in retry starts.

    Harbor retries environment startup after the task's build timeout. Its
    Docker backend currently lets asyncio cancellation abandon the first
    ``docker compose build`` client, so the retry runs two identical BuildKit
    solves for the same project. Reap the older client promptly and escalate
    if it ignores SIGTERM.
    """

    now = time.monotonic()
    stale_pids = set(
        _duplicate_compose_build_pids(_compose_build_processes(process_group_id))
    )
    for pid in tuple(terminating_since):
        if pid not in stale_pids:
            terminating_since.pop(pid, None)
    for pid in stale_pids:
        first_signal_at = terminating_since.setdefault(pid, now)
        selected_signal = (
            signal.SIGKILL
            if now - first_signal_at >= COMPOSE_REAP_GRACE_SECONDS
            else signal.SIGTERM
        )
        try:
            os.kill(pid, selected_signal)
        except ProcessLookupError:
            terminating_since.pop(pid, None)
    return len(stale_pids)


def _reap_remaining_compose_builds(process_group_id: int) -> None:
    """Terminate Compose build clients left after Harbor itself has exited."""

    for process in _compose_build_processes(process_group_id):
        try:
            os.kill(process.pid, signal.SIGTERM)
        except ProcessLookupError:
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
    cancel_event: Event | None = None,
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
        cancel_event: Optional worker-shutdown event. When set, terminate the
            Harbor process group promptly and raise ``HarborRunCancelled`` so
            the caller can release the PGMQ claim without counting a failure.

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

    # Force Compose's build onto its internal BuildKit path instead of
    # delegating to `docker buildx bake`. The worker image pins Compose v2.40.3,
    # the last compatible line where COMPOSE_BAKE=false is honored. Compose
    # v5.3.1 ignores this setting and always chooses Bake when BuildKit is on.
    # Under Stage-2 concurrency those `docker-buildx bake` clients deadlock on
    # futex_wait_queue, are never harvested, and eventually stall the queue.
    # Passed explicitly in the child env (not just inherited) so it survives any
    # sg/newgrp/login-shell hop Harbor makes when it shells out to Compose.
    child_env = {
        **os.environ,
        "COMPOSE_BAKE": "false",
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
    deadline = (
        None if wall_timeout_seconds is None else time.monotonic() + wall_timeout_seconds
    )
    duplicate_compose_terminations: dict[int, float] = {}
    while True:
        if cancel_event is not None and cancel_event.is_set():
            _stop_harbor_process_group(child)
            _reap_harbor_containers(task_id, environment)
            raise HarborRunCancelled(f"Harbor {agent} cancelled during worker shutdown")

        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            _stop_harbor_process_group(child)
            _reap_harbor_containers(task_id, environment)
            raise TimeoutError(
                f"Harbor {agent} timed out after {wall_timeout_seconds:g} seconds"
            )

        _reap_duplicate_compose_builds(child.pid, duplicate_compose_terminations)
        poll_timeout = HARBOR_CANCEL_POLL_SECONDS
        if remaining is not None:
            poll_timeout = min(poll_timeout, remaining)
        try:
            stdout, stderr = child.communicate(timeout=poll_timeout)
            break
        except subprocess.TimeoutExpired as error:
            if remaining is not None and poll_timeout >= remaining:
                _stop_harbor_process_group(child)
                _reap_harbor_containers(task_id, environment)
                raise TimeoutError(
                    f"Harbor {agent} timed out after {wall_timeout_seconds:g} seconds"
                ) from error
            continue

    _reap_remaining_compose_builds(child.pid)

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
    reward: int | float | None
    error: str | None


def _finite_numeric_reward(value: object) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not isfinite(float(value)):
        return None
    return value


def parse_harbor_outcome(job_result_path: Path | None) -> HarborOutcome:
    """Parse Harbor job result and return both reward and error (best-effort).

    Uses Harbor's JobResult and TrialResult Pydantic models for type-safe parsing.
    This automatically handles schema changes and provides better error messages.

    Args:
        job_result_path: Path to the job-level result.json

    Returns:
        HarborOutcome with:
        - reward: exact finite numeric reward (or None if unavailable)
        - error: best-effort exception message (or None)
    """
    if not job_result_path or not job_result_path.exists():
        return HarborOutcome(reward=None, error=None)

    try:
        # Use Harbor's JobResult model for type-safe parsing
        job_result = JobResult.model_validate_json(job_result_path.read_text(), strict=True)

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
                for reward_value in reward_map:
                    reward = _finite_numeric_reward(reward_value)
                    if reward is not None:
                        return HarborOutcome(reward=reward, error=error)

        # Method 2: Check trial results directly
        for trial_result in job_result.trial_results:
            if trial_result.verifier_result and trial_result.verifier_result.rewards:
                reward_value = trial_result.verifier_result.rewards.get("reward")
                if reward_value is not None:
                    return HarborOutcome(
                        reward=_finite_numeric_reward(reward_value),
                        error=error,
                    )

        # Method 3: Fallback - scan trial directories using TrialPaths
        job_root = job_result_path.parent
        for trial_dir in (p for p in job_root.iterdir() if p.is_dir()):
            try:
                trial_paths = TrialPaths(trial_dir)
                if not trial_paths.result_path.exists():
                    continue
                trial_result = TrialResult.model_validate_json(
                    trial_paths.result_path.read_text(), strict=True
                )

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
                        return HarborOutcome(
                            reward=_finite_numeric_reward(reward_value),
                            error=error,
                        )
            except Exception:
                # Not a valid trial directory, continue searching
                continue

    except Exception:
        return HarborOutcome(reward=None, error=None)

    return HarborOutcome(reward=None, error=error)
