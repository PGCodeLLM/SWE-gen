#!/usr/bin/env python3
"""Database-backed parallel orchestrator for ``swegen create``.

Workers atomically claim all eligible PRs for one repository from
``swegen.pr_tasks``. The claim transaction sets a future ``unlock_time`` and
increments ``swegen_retries`` before returning rows, while a repository advisory
lock prevents concurrent SWE-gen processes from allocating the same repo group.

Each invocation writes to a run directory. If ``--run-name`` is omitted, the
run name defaults to the current UTC timestamp under ``runs/``. If a run name is
specified, that existing run directory is reused. Database pass state, OBS
availability, and leases determine eligibility; ``--force-rebuild`` includes
already-passed rows and ``--include-obs-missing`` includes missing OBS rows.
Rows at or above ``[database].max-retries`` remain ineligible, including forced
rebuild runs, and lower-retry work is claimed first. ``primary_language`` values
listed in ``exclude_languages`` are omitted, and only configured ``pr_category``
values are eligible. When ``[orchestrator].produce_count`` is set, workers share
a run-level quota and stop after exactly that many fully successful instances.

Each run contains ``tasks/``, ``tasks_bz/``,
``orchestrator-logs/``, ``logs/``, ``harbor-jobs/``, and an
``orchestrator-progress.jsonl`` completion log. As soon as a PR's
``swegen create`` reaches NOP=0/Oracle=1, the single task is checked for reward
hacking. Only non-hacking tasks are copied to ``tasks_bz/``, postprocessed for
the restricted Huawei environment, uploaded to SWR, and marked passed in the
database. Local images are deleted only after a successful upload.

Within a package, each PR is processed sequentially by shelling out to:

    swegen create --repo <repo> --pr <pr> \
        --no-require-minimum-difficulty --no-require-issue

Example:
    python src/orchestrator.py --workers 4
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import random
import re
import shlex
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from reward_hacking_detector.hacking import (
    check_task_sync,
    load_llm_configs,
    write_instance_log,
)
from swegen.database import PRTaskDatabase
from swegen.model_settings import (
    configured_subprocess_env,
    load_database_settings,
    load_github_tokens,
    load_model_settings,
    load_openai_settings,
    load_orchestrator_settings,
    load_swr_settings,
    load_timeout_settings,
)
from swegen.production_quota import ProductionQuota
from swegen.proxy import add_proxy_setup, copy_proxy_certificate
from swegen.swr import upload_image_to_swr

# Run-local directory name for post-processed copies of successful tasks.
POSTPROCESSED_OUTPUT_NAME = "tasks_bz"
PROGRESS_JSONL_NAME = "orchestrator-progress.jsonl"
INSTANCE_STATUS_JSONL_NAME = "orchestrator-instance-status.jsonl"
PRODUCTION_QUOTA_NAME = "production-quota.json"
DEFAULT_RUNS_DIR = Path("runs")
DEFAULT_REPO_CACHE_DIR = Path("data_cache/repos")
RUN_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%SZ"
SWEGEN_IMAGE_SUFFIX = "-swegenimage"

# Slurm nodes to distribute across when --slurm is set.
SLURM_NODES = [
    "ecs-z00579134-20260707-bugfix-0003",
    "ecs-z00579134-20260707-bugfix-0002",
    "ecs-z00579134-20260707-bugfix-0005",
    "ecs-z00579134-20260707-bugfix-0006",
]

# Substrings in a failed `swegen create` run that indicate a transient
# network/API error worth retrying (vs. a genuine task failure like a trivial
# PR or unparseable LLM output). Matched against the tail of the run's output.
RETRYABLE_ERROR_SIGNATURES = (
    "socket connection was closed unexpectedly",
    "Connection error",
    "Connection reset",
    "ECONNRESET",
    "ETIMEDOUT",
    "Overloaded",
    "overloaded_error",
    "rate_limit_error",
    "Internal server error",
    "502 Bad Gateway",
    "503 Service Unavailable",
    "504 Gateway Timeout",
    "529",
)
# Substrings indicating the active GitHub token is rate limited / forbidden, so
# the run should be retried with a *different* token from the configured pool.
# Covers both the GitHub REST API (HTTP 403/429, abuse detection) and a git
# clone over HTTPS hitting the same limits.
GITHUB_RATE_LIMIT_SIGNATURES = (
    "API rate limit exceeded",
    "secondary rate limit",
    "abuse detection",
    "You have exceeded a secondary rate limit",
    "error: 403",
    "error: 429",
    "HTTP 403",
    "HTTP 429",
    "status code 403",
    "status code 429",
    "returned error: 403",
    "returned error: 429",
)

# Base seconds to back off between retries (scaled by attempt number).
RETRY_BACKOFF_SEC = 5

# Post-processing: the skeleton Dockerfile's base image is rewritten to the
# internal mirror so generated tasks build against it.
DOCKERFILE_BASE_REPLACEMENT = (
    "FROM swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/swesandbox"
)

LOG_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?\+00:00\s+")
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


@dataclass
class Entry:
    """A single PR to process."""

    repo: str
    pull_number: str
    base_commit: str = ""
    instance_id: str = ""
    swegen_retries: int = 0


@dataclass
class Outcome:
    """Result of one `swegen create` invocation."""

    worker_id: int
    entry: Entry
    returncode: int
    failure_reason: str = ""
    postprocess_status: str = ""
    hacking_status: str = ""
    swr_upload_status: str = ""
    swr_remote_ref: str = ""
    image_prune_allowed: bool = True
    image_names: tuple[str, ...] = ()
    image_ids: tuple[str, ...] = ()
    compose_projects: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.returncode == 0


@dataclass(frozen=True)
class ProcessResult:
    """Local result from a consumer's `swegen create` subprocess."""

    returncode: int
    failure_reason: str = ""
    image_names: tuple[str, ...] = ()
    image_ids: tuple[str, ...] = ()
    compose_projects: tuple[str, ...] = ()


@dataclass(frozen=True)
class ImagePruneResult:
    """Result of targeted Docker image removal for one completed instance."""

    requested: int
    removed: int
    missing: int
    failed: int
    image_ids: tuple[str, ...] = ()


class TimestampedLog:
    """Line-prefixing wrapper for orchestrator worker logs."""

    def __init__(self, fh):
        self._fh = fh
        self._at_line_start = True

    @staticmethod
    def _timestamp() -> str:
        return datetime.now(UTC).isoformat(timespec="seconds")

    def write(self, text: str) -> int:
        if not text:
            return 0

        for chunk in text.splitlines(keepends=True):
            if self._at_line_start:
                self._fh.write(f"{self._timestamp()} ")
            self._fh.write(chunk)
            self._at_line_start = chunk.endswith("\n")
        return len(text)

    def flush(self) -> None:
        self._fh.flush()

    def fileno(self) -> int:
        return self._fh.fileno()


def run_command_to_log(cmd: list[str], env: dict[str, str], log: TimestampedLog) -> int:
    """Run a command and timestamp each combined stdout/stderr line."""
    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
        bufsize=1,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        log.write(line)
        log.flush()
    return proc.wait()


def build_child_env(_args: argparse.Namespace) -> dict[str, str]:
    """Build child process environment only from swegen.toml-managed secrets."""
    return configured_subprocess_env("swegen-orchestrator")


def _timestamp_run_name() -> str:
    return datetime.now(UTC).strftime(RUN_TIMESTAMP_FORMAT)


def resolve_run_dir(runs_dir: Path, run_name: str | None) -> tuple[str, Path]:
    """Return the run name and directory, creating a fresh timestamp name by default."""
    if run_name:
        return run_name, runs_dir / run_name

    base_name = _timestamp_run_name()
    candidate = runs_dir / base_name
    suffix = 1
    while candidate.exists():
        name = f"{base_name}-{suffix:02d}"
        candidate = runs_dir / name
        suffix += 1
    return candidate.name, candidate


def resolve_run_layout(args: argparse.Namespace) -> None:
    """Populate derived run-local paths on parsed args."""
    args.run_name, args.run_dir = resolve_run_dir(args.runs_dir, args.run_name)
    args.state_dir = args.run_dir

    if args.tasks_dir is None:
        args.tasks_dir = args.run_dir / "tasks"
    if args.postprocessed_dir is None:
        args.postprocessed_dir = args.run_dir / POSTPROCESSED_OUTPUT_NAME
    if args.log_dir is None:
        args.log_dir = args.run_dir / "orchestrator-logs"
    if args.progress_jsonl is None:
        args.progress_jsonl = args.run_dir / PROGRESS_JSONL_NAME
    if args.instance_status_jsonl is None:
        args.instance_status_jsonl = args.run_dir / INSTANCE_STATUS_JSONL_NAME

    args.repo_cache_dir = args.repo_cache_dir or DEFAULT_REPO_CACHE_DIR


def create_run_dirs(args: argparse.Namespace) -> None:
    """Ensure the standard run directory layout exists."""
    for path in (
        args.run_dir,
        args.run_dir / "harbor-jobs",
        args.run_dir / "logs",
        args.log_dir,
        args.tasks_dir,
        args.postprocessed_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)
    args.repo_cache_dir.mkdir(parents=True, exist_ok=True)
    args.progress_jsonl.parent.mkdir(parents=True, exist_ok=True)
    args.instance_status_jsonl.parent.mkdir(parents=True, exist_ok=True)


def build_child_command(args: argparse.Namespace, node_log_dir: Path) -> list[str]:
    """Build the argv for the per-node orchestrator run (no --slurm).

    Secrets are intentionally NOT passed as flags (they would be visible via
    squeue/scontrol); they travel to the node through the exported environment.
    """
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--workers",
        str(args.workers),
        "--runs-dir",
        str(args.runs_dir),
        "--run-name",
        args.run_name,
        "--repo-cache-dir",
        str(args.repo_cache_dir),
        "--log-dir",
        str(node_log_dir),
        "--progress-jsonl",
        str(node_log_dir / PROGRESS_JSONL_NAME),
        "--instance-status-jsonl",
        str(node_log_dir / INSTANCE_STATUS_JSONL_NAME),
        # Shared, flat output dir — outputs are NOT subfoldered per node.
        "--output",
        str(args.tasks_dir),
        # Shared postprocessed-output tree for the post-processed copies.
        "--postprocessed-output",
        str(args.postprocessed_dir),
        "--swegen-bin",
        args.swegen_bin,
    ]
    if args.cc_timeout is not None:
        cmd += ["--cc-timeout", str(args.cc_timeout)]
    cmd += ["--transient-attempts", str(args.transient_attempts)]
    if args.force_rebuild:
        cmd.append("--force-rebuild")
    if args.include_obs_missing:
        cmd.append("--include-obs-missing")
    return cmd


def submit_slurm_jobs(args: argparse.Namespace, env: dict[str, str]) -> int:
    """Submit identical DB-backed workers to every configured Slurm node.

    Returns 0 if every non-empty chunk was submitted successfully, else 1.
    """
    cwd = os.getcwd()
    submitted: list[tuple[str, str]] = []  # (node, job_id)
    failures = 0

    for node in SLURM_NODES:
        node_log_dir = args.log_dir / node
        node_log_dir.mkdir(parents=True, exist_ok=True)
        child_cmd = build_child_command(args, node_log_dir)
        sbatch_cmd = [
            "sbatch",
            f"--nodelist={node}",
            "--nodes=1",
            "--ntasks=1",
            "--exclusive",
            f"--job-name=swegen-{node}",
            f"--chdir={cwd}",
            f"--output={node_log_dir / 'slurm-%j.out'}",
            "--export=ALL",
            f"--wrap={shlex.join(child_cmd)}",
        ]

        try:
            proc = subprocess.run(sbatch_cmd, env=env, capture_output=True, text=True)
        except FileNotFoundError:
            print(
                "error: 'sbatch' not found; is slurm installed / on PATH?",
                file=sys.stderr,
            )
            return 1

        if proc.returncode != 0:
            failures += 1
            print(
                f"  {node}: sbatch failed (rc={proc.returncode}): {proc.stderr.strip()}",
                file=sys.stderr,
            )
            continue

        # sbatch prints "Submitted batch job <id>"
        job_id = proc.stdout.strip().split()[-1] if proc.stdout.strip() else "?"
        submitted.append((node, job_id))
        print(
            f"  {node}: database-backed workers -> job {job_id} (logs: {node_log_dir}/)",
            flush=True,
        )

    print(
        f"\nSubmitted {len(submitted)} sbatch job(s); {failures} submission(s) failed.",
        flush=True,
    )
    if submitted:
        print(f"Track with: squeue -u {os.environ.get('USER', 'alex')}", flush=True)
    return 0 if failures == 0 and submitted else 1


def task_dir_name(repo: str, pull_number: str) -> str:
    """Compute the task directory name `swegen create` writes for a PR.

    Mirrors swegen's default naming (see create/orchestrator.py): the repo is
    lowercased with ``/`` replaced by ``__`` and suffixed with ``-<pr>``. e.g.
    ``0no-co/gql.tada`` PR 460 → ``0no-co__gql.tada-460``.
    """
    repo_slug = repo.lower().replace("/", "__")
    return f"{repo_slug}-{pull_number}"


def entry_instance_id(entry: Entry) -> str:
    return entry.instance_id or task_dir_name(entry.repo, entry.pull_number)


def _docker_image_name(name: str) -> str:
    """Mirror Harbor's Docker image-name sanitization."""
    name = name.lower()
    if not re.match(r"^[a-z0-9]", name):
        name = "0" + name
    return re.sub(r"[^a-z0-9._-]", "-", name)


def _docker_compose_project_name(name: str) -> str:
    """Mirror Harbor's Docker Compose project-name sanitization."""
    name = name.lower()
    if not re.match(r"^[a-z0-9]", name):
        name = "0" + name
    return re.sub(r"[^a-z0-9_-]", "-", name)


def _image_tag_for_instance(instance: str) -> str:
    """Return the SWE-gen image tag Harbor builds for an instance."""
    image_name = _docker_image_name(f"hb__{instance}")
    if not image_name.endswith(SWEGEN_IMAGE_SUFFIX):
        image_name = f"{image_name}{SWEGEN_IMAGE_SUFFIX}"
    return f"{image_name}:latest"


def _instance_harbor_job_dirs(harbor_jobs_dir: Path, instance: str) -> list[Path]:
    """Return Harbor job parent dirs that belong to one task instance."""
    if not harbor_jobs_dir.exists():
        return []

    prefixes = (
        f"{instance}-nop-",
        f"{instance}-oracle-",
        f"{instance}.nop.",
        f"{instance}.oracle.",
    )
    return sorted(
        path
        for path in harbor_jobs_dir.iterdir()
        if path.is_dir() and path.name.startswith(prefixes)
    )


def _instance_harbor_config_paths(state_dir: Path | None, instance: str) -> set[Path]:
    """Find Harbor trial config files currently recorded for one instance."""
    harbor_jobs_dir = (state_dir / "harbor-jobs") if state_dir else Path(".swegen/harbor-jobs")
    configs: set[Path] = set()
    for job_dir in _instance_harbor_job_dirs(harbor_jobs_dir, instance):
        configs.update(path.resolve() for path in job_dir.rglob("config.json"))
    return configs


def _docker_refs_from_trial_config(config_path: Path, instance: str) -> tuple[str, str] | None:
    """Best-effort Docker tag and Compose project extraction from a trial config."""
    try:
        config = json.loads(config_path.read_text())
    except (OSError, json.JSONDecodeError):
        return None

    trial_name = config.get("trial_name")
    if not isinstance(trial_name, str) or not trial_name.strip():
        return None

    environment = config.get("environment")
    if isinstance(environment, dict) and environment.get("type") != "docker":
        return None

    return (
        _image_tag_for_instance(instance),
        _docker_compose_project_name(trial_name.strip()),
    )


def collect_new_instance_docker_refs(
    state_dir: Path | None,
    instance: str,
    before_configs: set[Path],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Collect Docker tags and Compose projects created after a task started."""
    after_configs = _instance_harbor_config_paths(state_dir, instance)
    image_names: list[str] = []
    compose_projects: list[str] = []
    seen_images: set[str] = set()
    seen_projects: set[str] = set()

    for config_path in sorted(after_configs - before_configs):
        refs = _docker_refs_from_trial_config(config_path, instance)
        if refs is None:
            continue
        image_name, compose_project = refs
        if image_name not in seen_images:
            seen_images.add(image_name)
            image_names.append(image_name)
        if compose_project not in seen_projects:
            seen_projects.add(compose_project)
            compose_projects.append(compose_project)

    return tuple(image_names), tuple(compose_projects)


def _docker_no_such_image(output: str) -> bool:
    return "No such image" in output or "No such object" in output


def _docker_image_id(image_ref: str) -> tuple[str | None, bool]:
    """Return (image_id, missing) for an image ref."""
    proc = subprocess.run(
        ["docker", "image", "inspect", "--format", "{{.Id}}", image_ref],
        check=False,
        capture_output=True,
        text=True,
    )
    output = f"{proc.stdout or ''}\n{proc.stderr or ''}"
    if proc.returncode == 0:
        return proc.stdout.strip(), False
    if _docker_no_such_image(output):
        return None, True
    return None, False


def _docker_image_is_dangling(image_id: str) -> tuple[bool, bool]:
    """Return (is_dangling, missing) for an image id."""
    proc = subprocess.run(
        ["docker", "image", "inspect", "--format", "{{json .RepoTags}}", image_id],
        check=False,
        capture_output=True,
        text=True,
    )
    output = f"{proc.stdout or ''}\n{proc.stderr or ''}"
    if proc.returncode != 0:
        return False, _docker_no_such_image(output)

    try:
        repo_tags = json.loads(proc.stdout.strip() or "null")
    except json.JSONDecodeError:
        return False, False
    return not repo_tags, False


def _dangling_image_ids_for_compose_project(compose_project: str) -> tuple[str, ...]:
    proc = subprocess.run(
        [
            "docker",
            "image",
            "ls",
            "-a",
            "-q",
            "--filter",
            "dangling=true",
            "--filter",
            f"label=com.docker.compose.project={compose_project}",
            "--filter",
            "label=com.docker.compose.service=main",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return ()
    return tuple(line.strip() for line in proc.stdout.splitlines() if line.strip())


def collect_instance_docker_image_ids(
    image_names: tuple[str, ...], compose_projects: tuple[str, ...]
) -> tuple[str, ...]:
    """Resolve Docker image IDs for all known images used by an instance."""
    image_ids: list[str] = []
    seen_ids: set[str] = set()

    def add_image_id(image_id: str | None) -> None:
        if image_id and image_id not in seen_ids:
            seen_ids.add(image_id)
            image_ids.append(image_id)

    for image_name in dict.fromkeys(image_names):
        try:
            image_id, image_missing = _docker_image_id(image_name)
        except FileNotFoundError:
            return tuple(image_ids)
        if not image_missing:
            add_image_id(image_id)

    for compose_project in dict.fromkeys(compose_projects):
        try:
            dangling_ids = _dangling_image_ids_for_compose_project(compose_project)
        except FileNotFoundError:
            return tuple(image_ids)
        for image_id in dangling_ids:
            add_image_id(image_id)

    return tuple(image_ids)


def prune_docker_images(
    image_names: tuple[str, ...], compose_projects: tuple[str, ...]
) -> ImagePruneResult:
    """Remove exact Docker image tags and dangling images for completed trials."""
    unique_names = tuple(dict.fromkeys(image_names))
    unique_projects = tuple(dict.fromkeys(compose_projects))
    image_ids: set[str] = set()
    dangling_ids: set[str] = set()
    requested = len(unique_names)
    removed = 0
    missing = 0
    failed = 0

    for image_name in unique_names:
        try:
            image_id, image_missing = _docker_image_id(image_name)
        except FileNotFoundError:
            failed += len(unique_names) - removed - missing - failed
            return ImagePruneResult(
                requested=requested,
                removed=removed,
                missing=missing,
                failed=failed,
                image_ids=tuple(sorted(image_ids)),
            )

        if image_missing:
            missing += 1
            continue
        if not image_id:
            failed += 1
            continue
        image_ids.add(image_id)

        proc = subprocess.run(
            ["docker", "image", "rm", "--force", image_name],
            check=False,
            capture_output=True,
            text=True,
        )
        output = f"{proc.stdout or ''}\n{proc.stderr or ''}"
        if proc.returncode != 0 and not _docker_no_such_image(output):
            failed += 1

    for compose_project in unique_projects:
        dangling_ids.update(_dangling_image_ids_for_compose_project(compose_project))
    requested += len(dangling_ids)

    all_image_ids = tuple(sorted(image_ids | dangling_ids))

    for image_id in all_image_ids:
        try:
            is_dangling, image_missing = _docker_image_is_dangling(image_id)
        except FileNotFoundError:
            failed += 1
            continue

        if image_missing:
            removed += 1
            continue
        if not is_dangling:
            continue

        proc = subprocess.run(
            ["docker", "image", "rm", "--force", image_id],
            check=False,
            capture_output=True,
            text=True,
        )
        output = f"{proc.stdout or ''}\n{proc.stderr or ''}"
        if proc.returncode == 0:
            removed += 1
        elif _docker_no_such_image(output):
            missing += 1
        else:
            failed += 1

    return ImagePruneResult(
        requested=requested,
        removed=removed,
        missing=missing,
        failed=failed,
        image_ids=all_image_ids,
    )


def _load_legacy_progress_lists(progress_path: Path) -> tuple[list[str], list[str]]:
    """Load success/failure lists from pre-status-file progress records."""
    if not progress_path.exists():
        return [], []

    successful_instances: list[str] = []
    failed_instances: list[str] = []
    try:
        with progress_path.open(errors="replace") as fh:
            for line in fh:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                successes = record.get("successful_instances")
                failures = record.get("failed_instances")
                if isinstance(successes, list) and isinstance(failures, list):
                    successful_instances = [str(item) for item in successes]
                    failed_instances = [str(item) for item in failures]
    except OSError:
        return [], []

    return successful_instances, failed_instances


def _move_instance(instance: str, target: list[str], opposite: list[str]) -> None:
    """Record the latest status for an instance without duplicate list entries."""
    opposite[:] = [item for item in opposite if item != instance]
    if instance not in target:
        target.append(instance)


def _apply_instance_status_record(
    record: dict[str, object],
    successful_instances: list[str],
    failed_instances: list[str],
) -> None:
    """Apply one compact instance-status record to the in-memory lists."""
    instance = record.get("instance")
    status = record.get("status")
    if not isinstance(instance, str):
        return
    if status == "success":
        _move_instance(instance, successful_instances, failed_instances)
    elif status == "failure":
        _move_instance(instance, failed_instances, successful_instances)


def _replay_instance_status_jsonl(
    instance_status_path: Path,
    successful_instances: list[str],
    failed_instances: list[str],
) -> None:
    """Replay compact per-instance status updates, ignoring partial/corrupt lines."""
    if not instance_status_path.exists():
        return

    try:
        with instance_status_path.open(errors="replace") as fh:
            for line in fh:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(record, dict):
                    _apply_instance_status_record(record, successful_instances, failed_instances)
    except OSError:
        return


def _load_progress_lists(
    progress_path: Path, instance_status_path: Path | None = None
) -> tuple[list[str], list[str]]:
    """Load current success/failure lists from legacy progress and status logs."""
    successful_instances, failed_instances = _load_legacy_progress_lists(progress_path)
    if instance_status_path is not None:
        _replay_instance_status_jsonl(instance_status_path, successful_instances, failed_instances)
    return successful_instances, failed_instances


def write_progress_jsonl(
    progress_queue: queue.Queue[Outcome | None],
    progress_path: Path,
    instance_status_path: Path,
) -> None:
    """Write compact per-task progress and per-instance status JSONL records."""
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    instance_status_path.parent.mkdir(parents=True, exist_ok=True)
    successful_instances, failed_instances = _load_progress_lists(
        progress_path, instance_status_path
    )

    with progress_path.open("a") as progress_fh, instance_status_path.open("a") as status_fh:
        while True:
            outcome = progress_queue.get()
            try:
                if outcome is None:
                    break

                instance = entry_instance_id(outcome.entry)
                if outcome.ok:
                    status = "success"
                    _move_instance(instance, successful_instances, failed_instances)
                else:
                    status = "failure"
                    _move_instance(instance, failed_instances, successful_instances)

                prune_result = ImagePruneResult(
                    requested=0,
                    removed=0,
                    missing=0,
                    failed=0,
                    image_ids=outcome.image_ids,
                )
                if outcome.image_prune_allowed and (
                    outcome.image_names or outcome.compose_projects
                ):
                    prune_result = prune_docker_images(
                        outcome.image_names, outcome.compose_projects
                    )

                image_ids = tuple(dict.fromkeys((*outcome.image_ids, *prune_result.image_ids)))
                print(
                    f"[orchestrator] {status}: {instance} image tags: "
                    f"{json.dumps(list(outcome.image_names))}",
                    flush=True,
                )
                print(
                    f"[orchestrator] {status}: {instance} image ids: {json.dumps(list(image_ids))}",
                    flush=True,
                )
                if not outcome.image_prune_allowed:
                    print(
                        f"[orchestrator] {status}: {instance} retained local image "
                        "because SWR upload did not succeed",
                        flush=True,
                    )
                elif outcome.image_names or outcome.compose_projects:
                    print(
                        f"[orchestrator] {status}: {instance} pruned "
                        f"{prune_result.removed} image object(s); "
                        f"{prune_result.missing} already absent; "
                        f"{prune_result.failed} failed",
                        flush=True,
                    )
                else:
                    print(
                        f"[orchestrator] {status}: {instance} no image tags found to prune",
                        flush=True,
                    )

                timestamp = datetime.now(UTC).isoformat()
                total_successes = len(successful_instances)
                total_failures = len(failed_instances)
                total_processed = total_successes + total_failures
                status_record = {
                    "timestamp": timestamp,
                    "event": "instance_status",
                    "status": status,
                    "instance": instance,
                    "repo": outcome.entry.repo,
                    "pull_number": outcome.entry.pull_number,
                    "worker_id": outcome.worker_id,
                    "returncode": outcome.returncode,
                    "hacking_status": outcome.hacking_status,
                    "swr_upload_status": outcome.swr_upload_status,
                    "swr_remote_ref": outcome.swr_remote_ref,
                    "total_successes": total_successes,
                    "total_failures": total_failures,
                    "total_processed": total_processed,
                }
                if not outcome.ok:
                    status_record["failure_reason"] = outcome.failure_reason
                progress_record = {
                    "timestamp": timestamp,
                    "event": "task_finished",
                    "status": status,
                    "instance": instance,
                    "repo": outcome.entry.repo,
                    "pull_number": outcome.entry.pull_number,
                    "worker_id": outcome.worker_id,
                    "returncode": outcome.returncode,
                    "postprocess_status": outcome.postprocess_status,
                    "hacking_status": outcome.hacking_status,
                    "swr_upload_status": outcome.swr_upload_status,
                    "swr_remote_ref": outcome.swr_remote_ref,
                    "image_names": list(outcome.image_names),
                    "image_ids": list(image_ids),
                    "compose_projects": list(outcome.compose_projects),
                    "image_prune": {
                        "requested": prune_result.requested,
                        "removed": prune_result.removed,
                        "missing": prune_result.missing,
                        "failed": prune_result.failed,
                    },
                    "total_successes": total_successes,
                    "total_failures": total_failures,
                    "total_processed": total_processed,
                }
                if not outcome.ok:
                    progress_record["failure_reason"] = outcome.failure_reason
                status_fh.write(json.dumps(status_record) + "\n")
                status_fh.flush()
                progress_fh.write(json.dumps(progress_record) + "\n")
                progress_fh.flush()
            finally:
                progress_queue.task_done()


def pick_github_token(pool: list[str], exclude: set[str]) -> str | None:
    if not pool:
        return None
    candidates = [token for token in pool if token not in exclude] or pool
    return random.choice(candidates)


def fetch_issue_number(repo: str, pull_number: str, tokens: list[str] | None) -> str:
    """Look up the issue number linked to a PR via the GitHub API.

    Picks a random token from ``tokens`` and, on a rate-limit/forbidden response
    (HTTP 403/429), retries with a different token from the pool. Returns the
    linked issue number as a string, or "" if none is found or every attempt
    fails (the field is still written, just empty).
    """
    from swegen.create.pr_fetcher import GitHubPRFetcher

    pool = [t for t in (tokens or []) if t]
    tried: set[str] = set()
    token = pick_github_token(pool, tried)
    if token:
        tried.add(token)
    # At least one attempt even with no tokens (unauthenticated); otherwise one
    # attempt per available token so a rate-limited token can be rotated out.
    attempts = max(1, len(pool))
    last_err: Exception | None = None

    for _ in range(attempts):
        try:
            fetcher = GitHubPRFetcher(repo, int(pull_number), github_token=token)
            issues = fetcher.fetch_linked_issues()
        except Exception as e:  # network error, bad PR, rate limit, etc.
            last_err = e
            status = getattr(getattr(e, "response", None), "status_code", None)
            next_token = pick_github_token(pool, tried)
            if status in (403, 429) and next_token is not None:
                token = next_token
                tried.add(token)
                continue
            break
        else:
            if not issues:
                return ""
            # Prefer the lowest-numbered linked issue for determinism.
            return str(min(int(i["number"]) for i in issues if i.get("number") is not None))

    if last_err is not None:
        print(
            f"warning: could not fetch linked issue for {repo}#{pull_number}: {last_err}",
            file=sys.stderr,
        )
    return ""


def _toml_quote(value: str) -> str:
    """Render a Python string as a TOML basic string (quoted, escaped)."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _dockerfile_path(task_dir: Path) -> Path:
    return task_dir / "environment" / "Dockerfile"


def rewrite_dockerfile_for_bz(task_dir: Path, entry: Entry) -> list[str]:
    """Apply the wce1sr base, proxy, fetch, and dirty-checkout fixes."""
    dockerfile = _dockerfile_path(task_dir)
    if not dockerfile.is_file():
        return []

    text = dockerfile.read_text()
    changes: list[str] = []

    lines = text.splitlines(keepends=True)
    for index, line in enumerate(lines):
        if line.lstrip().upper().startswith("FROM "):
            newline = "\r\n" if line.endswith("\r\n") else "\n"
            replacement = DOCKERFILE_BASE_REPLACEMENT + newline
            if line != replacement:
                lines[index] = replacement
                changes.append("wce1sr base rewritten")
            break
    rendered = "".join(lines)

    rendered, proxy_changed = add_proxy_setup(rendered)
    if proxy_changed:
        changes.append("proxy CA setup added")
    copy_proxy_certificate(task_dir / "environment")

    # A detached SHA may not be present in a shallow clone. Prefix it with an
    # exact fetch, matching processing_examples/add_git_fetch_before_checkout.py.
    checkout_sha_re = re.compile(r"git checkout --detach (?P<sha>[0-9a-fA-F]{7,64})(?![0-9a-fA-F])")
    parts: list[str] = []
    last = 0
    for match in checkout_sha_re.finditer(rendered):
        sha = match.group("sha")
        prefix = f"git fetch --depth=1 origin {sha} && "
        parts.append(rendered[last : match.start()])
        if rendered[max(0, match.start() - len(prefix)) : match.start()] != prefix:
            parts.append(prefix)
            changes.append("git fetch before checkout added")
        parts.append(match.group(0))
        last = match.end()
    parts.append(rendered[last:])
    rendered = "".join(parts)

    # SWR base layers can contain dirty repositories. Reset and clean before
    # every detached checkout while retaining the task's real git clone.
    dirty_checkout_re = re.compile(
        r"(?<!git reset --hard && git clean -fdx && )"
        r"git checkout --detach (?P<target>FETCH_HEAD|[0-9a-fA-F]{7,64})"
    )

    def add_reset(match: re.Match[str]) -> str:
        changes.append("dirty repo reset added")
        return (
            f"git reset --hard && git clean -fdx && git checkout --detach {match.group('target')}"
        )

    rendered = dirty_checkout_re.sub(add_reset, rendered)
    if "git clone " not in rendered:
        raise ValueError("postprocessed Dockerfile must retain a git clone of the repository")

    if rendered != text:
        dockerfile.write_text(rendered)

    return changes


def write_cwm_metadata(task_dir: Path, entry: Entry, issue_number: str) -> bool:
    """Append a [cwm_task_metadata] table to <task_dir>/task.toml.

    Idempotent: if the table already exists, the file is left unchanged.
    Returns True if the table was written, False otherwise.
    """
    toml_path = task_dir / "task.toml"
    if not toml_path.is_file():
        return False
    existing = toml_path.read_text()
    if "[cwm_task_metadata]" in existing:
        return False
    block = (
        "\n[cwm_task_metadata]\n"
        f"repo_full_name = {_toml_quote(entry.repo)}\n"
        f"pr_id = {_toml_quote(entry.pull_number)}\n"
        f"issue_number = {_toml_quote(issue_number)}\n"
        'training_domain = "feature"\n'
        f"source_commit = {_toml_quote(entry.base_commit)}\n"
    )
    # Ensure exactly one blank line separates the new table from prior content.
    sep = "" if existing.endswith("\n") else "\n"
    toml_path.write_text(existing + sep + block)
    return True


def postprocess_task(
    entry: Entry,
    tasks_dir: Path,
    postprocessed_dir: Path,
    tokens: list[str] | None,
) -> str:
    """Copy one successful task into the postprocessed-output tree, then post-process.

    The original task under ``tasks_dir`` is left untouched. The copy under
    ``postprocessed_dir`` gets, in order:
      - the Dockerfile base image rewritten to the wce1sr swesandbox image
      - the bundled Huawei proxy CA installation
      - exact fetches before detached SHA checkouts
      - a hard reset/clean before checkout while retaining the real git clone
      - a ``[cwm_task_metadata]`` table (with linked issue number) on task.toml

    Returns a short status string for logging.
    """
    name = entry_instance_id(entry)
    src_dir = tasks_dir / name
    if not src_dir.is_dir():
        return f"skipped (no task dir {name})"

    dst_dir = postprocessed_dir / name
    if dst_dir.exists():
        shutil.rmtree(dst_dir)
    shutil.copytree(src_dir, dst_dir)

    issue_number = fetch_issue_number(entry.repo, entry.pull_number, tokens)
    dockerfile_changes = rewrite_dockerfile_for_bz(dst_dir, entry)
    toml_ok = write_cwm_metadata(dst_dir, entry, issue_number)

    parts = dockerfile_changes or ["Dockerfile unchanged"]
    parts.extend(
        [
            "task.toml updated" if toml_ok else "task.toml unchanged",
            f"issue={issue_number or 'none'}",
        ]
    )
    return "; ".join(parts)


def run_hacking_check(entry: Entry, tasks_dir: Path, state_dir: Path) -> tuple[bool, str]:
    """Run the standard reward-hacking detector against one produced task."""
    instance = entry_instance_id(entry)
    task_dir = tasks_dir / instance
    if not task_dir.is_dir():
        return False, f"Reward-hacking check failed: missing task directory {task_dir}"
    try:
        result = check_task_sync(task_dir)
        log_dir = state_dir / "hacking-logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        write_instance_log(
            log_dir / f"{instance}.log",
            instance,
            result.llm_results,
        )
    except Exception as exc:
        return False, f"Reward-hacking check unavailable ({type(exc).__name__}: {exc})"
    if result.is_hacking:
        return False, f"Reward hacking detected: {result.reason}"
    return True, result.reason


def _is_retryable_failure(output_tail: str) -> bool:
    """Whether a failed run's output looks like a transient network/API error."""
    return any(sig in output_tail for sig in RETRYABLE_ERROR_SIGNATURES)


def _is_github_rate_limited(output_tail: str) -> bool:
    """Whether a failed run's output looks like a GitHub token rate limit (403/429)."""
    return any(sig in output_tail for sig in GITHUB_RATE_LIMIT_SIGNATURES)


def _clean_log_line(line: str) -> str:
    """Remove local log adornments before storing a compact failure reason."""
    line = ANSI_ESCAPE_RE.sub("", line).strip()
    line = LOG_TIMESTAMP_RE.sub("", line).strip()
    return line


def _meaningful_failure_lines(output_tail: str) -> list[str]:
    """Return non-noise lines from a failed attempt's log tail."""
    lines: list[str] = []
    for raw_line in output_tail.splitlines():
        line = _clean_log_line(raw_line)
        if not line:
            continue
        if set(line) <= {"=", "#", "-", "─", "╭", "╮", "╰", "╯", "│"}:
            continue
        if line.startswith("$ "):
            continue
        lines.append(line)
    return lines


def failure_reason_from_output(output_tail: str, returncode: int) -> str:
    """Extract a short human-readable reason from a failed `swegen create` run."""
    if _is_github_rate_limited(output_tail):
        return "GitHub rate limit or forbidden response"
    if _is_retryable_failure(output_tail):
        return "Transient network/API error"

    lines = _meaningful_failure_lines(output_tail)
    lowered = [(line, line.lower()) for line in lines]

    priority_markers = (
        ("validation failed", "Validation failed (NOP or Oracle)"),
        ("cc did not complete task", None),
        ("cc session timed out", None),
        ("claude code session timed out", None),
        ("claude code session failed", None),
        ("skipped (trivial pr)", "Trivial PR"),
        ("trivial pr", None),
        ("missing linked issue", None),
        ("task already exists", None),
        ("fileexistserror", None),
        ("validationerror", "Validation failed (NOP or Oracle)"),
        ("trivialprerror", "Trivial PR"),
        ("missingissueerror", "Missing linked issue"),
    )
    for marker, summary in priority_markers:
        for line, lower in reversed(lowered):
            if marker in lower:
                if summary is not None:
                    return summary
                return line

    for prefix in ("Error:", "RuntimeError:", "ValueError:", "Exception:"):
        for line in reversed(lines):
            if line.startswith(prefix):
                return line

    if lines:
        return lines[-1]
    return f"swegen create exited with return code {returncode}"


def process_entry(
    worker_id: int,
    entry: Entry,
    tag: str,
    env: dict[str, str],
    swegen_bin: str,
    log,
    log_path: Path,
    cc_timeout: int | None,
    output_dir: Path | None,
    state_dir: Path | None,
    repo_cache_dir: Path | None,
    transient_attempts: int,
) -> ProcessResult:
    """Run `swegen create` for a single PR, writing to the open ``log`` handle.

    A failed run whose output matches a transient network/API error (e.g.
    "socket connection was closed unexpectedly") is attempted up to
    ``transient_attempts`` times with a short backoff; non-transient failures
    are not retried.

    Each fresh child process selects its GitHub credential from the pool in
    swegen.toml. Returns the final return code plus any Harbor image tags and
    Docker image IDs observed during this task's run.
    """
    instance = entry_instance_id(entry)
    before_configs = _instance_harbor_config_paths(state_dir, instance)

    base_cmd = [
        swegen_bin,
        "create",
        "--repo",
        entry.repo,
        "--pr",
        entry.pull_number,
        "--no-require-minimum-difficulty",
        "--no-require-issue",
        "--verbose",
    ]
    # Forward the output directory so tasks land where the orchestrator later
    # post-processes them.
    if output_dir is not None:
        base_cmd += ["--output", str(output_dir)]
    if state_dir is not None:
        base_cmd += ["--state-dir", str(state_dir)]
    if repo_cache_dir is not None:
        base_cmd += ["--repo-cache-dir", str(repo_cache_dir)]
    # Forward the Claude Code session timeout when set; otherwise let
    # `swegen create` use its own default.
    if cc_timeout is not None:
        base_cmd += ["--cc-timeout", str(cc_timeout)]
    base_cmd.append("--keep-image")

    returncode = 1
    final_output_tail = ""

    for attempt in range(1, transient_attempts + 1):
        # The orchestrator owns skip/rebuild decisions by run-local task dirs,
        # so bypass create.jsonl dedupe for every processed entry.
        cmd = base_cmd + ["--force"]
        attempt_note = "" if attempt == 1 else f" (retry {attempt}/{transient_attempts})"
        print(f"{tag} starting{attempt_note}", flush=True)

        log.write(f"\n{'=' * 80}\n{tag}{attempt_note}\n$ {' '.join(cmd)}\n{'=' * 80}\n")
        log.flush()
        start_size = os.fstat(log.fileno()).st_size

        try:
            returncode = run_command_to_log(cmd, env, log)
        except FileNotFoundError:
            # Not transient — abort retries for this entry.
            msg = f"could not find executable {swegen_bin!r}; is swegen installed / on PATH?"
            log.write(msg + "\n")
            print(f"{tag} ERROR: {msg}", flush=True)
            image_names, compose_projects = collect_new_instance_docker_refs(
                state_dir, instance, before_configs
            )
            image_ids = collect_instance_docker_image_ids(image_names, compose_projects)
            return ProcessResult(
                returncode=127,
                failure_reason=msg,
                image_names=image_names,
                image_ids=image_ids,
                compose_projects=compose_projects,
            )
        log.flush()

        if returncode == 0:
            break

        # Inspect the tail of this attempt's output to decide on a retry.
        end_size = os.fstat(log.fileno()).st_size
        output_tail = ""
        try:
            with open(log_path, errors="replace") as reader:
                reader.seek(max(start_size, end_size - 65536))
                output_tail = reader.read()
        except OSError:
            pass
        final_output_tail = output_tail

        rate_limited = _is_github_rate_limited(output_tail)
        if attempt < transient_attempts and (rate_limited or _is_retryable_failure(output_tail)):
            backoff = RETRY_BACKOFF_SEC * attempt
            if rate_limited:
                cause = "github rate limit (retry will reselect from swegen.toml token pool)"
            else:
                cause = "transient error"
            retry_msg = (
                f"{cause} (rc={returncode}); retrying in {backoff}s "
                f"[attempt {attempt + 1}/{transient_attempts}]"
            )
            log.write(f"{tag} {retry_msg}\n")
            log.flush()
            print(f"{tag} {retry_msg}", flush=True)
            time.sleep(backoff)
            continue

        # Either out of retries or a non-transient failure: stop.
        break

    image_names, compose_projects = collect_new_instance_docker_refs(
        state_dir, instance, before_configs
    )
    image_ids = collect_instance_docker_image_ids(image_names, compose_projects)
    status = "OK" if returncode == 0 else f"FAILED rc={returncode}"
    print(f"{tag} {status} (log: {log_path})", flush=True)
    failure_reason = ""
    if returncode != 0:
        failure_reason = failure_reason_from_output(final_output_tail, returncode)
    return ProcessResult(
        returncode=returncode,
        failure_reason=failure_reason,
        image_names=image_names,
        image_ids=image_ids,
        compose_projects=compose_projects,
    )


def process_claimed_entry(
    worker_id: int,
    entry: Entry,
    tag: str,
    database: PRTaskDatabase,
    env: dict[str, str],
    swegen_bin: str,
    log: TimestampedLog,
    log_path: Path,
    cc_timeout: int | None,
    output_dir: Path,
    state_dir: Path,
    repo_cache_dir: Path | None,
    transient_attempts: int,
    token_pool: list[str],
    postprocessed_dir: Path,
) -> Outcome:
    process_result = process_entry(
        worker_id,
        entry,
        tag,
        env,
        swegen_bin,
        log,
        log_path,
        cc_timeout,
        output_dir,
        state_dir,
        repo_cache_dir,
        transient_attempts,
    )
    returncode = process_result.returncode
    failure_reason = process_result.failure_reason
    hacking_status = "not run"
    postprocess_status = ""
    swr_upload_status = "not run"
    swr_remote_ref = ""
    image_prune_allowed = returncode != 0

    if returncode == 0:
        passed_hacking, hacking_reason = run_hacking_check(
            entry,
            output_dir,
            state_dir,
        )
        hacking_status = (
            f"passed: {hacking_reason}" if passed_hacking else f"failed: {hacking_reason}"
        )
        log.write(f"{tag} hacking check: {hacking_status}\n")
        log.flush()
        print(f"{tag} hacking check: {hacking_status}", flush=True)
        if not passed_hacking:
            returncode = 2
            failure_reason = hacking_reason

    if returncode == 0:
        try:
            postprocess_status = postprocess_task(
                entry,
                output_dir,
                postprocessed_dir,
                token_pool,
            )
            if postprocess_status.startswith("skipped"):
                raise RuntimeError(postprocess_status)
        except Exception as exc:
            postprocess_status = f"ERROR: {exc}"
            returncode = 3
            failure_reason = f"Postprocessing failed: {exc}"
        log.write(f"{tag} postprocess: {postprocess_status}\n")
        log.flush()
        print(f"{tag} postprocess: {postprocess_status}", flush=True)

    if returncode == 0:
        try:
            upload_result = upload_image_to_swr(
                entry_instance_id(entry),
                process_result.image_names,
                load_swr_settings(),
            )
            swr_upload_status = upload_result.status
            swr_remote_ref = upload_result.remote_ref
            image_prune_allowed = upload_result.success
        except Exception as exc:
            swr_upload_status = f"SWR upload failed ({type(exc).__name__}: {exc})"
            image_prune_allowed = False
        log.write(f"{tag} SWR upload: {swr_upload_status}\n")
        log.flush()
        print(f"{tag} SWR upload: {swr_upload_status}", flush=True)
        if not image_prune_allowed:
            returncode = 4
            failure_reason = swr_upload_status

    if returncode == 0:
        try:
            database.mark_swegen_passed(entry_instance_id(entry))
        except Exception as exc:
            returncode = 5
            failure_reason = f"Database success update failed: {exc}"
            image_prune_allowed = False

    return Outcome(
        worker_id=worker_id,
        entry=entry,
        returncode=returncode,
        failure_reason=failure_reason,
        postprocess_status=postprocess_status,
        hacking_status=hacking_status,
        swr_upload_status=swr_upload_status,
        swr_remote_ref=swr_remote_ref,
        image_prune_allowed=image_prune_allowed,
        image_names=tuple(
            dict.fromkeys(
                (
                    *process_result.image_names,
                    *((swr_remote_ref,) if image_prune_allowed and swr_remote_ref else ()),
                )
            )
        ),
        image_ids=process_result.image_ids,
        compose_projects=process_result.compose_projects,
    )


def run_consumer(
    worker_id: int,
    database: PRTaskDatabase,
    force_rebuild: bool,
    include_obs_missing: bool,
    lease_seconds_per_task: int,
    env: dict[str, str],
    swegen_bin: str,
    log_dir: Path,
    cc_timeout: int | None = None,
    output_dir: Path | None = None,
    state_dir: Path | None = None,
    repo_cache_dir: Path | None = None,
    transient_attempts: int = 3,
    github_tokens: list[str] | None = None,
    postprocessed_dir: Path | None = None,
    progress_queue: queue.Queue[Outcome | None] | None = None,
    production_quota: ProductionQuota | None = None,
) -> list[Outcome]:
    """Atomically claim and process repository packages until none remain."""
    if output_dir is None or state_dir is None or postprocessed_dir is None:
        raise ValueError("consumer requires output, state, and tasks_bz directories")
    token_pool = github_tokens or []
    outcomes: list[Outcome] = []
    log_path = log_dir / f"worker-{worker_id}.log"

    with log_path.open("w") as raw_log:
        log = TimestampedLog(raw_log)
        while True:
            reservation = production_quota.acquire() if production_quota is not None else None
            if production_quota is not None and reservation is None:
                break
            try:
                claimed = database.claim_repo_package(
                    force_rebuild=force_rebuild,
                    include_obs_missing=include_obs_missing,
                    lease_seconds_per_task=lease_seconds_per_task,
                )
            except Exception:
                if production_quota is not None:
                    assert reservation is not None
                    production_quota.complete(reservation, success=False)
                raise
            if not claimed:
                if production_quota is not None:
                    assert reservation is not None
                    production_quota.complete(reservation, success=False)
                break
            package = [
                Entry(
                    repo=item.repo,
                    pull_number=str(item.pull_number),
                    base_commit=item.base_commit,
                    instance_id=item.instance_id,
                    swegen_retries=item.swegen_retries,
                )
                for item in claimed
            ]

            repo = package[0].repo
            total = len(package)
            log.write(
                f"\n{'#' * 80}\n[worker {worker_id}] claimed package {repo} "
                f"({total} PR{'s' if total != 1 else ''})\n{'#' * 80}\n"
            )
            log.flush()

            for idx, entry in enumerate(package, 1):
                if idx > 1 and production_quota is not None:
                    reservation = production_quota.acquire()
                    if reservation is None:
                        unprocessed = claimed[idx - 1 :]
                        released = database.release_claims(unprocessed)
                        log.write(
                            f"[worker {worker_id}] production target reached; "
                            f"released {released}/{len(unprocessed)} unprocessed claim(s)\n"
                        )
                        log.flush()
                        return outcomes
                tag = f"[worker {worker_id}] ({idx}/{total}) {entry.repo}#{entry.pull_number}"
                assert production_quota is None or reservation is not None
                try:
                    outcome = process_claimed_entry(
                        worker_id,
                        entry,
                        tag,
                        database,
                        env,
                        swegen_bin,
                        log,
                        log_path,
                        cc_timeout,
                        output_dir,
                        state_dir,
                        repo_cache_dir,
                        transient_attempts,
                        token_pool,
                        postprocessed_dir,
                    )
                except Exception:
                    if production_quota is not None:
                        production_quota.complete(reservation, success=False)
                    database.release_claims(claimed[idx:])
                    raise
                if production_quota is not None:
                    production_quota.complete(reservation, success=outcome.ok)
                outcomes.append(outcome)
                if progress_queue is not None:
                    progress_queue.put(outcome)

    return outcomes


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Claim repository groups atomically from swegen.pr_tasks and run "
            "`swegen create` with parallel workers."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--workers",
        type=int,
        required=True,
        help="Number of parallel workers to split the input across.",
    )
    parser.add_argument(
        "--swegen-bin",
        default="swegen",
        help="Path to the swegen executable.",
    )
    parser.add_argument(
        "--cc-timeout",
        type=int,
        default=None,
        help="Timeout (seconds) for each Claude Code session, forwarded to "
        "`swegen create --cc-timeout`. Defaults to swegen's own default when omitted.",
    )
    parser.add_argument(
        "--transient-attempts",
        type=int,
        default=3,
        help="Attempts per PR when a run fails with a transient network/API "
        "error (e.g. socket closed). Set to 1 to disable retries.",
    )
    parser.add_argument(
        "--force-rebuild",
        "--force",
        dest="force_rebuild",
        action="store_true",
        help="Include database rows that already have swegen_bz_passed=true.",
    )
    parser.add_argument(
        "--include-obs-missing",
        action="store_true",
        help="Include rows with obs_exists=false (excluded by default).",
    )
    parser.add_argument(
        "--slurm",
        action="store_true",
        help=f"Distribute the input across slurm nodes {SLURM_NODES[0]}.."
        f"{SLURM_NODES[-1]} via sbatch (one job per node, each running "
        "--workers workers), then exit. Without this flag everything runs locally.",
    )
    parser.add_argument(
        "--runs-dir",
        type=Path,
        default=DEFAULT_RUNS_DIR,
        help="Root directory containing named run folders.",
    )
    parser.add_argument(
        "--run-name",
        default=None,
        help="Run folder name. Defaults to the current UTC timestamp.",
    )
    parser.add_argument(
        "--repo-cache-dir",
        type=Path,
        default=DEFAULT_REPO_CACHE_DIR,
        help="Shared git repo cache directory.",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=None,
        help="Directory for per-worker log files. Defaults to <run>/orchestrator-logs.",
    )
    parser.add_argument(
        "--progress-jsonl",
        type=Path,
        default=None,
        help="Per-task completion JSONL. Defaults to <run>/orchestrator-progress.jsonl.",
    )
    parser.add_argument(
        "--instance-status-jsonl",
        type=Path,
        default=None,
        help="Per-instance status JSONL. Defaults to <run>/orchestrator-instance-status.jsonl.",
    )
    parser.add_argument(
        "--output",
        "-o",
        "--tasks-dir",
        dest="tasks_dir",
        type=Path,
        default=None,
        help="Root task output dir. Defaults to <run>/tasks. (--tasks-dir is a deprecated alias.)",
    )
    parser.add_argument(
        "--postprocessed-output",
        dest="postprocessed_dir",
        type=Path,
        default=None,
        help="Directory for post-processed copies of successful tasks. Defaults "
        f"to <run>/{POSTPROCESSED_OUTPUT_NAME}.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if args.workers < 1:
        print("error: --workers must be >= 1", file=sys.stderr)
        return 2
    if args.transient_attempts < 1:
        print("error: --transient-attempts must be >= 1", file=sys.stderr)
        return 2
    try:
        database_settings = load_database_settings()
        database = PRTaskDatabase(database_settings)
        orchestrator_settings = load_orchestrator_settings()
        timeout_settings = load_timeout_settings()
        github_tokens = load_github_tokens()
        model_settings = load_model_settings()
        if not model_settings.model:
            raise ValueError("[model].model must be set in swegen.toml")
        if not (model_settings.api_key or model_settings.auth_token or model_settings.oauth_token):
            raise ValueError("Claude authentication must be set in swegen.toml [model]")
        load_openai_settings()
        # Validate upload configuration before any expensive task work begins.
        if not load_swr_settings().enabled:
            raise ValueError("[swr].enabled must be true for production orchestration")
        load_llm_configs()
    except Exception as exc:
        print(f"error: invalid swegen.toml configuration: {exc}", file=sys.stderr)
        return 2

    if args.cc_timeout is None:
        args.cc_timeout = timeout_settings.claude_code
    if not github_tokens:
        print(
            "warning: no GitHub token configured in swegen.toml [github]; swegen create and "
            "issue-number lookups will likely fail.",
            file=sys.stderr,
        )
    elif len(github_tokens) > 1:
        print(
            f"Using a pool of {len(github_tokens)} GitHub tokens "
            "(random per task, rotating on rate limit).",
            flush=True,
        )

    resolve_run_layout(args)
    create_run_dirs(args)
    production_quota: ProductionQuota | None = None
    if orchestrator_settings.produce_count is not None:
        stale_after_seconds = max(
            86400,
            timeout_settings.total_seconds_per_task(args.cc_timeout) * args.transient_attempts * 2,
        )
        production_quota = ProductionQuota(
            args.run_dir / PRODUCTION_QUOTA_NAME,
            orchestrator_settings.produce_count,
            stale_after_seconds=stale_after_seconds,
        )
    env = build_child_env(args)

    print(f"Run: {args.run_name} -> {args.run_dir}", flush=True)
    print(f"Repo cache: {args.repo_cache_dir}", flush=True)
    if production_quota is None:
        print("Production target: unbounded (process all eligible PRs)", flush=True)
    else:
        quota_snapshot = production_quota.snapshot()
        print(
            f"Production target: {quota_snapshot.successes}/{quota_snapshot.limit} "
            "successful instance(s)",
            flush=True,
        )
        if quota_snapshot.reached:
            print("Done: configured production target was already reached.", flush=True)
            return 0

    # Slurm workers all claim from the same database; database transactions
    # replace the old JSONL chunking and prevent overlap between nodes.
    if args.slurm:
        print(
            f"Starting database-backed allocation across "
            f"{len(SLURM_NODES)} node(s) via sbatch "
            f"(--workers {args.workers} per node):",
            flush=True,
        )
        return submit_slurm_jobs(args, env)

    num_consumers = args.workers
    lease_seconds_per_task = timeout_settings.lease_seconds_per_task(args.cc_timeout)
    print(
        f"Database allocation -> {num_consumers} consumer(s), "
        f"maximum {database_settings.max_retries} claim(s) per PR, "
        f"lease budget {lease_seconds_per_task}s per claimed PR "
        f"(logs in {args.log_dir}/)",
        flush=True,
    )
    print(
        "Eligible PR categories: " + ", ".join(database_settings.pr_categories),
        flush=True,
    )
    if database_settings.exclude_languages:
        print(
            "Excluded primary languages: " + ", ".join(database_settings.exclude_languages),
            flush=True,
        )

    args.postprocessed_dir.mkdir(parents=True, exist_ok=True)
    print(
        f"Post-processed copies of successful tasks -> {args.postprocessed_dir}/",
        flush=True,
    )
    print(f"Per-task progress JSONL -> {args.progress_jsonl}", flush=True)
    print(f"Per-instance status JSONL -> {args.instance_status_jsonl}", flush=True)

    progress_queue: queue.Queue[Outcome | None] = queue.Queue()
    progress_thread = threading.Thread(
        target=write_progress_jsonl,
        args=(progress_queue, args.progress_jsonl, args.instance_status_jsonl),
        name="progress-writer",
    )
    progress_thread.start()

    all_outcomes: list[Outcome] = []
    try:
        with ThreadPoolExecutor(max_workers=num_consumers) as executor:
            futures = {
                executor.submit(
                    run_consumer,
                    i,
                    database,
                    args.force_rebuild,
                    args.include_obs_missing,
                    lease_seconds_per_task,
                    env,
                    args.swegen_bin,
                    args.log_dir,
                    args.cc_timeout,
                    args.tasks_dir,
                    args.state_dir,
                    args.repo_cache_dir,
                    args.transient_attempts,
                    github_tokens,
                    args.postprocessed_dir,
                    progress_queue,
                    production_quota,
                ): i
                for i in range(num_consumers)
            }
            for future in as_completed(futures):
                all_outcomes.extend(future.result())
    finally:
        progress_queue.put(None)
        progress_thread.join()

    # Post-processing already ran incrementally per task (copied into
    # args.postprocessed_dir and rewritten as each PR succeeded).

    # Summary
    failures = [o for o in all_outcomes if not o.ok]
    succeeded = len(all_outcomes) - len(failures)
    final_quota_snapshot = production_quota.snapshot() if production_quota is not None else None
    print("\n" + "=" * 80)
    if not all_outcomes:
        if final_quota_snapshot is not None and final_quota_snapshot.reached:
            print("Done: configured production target was reached.")
        else:
            print("Done: no eligible database rows were available.")
        print("=" * 80)
        return 0
    print(f"Done: {succeeded}/{len(all_outcomes)} succeeded, {len(failures)} failed.")
    if final_quota_snapshot is not None:
        print(
            f"Production target: {final_quota_snapshot.successes}/"
            f"{final_quota_snapshot.limit} successful instance(s)."
        )
    if failures:
        print("Failed:")
        for o in failures:
            print(
                f"  - {o.entry.repo}#{o.entry.pull_number} "
                f"(worker {o.worker_id}, rc={o.returncode})"
            )
    print("=" * 80)

    if final_quota_snapshot is not None and final_quota_snapshot.reached:
        return 0
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
