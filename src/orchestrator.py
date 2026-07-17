#!/usr/bin/env python3
"""Parallel orchestrator for `swegen create`.

Reads a JSONL file where each line describes one PR to process:

    {"repo": "owner/repo", "pull_number": "1234"}

Work is organized as a producer-consumer pipeline. The entries are grouped into
one "package" per repo (all of that repo's PRs that still need processing); a
producer thread feeds those packages onto a queue, and ``--workers`` consumer
threads each pull a package, process its PRs sequentially, and then pull the
next available package. Keeping a whole repo inside a single consumer ensures
the shared per-repo git cache (data_cache/repos/<repo>) is never touched by two
consumers at once, while idle consumers immediately grab more work instead of
waiting on a fixed segment.

Each invocation writes to a run directory. If ``--run-name`` is omitted, the
run name defaults to the current UTC timestamp under ``runs/``. If a run name is
specified, that existing run directory is reused; PRs whose task directories
already exist under ``<run>/tasks`` are skipped unless ``--force`` is set. The
shared git clone cache lives under ``data_cache/repos`` by default.

Each run contains ``tasks/``, ``tasks_voyager_postprocessed/``,
``orchestrator-logs/``, ``logs/``, ``harbor-jobs/``, and an
``orchestrator-progress.jsonl`` completion log. As soon as a PR's
``swegen create`` run succeeds, that task is copied into
``tasks_voyager_postprocessed/`` and all post-processing is applied to the
*copy*, leaving the original task untouched. Post-processing rewrites the
Dockerfile base image to the internal mirror, moves the preloaded Voyager
repository into the path expected by the task, replaces the generated git clone
block with a checkout of that preloaded repository, and appends a
``[cwm_task_metadata]`` table (with the linked issue number) to task.toml.

Within a package, each PR is processed sequentially by shelling out to:

    swegen create --repo <repo> --pr <pr> \
        --no-require-minimum-difficulty --no-require-issue

Secrets (GITHUB_TOKEN, OPENAI_API_KEY, ANTHROPIC_API_KEY, ANTHROPIC_AUTH_TOKEN,
ANTHROPIC_BASE_URL, OPENAI_BASE_URL) may be passed as flags or inherited from
the environment; whatever is resolved is injected into each child process.

Example:
    python src/orchestrator.py prs.jsonl --workers 4 \
        --github-token "$GITHUB_TOKEN" --anthropic-base-url "$ANTHROPIC_BASE_URL"
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
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import requests
from dotenv import load_dotenv

from swegen.net import github_requests_kwargs, requests_ssl_kwargs

# Run-local directory name for post-processed copies of successful tasks.
POSTPROCESSED_OUTPUT_NAME = "tasks_voyager_postprocessed"
PROGRESS_JSONL_NAME = "orchestrator-progress.jsonl"
INSTANCE_STATUS_JSONL_NAME = "orchestrator-instance-status.jsonl"
DEFAULT_RUNS_DIR = Path("runs")
DEFAULT_REPO_CACHE_DIR = Path("data_cache/repos")
RUN_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%SZ"
SWEGEN_IMAGE_SUFFIX = "-swegenimage"

# The legacy in-process Slurm fan-out assumed a shared filesystem and obsolete
# node names.  It remains parseable only to emit a migration error; new Slurm
# runs are planned and staged by ``src/slurm_two_node.py``.
SLURM_NODES: list[str] = []

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
    "503 Server Error: Service Unavailable",
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
    "403 Client Error: Forbidden for url: https://api.github.com/",
    "429 Client Error: Too Many Requests for url: https://api.github.com/",
    "503 Server Error: Service Unavailable for url: https://api.github.com/",
)

# Base seconds to back off between retries (scaled by attempt number).
RETRY_BACKOFF_SEC = 5

# Batch runs should not let one Claude session occupy a worker indefinitely.
# Explicitly cap forwarded --cc-timeout values at three hours; shorter values
# remain valid for targeted runs.
MAX_CC_TIMEOUT_SECONDS = 3 * 60 * 60

# Post-processing: the skeleton Dockerfile's base image is rewritten to the
# internal mirror so generated tasks build against it.
DOCKERFILE_BASE_FROM = "FROM ubuntu:24.04"
DOCKERFILE_BASE_REPLACEMENT = (
    "FROM swr-aifm-code-data-platform-6sudmx.swr-pro.myhuaweicloud.com/"
    "swesandbox/ubuntu:24.04"
)

# Post-processing: Voyager repo images already contain the repository under
# /app/<owner>/<repo>, so generated Dockerfiles should move that checkout to the
# task's working path and then check out the fixed PR commit there.
VOYAGER_PATH_MARKER = (
    "# Move the preloaded Voyager repository to the path expected by the task."
)
FROM_RE = re.compile(r"^FROM\s+\S+(?P<suffix>.*)$")
HEAD_SHA_RE = re.compile(r"\b[0-9a-f]{40}\b")
LOG_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?\+00:00\s+"
)
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
GIT_FETCH_HEAD_RE = re.compile(
    r"\bgit fetch(?: --depth \d+)? origin (?P<sha>[0-9a-f]{40})\b"
)
GIT_CHECKOUT_RE = re.compile(
    r"^RUN cd (?P<path>\S+) && git checkout --detach (?P<sha>[0-9a-f]{40})$"
)

# Env vars forwarded to each `swegen create` subprocess. Maps the CLI flag name
# to the environment variable name.
SECRET_ENV_VARS = {
    "github_token": "GITHUB_TOKEN",
    "openai_api_key": "OPENAI_API_KEY",
    "anthropic_api_key": "ANTHROPIC_API_KEY",
    "anthropic_auth_token": "ANTHROPIC_AUTH_TOKEN",
    "anthropic_base_url": "ANTHROPIC_BASE_URL",
    "openai_base_url": "OPENAI_BASE_URL",
}

WORKER_PROXY_POOL_ENV = "SWEGEN_WORKER_PROXY_POOL"
CLAUDE_PROXY_POOL_ENV = "SWEGEN_CLAUDE_PROXY_POOL"
PROXY_CAPACITY_ENV = "SWEGEN_PROXY_WORKERS_PER_ENDPOINT"
DEFAULT_PROXY_CAPACITY = 16


@dataclass
class Entry:
    """A single PR to process."""

    repo: str
    pull_number: str
    base_commit: str = ""
    image_ref: str = ""


@dataclass
class Outcome:
    """Result of one `swegen create` invocation."""

    worker_id: int
    entry: Entry
    returncode: int
    failure_reason: str = ""
    postprocess_status: str = ""
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


def run_command_to_log(
    cmd: list[str], env: dict[str, str], log: TimestampedLog
) -> int:
    """Run a command and timestamp each combined stdout/stderr line."""
    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
        bufsize=1,
        # Give every PR attempt its own process group. This lets the stale-task
        # watchdog terminate one wedged Claude/Harbor tree without taking down
        # the long-lived multi-worker orchestrator or neighboring tasks.
        start_new_session=True,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        log.write(line)
        log.flush()
    return proc.wait()


def load_entries(jsonl_path: Path) -> list[Entry]:
    """Parse the input JSONL into a list of Entry, skipping blank lines."""
    entries: list[Entry] = []
    with jsonl_path.open() as fh:
        for lineno, raw in enumerate(fh, 1):
            line = raw.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as err:
                raise ValueError(f"{jsonl_path}:{lineno}: invalid JSON: {err}") from err
            try:
                repo = str(obj["repo"]).strip()
                pull_number = str(obj["pull_number"]).strip()
            except KeyError as err:
                raise ValueError(
                    f"{jsonl_path}:{lineno}: missing required key {err}"
                ) from err
            if not repo or not pull_number:
                raise ValueError(
                    f"{jsonl_path}:{lineno}: empty 'repo' or 'pull_number'"
                )
            # Optional: used by post-processing to populate task metadata.
            base_commit = str(obj.get("base_commit", "")).strip()
            image_ref = str(
                obj.get("image_ref") or obj.get("voyager_image_ref") or ""
            ).strip()
            entries.append(
                Entry(
                    repo=repo,
                    pull_number=pull_number,
                    base_commit=base_commit,
                    image_ref=image_ref,
                )
            )
    return entries


def _pr_desc_key(entry: Entry) -> tuple[int, int, str]:
    """Sort key ordering a repo's PRs from highest PR number to lowest.

    Numeric PR numbers come first (descending); any non-numeric values sort
    after them, alphabetically, so the result is always deterministic.
    """
    try:
        return (0, -int(entry.pull_number), "")
    except ValueError:
        return (1, 0, entry.pull_number)


def split_into_segments(items: list[Entry], n: int) -> list[list[Entry]]:
    """Partition entries into up to ``n`` balanced segments, one repo per segment.

    Every PR of a given repo is kept together in a single segment, so the shared
    per-repo git cache (data_cache/repos/<repo>) is never touched by two workers at
    once. Repos are distributed greedily (largest group first → least-loaded
    segment) to balance the number of PRs per segment as evenly as the
    one-repo-per-segment constraint allows. Within a segment, each repo's PRs are
    ordered from highest PR number to lowest, and same-repo PRs are contiguous.

    Returns at most ``min(n, number-of-repos)`` non-empty segments.
    """
    if not items:
        return []

    # Group by repo (first-seen order kept only for stable tie-breaking later).
    groups: dict[str, list[Entry]] = {}
    for entry in items:
        groups.setdefault(entry.repo, []).append(entry)

    # Order each repo's PRs highest → lowest.
    for entries in groups.values():
        entries.sort(key=_pr_desc_key)

    num_bins = max(1, min(n, len(groups)))
    bins: list[list[Entry]] = [[] for _ in range(num_bins)]
    loads = [0] * num_bins

    # Greedy longest-processing-time: assign the largest repo groups first, each
    # to the currently least-loaded segment (ties broken by lowest index).
    for _repo, entries in sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        target = min(range(num_bins), key=lambda i: (loads[i], i))
        bins[target].extend(entries)
        loads[target] += len(entries)

    return bins


def build_packages(
    items: list[Entry],
    output_dir: Path,
    force: bool,
    *,
    state_dir: Path | None = None,
    progress_path: Path | None = None,
    instance_status_path: Path | None = None,
    postprocessed_dir: Path | None = None,
) -> tuple[list[list[Entry]], int]:
    """Group entries into one "package" per repo for the producer-consumer queue.

    A package holds every PR of a single repo that still needs processing. Unless
    ``force`` is set, only instances recorded as successfully produced are
    dropped. A failed or interrupted task may still have a partial task directory;
    it must remain in the package so a resumed run can overwrite and retry it.
    Within a package the PRs are ordered highest PR number to lowest, matching the
    old per-segment ordering.

    Returns ``(packages, skipped)`` where ``skipped`` is the number of existing
    PRs filtered out. Largest packages are returned first so consumers start the
    longest-running repos earliest (better tail-latency under producer-consumer).
    """
    groups: dict[str, list[Entry]] = {}
    for entry in items:
        groups.setdefault(entry.repo, []).append(entry)

    packages: list[list[Entry]] = []
    skipped = 0
    for repo in sorted(groups):
        entries = sorted(groups[repo], key=_pr_desc_key)
        if force:
            kept = entries
        else:
            kept = [
                e
                for e in entries
                if not instance_successfully_produced(
                    e,
                    state_dir,
                    progress_path,
                    instance_status_path,
                    output_dir,
                    postprocessed_dir,
                )
            ]
            skipped += len(entries) - len(kept)
        if kept:
            packages.append(kept)

    # Longest package first: keeps the slowest repos from starting last.
    packages.sort(key=len, reverse=True)
    return packages, skipped


def build_child_env(args: argparse.Namespace) -> dict[str, str]:
    """Build the environment for child processes: inherit, then override with
    any resolved secrets."""
    env = os.environ.copy()
    for flag_name, env_name in SECRET_ENV_VARS.items():
        value = getattr(args, flag_name)
        if value:
            env[env_name] = value
    return env


def _proxy_pool(env: dict[str, str], key: str) -> list[str]:
    return [item.strip() for item in env.get(key, "").split(",") if item.strip()]


def _proxy_bypass_value(env: dict[str, str], proxy_urls: list[str]) -> str:
    """Return NO_PROXY with every proxy endpoint host included exactly."""
    entries: list[str] = []
    seen: set[str] = set()

    for value in (env.get("no_proxy", ""), env.get("NO_PROXY", "")):
        for item in value.split(","):
            item = item.strip()
            normalized = item.casefold()
            if item and normalized not in seen:
                seen.add(normalized)
                entries.append(item)

    for proxy_url in proxy_urls:
        host = urlsplit(proxy_url).hostname
        normalized = host.casefold() if host else ""
        if host and normalized not in seen:
            seen.add(normalized)
            entries.append(host)

    return ",".join(entries)


def _proxy_capacity(env: dict[str, str]) -> int:
    raw = env.get(PROXY_CAPACITY_ENV, str(DEFAULT_PROXY_CAPACITY)).strip()
    try:
        capacity = int(raw)
    except ValueError as error:
        raise ValueError(f"{PROXY_CAPACITY_ENV} must be an integer, got {raw!r}") from error
    if capacity <= 0:
        raise ValueError(f"{PROXY_CAPACITY_ENV} must be positive")
    return capacity


def validate_worker_proxy_config(env: dict[str, str], workers: int) -> list[str]:
    """Validate proxy pools and return human-readable worker assignments."""
    socks_pool = _proxy_pool(env, WORKER_PROXY_POOL_ENV)
    claude_pool = _proxy_pool(env, CLAUDE_PROXY_POOL_ENV)
    if not socks_pool and not claude_pool:
        return []
    if not socks_pool or not claude_pool:
        raise ValueError(
            f"{WORKER_PROXY_POOL_ENV} and {CLAUDE_PROXY_POOL_ENV} must both be set"
        )
    if len(socks_pool) != len(claude_pool):
        raise ValueError("worker SOCKS and Claude proxy pools must have equal lengths")

    capacity = _proxy_capacity(env)
    maximum = len(socks_pool) * capacity
    if workers > maximum:
        raise ValueError(
            f"{workers} workers exceed proxy capacity {maximum} "
            f"({len(socks_pool)} endpoints x {capacity})"
        )

    assignments: list[str] = []
    for index, socks_proxy in enumerate(socks_pool):
        first = index * capacity
        if first >= workers:
            break
        last = min(workers, first + capacity) - 1
        assignments.append(
            f"workers {first}-{last}: {socks_proxy} "
            f"(Claude bridge {claude_pool[index]})"
        )
    return assignments


def worker_child_env(base_env: dict[str, str], worker_id: int) -> dict[str, str]:
    """Return a child environment pinned to this worker's proxy endpoint."""
    socks_pool = _proxy_pool(base_env, WORKER_PROXY_POOL_ENV)
    if not socks_pool:
        return base_env
    claude_pool = _proxy_pool(base_env, CLAUDE_PROXY_POOL_ENV)
    capacity = _proxy_capacity(base_env)
    endpoint_index = worker_id // capacity
    if endpoint_index >= len(socks_pool) or endpoint_index >= len(claude_pool):
        raise ValueError(f"worker {worker_id} has no configured proxy endpoint")

    socks_proxy = socks_pool[endpoint_index]
    claude_proxy = claude_pool[endpoint_index]
    plain_http_proxy = base_env.get("GIT_PROXY", "").strip() or claude_proxy
    no_proxy = _proxy_bypass_value(base_env, socks_pool)
    env = base_env.copy()
    env.update(
        {
            # Some HTTP client stacks accept a socks5:// URL syntactically but
            # still send an HTTP CONNECT request to it. Route all HTTPS traffic
            # through our HTTP-to-SOCKS bridge so only the bridge ever speaks
            # the raw SOCKS5 protocol. Plain HTTP stays on the SG proxy.
            "http_proxy": plain_http_proxy,
            "https_proxy": claude_proxy,
            "HTTP_PROXY": plain_http_proxy,
            "HTTPS_PROXY": claude_proxy,
            "ALL_PROXY": claude_proxy,
            "SWEGEN_CLAUDE_PROXY": claude_proxy,
            "SWEGEN_CLAUDE_HTTP_PROXY": plain_http_proxy,
            "SWEGEN_ASSIGNED_SOCKS_PROXY": socks_proxy,
            "SWEGEN_PROXY_ENDPOINT_INDEX": str(endpoint_index),
            # A worker may probe or connect to its SOCKS endpoint directly.
            # Exact host entries are required because wildcard forms such as
            # ``10.*`` are not interpreted consistently across HTTP stacks.
            "no_proxy": no_proxy,
            "NO_PROXY": no_proxy,
        }
    )
    return env


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


def write_chunk(path: Path, segment: list[Entry]) -> None:
    """Write a segment of entries to a JSONL chunk file for a slurm child run.

    Preserves the fields the child needs for downstream post-processing and
    cwm_task_metadata.
    """
    with path.open("w") as fh:
        for entry in segment:
            fh.write(
                json.dumps(
                    {
                        "repo": entry.repo,
                        "pull_number": entry.pull_number,
                        "base_commit": entry.base_commit,
                        "image_ref": entry.image_ref,
                    }
                )
                + "\n"
            )


def build_child_command(
    args: argparse.Namespace, chunk_path: Path, node_log_dir: Path
) -> list[str]:
    """Build the argv for the per-node orchestrator run (no --slurm).

    Secrets are intentionally NOT passed as flags (they would be visible via
    squeue/scontrol); they travel to the node through the exported environment.
    """
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        str(chunk_path),
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
    cmd += ["--max-retries", str(args.max_retries)]
    if args.force:
        cmd.append("--force")
    return cmd


def submit_slurm_jobs(
    entries: list[Entry], args: argparse.Namespace, env: dict[str, str]
) -> int:
    """Submit one sbatch job per node over SLURM_NODES, then return.

    Returns 0 if every non-empty chunk was submitted successfully, else 1.
    """
    segments = split_into_segments(entries, len(SLURM_NODES))
    cwd = os.getcwd()
    submitted: list[tuple[str, str]] = []  # (node, job_id)
    failures = 0

    for node, segment in zip(SLURM_NODES, segments):
        if not segment:
            continue
        node_log_dir = args.log_dir / node
        node_log_dir.mkdir(parents=True, exist_ok=True)
        chunk_path = node_log_dir / "chunk.jsonl"
        write_chunk(chunk_path, segment)

        child_cmd = build_child_command(args, chunk_path, node_log_dir)
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
            proc = subprocess.run(
                sbatch_cmd, env=env, capture_output=True, text=True
            )
        except FileNotFoundError:
            print(
                "error: 'sbatch' not found; is slurm installed / on PATH?",
                file=sys.stderr,
            )
            return 1

        if proc.returncode != 0:
            failures += 1
            print(
                f"  {node}: sbatch failed (rc={proc.returncode}): "
                f"{proc.stderr.strip()}",
                file=sys.stderr,
            )
            continue

        # sbatch prints "Submitted batch job <id>"
        job_id = proc.stdout.strip().split()[-1] if proc.stdout.strip() else "?"
        submitted.append((node, job_id))
        print(
            f"  {node}: {len(segment)} entr"
            f"{'y' if len(segment) == 1 else 'ies'} -> job {job_id} "
            f"(logs: {node_log_dir}/)",
            flush=True,
        )

    print(
        f"\nSubmitted {len(submitted)} sbatch job(s); "
        f"{failures} submission(s) failed.",
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


def _instance_harbor_config_paths(
    state_dir: Path | None, instance: str
) -> set[Path]:
    """Find Harbor trial config files currently recorded for one instance."""
    harbor_jobs_dir = (
        (state_dir / "harbor-jobs") if state_dir else Path(".swegen/harbor-jobs")
    )
    configs: set[Path] = set()
    for job_dir in _instance_harbor_job_dirs(harbor_jobs_dir, instance):
        configs.update(path.resolve() for path in job_dir.rglob("config.json"))
    return configs


def _docker_refs_from_trial_config(
    config_path: Path, instance: str
) -> tuple[str, str] | None:
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


def _move_instance(
    instance: str, target: list[str], opposite: list[str]
) -> None:
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
                    _apply_instance_status_record(
                        record, successful_instances, failed_instances
                    )
    except OSError:
        return


def _load_progress_lists(
    progress_path: Path, instance_status_path: Path | None = None
) -> tuple[list[str], list[str]]:
    """Load current success/failure lists from legacy progress and status logs."""
    successful_instances, failed_instances = _load_legacy_progress_lists(progress_path)
    if instance_status_path is not None:
        _replay_instance_status_jsonl(
            instance_status_path, successful_instances, failed_instances
        )
    return successful_instances, failed_instances


def _create_log_has_success(create_log_path: Path, instance: str) -> bool:
    """Return True if run-local create.jsonl has a successful task record."""
    if not create_log_path.exists():
        return False

    try:
        with create_log_path.open(errors="replace") as fh:
            for line in fh:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(record, dict):
                    continue
                if record.get("task_id") == instance:
                    return True
                harbor = record.get("harbor")
                if isinstance(harbor, str) and Path(harbor).name == instance:
                    return True
    except OSError:
        return False

    return False


def _instance_artifact_exists(
    instance: str, output_dir: Path | None, postprocessed_dir: Path | None
) -> bool:
    """Return True if one of the run-local produced task directories exists."""
    candidate_roots = [
        path for path in (output_dir, postprocessed_dir) if path is not None
    ]
    if not candidate_roots:
        return True
    return any((root / instance).exists() for root in candidate_roots)


def instance_successfully_produced(
    entry: Entry,
    state_dir: Path | None,
    progress_path: Path | None,
    instance_status_path: Path | None,
    output_dir: Path | None = None,
    postprocessed_dir: Path | None = None,
) -> bool:
    """Check run-local state for a prior successful production of this instance."""
    instance = task_dir_name(entry.repo, entry.pull_number)
    if not _instance_artifact_exists(instance, output_dir, postprocessed_dir):
        return False

    if state_dir is not None and _create_log_has_success(
        state_dir / "create.jsonl", instance
    ):
        return True

    if progress_path is None:
        return False
    successful_instances, _failed_instances = _load_progress_lists(
        progress_path, instance_status_path
    )
    return instance in successful_instances


def write_progress_jsonl(
    progress_queue: "queue.Queue[Outcome | None]",
    progress_path: Path,
    instance_status_path: Path,
) -> None:
    """Write compact per-task progress and per-instance status JSONL records."""
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    instance_status_path.parent.mkdir(parents=True, exist_ok=True)
    successful_instances, failed_instances = _load_progress_lists(
        progress_path, instance_status_path
    )

    with progress_path.open("a") as progress_fh, instance_status_path.open(
        "a"
    ) as status_fh:
        while True:
            outcome = progress_queue.get()
            try:
                if outcome is None:
                    break

                instance = task_dir_name(
                    outcome.entry.repo, outcome.entry.pull_number
                )
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
                if outcome.image_names or outcome.compose_projects:
                    prune_result = prune_docker_images(
                        outcome.image_names, outcome.compose_projects
                    )

                image_ids = tuple(
                    dict.fromkeys((*outcome.image_ids, *prune_result.image_ids))
                )
                print(
                    f"[orchestrator] {status}: {instance} image tags: "
                    f"{json.dumps(list(outcome.image_names))}",
                    flush=True,
                )
                print(
                    f"[orchestrator] {status}: {instance} image ids: "
                    f"{json.dumps(list(image_ids))}",
                    flush=True,
                )
                if outcome.image_names or outcome.compose_projects:
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
                    "total_successes": total_successes,
                    "total_failures": total_failures,
                    "total_processed": total_processed,
                }
                runtime_metadata = {
                    "slurm_node": os.environ.get("SWEGEN_SLURM_NODE", "").strip(),
                    "slurm_route": os.environ.get("SWEGEN_SLURM_ROUTE", "").strip(),
                    "slurm_group": os.environ.get("SWEGEN_SLURM_GROUP", "").strip(),
                }
                status_record.update(
                    {key: value for key, value in runtime_metadata.items() if value}
                )
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
                progress_record.update(
                    {key: value for key, value in runtime_metadata.items() if value}
                )
                status_fh.write(json.dumps(status_record) + "\n")
                status_fh.flush()
                progress_fh.write(json.dumps(progress_record) + "\n")
                progress_fh.flush()
            finally:
                progress_queue.task_done()


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
            return str(
                min(int(i["number"]) for i in issues if i.get("number") is not None)
            )

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


def _replace_dockerfile_base(
    lines: list[str], image_ref: str | None
) -> tuple[list[str], bool]:
    if not lines or not lines[0].startswith("FROM "):
        return lines, False

    if image_ref:
        match = FROM_RE.match(lines[0])
        suffix = match.group("suffix") if match else ""
        replacement = f"FROM {image_ref}{suffix}"
        if lines[0] != replacement:
            return [replacement, *lines[1:]], True
        return lines, False

    if lines[0] == DOCKERFILE_BASE_FROM:
        return [DOCKERFILE_BASE_REPLACEMENT, *lines[1:]], True
    return lines, False


def _clone_block_end(lines: list[str], start: int) -> int:
    end = start + 1
    while end < len(lines) and lines[end - 1].rstrip().endswith("\\"):
        end += 1
    return end


def _extract_head_sha(lines: list[str]) -> str | None:
    for line in lines:
        match = GIT_FETCH_HEAD_RE.search(line)
        if match:
            return match.group("sha")
    for line in lines:
        match = HEAD_SHA_RE.search(line)
        if match:
            return match.group(0)
    return None


def _next_workdir(lines: list[str], start: int) -> str | None:
    for line in lines[start:]:
        stripped = line.strip()
        if stripped.startswith("WORKDIR "):
            return stripped.split(None, 1)[1]
    return None


def _existing_checkout(lines: list[str]) -> tuple[str, str] | None:
    for line in lines:
        match = GIT_CHECKOUT_RE.match(line.strip())
        if match:
            return match.group("path"), match.group("sha")
    return None


def _first_repo_workdir(lines: list[str]) -> str:
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("WORKDIR "):
            path = stripped.split(None, 1)[1]
            if path != "/app":
                return path
    return "/app/src"


def _remove_clone_comment(output: list[str]) -> None:
    if output and "Clone repo" in output[-1]:
        output.pop()
        while len(output) >= 2 and output[-1] == "" and output[-2] == "":
            output.pop()


def _replace_git_clone_with_checkout(
    lines: list[str], fallback_sha: str | None
) -> tuple[list[str], bool, str]:
    existing = _existing_checkout(lines)
    if existing is not None and not any(
        line.lstrip().startswith("RUN git clone ") for line in lines
    ):
        return lines, False, existing[0]

    output: list[str] = []
    changed = False
    checkout_path = existing[0] if existing is not None else "/app/src"
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.lstrip().startswith("RUN git clone "):
            end = _clone_block_end(lines, i)
            block = lines[i:end]
            head_sha = _extract_head_sha(block) or fallback_sha
            checkout_path = _next_workdir(lines, end) or checkout_path
            if head_sha is None:
                output.extend(block)
                i = end
                continue

            _remove_clone_comment(output)
            output.append(
                "# Checkout the preloaded repository at the PR HEAD commit (with fix applied)"
            )
            output.append(f"RUN cd {checkout_path} && git checkout --detach {head_sha}")
            output.append(f"RUN cd {checkout_path} && git submodule update --init || true")
            changed = True
            i = end
            continue

        output.append(line)
        i += 1

    return output, changed, checkout_path


def _has_voyager_path_adjustment(lines: list[str], repo: str) -> bool:
    source = f"/app/{repo}"
    return any(
        VOYAGER_PATH_MARKER in line or ("ln -s " in line and source in line)
        for line in lines
    )


def _insert_voyager_path_adjustment(
    lines: list[str], repo: str, checkout_path: str
) -> tuple[list[str], bool]:
    if not lines or not lines[0].startswith("FROM "):
        return lines, False
    if _has_voyager_path_adjustment(lines, repo):
        return lines, False

    source = f"/app/{repo}"
    target = checkout_path or "/app/src"
    target_parent = str(PurePosixPath(target).parent)
    block = [
        "",
        VOYAGER_PATH_MARKER,
        f"RUN mkdir -p {target_parent} && \\",
        f"    mv {source} {target} && \\",
        f"    ln -s {target} {source}",
        "",
    ]
    return [lines[0], *block, *lines[1:]], True


def rewrite_dockerfile_for_voyager(task_dir: Path, entry: Entry) -> list[str]:
    """Rewrite Dockerfile repo setup for Voyager's preloaded repo images."""
    dockerfile = _dockerfile_path(task_dir)
    if not dockerfile.is_file():
        return []

    text = dockerfile.read_text()
    trailing_newline = text.endswith("\n")
    lines = text.splitlines()
    changes: list[str] = []

    image_ref = entry.image_ref or None
    lines, base_changed = _replace_dockerfile_base(lines, image_ref)
    if base_changed:
        changes.append("voyager base rewritten" if image_ref else "base rewritten")

    fallback_sha = (
        entry.base_commit if HEAD_SHA_RE.fullmatch(entry.base_commit) else None
    )
    lines, clone_changed, checkout_path = _replace_git_clone_with_checkout(
        lines, fallback_sha
    )
    if clone_changed:
        changes.append("git clone replaced with checkout")
    elif any(line.lstrip().startswith("RUN git clone ") for line in lines):
        changes.append("git clone unchanged")
    elif _existing_checkout(lines) is not None:
        changes.append("checkout present")

    if not checkout_path:
        checkout_path = _first_repo_workdir(lines)
    lines, path_changed = _insert_voyager_path_adjustment(
        lines, entry.repo, checkout_path
    )
    if path_changed:
        changes.append("voyager symlink added")
    elif _has_voyager_path_adjustment(lines, entry.repo):
        changes.append("voyager symlink present")

    rendered = "\n".join(lines)
    if trailing_newline:
        rendered += "\n"
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
      - the Dockerfile base image rewritten to the internal mirror when needed
      - a Voyager path adjustment for the preloaded repository
      - the generated git clone block replaced with a deterministic checkout
      - a ``[cwm_task_metadata]`` table (with linked issue number) on task.toml

    Returns a short status string for logging.
    """
    name = task_dir_name(entry.repo, entry.pull_number)
    src_dir = tasks_dir / name
    if not src_dir.is_dir():
        return f"skipped (no task dir {name})"

    dst_dir = postprocessed_dir / name
    shutil.copytree(src_dir, dst_dir, dirs_exist_ok=True)

    issue_number = fetch_issue_number(entry.repo, entry.pull_number, tokens)
    dockerfile_changes = rewrite_dockerfile_for_voyager(dst_dir, entry)
    toml_ok = write_cwm_metadata(dst_dir, entry, issue_number)

    parts = dockerfile_changes or ["Dockerfile unchanged"]
    parts.extend(
        [
            "task.toml updated" if toml_ok else "task.toml unchanged",
            f"issue={issue_number or 'none'}",
        ]
    )
    return "; ".join(parts)


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


def _mask_token(token: str | None) -> str:
    """Render a token for logs without leaking it (prefix only)."""
    if not token:
        return "none"
    return f"{token[:8]}…" if len(token) > 8 else "…"


def preflight_github_tokens(tokens: list[str]) -> list[str]:
    """Return only tokens that can currently authenticate to GitHub.

    A failed pool must stop the batch before workers turn every queued PR into a
    false failure. Set ``SWEGEN_GITHUB_PREFLIGHT=0`` only for deliberate offline
    or mocked runs.
    """
    enabled = os.environ.get("SWEGEN_GITHUB_PREFLIGHT", "1").strip().lower()
    if enabled in {"0", "false", "no", "off"}:
        return list(tokens)

    healthy: list[str] = []
    for index, token in enumerate(tokens, start=1):
        try:
            response = requests.get(
                "https://api.github.com/user",
                params={"swegen_preflight": time.time_ns()},
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"token {token}",
                    "Cache-Control": "no-cache",
                },
                timeout=20,
                **github_requests_kwargs(),
                **requests_ssl_kwargs(),
            )
        except requests.RequestException as error:
            print(
                f"warning: GitHub token {index} ({_mask_token(token)}) preflight "
                f"failed: {type(error).__name__}: {error}",
                file=sys.stderr,
            )
            continue

        if response.status_code == 200:
            healthy.append(token)
            continue

        remaining = response.headers.get("X-RateLimit-Remaining", "unknown")
        reset_text = response.headers.get("X-RateLimit-Reset", "")
        reset_note = ""
        try:
            reset_seconds = max(0, int(reset_text) - int(time.time()))
            reset_note = f", reset in {reset_seconds}s"
        except (TypeError, ValueError):
            pass
        print(
            f"warning: GitHub token {index} ({_mask_token(token)}) is unavailable: "
            f"HTTP {response.status_code}, remaining={remaining}{reset_note}",
            file=sys.stderr,
        )

    return healthy


def pick_github_token(pool: list[str], exclude: set[str]) -> str | None:
    """Pick a random token from ``pool``, preferring ones not in ``exclude``.

    Returns None if the pool is empty. When every token has been excluded
    (all already tried), falls back to choosing randomly from the full pool.
    """
    if not pool:
        return None
    candidates = [t for t in pool if t not in exclude] or pool
    return random.choice(candidates)


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
    max_retries: int,
    token_pool: list[str],
    force: bool,
) -> ProcessResult:
    """Run `swegen create` for a single PR, writing to the open ``log`` handle.

    A failed run whose output matches a transient network/API error (e.g.
    "socket connection was closed unexpectedly") is retried up to ``max_retries``
    times with a short backoff; non-transient failures are not.

    When ``token_pool`` holds more than one token, each run injects a random one
    as GITHUB_TOKEN for cloning/API access; a run that fails with a GitHub
    rate-limit/forbidden error (HTTP 403/429) is retried with a *different* token
    from the pool. Returns the final return code plus any Harbor image tags
    and Docker image IDs observed during this task's run.
    """
    instance = task_dir_name(entry.repo, entry.pull_number)
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

    returncode = 1
    final_output_tail = ""

    # Pick a random GitHub token for this entry; rotate to a different one if we
    # hit a rate limit. Tracks which tokens we've already tried.
    current_token = pick_github_token(token_pool, set())
    tried_tokens: set[str] = {current_token} if current_token else set()

    for attempt in range(1, max_retries + 1):
        # The orchestrator owns skip/rebuild decisions by run-local task dirs,
        # so bypass create.jsonl dedupe for every processed entry.
        cmd = base_cmd + ["--force"]
        attempt_note = "" if attempt == 1 else f" (retry {attempt}/{max_retries})"
        token_note = f" [token {_mask_token(current_token)}]" if token_pool else ""
        print(f"{tag} starting{attempt_note}{token_note}", flush=True)

        # Inject the chosen token for this attempt without mutating the shared
        # base env (consumers run concurrently).
        attempt_env = env
        if current_token:
            attempt_env = {**env, "GITHUB_TOKEN": current_token}

        log.write(
            f"\n{'=' * 80}\n{tag}{attempt_note}{token_note}\n$ {' '.join(cmd)}\n{'=' * 80}\n"
        )
        log.flush()
        start_size = os.fstat(log.fileno()).st_size

        try:
            returncode = run_command_to_log(cmd, attempt_env, log)
        except FileNotFoundError:
            # Not transient — abort retries for this entry.
            msg = (
                f"could not find executable {swegen_bin!r}; "
                "is swegen installed / on PATH?"
            )
            log.write(msg + "\n")
            print(f"{tag} ERROR: {msg}", flush=True)
            image_names, compose_projects = collect_new_instance_docker_refs(
                state_dir, instance, before_configs
            )
            image_ids = collect_instance_docker_image_ids(
                image_names, compose_projects
            )
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
            with open(log_path, "r", errors="replace") as reader:
                reader.seek(max(start_size, end_size - 65536))
                output_tail = reader.read()
        except OSError:
            pass
        final_output_tail = output_tail

        rate_limited = _is_github_rate_limited(output_tail)
        if attempt < max_retries and (
            rate_limited or _is_retryable_failure(output_tail)
        ):
            backoff = RETRY_BACKOFF_SEC * attempt
            if rate_limited and len(token_pool) > 1:
                # Swap to a different token before retrying the clone/API.
                next_token = pick_github_token(token_pool, tried_tokens)
                tried_tokens.add(next_token)
                cause = (
                    f"github rate limit on token "
                    f"{_mask_token(current_token)}; rotating to "
                    f"{_mask_token(next_token)}"
                )
                current_token = next_token
            elif rate_limited:
                cause = "github rate limit (no alternate token available)"
            else:
                cause = "transient error"
            retry_msg = (
                f"{cause} (rc={returncode}); retrying in {backoff}s "
                f"[attempt {attempt + 1}/{max_retries}]"
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


def run_consumer(
    worker_id: int,
    work_queue: "queue.Queue[list[Entry] | None]",
    env: dict[str, str],
    swegen_bin: str,
    log_dir: Path,
    cc_timeout: int | None = None,
    output_dir: Path | None = None,
    state_dir: Path | None = None,
    repo_cache_dir: Path | None = None,
    max_retries: int = 3,
    github_tokens: list[str] | None = None,
    force: bool = False,
    postprocessed_dir: Path | None = None,
    progress_queue: "queue.Queue[Outcome | None] | None" = None,
    progress_path: Path | None = None,
    instance_status_path: Path | None = None,
) -> list[Outcome]:
    """Consumer thread: pull repo packages off ``work_queue`` until drained.

    Each ``get`` returns a package (all PRs of one repo) or ``None`` — the
    sentinel the producer enqueues once per consumer to signal shutdown. PRs
    within a package are processed sequentially; a whole repo stays inside one
    consumer so its shared git cache is never touched concurrently. When the
    package is done the consumer immediately pulls the next one. All of this
    consumer's runs are appended to a single per-consumer log file.

    As soon as a PR's run succeeds, that task is copied into ``postprocessed_dir``
    and post-processed there (see ``postprocess_task``), so post-processing
    happens incrementally rather than in a batch at the end.
    """
    token_pool = github_tokens or []
    worker_env = worker_child_env(env, worker_id)
    outcomes: list[Outcome] = []
    log_path = log_dir / f"worker-{worker_id}.log"

    with log_path.open("w") as raw_log:
        log = TimestampedLog(raw_log)
        assigned_proxy = worker_env.get("SWEGEN_ASSIGNED_SOCKS_PROXY")
        if assigned_proxy:
            log.write(
                f"[worker {worker_id}] LLM proxy: {assigned_proxy}; "
                f"Claude bridge: {worker_env['SWEGEN_CLAUDE_PROXY']}\n"
            )
            log.flush()
        while True:
            package = work_queue.get()
            try:
                if package is None:  # producer's shutdown sentinel
                    break
                repo = package[0].repo
                total = len(package)
                log.write(
                    f"\n{'#' * 80}\n[worker {worker_id}] package {repo} "
                    f"({total} PR{'s' if total != 1 else ''})\n{'#' * 80}\n"
                )
                log.flush()
                for idx, entry in enumerate(package, 1):
                    tag = (
                        f"[worker {worker_id}] ({idx}/{total}) "
                        f"{entry.repo}#{entry.pull_number}"
                    )
                    if not force and instance_successfully_produced(
                        entry,
                        state_dir,
                        progress_path,
                        instance_status_path,
                        output_dir,
                        postprocessed_dir,
                    ):
                        msg = f"{tag} skipped: already successful in this run"
                        log.write(msg + "\n")
                        log.flush()
                        print(msg, flush=True)
                        continue

                    process_result = process_entry(
                        worker_id,
                        entry,
                        tag,
                        worker_env,
                        swegen_bin,
                        log,
                        log_path,
                        cc_timeout,
                        output_dir,
                        state_dir,
                        repo_cache_dir,
                        max_retries,
                        token_pool,
                        force,
                    )
                    returncode = process_result.returncode
                    postprocess_status = ""
                    # Post-process immediately on success: copy the task into
                    # the postprocessed-output tree and apply the rewrites there.
                    if returncode == 0 and postprocessed_dir is not None:
                        try:
                            postprocess_status = postprocess_task(
                                entry,
                                output_dir,
                                postprocessed_dir,
                                token_pool,
                            )
                        except Exception as e:  # never let postprocess abort work
                            postprocess_status = f"ERROR: {e}"
                        log.write(f"{tag} postprocess: {postprocess_status}\n")
                        log.flush()
                        print(f"{tag} postprocess: {postprocess_status}", flush=True)

                    outcome = Outcome(
                        worker_id=worker_id,
                        entry=entry,
                        returncode=returncode,
                        failure_reason=process_result.failure_reason,
                        postprocess_status=postprocess_status,
                        image_names=process_result.image_names,
                        image_ids=process_result.image_ids,
                        compose_projects=process_result.compose_projects,
                    )
                    outcomes.append(outcome)
                    if progress_queue is not None:
                        progress_queue.put(outcome)
            finally:
                work_queue.task_done()

    return outcomes


def producer(
    work_queue: "queue.Queue[list[Entry] | None]",
    packages: list[list[Entry]],
    num_consumers: int,
) -> None:
    """Producer thread: enqueue every package, then one sentinel per consumer.

    The trailing ``None`` sentinels let each consumer exit cleanly once the
    queue is drained.
    """
    for package in packages:
        work_queue.put(package)
    for _ in range(num_consumers):
        work_queue.put(None)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run `swegen create` across many PRs using parallel workers.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "jsonl",
        type=Path,
        help='Input JSONL; each line: {"repo": "owner/repo", "pull_number": "123"}',
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
        "--max-retries",
        type=int,
        default=3,
        help="Max attempts per PR when a run fails with a transient network/API "
        "error (e.g. socket closed). Set to 1 to disable retries.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild every PR (passing `swegen create --force`). Without this, "
        "PRs whose task directory already exists under --output are skipped so a "
        "rerun only fills the gaps.",
    )
    parser.add_argument(
        "--slurm",
        action="store_true",
        help="Deprecated legacy Slurm mode. Use src/slurm_two_node.py, which "
        "stages node-local workspaces and preserves the 8+8+8 proxy topology.",
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
        help="Root task output dir. Defaults to <run>/tasks. "
        "(--tasks-dir is a deprecated alias.)",
    )
    parser.add_argument(
        "--postprocessed-output",
        dest="postprocessed_dir",
        type=Path,
        default=None,
        help="Directory for post-processed copies of successful tasks. Defaults "
        f"to <run>/{POSTPROCESSED_OUTPUT_NAME}.",
    )
    # Secrets: default to None here (do NOT pull from os.environ, or --help
    # would print the resolved secret values). When a flag is omitted, the
    # child still inherits the value from the environment via build_child_env.
    for flag_name, env_name in SECRET_ENV_VARS.items():
        parser.add_argument(
            f"--{flag_name.replace('_', '-')}",
            default=None,
            help=f"Value for {env_name} (defaults to inheriting it from the environment).",
        )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    # `swegen` loads .env in its Typer entry point; this standalone batch
    # orchestrator needs the same behavior when invoked directly.
    load_dotenv()
    args = parse_args(argv)

    if args.workers < 1:
        print("error: --workers must be >= 1", file=sys.stderr)
        return 2
    if args.cc_timeout is not None and args.cc_timeout > MAX_CC_TIMEOUT_SECONDS:
        print(
            f"Capping --cc-timeout from {args.cc_timeout}s to "
            f"{MAX_CC_TIMEOUT_SECONDS}s (3 hours).",
            flush=True,
        )
        args.cc_timeout = MAX_CC_TIMEOUT_SECONDS
    if not args.jsonl.exists():
        print(f"error: input file not found: {args.jsonl}", file=sys.stderr)
        return 2

    entries = load_entries(args.jsonl)
    if not entries:
        print(f"error: no entries found in {args.jsonl}", file=sys.stderr)
        return 2

    # Resolve the pool of GitHub tokens used for cloning + issue-number lookups.
    # An explicit token (--github-token flag or GITHUB_TOKEN env var) overrides
    # the swegen.toml pool entirely; otherwise the orchestrator rotates between
    # the tokens in [github].gh_tokens, switching tokens when one is rate limited.
    explicit_token = args.github_token or os.environ.get("GITHUB_TOKEN")
    if explicit_token:
        github_tokens = [explicit_token]
    else:
        try:
            from swegen.model_settings import load_github_tokens

            github_tokens = load_github_tokens()
        except Exception:
            github_tokens = []
    configured_github_tokens = len(github_tokens)
    if github_tokens:
        github_tokens = preflight_github_tokens(github_tokens)
        if not github_tokens:
            print(
                "error: none of the configured GitHub tokens passed preflight; "
                "refusing to mark queued PRs as failed. Retry after the token "
                "rate limits or GitHub 503 responses recover.",
                file=sys.stderr,
            )
            return 3
        if len(github_tokens) != configured_github_tokens:
            print(
                f"Using {len(github_tokens)}/{configured_github_tokens} GitHub "
                "tokens that passed preflight.",
                flush=True,
            )
    if not github_tokens:
        print(
            "warning: no GitHub token configured (via --github-token, environment, "
            "or swegen.toml [github].gh_tokens/token); swegen create and "
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
    env = build_child_env(args)

    try:
        proxy_assignments = validate_worker_proxy_config(env, args.workers)
    except ValueError as error:
        print(f"error: invalid worker proxy configuration: {error}", file=sys.stderr)
        return 2

    print(f"Run: {args.run_name} -> {args.run_dir}", flush=True)
    print(f"Repo cache: {args.repo_cache_dir}", flush=True)
    for assignment in proxy_assignments:
        print(f"Proxy assignment: {assignment}", flush=True)

    if args.slurm:
        print(
            "error: the legacy --slurm mode assumes a shared filesystem and "
            "obsolete node names. Use `python src/slurm_two_node.py ...` instead.",
            file=sys.stderr,
        )
        return 2

    # Group the entries into one package per repo, dropping only instances that
    # were already recorded as successful. Failed/interrupted task directories
    # are retried and overwritten by `swegen create --force`.
    packages, skipped = build_packages(
        entries,
        args.tasks_dir,
        args.force,
        state_dir=args.run_dir,
        progress_path=args.progress_jsonl,
        instance_status_path=args.instance_status_jsonl,
        postprocessed_dir=args.postprocessed_dir,
    )
    if skipped:
        print(
            f"Skipping {skipped} PR(s) already recorded as successful in "
            f"{args.run_dir}/ (use --force to rebuild).",
            flush=True,
        )
    if not packages:
        print("Nothing to do: all PRs already exist (use --force to rebuild).")
        return 0

    remaining = sum(len(p) for p in packages)
    num_consumers = max(1, min(args.workers, len(packages)))
    print(
        f"Loaded {len(entries)} entries -> {remaining} PR(s) in {len(packages)} "
        f"repo package(s), {num_consumers} consumer(s) (logs in {args.log_dir}/)",
        flush=True,
    )
    for package in packages:
        print(
            f"  {package[0].repo}: {len(package)} "
            f"PR{'s' if len(package) != 1 else ''}"
        )

    args.postprocessed_dir.mkdir(parents=True, exist_ok=True)
    print(
        f"Post-processed copies of successful tasks -> {args.postprocessed_dir}/",
        flush=True,
    )
    print(f"Per-task progress JSONL -> {args.progress_jsonl}", flush=True)
    print(f"Per-instance status JSONL -> {args.instance_status_jsonl}", flush=True)

    # Producer-consumer: a producer thread feeds repo packages onto the queue;
    # each consumer pulls a package, processes its PRs, then pulls the next.
    work_queue: "queue.Queue[list[Entry] | None]" = queue.Queue()
    producer_thread = threading.Thread(
        target=producer,
        args=(work_queue, packages, num_consumers),
        name="package-producer",
    )
    producer_thread.start()

    progress_queue: "queue.Queue[Outcome | None]" = queue.Queue()
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
                    work_queue,
                    env,
                    args.swegen_bin,
                    args.log_dir,
                    args.cc_timeout,
                    args.tasks_dir,
                    args.state_dir,
                    args.repo_cache_dir,
                    args.max_retries,
                    github_tokens,
                    args.force,
                    args.postprocessed_dir,
                    progress_queue,
                    args.progress_jsonl,
                    args.instance_status_jsonl,
                ): i
                for i in range(num_consumers)
            }
            for future in as_completed(futures):
                all_outcomes.extend(future.result())
    finally:
        progress_queue.put(None)
        progress_thread.join()
        producer_thread.join()

    # Post-processing already ran incrementally per task (copied into
    # args.postprocessed_dir and rewritten as each PR succeeded).

    # Summary
    failures = [o for o in all_outcomes if not o.ok]
    succeeded = len(all_outcomes) - len(failures)
    print("\n" + "=" * 80)
    print(f"Done: {succeeded}/{len(all_outcomes)} succeeded, {len(failures)} failed.")
    if failures:
        print("Failed:")
        for o in failures:
            print(
                f"  - {o.entry.repo}#{o.entry.pull_number} "
                f"(worker {o.worker_id}, rc={o.returncode})"
            )
    print("=" * 80)

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
