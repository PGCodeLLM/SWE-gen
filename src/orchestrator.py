#!/usr/bin/env python3
"""Parallel orchestrator for `swegen create`.

Reads a JSONL file where each line describes one PR to process:

    {"repo": "owner/repo", "pull_number": "1234"}

Work is organized as a producer-consumer pipeline. The entries are grouped into
one "package" per repo (all of that repo's PRs that still need processing); a
producer thread feeds those packages onto a queue, and ``--workers`` consumer
threads each pull a package, process its PRs sequentially, and then pull the
next available package. Keeping a whole repo inside a single consumer ensures
the shared per-repo git cache (.swegen/repos/<repo>) is never touched by two
consumers at once, while idle consumers immediately grab more work instead of
waiting on a fixed segment.

By default a PR whose task directory already exists under ``--output`` is
skipped (so reruns only fill gaps); pass ``--force`` to rebuild every PR.

As soon as a PR's ``swegen create`` run succeeds, that task is copied into the
postprocessed-output tree (``--postprocessed-output``, default
``tasks_voyager_postprocessed``) and all post-processing is applied to the *copy*,
leaving the original task untouched. Post-processing copies obs_download.py into
the task's environment/, rewrites the Dockerfile base image to the internal
mirror, replaces the git-clone block with an obs_download fetch, and appends a
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
from pathlib import Path

# Helper file copied into every postprocessed task's environment/ folder during
# post-processing. The rewritten Dockerfile COPYs it into the image and runs it
# to fetch the repo (replacing the original git clone).
OBS_DOWNLOAD_SRC = (
    Path(__file__).resolve().parent / "swegen" / "postprocessing" / "obs_download.py"
)

# Default directory for the post-processed copies of successful tasks. The
# originals under --output are left untouched; postprocessed copies land here
# (resolved relative to --output's parent unless --postprocessed-output is given).
POSTPROCESSED_OUTPUT_NAME = "tasks_voyager_postprocessed"

# Slurm nodes to distribute across when --slurm is set (lux-3-bm-cpu-[01-10]).
SLURM_NODES = [f"lux-3-bm-cpu-{i:02d}" for i in range(1, 11) if i != 8] #CPU 8 is borked

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
DOCKERFILE_BASE_FROM = "FROM ubuntu:24.04"
DOCKERFILE_BASE_REPLACEMENT = (
    "FROM swr-aifm-code-data-platform-6sudmx.swr-pro.myhuaweicloud.com/"
    "swesandbox/ubuntu:24.04"
)

# Post-processing: the skeleton's `RUN git clone <url> src && ... git submodule
# update` block (see swegen/create/task_skeleton.py generate_dockerfile) is
# replaced with an obs_download fetch. The repo URL has already been filled in
# by the time post-processing runs, so it is captured from the matched block and
# reused verbatim in the replacement.
DOCKERFILE_CLONE_RE = re.compile(
    r"RUN git clone (?P<url>\S+) src &&.*?git submodule update --init --recursive",
    re.DOTALL,
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


@dataclass
class Entry:
    """A single PR to process."""

    repo: str
    pull_number: str
    base_commit: str = ""


@dataclass
class Outcome:
    """Result of one `swegen create` invocation."""

    worker_id: int
    entry: Entry
    returncode: int

    @property
    def ok(self) -> bool:
        return self.returncode == 0


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
            entries.append(
                Entry(repo=repo, pull_number=pull_number, base_commit=base_commit)
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
    per-repo git cache (.swegen/repos/<repo>) is never touched by two workers at
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
    items: list[Entry], output_dir: Path, force: bool
) -> tuple[list[list[Entry]], int]:
    """Group entries into one "package" per repo for the producer-consumer queue.

    A package holds every PR of a single repo that still needs processing. Unless
    ``force`` is set, PRs whose task directory already exists under ``output_dir``
    are dropped (a rerun only fills the gaps); when every PR of a repo is dropped,
    that repo produces no package at all. Within a package the PRs are ordered
    highest PR number to lowest, matching the old per-segment ordering.

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
                if not (output_dir / task_dir_name(e.repo, e.pull_number)).exists()
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


def write_chunk(path: Path, segment: list[Entry]) -> None:
    """Write a segment of entries to a JSONL chunk file for a slurm child run.

    Preserves the fields the child needs (base_commit is required downstream for
    cwm_task_metadata).
    """
    with path.open("w") as fh:
        for entry in segment:
            fh.write(
                json.dumps(
                    {
                        "repo": entry.repo,
                        "pull_number": entry.pull_number,
                        "base_commit": entry.base_commit,
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
        "--log-dir",
        str(node_log_dir),
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


def copy_obs_download(task_dir: Path, source: Path) -> bool:
    """Copy ``source`` (obs_download.py) into ``task_dir/environment``.

    The environment/ folder is created if it doesn't already exist. Returns True
    on success, False if the source file is missing.
    """
    if not source.is_file():
        return False
    env_dir = task_dir / "environment"
    env_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, env_dir / source.name)
    return True


def task_dir_name(repo: str, pull_number: str) -> str:
    """Compute the task directory name `swegen create` writes for a PR.

    Mirrors swegen's default naming (see create/orchestrator.py): the repo is
    lowercased with ``/`` replaced by ``__`` and suffixed with ``-<pr>``. e.g.
    ``0no-co/gql.tada`` PR 460 → ``0no-co__gql.tada-460``.
    """
    repo_slug = repo.lower().replace("/", "__")
    return f"{repo_slug}-{pull_number}"


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


def rewrite_dockerfile_base(task_dir: Path) -> bool:
    """Replace the base image in <task_dir>/environment/Dockerfile.

    Returns True if the file was found and rewritten, False otherwise.
    """
    dockerfile = task_dir / "environment" / "Dockerfile"
    if not dockerfile.is_file():
        return False
    text = dockerfile.read_text()
    if DOCKERFILE_BASE_FROM not in text:
        return False
    dockerfile.write_text(text.replace(DOCKERFILE_BASE_FROM, DOCKERFILE_BASE_REPLACEMENT))
    return True


def rewrite_dockerfile_clone(task_dir: Path) -> bool:
    """Replace the git-clone block in <task_dir>/environment/Dockerfile.

    Swaps the skeleton's ``RUN git clone <url> src && ... git submodule update``
    block for a ``COPY obs_download.py`` + obs_download fetch of the same repo.
    The repo URL is captured from the matched block (already filled in by the
    time post-processing runs) and reused verbatim. Returns True if the block
    was found and rewritten, False otherwise.
    """
    dockerfile = task_dir / "environment" / "Dockerfile"
    if not dockerfile.is_file():
        return False
    text = dockerfile.read_text()
    match = DOCKERFILE_CLONE_RE.search(text)
    if not match:
        return False
    url = match.group("url")
    replacement = (
        # Install uv and a managed Python with the OBS SDK, then fetch the repo
        # tarball from OBS via obs_download.py instead of cloning from git.
        "RUN curl -LsSf https://astral.sh/uv/install.sh | sh\n"
        'ENV PATH="/root/.local/bin:${PATH}"\n'
        "RUN uv venv --managed-python --python 3.12 /opt/obs-python\n"
        "RUN uv pip install --python /opt/obs-python/bin/python esdk-obs-python "
        "--trusted-host pypi.org\n"
        "COPY obs_download.py /usr/local/bin/obs_download.py\n"
        f"RUN REPO_FULL_NAME=\"$(echo '{url}' "
        "| sed -E 's#^[a-z]+://[^/]+/##; s/\\.git$//')\" && \\\n"
        '    /opt/obs-python/bin/python /usr/local/bin/obs_download.py '
        '"$REPO_FULL_NAME" src && \\\n'
        "    cd src && \\\n"
        "    git submodule update --init --recursive"
    )
    dockerfile.write_text(text[: match.start()] + replacement + text[match.end() :])
    return True


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
    obs_src: Path | None,
    tokens: list[str] | None,
) -> str:
    """Copy one successful task into the postprocessed-output tree, then post-process.

    The original task under ``tasks_dir`` is left untouched. The copy under
    ``postprocessed_dir`` gets, in order:
      - obs_download.py copied into environment/
      - the Dockerfile base image rewritten to the internal mirror
      - the git-clone block replaced with an obs_download fetch
      - a ``[cwm_task_metadata]`` table (with linked issue number) on task.toml

    Returns a short status string for logging.
    """
    name = task_dir_name(entry.repo, entry.pull_number)
    src_dir = tasks_dir / name
    if not src_dir.is_dir():
        return f"skipped (no task dir {name})"

    dst_dir = postprocessed_dir / name
    shutil.copytree(src_dir, dst_dir, dirs_exist_ok=True)

    obs_ok = copy_obs_download(dst_dir, obs_src) if obs_src is not None else False
    issue_number = fetch_issue_number(entry.repo, entry.pull_number, tokens)
    base_ok = rewrite_dockerfile_base(dst_dir)
    clone_ok = rewrite_dockerfile_clone(dst_dir)
    toml_ok = write_cwm_metadata(dst_dir, entry, issue_number)

    parts = [
        "obs_download copied" if obs_ok else "obs_download MISSING",
        "base rewritten" if base_ok else "base unchanged",
        "clone replaced" if clone_ok else "clone unchanged",
        "task.toml updated" if toml_ok else "task.toml unchanged",
        f"issue={issue_number or 'none'}",
    ]
    return "; ".join(parts)


def _is_retryable_failure(output_tail: str) -> bool:
    """Whether a failed run's output looks like a transient network/API error."""
    return any(sig in output_tail for sig in RETRYABLE_ERROR_SIGNATURES)


def _is_github_rate_limited(output_tail: str) -> bool:
    """Whether a failed run's output looks like a GitHub token rate limit (403/429)."""
    return any(sig in output_tail for sig in GITHUB_RATE_LIMIT_SIGNATURES)


def _mask_token(token: str | None) -> str:
    """Render a token for logs without leaking it (prefix only)."""
    if not token:
        return "none"
    return f"{token[:8]}…" if len(token) > 8 else "…"


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
    max_retries: int,
    token_pool: list[str],
    force: bool,
) -> int:
    """Run `swegen create` for a single PR, writing to the open ``log`` handle.

    A failed run whose output matches a transient network/API error (e.g.
    "socket connection was closed unexpectedly") is retried up to ``max_retries``
    times with a short backoff; non-transient failures are not.

    When ``token_pool`` holds more than one token, each run injects a random one
    as GITHUB_TOKEN for cloning/API access; a run that fails with a GitHub
    rate-limit/forbidden error (HTTP 403/429) is retried with a *different* token
    from the pool. Returns the final return code.
    """
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
    # copies obs_download.py.
    if output_dir is not None:
        base_cmd += ["--output", str(output_dir)]
    # Forward the Claude Code session timeout when set; otherwise let
    # `swegen create` use its own default.
    if cc_timeout is not None:
        base_cmd += ["--cc-timeout", str(cc_timeout)]

    returncode = 1

    # Pick a random GitHub token for this entry; rotate to a different one if we
    # hit a rate limit. Tracks which tokens we've already tried.
    current_token = pick_github_token(token_pool, set())
    tried_tokens: set[str] = {current_token} if current_token else set()

    for attempt in range(1, max_retries + 1):
        # Force-overwrite when explicitly requested, or when regenerating over
        # partial output left by a failed earlier attempt.
        cmd = base_cmd + (["--force"] if (force or attempt > 1) else [])
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
            proc = subprocess.run(
                cmd,
                env=attempt_env,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            returncode = proc.returncode
        except FileNotFoundError:
            # Not transient — abort retries for this entry.
            msg = (
                f"could not find executable {swegen_bin!r}; "
                "is swegen installed / on PATH?"
            )
            log.write(msg + "\n")
            print(f"{tag} ERROR: {msg}", flush=True)
            return 127
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

    status = "OK" if returncode == 0 else f"FAILED rc={returncode}"
    print(f"{tag} {status} (log: {log_path})", flush=True)
    return returncode


def run_consumer(
    worker_id: int,
    work_queue: "queue.Queue[list[Entry] | None]",
    env: dict[str, str],
    swegen_bin: str,
    log_dir: Path,
    cc_timeout: int | None = None,
    output_dir: Path | None = None,
    max_retries: int = 3,
    github_tokens: list[str] | None = None,
    force: bool = False,
    postprocessed_dir: Path | None = None,
    obs_src: Path | None = None,
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
    outcomes: list[Outcome] = []
    log_path = log_dir / f"worker-{worker_id}.log"

    with log_path.open("w") as log:
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
                    returncode = process_entry(
                        worker_id,
                        entry,
                        tag,
                        env,
                        swegen_bin,
                        log,
                        log_path,
                        cc_timeout,
                        output_dir,
                        max_retries,
                        token_pool,
                        force,
                    )
                    outcomes.append(
                        Outcome(worker_id=worker_id, entry=entry, returncode=returncode)
                    )

                    # Post-process immediately on success: copy the task into
                    # the postprocessed-output tree and apply the rewrites there.
                    if returncode == 0 and postprocessed_dir is not None:
                        try:
                            status = postprocess_task(
                                entry,
                                output_dir,
                                postprocessed_dir,
                                obs_src,
                                token_pool,
                            )
                        except Exception as e:  # never let postprocess abort work
                            status = f"ERROR: {e}"
                        log.write(f"{tag} postprocess: {status}\n")
                        log.flush()
                        print(f"{tag} postprocess: {status}", flush=True)
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
        help=f"Distribute the input across slurm nodes {SLURM_NODES[0]}.."
        f"{SLURM_NODES[-1]} via sbatch (one job per node, each running "
        "--workers workers), then exit. Without this flag everything runs locally.",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=Path("orchestrator-logs"),
        help="Directory for per-worker log files.",
    )
    parser.add_argument(
        "--output",
        "-o",
        "--tasks-dir",
        dest="tasks_dir",
        type=Path,
        default=Path("tasks"),
        help="Root output dir, forwarded to `swegen create --output`, where tasks "
        "are written. The originals here are left untouched; post-processed copies "
        "go under --postprocessed-output. (--tasks-dir is a deprecated alias.)",
    )
    parser.add_argument(
        "--postprocessed-output",
        dest="postprocessed_dir",
        type=Path,
        default=None,
        help="Directory for the post-processed copies of successful tasks. As each "
        "PR succeeds, its task is copied here and the obs_download/Dockerfile/"
        "task.toml rewrites are applied to the copy. Defaults to "
        f"'{POSTPROCESSED_OUTPUT_NAME}' alongside --output.",
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
    args = parse_args(argv)

    if args.workers < 1:
        print("error: --workers must be >= 1", file=sys.stderr)
        return 2
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

    env = build_child_env(args)
    args.log_dir.mkdir(parents=True, exist_ok=True)

    # Resolve the postprocessed-output tree (where post-processed copies of
    # successful tasks land). Defaults to a sibling of --output.
    if args.postprocessed_dir is None:
        args.postprocessed_dir = args.tasks_dir.parent / POSTPROCESSED_OUTPUT_NAME

    if not OBS_DOWNLOAD_SRC.is_file():
        print(
            f"warning: {OBS_DOWNLOAD_SRC} not found; obs_download.py will NOT be "
            "copied into postprocessed tasks and the Dockerfile fetch will fail.",
            file=sys.stderr,
        )

    # Slurm mode: fan the input out across nodes via sbatch (one job per node,
    # each running --workers workers locally), then exit. Each node logs to its
    # own subfolder; all nodes share the flat output dir.
    if args.slurm:
        print(
            f"Distributing {len(entries)} entries across up to "
            f"{len(SLURM_NODES)} node(s) via sbatch "
            f"(--workers {args.workers} per node):",
            flush=True,
        )
        return submit_slurm_jobs(entries, args, env)

    # Group the entries into one package per repo, dropping PRs whose task dir
    # already exists (unless --force). Empty repos yield no package.
    packages, skipped = build_packages(entries, args.tasks_dir, args.force)
    if skipped:
        print(
            f"Skipping {skipped} PR(s) whose task dir already exists under "
            f"{args.tasks_dir}/ (use --force to rebuild).",
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

    # Producer-consumer: a producer thread feeds repo packages onto the queue;
    # each consumer pulls a package, processes its PRs, then pulls the next.
    work_queue: "queue.Queue[list[Entry] | None]" = queue.Queue()
    producer_thread = threading.Thread(
        target=producer,
        args=(work_queue, packages, num_consumers),
        name="package-producer",
    )
    producer_thread.start()

    all_outcomes: list[Outcome] = []
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
                args.max_retries,
                github_tokens,
                args.force,
                args.postprocessed_dir,
                OBS_DOWNLOAD_SRC,
            ): i
            for i in range(num_consumers)
        }
        for future in as_completed(futures):
            all_outcomes.extend(future.result())

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
