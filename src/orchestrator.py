#!/usr/bin/env python3
"""Parallel orchestrator for `swegen create`.

Reads a JSONL file where each line describes one PR to process:

    {"repo": "owner/repo", "pull_number": "1234"}

Splits the entries into ``--workers`` roughly-equal segments and runs the
segments concurrently. Within a segment, each PR is processed sequentially by
shelling out to:

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
import random
import shlex
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

# Helper file copied into every generated task's environment/ folder after the
# run completes (used by downstream postprocessing).
OBS_DOWNLOAD_SRC = (
    Path(__file__).resolve().parent / "swegen" / "postprocessing" / "obs_download.py"
)

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
        "--swegen-bin",
        args.swegen_bin,
    ]
    if args.cc_timeout is not None:
        cmd += ["--cc-timeout", str(args.cc_timeout)]
    cmd += ["--max-retries", str(args.max_retries)]
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


def distribute_obs_download(tasks_dir: Path, source: Path) -> tuple[int, int]:
    """Copy ``source`` into every ``tasks_dir/<subfolder>/environment`` folder.

    Returns (copied, skipped). A subfolder is skipped only if it isn't a
    directory; the environment/ folder is created if it doesn't already exist.
    """
    copied = 0
    skipped = 0
    if not tasks_dir.is_dir():
        print(
            f"warning: tasks dir {tasks_dir} does not exist; "
            "nothing to copy obs_download.py into.",
            file=sys.stderr,
        )
        return copied, skipped

    for subfolder in sorted(tasks_dir.iterdir()):
        if not subfolder.is_dir():
            skipped += 1
            continue
        env_dir = subfolder / "environment"
        env_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, env_dir / source.name)
        copied += 1
    return copied, skipped


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


def postprocess_task(entry: Entry, output_dir: Path, tokens: list[str] | None) -> str:
    """Apply Dockerfile + task.toml post-processing for one successful task.

    Returns a short status string for logging.
    """
    task_dir = output_dir / task_dir_name(entry.repo, entry.pull_number)
    if not task_dir.is_dir():
        return f"skipped (no task dir {task_dir.name})"

    issue_number = fetch_issue_number(entry.repo, entry.pull_number, tokens)
    dockerfile_ok = rewrite_dockerfile_base(task_dir)
    toml_ok = write_cwm_metadata(task_dir, entry, issue_number)

    parts = [
        "Dockerfile rewritten" if dockerfile_ok else "Dockerfile unchanged",
        "task.toml updated" if toml_ok else "task.toml unchanged",
        f"issue={issue_number or 'none'}",
    ]
    return "; ".join(parts)


def run_postprocessing(
    outcomes: list[Outcome],
    output_dir: Path,
    tokens: list[str] | None,
    max_workers: int,
) -> None:
    """Run post-processing for all successful tasks, in parallel."""
    successes = [o.entry for o in outcomes if o.ok]
    if not successes:
        print("No successful tasks to post-process.", flush=True)
        return

    print(
        f"Post-processing {len(successes)} successful task(s) "
        f"(Dockerfile base image + cwm_task_metadata)...",
        flush=True,
    )
    workers = max(1, min(max_workers, len(successes)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(postprocess_task, entry, output_dir, tokens): entry
            for entry in successes
        }
        for future in as_completed(futures):
            entry = futures[future]
            try:
                status = future.result()
            except Exception as e:  # defensive: never let one task abort the rest
                status = f"ERROR: {e}"
            print(f"  {entry.repo}#{entry.pull_number}: {status}", flush=True)


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


def run_worker(
    worker_id: int,
    segment: list[Entry],
    env: dict[str, str],
    swegen_bin: str,
    log_dir: Path,
    cc_timeout: int | None = None,
    output_dir: Path | None = None,
    max_retries: int = 3,
    github_tokens: list[str] | None = None,
) -> list[Outcome]:
    """Process one worker's segment sequentially, logging to a per-worker file.

    A failed `swegen create` whose output matches a transient network/API error
    (e.g. "socket connection was closed unexpectedly") is retried up to
    ``max_retries`` times with a short backoff; non-transient failures are not.

    When ``github_tokens`` holds more than one token, each run injects a random
    one as GITHUB_TOKEN for cloning/API access; a run that fails with a GitHub
    rate-limit/forbidden error (HTTP 403/429) is retried with a *different*
    token from the pool.
    """
    token_pool = github_tokens or []
    outcomes: list[Outcome] = []
    log_path = log_dir / f"worker-{worker_id}.log"
    total = len(segment)

    with log_path.open("w") as log:
        for idx, entry in enumerate(segment, 1):
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
            # Forward the output directory so tasks land where the orchestrator
            # later copies obs_download.py.
            if output_dir is not None:
                base_cmd += ["--output", str(output_dir)]
            # Forward the Claude Code session timeout when set; otherwise let
            # `swegen create` use its own default.
            if cc_timeout is not None:
                base_cmd += ["--cc-timeout", str(cc_timeout)]

            tag = f"[worker {worker_id}] ({idx}/{total}) {entry.repo}#{entry.pull_number}"
            returncode = 1

            # Pick a random GitHub token for this entry; rotate to a different
            # one if we hit a rate limit. Tracks which tokens we've already tried.
            current_token = pick_github_token(token_pool, set())
            tried_tokens: set[str] = {current_token} if current_token else set()

            for attempt in range(1, max_retries + 1):
                # Regenerate over any partial output left by a failed attempt.
                cmd = base_cmd + (["--force"] if attempt > 1 else [])
                attempt_note = "" if attempt == 1 else f" (retry {attempt}/{max_retries})"
                token_note = (
                    f" [token {_mask_token(current_token)}]" if token_pool else ""
                )
                print(f"{tag} starting{attempt_note}{token_note}", flush=True)

                # Inject the chosen token for this attempt without mutating the
                # shared base env (workers run concurrently).
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
                    returncode = 127
                    break
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
            outcomes.append(Outcome(worker_id=worker_id, entry=entry, returncode=returncode))

    return outcomes


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
        "are written; obs_download.py is copied into each <task>/environment after "
        "the run. (--tasks-dir is a deprecated alias.)",
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

    segments = split_into_segments(entries, args.workers)

    print(
        f"Loaded {len(entries)} entries -> {len(segments)} worker(s) "
        f"(logs in {args.log_dir}/)",
        flush=True,
    )
    for i, segment in enumerate(segments):
        print(f"  worker {i}: {len(segment)} entr{'y' if len(segment) == 1 else 'ies'}")

    all_outcomes: list[Outcome] = []
    with ThreadPoolExecutor(max_workers=len(segments)) as executor:
        futures = {
            executor.submit(
                run_worker,
                i,
                segment,
                env,
                args.swegen_bin,
                args.log_dir,
                args.cc_timeout,
                args.tasks_dir,
                args.max_retries,
                github_tokens,
            ): i
            for i, segment in enumerate(segments)
        }
        for future in as_completed(futures):
            all_outcomes.extend(future.result())

    # Copy obs_download.py into every generated task's environment/ folder.
    if not OBS_DOWNLOAD_SRC.is_file():
        print(
            f"warning: {OBS_DOWNLOAD_SRC} not found; skipping obs_download.py copy.",
            file=sys.stderr,
        )
    else:
        copied, _ = distribute_obs_download(args.tasks_dir, OBS_DOWNLOAD_SRC)
        print(
            f"Copied {OBS_DOWNLOAD_SRC.name} into {copied} "
            f"task environment folder(s) under {args.tasks_dir}/.",
            flush=True,
        )

    # Post-process successful tasks: rewrite the Dockerfile base image and add
    # [cwm_task_metadata] to task.toml.
    run_postprocessing(all_outcomes, args.tasks_dir, github_tokens, args.workers)

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
