<p align="center">
  <a href="https://github.com/abundant-ai/swe-gen">
    <img src="assets/swe-gen-wide.png" style="height: 10em" alt="SWE-gen llama genie" />
  </a>
</p>

<p align="center">
  <a href="https://www.python.org/downloads/">
    <img alt="Python" src="https://img.shields.io/badge/python-3.12+-blue.svg">
  </a>
  <a href="https://opensource.org/licenses/Apache-2.0">
    <img alt="License" src="https://img.shields.io/badge/License-Apache%202.0-blue.svg">
  </a>
  <a href="https://pypi.org/project/swegen/">
    <img alt="PyPI" src="https://img.shields.io/pypi/v/swegen.svg">
  </a>
</p>

# SWE-gen

> Convert merged GitHub PRs into [Harbor](https://github.com/laude-institute/harbor) tasks automatically.

## Overview

Automates task creation from real bug fixes in open-source GitHub repos. Works with **any programming language**: Claude Code analyzes the repo to detect language, build system, and test framework.

Each task reverses a merged PR to recreate the buggy state, verifies tests fail on baseline, and pass after applying the fix. Fully containerized with all dependencies installed at build time.

## News
<!-- - [03/2026] 🤓 **[SWE-gen-fn](https://github.com/abundant-ai/SWE-gen-fn)**: 1,000 functional programming tasks! -->
- [03/2026] ☁️ Run Harbor tasks on the cloud with [oddish](https://github.com/abundant-ai/oddish)!
- [02/2026] 🦀 **[SWE-gen-Rust](https://github.com/abundant-ai/SWE-gen-Rust)**, **[SWE-gen-Go](https://github.com/abundant-ai/SWE-gen-Go)**, and **[SWE-gen-Cpp](https://github.com/abundant-ai/SWE-gen-Cpp)** released!
- [02/2026] ☕ **[SWE-gen-Java](https://github.com/abundant-ai/SWE-gen-Java)**: 1,000 JVM tasks!
- [01/2026] 🔥 **[SWE-gen-JS](https://github.com/abundant-ai/SWE-gen-JS)** released: 1,000 JS/TS task dataset generated with SWE-gen
- [01/2026] 📖 **[blog post](https://www.rishidesai.org/posts/swe-gen/)** is out!

## Quick Start

```bash
# Install
uv pip install swegen

# Generate a task from a merged PR
swegen create --repo axios/axios --pr 7150
```

## Installation

```bash
uv pip install swegen
```

Copy the central configuration example and fill in its credentials, endpoints,
and models:

```bash
cp swegen.toml.example swegen.toml
```

SWE-gen ignores inherited API-key and model environment variables so every
worker uses the same explicit configuration.

**Note:** Cloud sandbox environments (Daytona, E2B, Modal, etc.) require additional API keys.

## Usage

**Commands:**
- `swegen create` — Generate a task from a merged PR
- `swegen validate` — Validate existing task (NOP + Oracle)
- `swegen analyze` — Deep analysis with agent trials to verify task quality

### Generate a Task

```bash
swegen create --repo <owner/repo> --pr <num>
```

<details>
<summary>Options</summary>

- `--output PATH` — Output directory for generated tasks (default: `tasks`)
- `--state-dir PATH` — State directory for cache/logs (default: `.swegen`)
- `--cc-timeout N` — Claude Code session timeout in seconds (default: 3200)
- `--env, -e TYPE` — Environment type: `docker`, `daytona`, `e2b`, `modal`, `runloop`, `gke` (default: `docker`)
- `--no-validate` — Skip Harbor validations
- `--force` — Bypass local dedupe and regenerate
- `--no-cache` — Disable cached artifacts from previous tasks
- `--no-require-minimum-difficulty` — Skip 3+ file and LLM substantiality checks
- `--min-source-files N` — Minimum number of source files required (default: 3, tests excluded)
- `--max-source-files N` — Maximum number of source files to avoid large refactors (default: 10, tests excluded)
- `--no-require-issue` — Allow PRs without linked issues (uses PR body/title for instructions)
- `-v, --verbose` / `-q, --quiet`

</details>

### Validate Existing Tasks

Verify that a task passes NOP (baseline fails) and Oracle (solution succeeds) agents:

```bash
swegen validate <task_id>
```

### Analyze Task Quality

Run agent trials to verify a task is well-specified and solvable:

```bash
swegen analyze <task_id>
```

<details>
<summary>What analyze does</summary>

1. Static quality check (`harbor tasks check`)
2. Baseline validation (nop fails, oracle passes)
3. Run N agent trials
4. Trial classification (identifies TASK vs AGENT problems)
5. Task verdict synthesis with actionable recommendations

**Classification categories:**
- `GOOD_SUCCESS` — Agent solved it correctly
- `BAD_SUCCESS` — Agent cheated or tests too permissive
- `GOOD_FAILURE` — Agent failed due to its own limitations
- `BAD_FAILURE` — Agent failed due to task issues (underspecified, brittle tests, etc.)
- `HARNESS_ERROR` — Infrastructure problem

</details>

## Task Requirements

<details>
<summary>Valid PR criteria</summary>

**Languages:** Any (Python, JavaScript, TypeScript, Go, Rust, Ruby, Java, etc.)

**Valid PRs must:**
- Be merged to primary branch with accessible fork
- Include test changes and corresponding fix
- Have a linked issue for high-quality instructions (bypass with `--no-require-issue`)
- Modify 3-10 source files (configurable with `--min-source-files` and `--max-source-files`, bypass with `--no-require-minimum-difficulty`)
- Pass LLM substantiality evaluation (bypass with `--no-require-minimum-difficulty`)
- Fail tests on reversed baseline, pass after applying fix
- Exclude documentation-only, formatting-only, or version-bump-only changes

</details>

## How It Works

<details>
<summary>Pipeline details</summary>

The pipeline uses a **language-agnostic approach**:

1. **Fetch & Analyze** — Get PR metadata via GitHub API, clone repo, identify test files
2. **Evaluate** — LLM evaluates PR substantiality and generates task instructions
3. **Generate Skeleton** — Create Dockerfile and test.sh with TODOs for Claude Code
4. **Claude Code Completion** — CC analyzes repo, detects language/runtime/build system, fills in skeleton
5. **Validation** — Run NOP (reward=0) and Oracle (reward=1) agents
6. **Iteration** — CC iterates until both agents pass

**Key Details:**
- Dockerfile clones at HEAD, then applies `bug.patch` to revert to buggy BASE state
- Test files stored in `task/tests/` and copied at runtime (prevents agent tampering)
- `fix.patch` (solution) excludes tests/CI, contains all other PR changes
- Dependencies installed at build time; runtime doesn't require internet access
- Successful tasks are cached as references to speed up future tasks from the same repo
- PR evaluation uses LLM to check substantiality and generate instructions

</details>

## Database-backed production pipeline

[`src/orchestrator.py`](src/orchestrator.py) reads work from the PostgreSQL relation configured as `[database].table` in `swegen.toml` (normally `swegen.pr_tasks`):

```bash
uv run python src/orchestrator.py --workers 8
```

Repository groups are claimed atomically. The claim transaction excludes future `unlock_time` values, increments `swegen_retries`, and sets a lease derived from the configured Docker, Claude Code, Harbor, hacking-check, and SWR timeouts. Rows with `swegen_bz_passed=true` and `obs_exists=false` are skipped by default; use `--force-rebuild` or `--include-obs-missing` to include them. Rows whose `swegen_retries` have reached `[database].max-retries` are always skipped, and lower-retry PRs are prioritized over higher-retry PRs.

A task is successful only after the complete production gate:

1. Harbor validation produces NOP reward `0` and Oracle reward `1`.
2. Every `[[hacking.llm]]` configured in `swegen.toml` returns `is_hacking=false` for the newly generated task.
3. The task is copied from `<run>/tasks/` to `<run>/tasks_bz/` and postprocessed.
4. The retained Harbor image is uploaded to Huawei SWR.
5. The database row is updated with `swegen_bz_passed=true`.

The `tasks_bz` Dockerfile uses the wce1sr `swesandbox` base, installs the bundled Huawei proxy CA, retains a real clone of the source repository, fetches detached SHAs when necessary, and resets/cleans the repository before checkout to tolerate dirty SWR layers. The local Docker image is pruned only after its SWR upload succeeds.

All gate results and failure reasons—including reward-hacking diagnoses—are written to `orchestrator-progress.jsonl` and `orchestrator-instance-status.jsonl`. API credentials, endpoints, model names, database settings, hacking-checker settings, and SWR credentials are centralized in `swegen.toml`; inherited API/model environment variables are ignored.

## Datasets

<p>
  <a href="https://github.com/abundant-ai/SWE-gen-JS">
    <img src="assets/swegen-js-banner.jpg" width="340" height="170" alt="SWE-gen-JS" />
  </a>&nbsp;&nbsp;
  <a href="https://github.com/abundant-ai/SWE-gen-Java">
    <img src="assets/swegen-java-banner.jpg" width="340" height="170" alt="SWE-gen-Java" />
  </a>
</p>
<p>
  <a href="https://github.com/abundant-ai/SWE-gen-Rust">
    <img src="assets/swegen-rust-banner.jpg" width="340" height="170" alt="SWE-gen-Rust" />
  </a>&nbsp;&nbsp;
  <a href="https://github.com/abundant-ai/SWE-gen-Go">
    <img src="assets/swegen-go-banner.jpg" width="340" height="170" alt="SWE-gen-Go" />
  </a>
</p>
<p>
  <a href="https://github.com/abundant-ai/SWE-gen-Cpp">
    <img src="assets/swegen-cpp-banner.jpg" width="340" height="170" alt="SWE-gen-Cpp" />
  </a>&nbsp;&nbsp;
  <a href="https://github.com/abundant-ai/SWE-gen-FN">
    <img src="assets/swegen-fn-banner.jpg" width="340" height="170" alt="SWE-gen-FN" />
  </a>
</p>

## License

[Apache License 2.0](LICENSE)
