# PGMQ + K3s Worker Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run one Kubernetes worker per stage and complete one unseen Harbor task through SWEgen, NOP/Oracle, reward-hack checking, and SWR push using PostgreSQL and PGMQ as the only authoritative pipeline state.

**Architecture:** A common long-lived worker claims one stage-specific PGMQ message, heartbeats visibility during long work, materializes task files from normalized PostgreSQL rows, and atomically records completion, enqueues the successor, and archives the source. Four K3s Deployments reuse one image; Harbor builds use the host Docker socket while kubelet and a separately gated Docker cleaner bound the independent containerd and BuildKit stores.

**Tech Stack:** Python 3.12, psycopg 3, PostgreSQL, SQL-only PGMQ 1.12, Pydantic 2, pytest, Docker/BuildKit, K3s v1.36.2, Kubernetes YAML, SWR.

---

### Task 1: Add pipeline database tables and typed records

**Files:**
- Modify: `src/swegen/schema.sql`
- Create: `src/swegen/pipeline/__init__.py`
- Create: `src/swegen/pipeline/models.py`
- Create: `tests/test_pipeline_models.py`

- [ ] **Step 1: Write failing model and schema-contract tests**

Add tests for fixed task/result states, expected-rejection semantics, safe task
identity, and the new table/index/constraint names. The core assertions are:

```python
def test_stage_execution_rejects_without_handoff() -> None:
    execution = StageExecution.rejected({"reason": "NOP reward was 1"})
    assert execution.status is StageResultStatus.REJECTED
    assert execution.should_handoff is False


def test_pipeline_schema_has_idempotent_stage_key() -> None:
    sql = Path("src/swegen/schema.sql").read_text()
    assert "CREATE TABLE IF NOT EXISTS pipeline_tasks" in sql
    assert "CREATE TABLE IF NOT EXISTS pipeline_task_files" in sql
    assert "CREATE TABLE IF NOT EXISTS pipeline_stage_results" in sql
    assert "PRIMARY KEY (task_id, task_version, stage, attempt)" in sql
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `uv run pytest tests/test_pipeline_models.py -q`

Expected: import failures for `swegen.pipeline.models` and missing schema text.

- [ ] **Step 3: Implement the models and idempotent DDL**

Define these exact public types:

```python
class PipelineTaskState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    REJECTED = "rejected"
    FAILED = "failed"
    COMPLETED = "completed"


class StageResultStatus(StrEnum):
    SUCCEEDED = "succeeded"
    REJECTED = "rejected"
    FAILED = "failed"


@dataclass(frozen=True)
class StageExecution:
    status: StageResultStatus
    result: dict[str, object]
    files: tuple[TaskFile, ...] = ()

    @property
    def should_handoff(self) -> bool:
        return self.status is StageResultStatus.SUCCEEDED
```

Add table checks for task state, stage enum values (`generate`, `validate`,
`reward`, `push`), result status, positive version/attempt, safe file sizes, the
task/file foreign key, and the stage-result primary key. Use `TIMESTAMPTZ` and
`JSONB`; do not add credentials or JSONL paths.

- [ ] **Step 4: Run tests and commit**

Run: `uv run pytest tests/test_pipeline_models.py -q`

Expected: PASS.

Commit:

```bash
git add src/swegen/schema.sql src/swegen/pipeline tests/test_pipeline_models.py
git commit -m "feat: add distributed pipeline task schema"
```

### Task 2: Implement safe task-file capture and materialization

**Files:**
- Create: `src/swegen/pipeline/task_store.py`
- Create: `tests/test_pipeline_task_store.py`

- [ ] **Step 1: Write failing path, digest, and database-operation tests**

Cover regular nested files, executable modes, binary content, path traversal,
symlinks, per-file/total limits, digest mismatch, deterministic ordering, task
lookup, file replacement, and idempotent stage-result inserts. Use a recording
psycopg-shaped connection and real temporary directories.

```python
def test_capture_rejects_symlink(tmp_path: Path) -> None:
    root = tmp_path / "task"
    root.mkdir()
    (root / "target").write_text("x")
    (root / "link").symlink_to("target")
    with pytest.raises(TaskFileError, match="symlink"):
        capture_task_files(root)


def test_materialize_verifies_digest(tmp_path: Path) -> None:
    stored = TaskFile(path="tests/test.sh", content=b"exit 0\n", mode=0o755,
                      sha256="0" * 64)
    with pytest.raises(TaskFileError, match="digest"):
        materialize_task_files([stored], tmp_path / "out")
```

- [ ] **Step 2: Run tests and verify RED**

Run: `uv run pytest tests/test_pipeline_task_store.py -q`

Expected: missing-module/import failures.

- [ ] **Step 3: Implement the minimal store**

Provide:

```python
def capture_task_files(root: Path, *, max_file_bytes: int, max_task_bytes: int) -> tuple[TaskFile, ...]: ...
def materialize_task_files(files: Iterable[TaskFile], destination: Path) -> None: ...

class TaskStore:
    def get_task(self, connection, task_id: str, task_version: int) -> PipelineTask: ...
    def load_files(self, connection, task_id: str, task_version: int) -> tuple[TaskFile, ...]: ...
    def replace_files(self, connection, task: PipelineTask, files: tuple[TaskFile, ...]) -> None: ...
    def record_stage_result(self, connection, claim, execution, *, started_at, worker_id, node_name) -> bool: ...
    def record_terminal_failure(self, connection, claim, error, *, started_at, worker_id, node_name) -> bool: ...
```

Use parameterized SQL only. `record_stage_result()` returns `True` only when
`INSERT ... ON CONFLICT DO NOTHING RETURNING` inserts the idempotency key. When
new, replace generated files if present, update task state/current stage, and
insert the existing `pushed_images` inventory row for a successful push.

- [ ] **Step 4: Run focused tests and commit**

Run: `uv run pytest tests/test_pipeline_task_store.py -q`

Expected: PASS.

Commit:

```bash
git add src/swegen/pipeline/task_store.py tests/test_pipeline_task_store.py
git commit -m "feat: store Harbor task files in Postgres"
```

### Task 3: Add atomic terminal completion to PGMQ

**Files:**
- Modify: `src/swegen/queueing/pgmq.py`
- Modify: `tests/test_pgmq_queue.py`

- [ ] **Step 1: Write a failing terminal-completion test**

```python
def test_complete_terminal_records_and_archives_without_successor() -> None:
    connection = RecordingConnection([pgmq_row(message)], [(True,)])
    queue = PgmqQueue()

    inserted = queue.complete_terminal(
        connection,
        claim,
        complete_stage=lambda _connection, _claim: True,
    )

    assert inserted is True
    assert [event.name for event in connection.events] == [
        "transaction-enter", "callback", "archive", "transaction-exit"
    ]
    assert all("pgmq.send" not in query for query, _ in connection.calls)
```

Also test a duplicate callback (`False`) and a non-boolean callback result.

- [ ] **Step 2: Run and verify RED**

Run: `uv run pytest tests/test_pgmq_queue.py -q`

Expected: `PgmqQueue.complete_terminal` is missing.

- [ ] **Step 3: Implement terminal completion**

```python
def complete_terminal(self, connection, current, *, complete_stage) -> bool:
    self._validate_current_claim(current)
    with connection.transaction():
        newly_completed = _require_boolean_callback_result(
            "Terminal completion", complete_stage(connection, current)
        )
        self.archive(connection, current)
        return newly_completed
```

- [ ] **Step 4: Run queue tests and commit**

Run: `uv run pytest tests/test_pgmq_queue.py -q`

Expected: all queue tests PASS.

Commit:

```bash
git add src/swegen/queueing/pgmq.py tests/test_pgmq_queue.py
git commit -m "feat: archive expected terminal queue outcomes"
```

### Task 4: Build the worker loop and visibility heartbeat

**Files:**
- Create: `src/swegen/pipeline/worker.py`
- Create: `tests/test_pipeline_worker.py`

- [ ] **Step 1: Write failing claim/handoff/retry/shutdown tests**

Use injected connection factories, queue/store fakes, and a no-op action. Cover:

- empty poll returns without work;
- claim commits before stage execution;
- heartbeat uses an independent connection and stops after execution;
- success creates the exact successor message with a fresh event UUID and the
  same task/version/trace;
- rejection calls `complete_terminal` and emits no successor;
- exception retries before maximum delivery count;
- exhausted exception records terminal failure and dead-letters;
- SIGTERM stops future claims without interrupting the active action.

```python
def test_rejected_execution_archives_without_handoff() -> None:
    action = FakeAction(StageExecution.rejected({"reason": "hacking"}))
    worker = make_worker(action=action, claim=reward_claim())
    assert worker.run_once() is True
    assert worker.queue.completed_terminal == [reward_claim()]
    assert worker.queue.handoffs == []
```

- [ ] **Step 2: Run and verify RED**

Run: `uv run pytest tests/test_pipeline_worker.py -q`

Expected: missing worker module.

- [ ] **Step 3: Implement the worker engine**

Add `WorkerSettings`, `ClaimHeartbeat`, `PipelineWorker.run_once()`,
`PipelineWorker.run_forever()`, stage parsing, redaction, and CLI setup. Default
settings: one claim, 300-second visibility, 60-second heartbeat, 10-second poll,
three deliveries, and 300-second retry visibility. Use the existing `db` pool
and `PgmqQueue`; never hold a database transaction during stage work.

- [ ] **Step 4: Run tests and commit**

Run: `uv run pytest tests/test_pipeline_worker.py -q`

Expected: PASS.

Commit:

```bash
git add src/swegen/pipeline/worker.py tests/test_pipeline_worker.py
git commit -m "feat: add resilient PGMQ stage worker loop"
```

### Task 5: Implement all four stage actions

**Files:**
- Create: `src/swegen/pipeline/actions.py`
- Create: `tests/test_pipeline_actions.py`

- [ ] **Step 1: Write failing stage-behavior tests**

Test command construction and classification through injected callables:

```python
def test_generate_disables_inline_final_validation(tmp_path: Path) -> None:
    command = build_generate_command(task, tmp_path)
    assert command[:2] == ["swegen", "create"]
    assert "--no-validate" in command
    assert "--force" in command
    assert "--no-require-minimum-difficulty" in command
    assert "--no-require-issue" in command


@pytest.mark.parametrize(
    ("nop", "oracle", "status"),
    [(0, 1, StageResultStatus.SUCCEEDED), (1, 1, StageResultStatus.REJECTED),
     (0, 0, StageResultStatus.REJECTED)],
)
def test_validate_classifies_rewards(nop: int, oracle: int, status) -> None: ...


def test_reward_infrastructure_error_raises_for_retry() -> None: ...
def test_reward_hacking_verdict_is_rejected() -> None: ...
def test_push_skips_build_when_manifest_exists() -> None: ...
def test_push_removes_local_image_after_success() -> None: ...
```

- [ ] **Step 2: Run and verify RED**

Run: `uv run pytest tests/test_pipeline_actions.py -q`

Expected: missing action module.

- [ ] **Step 3: Implement stage actions by reusing existing helpers**

Generate uses the `swegen create` CLI and captures the resulting task tree.
Validate uses `run_harbor_agent()` and `parse_harbor_outcome()` with NOP before
Oracle. Reward uses `build_test_bundle()` and
`check_instance_with_fallback()` with environment-configured endpoint/model/key.
Push uses `image_exists_in_registry()`, `build_image_direct()`,
`push_to_registry()`, and `remove_local_image()`.

Use a bounded logged-command helper rather than retaining unlimited subprocess
output. A valid rejection returns `StageExecution.rejected`; only infrastructure
or execution failures raise an exception for PGMQ retry.

- [ ] **Step 4: Run action and worker tests and commit**

Run:

```bash
uv run pytest tests/test_pipeline_actions.py tests/test_pipeline_worker.py -q
```

Expected: PASS.

Commit:

```bash
git add src/swegen/pipeline/actions.py tests/test_pipeline_actions.py src/swegen/pipeline/worker.py
git commit -m "feat: connect PGMQ workers to all pipeline stages"
```

### Task 6: Add enqueue, status, materialize, and ZIP export commands

**Files:**
- Create: `src/swegen/pipeline/cli.py`
- Create: `tests/test_pipeline_cli.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: Write failing command tests**

Cover canonical task IDs, atomic task+generate enqueue, duplicate refusal,
queue/task status output, digest-checked materialization, and on-demand ZIP
contents. The enqueue callback must send only when the task insert returns a
row.

- [ ] **Step 2: Run and verify RED**

Run: `uv run pytest tests/test_pipeline_cli.py -q`

Expected: missing CLI module/entry point.

- [ ] **Step 3: Implement the CLI**

Expose a `swegen-pipeline` entry point with:

```text
swegen-pipeline enqueue --repo OWNER/REPO --pr N [--task-version 1]
swegen-pipeline status [--task-id ID]
swegen-pipeline materialize TASK_ID DESTINATION
swegen-pipeline export-zip TASK_ID OUTPUT.zip
```

`enqueue` creates a UUID trace/event and performs task insert + PGMQ send in one
transaction. `export-zip` writes file rows directly into a new ZIP and does not
retain an extracted directory.

- [ ] **Step 4: Run tests and commit**

Run: `uv run pytest tests/test_pipeline_cli.py -q`

Expected: PASS.

Commit:

```bash
git add src/swegen/pipeline/cli.py tests/test_pipeline_cli.py pyproject.toml uv.lock
git commit -m "feat: add distributed pipeline operations CLI"
```

### Task 7: Package the worker and Kubernetes resources

**Files:**
- Create: `.dockerignore`
- Create: `deploy/k3s/Dockerfile.worker`
- Create: `deploy/k3s/swegen-pipeline.yaml`
- Create: `deploy/k3s/create-secrets.sh`
- Create: `deploy/k3s/build-import-worker.sh`
- Create: `deploy/k3s/docker-cache-cleaner.sh`
- Create: `tests/test_k3s_pipeline_manifests.py`

- [ ] **Step 1: Write failing static packaging tests**

Parse every YAML document and assert one namespace, one ConfigMap, four
Deployments with one replica each, stage-specific arguments, secret references,
workspace `emptyDir.sizeLimit`, Docker socket hostPath only on generate,
validate, and push, node selectors that avoid `7.244.2.110`, and an opt-in
cache-cleaner DaemonSet that never prunes volumes.

Assert the Dockerfile pins Node 22 and Claude Code `2.1.206`, uses the lockfile,
contains Docker CLI, and starts the pipeline worker.

- [ ] **Step 2: Run and verify RED**

Run: `uv run pytest tests/test_k3s_pipeline_manifests.py -q`

Expected: missing packaging files.

- [ ] **Step 3: Implement image and manifests**

Use one image and four Deployments. Set resource requests/limits and a long
termination grace period. Mount:

```yaml
- name: workspace
  emptyDir:
    sizeLimit: 500Gi
- name: repo-cache
  hostPath:
    path: /data/swegen-k3s/cache
    type: DirectoryOrCreate
- name: docker-sock
  hostPath:
    path: /var/run/docker.sock
    type: Socket
```

Mount the Docker socket only where required. Secrets are created with a
dry-run/apply pipe and never rendered to a repository file. The cleaner runs
`docker buildx prune --force --filter until=6h --reserved-space 20GB --max-used-space 200GB --min-free-space 2TB` and
`docker image prune -a --force --filter until=24h`; it contains no volume-prune
command and schedules only on nodes labeled
`swegen.pgcode/docker-gc=enabled`.

- [ ] **Step 4: Run packaging tests, build image, and commit**

Run:

```bash
uv run pytest tests/test_k3s_pipeline_manifests.py -q
docker build -f deploy/k3s/Dockerfile.worker -t swegen-worker:e2e .
```

Expected: tests pass and image build exits zero.

Commit:

```bash
git add .dockerignore deploy/k3s tests/test_k3s_pipeline_manifests.py
git commit -m "feat: package PGMQ workers for k3s"
```

### Task 8: Run repository verification before deployment

**Files:**
- Verify all new pipeline and packaging files

- [ ] **Step 1: Format and lint changed Python**

Run:

```bash
uv run ruff format --check src/swegen/pipeline src/swegen/queueing tests/test_pipeline_*.py tests/test_k3s_pipeline_manifests.py
uv run ruff check src/swegen/pipeline src/swegen/queueing tests/test_pipeline_*.py tests/test_k3s_pipeline_manifests.py
```

Expected: both exit zero.

- [ ] **Step 2: Run focused and full tests**

Run:

```bash
uv run pytest tests/test_pgmq_queue.py tests/test_pipeline_models.py tests/test_pipeline_task_store.py tests/test_pipeline_worker.py tests/test_pipeline_actions.py tests/test_pipeline_cli.py tests/test_k3s_pipeline_manifests.py -q
uv run pytest -q
```

Expected: all focused tests and the complete suite pass.

- [ ] **Step 3: Apply schema and inspect live capabilities**

Authenticate interactively, run `src/swegen/schema.sql`, and verify the three
new tables, exact primary keys, all five PGMQ queues, and zero processing
messages before enqueueing.

### Task 9: Deploy one worker pod per stage

**Files:**
- Execute: `deploy/k3s/create-secrets.sh`
- Execute: `deploy/k3s/build-import-worker.sh`
- Apply: `deploy/k3s/swegen-pipeline.yaml`

- [ ] **Step 1: Import the worker image on selected nodes**

Build once, save to a `mktemp` archive, import into K3s containerd on
`7.244.3.200`, `7.244.3.78`, and `7.244.1.209`, verify the image exists, then
delete only that explicit temporary archive.

- [ ] **Step 2: Create Kubernetes Secrets without displaying values**

Use the existing root-only SWE-gen config/model credential files and Docker
config. Prompt silently for the database password. Verify only Secret names and
key names, never decoded contents.

- [ ] **Step 3: Apply and verify workers**

Run:

```bash
sudo k3s kubectl apply -f deploy/k3s/swegen-pipeline.yaml
sudo k3s kubectl -n swegen-pipeline rollout status deployment/swegen-generate --timeout=5m
sudo k3s kubectl -n swegen-pipeline rollout status deployment/swegen-validate --timeout=5m
sudo k3s kubectl -n swegen-pipeline rollout status deployment/swegen-reward --timeout=5m
sudo k3s kubectl -n swegen-pipeline rollout status deployment/swegen-push --timeout=5m
```

Expected: exactly four Ready worker pods, one per stage, none on
`7.244.2.110`.

### Task 10: Execute and prove the live end-to-end task

**Files:**
- Runtime state only; no JSONL ledger is authoritative

- [ ] **Step 1: Select and enqueue one unseen PR**

Choose a source-list candidate absent from `pipeline_tasks`, `create_success`,
and existing task directories. Record the repo/PR/task ID in the operator log,
then run `swegen-pipeline enqueue` exactly once.

- [ ] **Step 2: Monitor every transition**

Continuously inspect task status, PGMQ metrics, stage-result rows, pod status,
and bounded log tails. If a stage fails, use the systematic-debugging skill,
write a failing regression test, fix it, rebuild/import the image, roll only the
affected Deployment, and continue the same trace unless the task reached a
legitimate rejection.

- [ ] **Step 3: Verify final acceptance evidence**

Query PostgreSQL for one completed task, four ordered successful stage results,
stored file count/bytes, and the `pushed_images.pushed=true` row. Confirm all
processing queues are empty, the task is absent from `swegen_dead`, the remote
manifest resolves, no task Harbor containers remain, and participating hosts'
Docker/containerd usage is recorded.

- [ ] **Step 4: Export the task archive from PostgreSQL**

Run `swegen-pipeline export-zip TASK_ID /tmp/TASK_ID.zip`, inspect its file list,
and delete only that explicit temporary ZIP after validation.

### Task 11: Final verification, documentation, and push

**Files:**
- Modify: `README.md` or a focused operations document under `docs/`
- Update: this plan's checkboxes as work completes

- [ ] **Step 1: Document live commands and cleanup boundaries**

Document scaling replica counts, inspecting/dead-lettering queues, replaying a
new task version, task ZIP export, kubelet/containerd GC versus host Docker
BuildKit GC, SWR retention ownership, and rollback to the still-running Slurm
services.

- [ ] **Step 2: Run fresh final verification**

Repeat focused lint, focused tests, the full suite, `kubectl get pods`, live DB
acceptance queries, `docker manifest inspect`, and both worktree status checks.

- [ ] **Step 3: Commit and push**

```bash
git add docs README.md
git commit -m "docs: record k3s pipeline operations and e2e proof"
git push origin slurm-swegen
```

Expected: `origin/slurm-swegen` contains the implementation and evidence-backed
operations documentation; the integration and main worktrees are clean.
