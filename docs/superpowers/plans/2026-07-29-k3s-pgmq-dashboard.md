# k3s/PGMQ Pipeline Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a separate live dashboard on port 8766 for the PostgreSQL/PGMQ+k3s pipeline, including exact active-stage timing, while leaving the legacy port-8765 dashboard unchanged.

**Architecture:** Add a small `swegen.dashboard` package with independent PostgreSQL and kubectl collectors, a last-good snapshot cache, and a dependency-light HTTP server with inline HTML/JavaScript. Add a `pipeline_stage_activity` table maintained by the existing worker claim/heartbeat/completion lifecycle so active tasks expose trustworthy worker, node, start, and heartbeat timestamps. Keep all legacy dashboard code and routes untouched.

**Tech Stack:** Python 3.12, psycopg 3, PGMQ SQL metrics, `kubectl -o json`, `ThreadingHTTPServer`, vanilla HTML/CSS/JavaScript, pytest.

---

## File structure

- Create `src/swegen/dashboard/__init__.py`: public dashboard package marker.
- Create `src/swegen/dashboard/distributed_status.py`: pure aggregation helpers plus PostgreSQL and kubectl collectors.
- Create `src/swegen/dashboard/server.py`: independent last-good cache, HTTP routes, and dashboard HTML.
- Create `src/run_pipeline_dashboard.py`: port-8766 command-line entry point.
- Modify `src/swegen/schema.sql`: durable live activity table and indexes.
- Modify `src/swegen/pipeline/task_store.py`: transactional activity start, heartbeat, and cleanup operations.
- Modify `src/swegen/pipeline/worker.py`: write activity at claim, refresh it with PGMQ heartbeats, and remove it on terminal completion.
- Modify `tests/test_pipeline_models.py`: schema contract tests.
- Modify `tests/test_pipeline_task_store.py`: activity SQL lifecycle tests.
- Modify `tests/test_pipeline_worker.py`: claim, heartbeat, completion, retry, and stale activity behavior.
- Create `tests/test_distributed_dashboard.py`: collectors, queue math, task timing, throughput, and outage behavior.
- Create `tests/test_pipeline_dashboard_server.py`: port default, routes, cache, HTML, and partial failure behavior.

### Task 1: Persist active stage timing

**Files:**
- Modify: `src/swegen/schema.sql`
- Modify: `src/swegen/pipeline/task_store.py`
- Modify: `src/swegen/pipeline/worker.py`
- Test: `tests/test_pipeline_models.py`
- Test: `tests/test_pipeline_task_store.py`
- Test: `tests/test_pipeline_worker.py`

- [ ] **Step 1: Write failing schema tests**

Assert the schema creates `pipeline_stage_activity` keyed by task/version/stage, records PGMQ message/read count, worker/node, `started_at`, and `heartbeat_at`, validates positive identifiers and ordered timestamps, and indexes `heartbeat_at`.

- [ ] **Step 2: Run schema tests and verify RED**

Run: `uv run pytest tests/test_pipeline_models.py -q`

Expected: failure because `pipeline_stage_activity` is absent.

- [ ] **Step 3: Add the activity table**

Add an idempotent table with a foreign key to `pipeline_tasks`, fixed stage check, positive PGMQ fields, nonblank worker/node checks, and `heartbeat_at >= started_at`.

- [ ] **Step 4: Run schema tests and verify GREEN**

Run: `uv run pytest tests/test_pipeline_models.py -q`

Expected: pass.

- [ ] **Step 5: Write failing task-store tests**

Specify these APIs:

```python
store.record_stage_activity(connection, claim, started_at=now, worker_id="pod-a", node_name="node-a")
store.heartbeat_stage_activity(connection, claim, heartbeat_at=later, worker_id="pod-a")
store.clear_stage_activity(connection, claim)
```

Tests require idempotent claim upsert, ownership-checked heartbeat, and exact task/stage/message deletion.

- [ ] **Step 6: Run task-store tests and verify RED**

Run the new tests from `tests/test_pipeline_task_store.py`; expect missing-method failures.

- [ ] **Step 7: Implement minimal task-store activity methods**

Use caller-owned transactions and parameterized SQL. A retry delivery updates read count, worker/node, start, and heartbeat for the same task stage. Heartbeats update only the same task/version/stage/message and worker. Cleanup deletes only that claimed activity row.

- [ ] **Step 8: Run task-store tests and verify GREEN**

Run: `uv run pytest tests/test_pipeline_task_store.py -q`

Expected: pass.

- [ ] **Step 9: Write failing worker lifecycle tests**

Require activity registration before the action runs, heartbeat updates alongside successful PGMQ heartbeat calls, and activity cleanup inside successful, rejected, failed/dead-letter terminal transactions. Retriable and lease-lost executions retain a stale row that the dashboard can identify by heartbeat age.

- [ ] **Step 10: Run worker tests and verify RED**

Run the new tests from `tests/test_pipeline_worker.py`; expect missing activity calls.

- [ ] **Step 11: Implement worker activity lifecycle**

Register activity immediately before entering the action workspace. Extend `ClaimHeartbeat` with an optional callback invoked after each confirmed PGMQ heartbeat. Delete activity through the terminal completion callbacks; do not delete it when ownership is uncertain or the message remains retryable.

- [ ] **Step 12: Run worker tests and verify GREEN**

Run: `uv run pytest tests/test_pipeline_worker.py -q`

Expected: pass.

### Task 2: Aggregate PostgreSQL pipeline telemetry

**Files:**
- Create: `src/swegen/dashboard/__init__.py`
- Create: `src/swegen/dashboard/distributed_status.py`
- Create: `tests/test_distributed_dashboard.py`

- [ ] **Step 1: Write failing tests for queue and task aggregation**

Use real dict-row fixtures. Require visible/in-flight math, dead queue separation, state/current-stage counts, total elapsed time, completed-stage wait/run duration, active-stage run/heartbeat age, PGMQ read-count attempts, worker/node/error fields, and successful completion rates/counts for 60/300/900-second windows.

- [ ] **Step 2: Run aggregation tests and verify RED**

Run: `uv run pytest tests/test_distributed_dashboard.py -q`

Expected: import or missing-function failure.

- [ ] **Step 3: Implement pure aggregation helpers**

Define stable JSON-safe output with four fixed stages. Derive a completed stage's queued time from task creation or predecessor success finish. Use `pipeline_stage_activity` for active start and heartbeat times. Clamp negative durations to zero and preserve bounded errors.

- [ ] **Step 4: Run aggregation tests and verify GREEN**

Run: `uv run pytest tests/test_distributed_dashboard.py -q`

Expected: aggregation tests pass.

- [ ] **Step 5: Write failing PostgreSQL collector tests**

Inject a connection factory and assert the collector executes bounded, read-only queries for tasks/results/activity and `pgmq.metrics_all()`, returns at most the configured recent-task limit, and redacts exception text before exposing it.

- [ ] **Step 6: Run collector tests and verify RED**

Run the collector-specific tests; expect the collector to be absent.

- [ ] **Step 7: Implement `PipelineStatusCollector`**

Use one connection and `SET LOCAL statement_timeout` inside a read-only transaction. Return source metadata plus queues, task counts, recent tasks, and throughput without calling schema bootstrap code.

- [ ] **Step 8: Run collector tests and verify GREEN**

Run: `uv run pytest tests/test_distributed_dashboard.py -q`

Expected: pass.

### Task 3: Collect k3s cluster and worker health

**Files:**
- Modify: `src/swegen/dashboard/distributed_status.py`
- Modify: `tests/test_distributed_dashboard.py`

- [ ] **Step 1: Write failing kubectl JSON mapping tests**

Cover Ready and NotReady nodes, pressure conditions, deployment desired/ready/available/unavailable counts, pod phase/readiness/restarts, stage label mapping, node distribution, unknown stages, empty lists, command timeouts, nonzero exits, and invalid JSON.

- [ ] **Step 2: Run kubectl collector tests and verify RED**

Run the new tests; expect missing collector/mapping functions.

- [ ] **Step 3: Implement `K3sStatusCollector`**

Invoke argv arrays only:

```text
kubectl --request-timeout=3s get nodes -o json
kubectl --request-timeout=3s -n swegen-pipeline get deployments,pods -l app.kubernetes.io/name=swegen-worker -o json
```

Accept optional kubeconfig/context/namespace and an injected runner. Bound timeout/output errors and never invoke a shell.

- [ ] **Step 4: Run kubectl collector tests and verify GREEN**

Run: `uv run pytest tests/test_distributed_dashboard.py -q`

Expected: pass.

### Task 4: Serve the separate port-8766 dashboard safely

**Files:**
- Create: `src/swegen/dashboard/server.py`
- Create: `src/run_pipeline_dashboard.py`
- Create: `tests/test_pipeline_dashboard_server.py`

- [ ] **Step 1: Write failing cache tests**

Require independent k3s and PostgreSQL refreshes, last-good retention, per-source fetched/stale/error metadata, thread safety, and one collector failure not hiding the other source.

- [ ] **Step 2: Run cache tests and verify RED**

Run: `uv run pytest tests/test_pipeline_dashboard_server.py -q`

Expected: import or missing-class failure.

- [ ] **Step 3: Implement the snapshot cache**

Refresh both collectors independently every five seconds. Serialize one immutable JSON body under a lock. Mark data stale using source-specific age thresholds and retain prior data after errors.

- [ ] **Step 4: Run cache tests and verify GREEN**

Run cache-specific tests; expect pass.

- [ ] **Step 5: Write failing HTTP and HTML tests**

Require `/` HTML, `/api/pipeline/status` JSON, `/healthz`, 404 behavior, `Cache-Control: no-store`, default host `127.0.0.1`, default port `8766`, four stage cards, nodes, queues, dead letters, throughput windows, task state cards, expandable per-task timelines, safe text rendering, and source-disconnected banners. Assert the legacy `run_dashboard.py` default remains 8765 and its routes are not imported or modified.

- [ ] **Step 6: Run server tests and verify RED**

Run the new HTTP/HTML tests; expect missing server/entry point failures.

- [ ] **Step 7: Implement server, CLI, and inline UI**

Use `ThreadingHTTPServer`, a background refresh thread, graceful SIGTERM/SIGINT shutdown, and JSON-only DOM construction with `textContent`. The root UI polls `/api/pipeline/status` every five seconds and keeps displaying the last response when a fetch fails.

- [ ] **Step 8: Run server tests and verify GREEN**

Run: `uv run pytest tests/test_pipeline_dashboard_server.py -q`

Expected: pass.

### Task 5: Integrate, launch, and verify

**Files:**
- Review all files above; do not modify `src/run_dashboard.py` or `tests/test_run_dashboard.py` unless a regression test requires an assertion-only change.

- [ ] **Step 1: Run focused tests**

```bash
uv run pytest \
  tests/test_pipeline_models.py \
  tests/test_pipeline_task_store.py \
  tests/test_pipeline_worker.py \
  tests/test_distributed_dashboard.py \
  tests/test_pipeline_dashboard_server.py \
  tests/test_run_dashboard.py -q
```

Expected: all pass.

- [ ] **Step 2: Run formatting and static checks**

```bash
uv run ruff format --check src/swegen/dashboard src/run_pipeline_dashboard.py \
  src/swegen/pipeline/task_store.py src/swegen/pipeline/worker.py \
  tests/test_distributed_dashboard.py tests/test_pipeline_dashboard_server.py
uv run ruff check src/swegen/dashboard src/run_pipeline_dashboard.py \
  src/swegen/pipeline/task_store.py src/swegen/pipeline/worker.py \
  tests/test_distributed_dashboard.py tests/test_pipeline_dashboard_server.py
git diff --check
```

Expected: all pass with no warnings.

- [ ] **Step 3: Apply the idempotent schema to the live database**

Use an existing worker pod's configured PostgreSQL environment to execute only the new table/index DDL. Query `information_schema` afterward to prove the table and columns exist without printing credentials.

- [ ] **Step 4: Launch the new service on port 8766**

Start `src/run_pipeline_dashboard.py --host 0.0.0.0 --port 8766` under a dedicated recoverable service/process without changing or restarting the legacy port-8765 dashboard. Confirm listeners and both health endpoints independently.

- [ ] **Step 5: Smoke-test live JSON and page rendering**

Fetch `http://127.0.0.1:8766/api/pipeline/status` and assert four Ready cluster nodes, four stage keys, queue metrics, task counts, and no credentials. Fetch `/` and confirm HTTP 200. Confirm `http://127.0.0.1:8765/healthz` remains unchanged if the legacy service is running.

- [ ] **Step 6: Review worktree state**

Run `git status --short` and `git diff --check`. Report only dashboard/activity files changed by this task plus pre-existing unrelated modifications. Do not commit or push.
