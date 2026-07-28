# PGMQ Queue Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a typed, transaction-safe PGMQ queue foundation for the four-stage SWE-gen pipeline without modifying the concurrent ledger implementation.

**Architecture:** Immutable Pydantic messages map pipeline stages to fixed PGMQ queues. A driver-light adapter accepts an existing psycopg-compatible connection and owns claims, visibility heartbeats, atomic handoff/dead-letter transactions, and metrics; bootstrap SQL accepts either a compatible extension or a capability-checked SQL-only installation.

**Tech Stack:** Python 3.12, Pydantic 2, PostgreSQL, PGMQ, pytest, Ruff.

---

Git commit steps from the standard workflow are intentionally deferred because
the user requested no git operations while the ledger/backfill agent is active.

### Task 1: Define the queue contract

**Files:**
- Create: `src/swegen/queueing/__init__.py`
- Create: `src/swegen/queueing/models.py`
- Test: `tests/test_pgmq_queue.py`

- [ ] **Step 1: Write failing validation and routing tests**

Add tests that construct a valid version-1 message, reject an unknown field,
reject a naive `enqueued_at`, reject empty task IDs and non-positive versions,
and assert the exact stage/queue sequence:

```python
assert queue_for_stage(PipelineStage.GENERATE) is QueueName.GENERATE
assert PipelineStage.GENERATE.next_stage is PipelineStage.VALIDATE
assert PipelineStage.PUSH.next_stage is None
```

- [ ] **Step 2: Verify the tests fail for the missing package**

Run: `uv run pytest tests/test_pgmq_queue.py -q`

Expected: collection fails because `swegen.queueing` does not exist.

- [ ] **Step 3: Implement immutable models and mappings**

Create `PipelineStage`, `QueueName`, `QueueMessage`, `ClaimedMessage`,
`QueueMetrics`, `RetryDisposition`, and `queue_for_stage`. Configure Pydantic
with `extra="forbid"` and `frozen=True`; require timezone-aware datetimes.

- [ ] **Step 4: Verify model tests pass**

Run: `uv run pytest tests/test_pgmq_queue.py -q`

Expected: all Task 1 tests pass.

### Task 2: Add primitive PGMQ operations

**Files:**
- Create: `src/swegen/queueing/pgmq.py`
- Modify: `src/swegen/queueing/__init__.py`
- Modify: `tests/test_pgmq_queue.py`

- [ ] **Step 1: Write failing send, claim, heartbeat, archive, and metrics tests**

Use a recording connection whose `execute()` returns configured rows. Assert
fully qualified calls and parameters, including:

```python
("swegen_generate", message.model_dump_json(), 0)
("swegen_validate", 300, 1)
("swegen_validate", claimed.msg_id, 300)
```

Cover both tuple rows and mapping rows and verify malformed queue payloads raise
Pydantic validation errors.

- [ ] **Step 2: Verify the new tests fail for missing operations**

Run: `uv run pytest tests/test_pgmq_queue.py -q`

Expected: import or attribute failures for `PgmqQueue`.

- [ ] **Step 3: Implement the connection protocol and primitive operations**

Implement `send`, `claim`, `heartbeat`, `archive`, and `metrics`. Select stable
named PGMQ record fields, cast outbound JSON text with `%s::jsonb`, derive
processing queues from the message stage, and raise `QueueOperationError` when
an expected row or archive result is absent.

- [ ] **Step 4: Verify primitive operation tests pass**

Run: `uv run pytest tests/test_pgmq_queue.py -q`

Expected: all Task 1-2 tests pass.

### Task 3: Enforce transactional handoffs

**Files:**
- Modify: `src/swegen/queueing/pgmq.py`
- Modify: `tests/test_pgmq_queue.py`

- [ ] **Step 1: Write failing atomic handoff tests**

Test `complete_and_handoff()` with a transaction-recording fake. Assert the
order `transaction-enter`, ledger callback, `pgmq.send`, `pgmq.archive`,
`transaction-exit`; assert the same task ID, task version, trace ID, and exact
successor stage are required and that the successor has a fresh event ID. Test
that a callback returning `False` archives a duplicate without sending another
successor, while a non-boolean return rolls back. Test the final push handoff
archives without a send. Test callback and archive failures leave the exception
visible to the transaction context.

- [ ] **Step 2: Verify the handoff tests fail**

Run: `uv run pytest tests/test_pgmq_queue.py -q`

Expected: `complete_and_handoff` is missing.

- [ ] **Step 3: Implement validated handoff orchestration**

Add a `StageCompletion` callback protocol returning whether the idempotent
ledger insert was new, and implement the transaction order from the design.
Return the next PGMQ message ID, or `None` for the final or duplicate stage.
Do not call `commit()` or import the ledger repository.

- [ ] **Step 4: Verify handoff tests pass**

Run: `uv run pytest tests/test_pgmq_queue.py -q`

Expected: all Task 1-3 tests pass.

### Task 4: Add retry and atomic dead-letter policy

**Files:**
- Modify: `src/swegen/queueing/pgmq.py`
- Modify: `tests/test_pgmq_queue.py`

- [ ] **Step 1: Write failing retry/dead-letter tests**

For `read_count < max_deliveries`, assert `set_vt` is called and the result is
`RetryDisposition.RETRY`. At the limit, assert transaction order is optional
ledger callback, `pgmq.send("swegen_dead", ...)`, source archive, and result
`RetryDisposition.DEAD_LETTER`. Assert a callback returning `False` suppresses
duplicate dead-letter emission while still archiving the source. Reject
non-positive delivery limits and delays, and reject non-boolean callback
results.

- [ ] **Step 2: Verify the policy tests fail**

Run: `uv run pytest tests/test_pgmq_queue.py -q`

Expected: `retry_or_dead_letter` is missing.

- [ ] **Step 3: Implement the retry/dead-letter policy**

Keep retries in place by changing visibility. For dead letters, send the same
validated identifier payload to the dead queue and archive the source inside
one connection transaction. Failure detail remains the callback's ledger
responsibility.

- [ ] **Step 4: Verify policy tests pass**

Run: `uv run pytest tests/test_pgmq_queue.py -q`

Expected: all Task 1-4 tests pass.

### Task 5: Add safe queue bootstrap SQL

**Files:**
- Create: `src/swegen/queueing/bootstrap.sql`
- Modify: `tests/test_pgmq_queue.py`

- [ ] **Step 1: Write a failing bootstrap contract test**

Load the packaged SQL and assert it checks `pg_extension`, contains five fully
qualified `pgmq.create` calls, contains every fixed queue name, requires PGMQ
`>=1.5.0,<2.0.0`, and falls back to exact `to_regprocedure` checks for `create`,
`send`, `read`, `read_with_poll`, `set_vt`, `archive`, and `metrics`. Also assert
it checks the `pgmq.metrics_result.queue_visible_length` composite field and
does not contain `CREATE EXTENSION`, credentials, or application table DDL.

- [ ] **Step 2: Verify the bootstrap test fails because the file is absent**

Run: `uv run pytest tests/test_pgmq_queue.py -q`

Expected: `FileNotFoundError` or resource lookup failure.

- [ ] **Step 3: Write the bootstrap SQL**

Use a `DO` block to validate a present extension against the reviewed
`>=1.5.0,<2.0.0` range. When there is no extension row, require the complete
reviewed SQL-only API and metrics composite field before running one
`SELECT pgmq.create(...)` per queue. Do not install extensions or modify the
`public` schema.

- [ ] **Step 4: Verify the bootstrap contract passes**

Run: `uv run pytest tests/test_pgmq_queue.py -q`

Expected: all focused tests pass.

### Task 6: Install the pinned SQL-only distribution

**Files:**
- Read: upstream `pgmq-1.12.0.zip`
- Execute: upstream `sql/pgmq.sql`
- Execute: `src/swegen/queueing/bootstrap.sql`

- [ ] **Step 1: Download and verify the upstream artifact**

Download `https://api.pgxn.org/dist/pgmq/1.12.0/pgmq-1.12.0.zip` into a
temporary directory and verify SHA-1
`e8b2eafe878e3b68cba92b874452d6da01d2c19b`. Stop without executing SQL if the
checksum differs.

- [ ] **Step 2: Install SQL objects and queues atomically**

Connect directly to database `swegen_distributed` and run `psql -1` with
`ON_ERROR_STOP=1`, first loading the distribution's `sql/pgmq.sql` and then the
repository bootstrap. Supply the
password interactively; never place it in a command, environment variable, or
file.

- [ ] **Step 3: Verify production queues and SQL-only capabilities**

Query the PGMQ catalogs and metrics to confirm all five fixed queues exist,
each required function signature resolves, the metrics composite has
`queue_visible_length`, and every production queue is empty.

- [ ] **Step 4: Run a rollback-only lifecycle smoke test**

Inside a transaction, create `swegen_smoke`, send and read one identifier-only
message, change its visibility, archive it, then roll back. Confirm afterward
that neither the temporary queue nor its backing objects remain.

SQL-only upgrades are manual because there is no `pg_extension` version row.
Pin and verify each new release, review its migration SQL and API compatibility,
back up the database, and apply the reviewed upgrade transactionally.

### Task 7: Verify the isolated slice

**Files:**
- Verify: `src/swegen/queueing/*.py`
- Verify: `tests/test_pgmq_queue.py`
- Verify: complete repository test suite

- [ ] **Step 1: Format and lint only the new files**

Run:

```bash
uv run ruff format --check src/swegen/queueing tests/test_pgmq_queue.py
uv run ruff check src/swegen/queueing tests/test_pgmq_queue.py
```

Expected: both commands exit zero.

- [ ] **Step 2: Run the focused tests**

Run: `uv run pytest tests/test_pgmq_queue.py -q`

Expected: all focused tests pass.

- [ ] **Step 3: Run the complete suite**

Run: `uv run pytest -q`

Expected: no regressions relative to the existing 265-test baseline.

- [ ] **Step 4: Verify the live deployment gate**

Connect to `swegen_distributed` and check both supported deployment modes:

```sql
SELECT extversion FROM pg_extension WHERE extname = 'pgmq';
SELECT to_regprocedure('pgmq.send(text,jsonb,integer)');
```

Expected: either one supported extension version or a null extension row with
the complete capability-checked SQL-only API installed.
