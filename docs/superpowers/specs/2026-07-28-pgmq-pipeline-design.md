# PGMQ Pipeline Queue Foundation Design

## Scope

Build the first isolated queueing slice for the PostgreSQL-backed SWE-gen
pipeline:

```text
SWEgen -> NOP/Oracle -> Reward Hack Checker -> SWR Pusher
```

This slice defines the queue contract, a PGMQ adapter, transactional handoff
orchestration, retry/dead-letter behavior, queue metrics, and bootstrap SQL. It
does not connect existing Slurm workers to the queues, alter the ledger schema,
install host-level PGMQ extension files, or materialize task archives.

The PostgreSQL database is `swegen_distributed`. Application tables remain in
`public`; PGMQ owns its internal `pgmq` schema.

## Considered approaches

### Direct PGMQ adapter with a narrow transaction boundary (selected)

Expose typed queue messages and a small adapter that accepts an existing
psycopg-compatible connection. A handoff method invokes a ledger callback,
sends the next message, and archives the current message inside one connection
transaction. This keeps the queue layer independent of the concurrent ledger
implementation while making the required atomic boundary explicit and
testable.

### Backend-neutral queue abstraction

Define a broad interface intended to support PGMQ, Redis Streams, and RabbitMQ.
This would preserve backend optionality but would either expose only a weak
least-common-denominator API or hide PGMQ's valuable PostgreSQL transaction
semantics. The project has already selected PGMQ, so this abstraction is not
needed now.

### Ledger-specific stored procedures

Put stage completion, enqueue, and archive into PostgreSQL procedures. This
could provide a strong database boundary, but it would couple this work to the
ledger tables while those tables are still being implemented. A later
integration can add stored procedures if profiling or operational experience
justifies them.

## Queue and stage contract

Durable, non-partitioned queues are created with fully qualified PGMQ calls:

| Stage | Queue |
| --- | --- |
| Generate Harbor task | `swegen_generate` |
| NOP/Oracle validation | `swegen_validate` |
| Reward-hack checking | `swegen_reward` |
| SWR push | `swegen_push` |
| Terminal failures | `swegen_dead` |

All names are below PGMQ's 47-character limit. The four processing queues map
one-to-one to pipeline stages; the dead-letter queue retains the failed
message's original stage.

Queue messages contain identifiers and routing metadata only:

```text
schema_version
event_id
task_id
task_version
stage
attempt
trace_id
enqueued_at
```

`schema_version` is initially `1`. UUIDs identify an event and its end-to-end
trace; every stage handoff uses a fresh event ID while retaining the trace ID.
`task_id` is a non-empty string so the adapter does not pre-empt the
ledger's final key type. `task_version` and `attempt` are positive integers.
`enqueued_at` must be timezone-aware. Unknown fields are rejected so artifact
contents, credentials, and ad-hoc JSON cannot silently enter the queue.

## Components

`src/swegen/queueing/models.py` owns immutable Pydantic models, stage ordering,
and stage-to-queue mapping. It has no database dependency.

`src/swegen/queueing/pgmq.py` owns SQL calls and transaction orchestration. It
uses a structural connection protocol rather than importing psycopg, because
the ledger/backfill work is adding the concrete database dependency in a
different worktree. JSON is passed as text and cast to `jsonb`, keeping the
adapter compatible with normal psycopg connections without driver-specific
JSON wrappers.

`src/swegen/queueing/bootstrap.sql` first accepts a database-managed `pgmq`
extension when it is a reviewed 1.x release at or above 1.5.0. If no extension
row exists, it accepts a SQL-only installation only after finding every exact
function signature used by the adapter and the
`pgmq.metrics_result.queue_visible_length` field. PGMQ 1.5 is the first release
containing the complete SQL surface used by this adapter. An unreviewed 2.x
release is rejected until compatibility is confirmed. The bootstrap creates
the five queues and intentionally contains no `CREATE EXTENSION`, database
credentials, or application-table DDL.

## SQL-only deployment and upgrades

The target PostgreSQL host does not expose PGMQ through
`pg_available_extensions`, and host access is outside this deployment's scope.
The selected fallback installs the official PGMQ 1.12.0 SQL objects directly
into the same `swegen_distributed` database so ledger changes, queue sends, and
source-message archives can still share a PostgreSQL transaction.

The reviewed source is the `pgmq-1.12.0.zip` PGXN distribution at
`https://api.pgxn.org/dist/pgmq/1.12.0/pgmq-1.12.0.zip`, pinned to SHA-1
`e8b2eafe878e3b68cba92b874452d6da01d2c19b`. Deployment verifies that checksum
before executing the distribution's `sql/pgmq.sql` and this
repository's bootstrap in one `ON_ERROR_STOP` transaction.

A SQL-only installation has no `pg_extension` ownership or version metadata.
Consequently, `ALTER EXTENSION UPDATE` cannot manage it: upgrades are manual
operations. Each upgrade must pin and verify a new upstream artifact, review
the upstream migration path and required API signatures, take an appropriate
database backup, and apply the reviewed SQL transactionally before changing
the recorded deployment version.

## Claiming and visibility

Workers claim messages with `pgmq.read` or `pgmq.read_with_poll`. A claim
records the PGMQ message ID, read count, enqueue timestamp, visibility deadline,
and validated application message. Workers extend a live claim with
`pgmq.set_vt`. Visibility, quantity, polling, and delay values are validated as
bounded integers before SQL execution.

Primitive operations use the caller's transaction. A worker must commit and
release the short claim transaction before starting a long Harbor/container
run, and commit each heartbeat promptly so other workers observe the new
visibility deadline. It must not keep the claim transaction open while doing
the actual stage work. Completion uses a fresh short connection transaction.

The adapter selects named fields from PGMQ records rather than relying on the
extension's complete record layout. This isolates the application from newer
optional fields such as headers and `last_read_at`.

## Atomic stage handoff

The caller supplies a psycopg-compatible connection and a ledger completion
callback. The callback must return the boolean `True` only when it records a new
stage-attempt completion and `False` when the idempotency key was already
present; any other return type is an error and rolls back. The adapter opens
`connection.transaction()` and performs, in order:

1. Validate that the claimed queue matches the current stage and that the next
   message is the exact successor for the same task version and trace with a
   fresh event ID.
2. Invoke the ledger callback with the same connection.
3. When the callback returned `True`, enqueue the next-stage message, except
   after the final push stage. A duplicate completion does not fan out.
4. Archive the current PGMQ message.

Any callback, enqueue, or archive failure exits the transaction with an
exception. There is no adapter-level `commit()`, so an existing outer
transaction remains authoritative and psycopg may implement the inner boundary
as a savepoint. Worker integrations should not wrap this handoff in an
unrelated long-lived transaction.

Duplicate delivery remains possible and expected. The ledger completion
callback must use its task/stage-attempt uniqueness constraint and report
whether its insert won, making repeated completion harmless. Queue messages do
not carry task files or result data; workers re-read authoritative state from
PostgreSQL.

## Retry and dead-letter behavior

PGMQ's `read_ct` is the delivery count. Before the configured maximum, a failed
message stays in its current queue and `set_vt` schedules its next visibility.
At the maximum, a transaction invokes an optional ledger failure callback,
sends the unchanged identifier message to `swegen_dead` when the callback
reports a newly recorded terminal failure, and archives the source message. If
no callback is supplied, the adapter emits the dead letter. Failure details
belong in PostgreSQL, not in the queue payload.

The message's `attempt` is the ledger stage-attempt identity and is distinct
from PGMQ delivery count. A future worker integration may create a new ledger
attempt and message when policy requires a clean re-execution; this foundation
does not infer that policy.

## Metrics and operations

The adapter exposes `pgmq.metrics` as a typed snapshot including total, visible,
and lifetime message counts plus oldest/newest ages. Queue creation is
idempotent through `pgmq.create`. Bootstrap accepts either a PGMQ extension in
the range `>=1.5.0,<2.0.0` or the complete reviewed SQL-only API. Partitioned
queues and `pg_partman` are deferred until observed queue volume and retention
requirements justify them.

## Testing and acceptance

Unit tests use a recording, DB-API-shaped fake connection. They verify message
validation, exact fully qualified SQL, row decoding, heartbeat behavior,
transaction ordering, rollback propagation, retries, atomic dead-lettering,
metrics, and the absence of `CREATE EXTENSION` in bootstrap SQL.

Acceptance for this slice requires focused queue tests, Ruff on the new Python
files, the complete existing pytest suite, atomic installation in
`swegen_distributed`, a rollback-only queue lifecycle smoke test, and empty
metrics for all five production queues.
