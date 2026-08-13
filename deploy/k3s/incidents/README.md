# Incident SQL — forensic records, not migrations

These files are **one-off scripts that were already run** against
`swegen_distributed` to recover from a specific, dated incident. They are
forensic records, **not re-runnable migrations**, and nothing applies them
automatically.

Do not run one to "set up" or "repair" an environment. Each was written against
the exact queue and table state of its own incident; re-running one today would
requeue work that has since completed, or rebuild a queue from a snapshot that
no longer reflects reality. `fix-reward-queue-schema-20260812.sql` in particular
purges an entire queue.

Cumulative schema state lives in the `migrate-*.sql` files one directory up, in
`deploy/k3s/`. Those are the files to read to understand the current schema;
these are the files to read to understand **why the queues hold what they hold**.

Read them for that history: each explains the failure mode (a CoreDNS outage
after a pod-CIDR widening, a Docker daemon outage, build-slot starvation
timeouts, reward 401s, a queue schema mismatch that stalled workers for an
hour), which rows it touched, and — just as importantly — which rows it
deliberately left alone. Most write an audit table recording exactly what was
requeued. When a task's `stage_attempt` looks inexplicably high, or a queue
depth jumped on a particular date, the explanation is usually here.

Naming is `<action>-<subject>-<YYYYMMDD>.sql`:

- `requeue-*` — put tasks lost to an infrastructure failure back on a queue.
- `recover-*` — repair rows that a bad worker image wrote incorrectly.
- `fix-*` — correct queue or schema damage caused by an earlier script here.

Two of these files are referenced by `tests/test_k3s_pipeline_manifests.py`,
which asserts the requeue transactions are transactional, idempotent, and
audited. Keep the filenames stable, or update those tests alongside.
