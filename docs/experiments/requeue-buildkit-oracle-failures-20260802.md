# Validate failure recovery audit — 2026-08-02

Recovery ID: `20260802_buildkit_oracle_requeue_v1`

The recovery transaction ran from `2026-08-02 11:25:30.989343 +08:00`
through `11:25:33.553299 +08:00`. It selected the latest authoritative
production Validate result per `(task_id, task_version)`, guarded against live
activity, existing Validate/Validate Repaired/Repair payloads, later successful
stages, advanced task state, and experiment/canary workers.

## Enqueued tasks

| Classification | Destination | Count |
| --- | --- | ---: |
| BuildKit, Docker/Compose, timeout, or network execution failure | Validate | 4,407 |
| NOP=0 / Oracle=0 (`unexpected_oracle_reward`) | Repair | 3,509 |
| Patch/application artifact failure | Repair | 525 |
| **Total** |  | **8,441** |

The original three-argument PGMQ send compatibility function redirected the
4,407 Validate retries to Validate Repaired because their attempt was greater
than one. A corrective serializable transaction moved 4,387 still-visible
messages to the normal `swegen_validate` FIFO through the underlying
four-argument send function. Twenty already-claimed messages were deliberately
left in `swegen_validate_repaired`; their actual queue identity is recorded in
the audit table.

Production Repair remained at zero replicas. Its 4,034 newly enqueued tasks
will wait until Repair is enabled.

## Exclusions

- 768 latest `unexpected_nop_reward` results were left untouched.
- 68 latest experiment/canary results were excluded; 64 were still terminal in
  Validate, three had completed Push, and one had advanced to Reward rejection.
- 53 failed/current-Validate results were ambiguous and had no explicit
  BuildKit, Docker/Compose, timeout, network, Oracle, or patch evidence.
- Existing queue payloads, fresh stage activity, advanced tasks, and tasks with
  later successful Validate/Repair/Reward/Push results were excluded by SQL.

## Queue snapshots

Immediately before the recovery:

| Queue | Total | Visible |
| --- | ---: | ---: |
| `swegen_validate` | 1,387 | 1,244 |
| `swegen_validate_repaired` | 0 | 0 |
| `swegen_repair` | 4,225 | 4,225 |

After normalization at `11:28:28 +08:00`:

| Queue | Total | Visible |
| --- | ---: | ---: |
| `swegen_validate` | 5,732 | 5,606 |
| `swegen_validate_repaired` | 17 | 0 |
| `swegen_repair` | 8,259 | 8,259 |

Queue counts are live and can fall as Validate workers consume tasks.

## Post-commit checks

- Audit rows: 8,441; distinct tasks: 8,441; distinct event IDs: 8,441.
- Every audit row matched exactly one authoritative PGMQ message by actual
  queue, message ID, and event ID across live and archived queues.
- Missing authoritative messages: 0.
- Multiple authoritative messages: 0.
- Audited tasks with more than one live payload across Validate, Validate
  Repaired, and Repair: 0.
- Live payload/task-stage mismatches: 0.
- At the postflight snapshot, 4,034 Repair tasks were queued; 4,404 Validate
  tasks were queued and three had already completed another failed Validate
  attempt. Seventeen of the original twenty claimed Validate Repaired messages
  remained in flight.

## Durable exact task list

Exact instance IDs, source and target attempts, classifications, destinations,
event IDs, and PGMQ message IDs are stored centrally in
`pipeline_manual_requeue_audit`. Retrieve the complete list with:

```sql
SELECT task_id, task_version, classification, target_stage, target_queue,
       source_attempt, target_attempt, event_id, pgmq_msg_id, enqueued_at
FROM pipeline_manual_requeue_audit
WHERE recovery_id = '20260802_buildkit_oracle_requeue_v1'
ORDER BY target_queue, classification, task_id, task_version;
```

Operational SQL:

- `docs/experiments/requeue-buildkit-oracle-failures-20260802.sql`
- `docs/experiments/normalize-buildkit-retries-to-fresh-validate-20260802.sql`

