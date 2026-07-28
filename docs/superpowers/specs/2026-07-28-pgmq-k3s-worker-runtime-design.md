# PGMQ + K3s Worker Runtime Design

## Scope

Connect the existing PostgreSQL ledger and PGMQ queue foundation to a live,
four-stage Kubernetes relay:

```text
swegen_generate -> swegen_validate -> swegen_reward -> swegen_push
```

The initial rollout runs exactly one long-lived worker pod per stage. Each
worker claims one identifier-only PGMQ message, reconstructs authoritative task
state from PostgreSQL, performs its stage, records the result, sends the sole
valid successor, and archives the source message in one PostgreSQL transaction.

This design includes task-file storage, worker lifecycle, Kubernetes packaging,
credential handling, Docker/cache controls, and the live end-to-end acceptance
test. It does not remove the JSONL rollback backend, stop the existing Slurm
services, introduce an in-cluster registry, or autoscale beyond one replica per
stage.

The database remains `swegen_distributed`, with application tables in `public`
and PGMQ internals in `pgmq`.

## Selected approach

### Long-lived, stage-specific Deployments

One reusable worker image is deployed four times with a fixed `--stage`
argument. Each Deployment consumes only its own queue and has one replica for
the proof rollout. This directly matches the desired shared-queue worker model,
keeps Kubernetes RBAC unnecessary, and makes horizontal scaling a replica-count
change later.

Two alternatives were considered:

- A dispatcher that creates one Kubernetes Job per queue message would improve
  process and filesystem isolation, but it needs Kubernetes API credentials,
  another control loop, Job cleanup policy, and a second retry system. It is a
  better later option if untrusted task execution must be isolated per item.
- A monolithic worker that dynamically consumes every stage would reduce
  manifests but couple unrelated resource profiles and make stage-specific
  scaling and failure diagnosis harder.

### Normalized task-file rows

Task metadata and files are stored in three new public tables:

- `pipeline_tasks`: immutable identity (`task_id`, version, repository, PR,
  trace ID) plus current pipeline state.
- `pipeline_task_files`: one regular file per relative POSIX path, including
  bytes, mode, size, and SHA-256 digest.
- `pipeline_stage_results`: one terminal result per
  `(task_id, task_version, stage, attempt)`.

A normalized file table is selected over a ZIP blob or one column per known
file. It supports arbitrary Harbor fixtures, validates paths independently,
allows digest verification, and can materialize or export a task without
retaining an authoritative directory tree. PostgreSQL TOAST handles large
`bytea` values; configurable per-file and per-task limits prevent accidentally
ingesting caches, repositories, or Harbor job directories.

Task directories are disposable views. Workers write a fresh directory from
the file rows immediately before a stage and delete it afterward. A future
archive/export command will materialize the same rows and create a ZIP in real
time; no JSONL ledger or permanent archive directory is required.

## Database contract

`pipeline_tasks` uses `(task_id, task_version)` as its primary key and a unique
`(repo, pr, task_version)` constraint. Its state is one of `queued`, `running`,
`rejected`, `failed`, or `completed`; `current_stage` identifies the stage
responsible for the next transition. The row also stores timestamps and the
most recent terminal error or rejection reason.

`pipeline_task_files` has a foreign key to `pipeline_tasks` with cascade delete.
Paths must be non-empty, relative POSIX paths without `.` or `..` components.
Symlinks and non-regular files are rejected. Materialization verifies every
stored SHA-256 digest before writing and applies only permission bits.

`pipeline_stage_results` stores `succeeded`, `rejected`, or `failed`, the PGMQ
message ID, delivery count, worker and node identity, start/finish timestamps,
structured JSON result data, and a redacted error or rejection reason. Its
primary key is the stage-attempt idempotency key used by the PGMQ completion
methods.

Enqueueing a new task is atomic: insert the task row, send the generate message,
and commit together. An existing task version is not silently re-enqueued.

Successful stage completion is also atomic:

1. Insert the idempotent stage result.
2. For generation, replace the authoritative task-file rows in the same
   transaction.
3. Update `pipeline_tasks` to the successor stage or final completed state.
4. Send exactly one successor message when the result insert was new.
5. Archive the source PGMQ message.

A duplicate delivery finds the existing stage result, emits no successor, and
still archives its duplicate source message.

An expected policy rejection, such as invalid NOP/Oracle rewards or a valid
reward-hacking verdict, is not an infrastructure dead letter. A dedicated
`complete_terminal()` queue operation records the idempotent rejected result,
marks the task rejected, and archives the current message without sending a
successor. The dead-letter queue is reserved for exhausted execution errors.

## Worker lifecycle

`python -m swegen.pipeline.worker --stage <stage>` runs the common loop:

1. Claim at most one message with short polling and commit the claim
   transaction immediately.
2. Start a background visibility heartbeat using independent short database
   transactions.
3. Load task metadata and, when required, materialize task files into a new
   temporary workspace.
4. Run the stage implementation and collect a small structured result.
5. Stop the heartbeat and perform the atomic completion/handoff transaction.
6. Delete the workspace and continue polling.

SIGTERM stops new claims but permits the active stage to finish within the pod's
termination grace period. PGMQ visibility remains the crash-recovery mechanism.

Stage actions return either success or an expected rejection; unexpected
exceptions are redacted and retried in place using PGMQ delivery count. At the
configured maximum delivery count, the worker records a failed stage, marks the
task failed, moves the identifier message to `swegen_dead`, and archives the
source atomically. Failure details remain in PostgreSQL, never in the queue
payload.

## Stage implementations

### Generate

Load repository and PR metadata from `pipeline_tasks` and execute:

```text
swegen create --repo ... --pr ... --no-validate --force
```

The output, state, and repository-cache paths are worker-controlled. Difficulty
and linked-issue gates are disabled for the explicit queue item, matching the
existing Slurm orchestrator. `SWEGEN_LEDGER_BACKEND=postgres` prevents new
authoritative JSONL ledgers. On success, the generated Harbor task directory is
validated as a regular-file tree and stored in `pipeline_task_files` inside the
completion transaction.

### Validate

Materialize the task and call the existing Harbor runner sequentially:

- NOP must produce reward `0`.
- Oracle must produce reward `1`.

NOP retains its image only long enough for Oracle reuse; Oracle requests normal
Harbor image deletion. The existing orphan-container reaper remains active.
Both observed rewards and result paths are stored in the stage result.

### Reward-hack check

Materialize the task tests, build the existing reward-hacking bundle, and call
the existing OpenAI-compatible checker with `gpt-5.3-codex-spark` by default.
An infrastructure/parser error is retryable; a valid `is_hacking=true` verdict
uses the terminal-rejection transaction and does not enter the dead-letter or
push queues. A clean verdict records the model, framework, reason, and fallback
metadata before enqueueing the push stage.

### SWR push

Materialize the task, build its Dockerfile with the existing direct-build
helper, and push one deterministic task tag to the configured SWR repository.
The worker first checks whether the manifest already exists, making a repeated
push idempotent. A successful push records both `pipeline_stage_results` and the
existing `pushed_images` ledger table, then removes local source and remote tags.

## Kubernetes packaging and scheduling

The repository provides:

- a worker Dockerfile containing Python 3.12, the locked SWE-gen environment,
  Docker CLI, Git, Node 22, and the pinned Claude Code CLI;
- a namespace, ConfigMap, Secret templates, and four Deployments;
- an optional node-scoped Docker cache-cleaner DaemonSet gated by a node label;
- a manifest contract test that parses every YAML document.

No application PersistentVolume is used. PostgreSQL is authoritative;
`emptyDir` holds the active task workspace and has a size limit. A hostPath
under `/data/swegen-k3s/cache` is a disposable repository/model cache only.

For the first end-to-end run, pods avoid `7.244.2.110`, where a Slurm job and a
large active Docker cache are present. Generate and push may share the lightly
loaded `7.244.3.78` node because the stages are sequential; validate runs on
`7.244.3.200`; reward checking runs on `7.244.1.209` and does not use Docker.

The proof rollout imports the same locally built worker image into K3s
containerd on the selected nodes and uses `imagePullPolicy: IfNotPresent`.
Production rollout should publish this image to a private registry and use an
image-pull Secret.

## Credential handling

Secrets are created directly in Kubernetes without writing rendered Secret
YAML to disk or printing decoded values:

- database password;
- the existing private `swegen.toml` and model credential environment;
- reward-checker credentials;
- the controller's authenticated Docker `config.json` for both SWR registries.

Non-secret endpoints, model names, database host/name, queue timing, and
resource settings live in a ConfigMap. Secret volumes are read-only and
default to mode `0400`. Worker logs redact credential-shaped values and store
only bounded stderr/stdout tails in PostgreSQL.

## Container, BuildKit, and registry storage policy

K3s/containerd stores only worker and Kubernetes images. Kubelet already owns
that store with image GC thresholds 70/55 percent, a ten-minute minimum age,
and a six-hour maximum unused age. External `ctr` pruning is not used.

Harbor task builds use the host Docker daemon through `/var/run/docker.sock`,
so their images and BuildKit cache live in the separate `/data/docker` store.
For the proof rollout, every stage removes task-specific containers/images and
the pusher removes its final local image after upload. The optional cleaner runs
only on explicitly labeled nodes and invokes Buildx GC with an age filter,
reserved-space floor, maximum-used-space target, and minimum-free-space target;
it prunes dangling or old unused images but never volumes. This avoids racing
kubelet and avoids touching active Docker objects. The heavily loaded
`7.244.2.110` node is not labeled for cleanup during the proof.

No in-cluster image registry is introduced. Final task images go to the
existing external SWR registry under deterministic tags, so retries do not
create tag proliferation. Registry quota and retention remain SWR-side
operations; they must preserve accepted task tags and can remove only superseded
versions or explicitly retired tasks. The `pushed_images` table is the inventory
used to reconcile registry contents and retention decisions.

## Testing and acceptance

Implementation follows test-first development for task storage, path safety,
transactional callbacks, heartbeat/retry behavior, stage command construction,
reward verdict handling, push idempotency, and manifest structure.

The live acceptance task must be a repository/PR pair absent from
`pipeline_tasks`. Completion requires all of the following evidence:

- exactly one Ready pod for generate, validate, reward, and push;
- one task row reaches `completed` at stage `push`;
- task files exist in PostgreSQL and rematerialize with matching digests;
- four successful stage-result rows exist in order;
- all four processing queues are empty and the task is absent from
  `swegen_dead`;
- NOP reward is `0`, Oracle reward is `1`, and reward-hack verdict is clean;
- `pushed_images` contains a `pushed=true` row and the SWR manifest resolves;
- no task Harbor containers remain on the participating Docker hosts;
- worker pod restarts are zero or fully explained, and cache/disk metrics remain
  within the configured cleanup policy.
