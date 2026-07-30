# SWE-gen K3s Docker/BuildKit debug handoff

## Purpose and safety boundary

This document hands the current Docker/BuildKit contention investigation to
another debug agent. It is intentionally operational: it records the live
cluster shape, the relevant repository and host paths, observed contention,
safe diagnostics, and a staged remediation/test plan.

The measurements below are point-in-time snapshots. The pipeline is active, so
container, process, queue, and cache counts can change between commands.

**Snapshot window:** 2026-07-30 13:11-13:14 CST (+08:00)

No cluster configuration, Docker daemon, K3s service, cache, container,
deployment, queue, or git state was changed while collecting this snapshot.

Do not restart Docker or K3s, prune anything, kill build processes, or scale
production workloads merely to reproduce these measurements. Coordinate any
mutation with the pipeline owner and preserve PGMQ delivery leases.

## Executive summary

The four nodes have 192 logical CPUs and approximately 369 GiB RAM each, but
the current bottleneck is not CPU availability. The important findings are:

1. Live Repair has grown to **128 Pods**, distributed as 31-33 Pods per node.
   Validate has 96 Pods, exactly 24 per node. Therefore each node has 55-57
   Validate+Repair processes that can eventually enter Harbor/Docker work.
   Node 7.244.3.200 also has 16 Push Pods, which can build images.
2. Host Docker is a separate runtime from K3s containerd. Worker Pods mount
   /var/run/docker.sock, so Harbor build and test containers are created by the
   host Docker daemon outside Kubernetes Pod resource accounting.
3. At the snapshot, nodes had approximately 24-37 concurrent
   docker compose build clients and 19-27 BuildKit executor processes. Several
   executor processes had been alive for more than 30 minutes.
4. Dockerd logs show a high rate of BuildKit session failures, including
   "only one connection allowed", fatal session healthchecks, canceled solves,
   and containers that do not exit within the daemon grace period.
5. Repair's Claude/Harbor path does not inherit the explicit
   COMPOSE_BAKE=false / DOCKER_BUILDKIT=1 environment used by the normal
   Validate Harbor wrapper. Live Repair work is visibly spawning
   docker-buildx bake clients.
6. Node 7.244.3.78 is under severe unrelated/local disk contention. Its I/O PSI
   full average was approximately 67%, with 30% iowait in one vmstat sample.
   Docker inventory calls themselves became slow or timed out.
7. Embedded Docker BuildKit cache is already above the configured 200 GB target
   on 7.244.2.110. Docker images and volumes are separate stores and include
   unrelated host workloads, so blanket prune commands are unsafe.
8. The first remediation should be a **node-wide admission limit shared by
   Validate, Repair, and Push**, not more BuildKit daemons. Start at 8 build
   slots on .78 and 16 on each other node.

## Cluster nodes

| Kubernetes node | IP | K3s role | CPU | Memory | Kubelet | K3s containerd |
| --- | --- | --- | ---: | ---: | --- | --- |
| ecs-z00579134-20260707-bugfix-0002 | 7.244.3.78 | server/control-plane | 192 | ~369 GiB | v1.36.2+k3s1 | 2.3.2-k3s2 |
| ecs-z00579134-20260707-bugfix-0003 | 7.244.3.200 | server/control-plane, cluster-init | 192 | ~369 GiB | v1.36.2+k3s1 | 2.3.2-k3s2 |
| ecs-z00579134-20260707-bugfix-0005 | 7.244.2.110 | server/control-plane | 192 | ~369 GiB | v1.36.2+k3s1 | 2.3.2-k3s2 |
| ecs-z00579134-20260707-bugfix-0006 | 7.244.1.209 | agent | 192 | ~369 GiB | v1.36.2+k3s1 | 2.3.2-k3s2 |

Kubernetes reports all 192 CPUs as allocatable on every node. This does not
account for host processes or Docker-launched Harbor containers.

## Live Pod and deployment state

### Build-relevant placement

| Stage/workload | .78 | .200 | .110 | .209 | Total |
| --- | ---: | ---: | ---: | ---: | ---: |
| Validate | 24 | 24 | 24 | 24 | 96 |
| Repair | 33 | 31 | 33 | 31 | 128 |
| Push | 0 | 16 | 0 | 0 | 16 |
| Generate | 0 | 0 | 4 | 0 | 4 |
| Reward | 0 | 0 | 0 | 16 | 16 |
| Recovery audit | 1 | 1 | 0 | 1 | 3 |

All 128 Repair and 96 Validate Pods were Ready at the snapshot.

Generate runs the generation command with --no-validate and does not mount the
Docker socket in the checked-in manifest, so it is not part of the main
host-Docker concurrency budget. Reward is LLM/test analysis rather than a
Docker build stage. Push can rebuild images and must share the Docker admission
budget with Validate and Repair.

The three unlabelled live audit Pods were:

- swegen-recovery-audit-78
- swegen-recovery-audit-200
- swegen-recovery-audit-209

### Live Deployment values

| Deployment | Desired/Ready | Live image | CPU request | Memory request |
| --- | ---: | --- | ---: | ---: |
| swegen-generate | 4/4 | swegen-worker:e2e | 500m | 1536Mi |
| swegen-validate | 96/96 | swegen-worker:validate-compose-reap-20260730 | 2 | 4Gi |
| swegen-repair | 128/128 | swegen-worker:repair-handoff-fix-20260730 | 2 | 4Gi |
| swegen-reward | 16/16 | swegen-worker:e2e | 1 | 2Gi |
| swegen-push | 16/16 | swegen-worker:e2e-ca-20260729 | 2 | 4Gi |

There is live-versus-manifest drift. For example, the checked-in pipeline
manifest still contains bootstrap replica counts that differ from some live
Deployment counts. Inspect the live objects before applying the whole manifest;
do not use an unqualified kubectl apply as a BuildKit debugging step.

## Repository and code paths

### Worktree

- Active K3s branch worktree:
  /data/work/alex/SWE-gen/.worktrees/slurm-swegen
- Repository root referenced by the user:
  /data/work/alex/SWE-gen

### Deployment files

- deploy/k3s/swegen-pipeline.yaml
- deploy/k3s/Dockerfile.worker
- deploy/k3s/build-import-worker.sh
- deploy/k3s/create-secrets.sh
- deploy/k3s/README.md
- deploy/k3s/migrate-repair-stage.sql

### Build and cleanup code

- src/swegen/tools/harbor_runner.py
  - run_harbor_agent()
  - process-group cancellation
  - duplicate Compose-build client detection
  - task-container reaping
- src/swegen/pipeline/actions.py
  - validate_action()
  - repair_action()
  - push_action()
- src/swegen/pipeline/worker.py
  - PGMQ claim lifecycle
  - per-delivery temporary workspace lifecycle
  - Repair seeding and retry limits
- src/swegen/pipeline/task_store.py
  - PostgreSQL result/activity handoff
- src/swegen/queueing/pgmq.py
  - PGMQ queue operations and metrics
- src/swegen/schema.sql
  - pipeline_stage_results and pipeline_stage_activity

### Relevant tests

- tests/test_harbor_runner.py
- tests/test_pipeline_actions.py
- tests/test_pipeline_worker.py
- tests/test_pipeline_task_store.py
- tests/test_k3s_pipeline_manifests.py

## Host paths and persistence

| Purpose | Host path | Notes |
| --- | --- | --- |
| Per-delivery worker workspaces | /data/swegen-k3s/workspaces | Node-local hostPath. Individual delivery directories are TemporaryDirectory workspaces and are normally removed after durable completion. |
| Shared generation repo cache | /data/swegen-k3s/cache | Node-local hostPath. Configured repo cache is /data/swegen-k3s/cache/repos. |
| Host Docker root | /data/docker | Host Docker images, containers, volumes, embedded BuildKit state, and Docker's containerd image-store data. |
| K3s root | /data/k3s | K3s server/agent state and K3s containerd. |
| K3s containerd store | /data/k3s/agent/containerd | K3s Pod image/content/snapshot storage. |
| Kubelet root | /data/kubelet | Small in the current deployment; K3s is explicitly configured around /data paths. |

PostgreSQL is the authoritative source for generated task files and pipeline
state. Workspace directories are operational scratch space, not the durable
task archive. A missing old workspace is not by itself data loss.

## Runtime and tool versions

### Host

| Node | Docker Engine | Host Compose | Host Buildx | Embedded BuildKit |
| --- | --- | --- | --- | --- |
| .78 | 29.6.1 | 5.3.1 | 0.35.0 | 0.31.1 |
| .200 | 29.6.1 | 5.3.1 | 0.35.0 | 0.31.1 |
| .110 | 29.6.2 | 5.3.1 | 0.35.0 | 0.31.2 |
| .209 | 29.6.2 | 5.3.1 | 0.35.0 | 0.31.2 |

K3s is v1.36.2+k3s1 and its bundled containerd is v2.3.2-k3s2 on all nodes.

### Worker image

A live Validate and Repair Pod both reported:

- Docker Compose v2.40.3
- Docker Buildx v0.36.0
- selected builder: default, driver docker

The worker image's pinned Compose binary is intentional. Host Compose 5.3.1 is
not the CLI used inside normal worker Pods, although host processes show the
containerized CLI command lines.

## Docker and BuildKit per-node snapshot

All nodes use:

- Docker root: /data/docker
- filesystem: ext4 on /dev/vdb1 mounted at /data
- Docker storage driver: overlayfs with the Docker containerd snapshotter
- embedded builder GC enabled with defaultKeepStorage=200GB

### Disk, images, volumes, and embedded BuildKit

| Node | /data usage | Docker images | Docker volumes | Embedded BuildKit cache | Notes |
| --- | --- | --- | --- | --- | --- |
| .78 | 20T/32T, 64% | 26 images at initial snapshot | 47 volumes seen earlier; shared host | Current docker system df and builder du became unresponsive at 13:12. Last successful measurement at approximately 12:36 was 70.45GB total, ~55GB reclaimable by builder-du. | Docker API slowness is itself a contention signal. Do not assume the last successful cache number is current. |
| .200 | 418G/32T, 2% | 153.6GB, 132.7GB reported reclaimable | 0 | 72.62GB total; 19.96-33.44GB reported reclaimable depending on command view | 118 images were reported by docker info moments before system-df reported 116; live churn explains the difference. |
| .110 | 740G/32T, 3% | 125.4GB, 103GB reclaimable | 841MB, all reported reclaimable | 231-232GB total; 191-204GB reclaimable | Already above the configured 200GB defaultKeepStorage target. GC is periodic, not a synchronous hard cap. |
| .209 | 606G/32T, 2% | 26GB, 18.23GB reclaimable | 61.17GB, all reported reclaimable | 93.6-93.8GB total; 9.5-32.3GB reclaimable depending on view | Volumes include unrelated workloads and old builders; global volume prune is unsafe. |

The differing reclaimable totals from docker system df and docker builder du
are normal enough to treat as approximate: records can be active, shared, or
change while the commands run.

### Active/stale build pressure

| Node | Compose build clients | Compose clients >30m | Buildx bake clients | Bake clients attributable to Repair | BuildKit executors | Executors >30m |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| .78 | 37 | 2 | 13 | 13 | 19 | 1 |
| .200 | 28 | 0 | 3 | 3 | 25 | 10 |
| .110 | 24 | 0 | 3 | 3 | 26 | 11 |
| .209 | 24 | 0 | 3 | 3 | 27 | 7 |

These are process snapshots, not proofs that every old executor is permanently
orphaned. Some package installation or repository builds are legitimately
slow. However, the combination of old executors, fatal BuildKit session
healthchecks, canceled solves, and poor throughput is strong evidence that the
current admission level is too high.

### Docker container counts

| Node | Total | Running | Stopped | Compose/runtime notes |
| --- | ---: | ---: | ---: | --- |
| .78 | 32 | 2 | 30 | One running buildx_buildkit_harbor-builder0 container; Docker inventory was partially unresponsive. |
| .200 | 7 | 6 | 1 | 4 running Compose-labelled containers in the snapshot. |
| .110 | 8 | 7 | 1 | 5 running and 6 total Compose-labelled containers. |
| .209 | 5-6 | 5-6 | 0 | Count changed during the snapshot due to live task churn; 6 Compose-labelled containers were later observed. |

The number of Docker containers is much smaller than the number of Kubernetes
worker Pods because workers are Docker clients. BuildKit executor runc
processes are build-step execution processes, not durable docker ps containers.

### Builder topology

Worker Pods on every node select the Docker default builder using the embedded
docker driver.

On .78, the host root user's buildx state additionally showed:

- harbor-builder: docker-container driver, error state
- mybuilder: docker-container driver, inactive
- buildx_buildkit_harbor-builder0: running for approximately two hours

This extra .78 builder is not the builder selected inside sampled worker Pods.
It should be audited during a controlled maintenance window, but it must not be
deleted while its ownership or active users are unknown.

## K3s/containerd storage

K3s containerd is separate from host Docker.

| Node | CRI images | Sum of CRI image sizes | CRI containers total/running | /data/k3s/agent/containerd |
| --- | ---: | ---: | ---: | --- |
| .78 | 8 | ~3.67GB | 58/58 | Current du did not complete during the overloaded snapshot; record as unavailable rather than waiting. |
| .200 | 11 | ~4.32GB | 76/75 | ~16GB |
| .110 | 7 | ~3.67GB | 62/61 | ~14GB |
| .209 | 7 | ~3.67GB | 72/72 | ~14GB |

CRI container counts include Pod sandboxes and historical container records;
they are not equivalent to worker Pod count.

### Kubelet image GC

Every node has:

`yaml
imageGCHighThresholdPercent: 70
imageGCLowThresholdPercent: 55
imageMinimumGCAge: 10m
imageMaximumGCAge: 6h
evictionHard:
  imagefs.available: "15%"
  nodefs.available: "10%"
  nodefs.inodesFree: "5%"
evictionMinimumReclaim:
  imagefs.available: "5%"
  nodefs.available: "5%"
`

These settings govern K3s/containerd images used by Kubernetes Pods. They do
not clean host Docker images, Docker volumes, Harbor containers, or Docker
BuildKit cache.

## Docker daemon builder GC

The redacted relevant portion of /etc/docker/daemon.json is:

`json
{
  "data-root": "/data/docker",
  "builder": {
    "gc": {
      "enabled": true,
      "defaultKeepStorage": "200GB"
    }
  }
}
`

Proxy credentials and endpoints are deliberately omitted from this handoff.
Do not paste the full daemon.json into tickets or logs.

defaultKeepStorage influences the embedded Docker builder's periodic GC
policies. It is not:

- a hard synchronous cache limit;
- a Docker image-retention policy;
- a container/volume cleanup setting;
- a standalone buildkitd configuration;
- a K3s/containerd image-GC setting.

## Recent dockerd contention evidence

### Fifteen-minute counts

| Node | "only one connection allowed" | Fatal healthchecks | Solve errors | Kill/exit timeout messages |
| --- | ---: | ---: | ---: | ---: |
| .78 | 29 | 15 | 3 | 9 |
| .200 | 241 | 121 | 36 | 18 |
| .110 | 185 | 91 | 18 | 21 |
| .209 | 187 | 92 | 24 | 15 |

### Sixty-minute counts

| Node | "only one connection allowed" | Fatal healthchecks | Solve errors | Kill/exit timeout messages |
| --- | ---: | ---: | ---: | ---: |
| .78 | 310 | 155 | 60 | 42 |
| .200 | 768 | 384 | 106 | 72 |
| .110 | 639 | 318 | 75 | 70 |
| .209 | 586 | 291 | 89 | 45 |

Representative messages include:

`text
healthcheck failed fatally
session healthcheck failed fatally: ... only one connection allowed
/moby.buildkit.v1.Control/Solve ... context canceled
Container failed to exit within 10s of signal 15
container ... is taking a long time to exit after kill
failed to kill process ... runc did not terminate successfully
`

These counts include failures caused by client cancellation and genuinely bad
task builds as well as daemon contention. They should be tracked as a rate per
completed task during testing rather than interpreted as one failure per task.

## Node .78 special condition

At 13:14 CST, .78 reported:

- load average approximately 81/85/105;
- CPU PSI near zero;
- I/O PSI some avg60 approximately 69%;
- I/O PSI full avg60 approximately 67%;
- one vmstat sample with approximately 30% iowait;
- only approximately 61% CPU busy/idle accounting despite the high load;
- docker system df and subsequent detailed inventory calls becoming slow or
  incomplete.

Earlier read-only process inspection showed unrelated host jobs performing
large sustained reads/writes under /data. Kubernetes does not account for
these host processes when scheduling Pods.

Do not assign .78 the same initial build admission limit as the clean nodes.
Do not conclude that its 192 CPUs imply 192 useful concurrent Docker builds.

## Immediate Repair COMPOSE_BAKE issue

The live Validate, Repair, and Push Deployment templates do not define:

- DOCKER_BUILDKIT
- BUILDX_BUILDER
- COMPOSE_BAKE

The sampled worker Pod environments likewise had none of these variables at
the top level.

src/swegen/tools/harbor_runner.py currently constructs an explicit child
environment for normal Validate Harbor calls:

`python
child_env = {
    **os.environ,
    "COMPOSE_BAKE": "false",
    "DOCKER_BUILDKIT": "1",
}
`

That protects validate_action(), which calls run_harbor_agent().

repair_action() instead calls run_claude_code_session(..., validate=True,
repair=True). Claude Code then runs Harbor/Docker commands inside its own
session. That path does not pass through run_harbor_agent()'s child_env.

Evidence:

- live Buildx Bake commands contained
  /data/swegen-k3s/workspaces/repair-... paths;
- all sampled Bake clients in the per-node process count were attributable to
  Repair;
- sampled normal Validate Compose clients on .200, .110, and .209 had
  COMPOSE_BAKE=false and DOCKER_BUILDKIT=1 in /proc/PID/environ;
- the live Repair Deployment and sampled Repair Pod did not define them.

Recommended immediate code/config correction:

`text
DOCKER_BUILDKIT=1
BUILDX_BUILDER=default
COMPOSE_BAKE=false
`

Set these on every Docker-capable worker Deployment and explicitly propagate
them into Claude Code and Harbor subprocess environments. A top-level Pod
variable is useful, but subprocess propagation should remain explicit because
Claude/login-shell/plugin execution can otherwise alter the environment.

This correction does not replace concurrency admission. Compose's internal
builder still submits work to the same embedded dockerd BuildKit.

## Repair runtime state relevant to build pressure

Repair is live at 128/128 Ready Pods using:

`text
swegen-worker:repair-handoff-fix-20260730
`

This image contains the PostgreSQL handoff fix that aliases:

`sql
SELECT COALESCE(MAX(attempt), 0) + 1 AS next_attempt
`

The previous missing alias caused completed Repair sessions to lose their
handoff at PostgreSQL result recording, so an earlier zero-result signal was
not a pure model or BuildKit signal.

For BuildKit debugging, the important current facts are:

- 128 long-running Repair sessions can invoke Docker through Claude;
- 31-33 are placed on each node;
- Repair timeout is configured at 14,400 seconds;
- max repair attempts is 3;
- terminationGracePeriodSeconds for Repair is 18,000 seconds;
- Repair-spawned Bake clients are present;
- some build executors are older than 30 minutes.

Do not scale Repair further until node-wide Docker admission exists and
corrected workers show terminal Repair results followed by Validate handoff.

## Storage-system distinctions

### 1. Docker embedded BuildKit

- Selected as builder default with driver docker.
- Runs inside dockerd.
- Cache is reported by docker builder du / docker system df.
- Controlled by daemon.json builder.gc.
- Cannot be given worker.oci.max-parallelism through daemon.json.
- Built images are automatically available to the same Docker engine.

### 2. Standalone or docker-container BuildKit

- Has a separate buildkitd process and state root/volume.
- Controlled by /etc/buildkit/buildkitd.toml or a buildx builder config.
- Supports worker.oci.max-parallelism and independent GC thresholds.
- Results are not automatically in Docker's image store unless load/default-load
  is configured.
- A builder per Pod or per build would multiply daemons, caches, threads, and
  metadata contention. Do not use that topology.

### 3. Docker images, containers, networks, and volumes

- Stored under the host Docker root but distinct from BuildKit cache records.
- docker builder/buildx prune does not perform complete image/container/volume
  cleanup.
- Harbor runtime containers and task volumes can survive failed Compose
  teardown.
- These hosts contain unrelated Docker workloads and old builder volumes.
  Global volume/image pruning may destroy non-SWE-gen data.

### 4. K3s containerd images and containers

- Stored under /data/k3s/agent/containerd.
- Managed by kubelet image/container GC.
- Used for Kubernetes worker Pod images and Pod sandboxes.
- Unaffected by Docker builder prune.
- Manual crictl/ctr deletion can break running Pods or fight kubelet state.

### 5. External SWR/Harbor registry

- Remote registry retention and registry GC are separate again.
- Local Docker pruning does not reduce remote registry usage.
- Registry retention does not clean any node-local Docker or containerd store.

## Recommended node-wide admission configuration

The admission limit must be shared across all Pods and all Docker-building
stages on one node.

| Node | Initial total build slots | Reason |
| --- | ---: | --- |
| 7.244.3.78 | 8 | Severe I/O PSI and unrelated host disk load. |
| 7.244.3.200 | 16 | Clean disk pressure, but many current BuildKit session errors and Push co-location. |
| 7.244.2.110 | 16 | Clean disk pressure; cache is already above 200GB. |
| 7.244.1.209 | 16 | Clean disk pressure; large unrelated Docker volume footprint. |

Count these as total node slots, not per Deployment. For example, .200's 24
Validate Pods, 31 Repair Pods, and 16 Push Pods must compete for the same 16
build slots.

### Preferred implementation

Use a Docker CLI wrapper plus a node-local hostPath containing slot lock files:

1. Mount the same node-local directory into every Validate, Repair, and Push
   Pod, for example /run/swegen-build-slots.
2. Place the wrapper earlier in PATH than /usr/bin/docker.
3. Intercept only build-producing forms:
   - docker build
   - docker buildx build
   - docker buildx bake
   - docker compose ... build
4. Acquire one of N flock locks before invoking the real Docker CLI.
5. Keep the lock file descriptor open for the child process lifetime.
6. Continue the PGMQ visibility heartbeat while waiting.
7. Export wait duration and selected slot as metrics.

flock releases automatically if the process or Pod dies. A container-local
Python semaphore is insufficient because it is not shared between Pods.

Do not hold a build slot for an entire multi-hour Repair LLM session. Acquire
around each Docker build command so model thinking does not consume scarce
Docker capacity.

## Cache and cleanup proposal

### Embedded BuildKit GC

After admission control is stable, reduce defaultKeepStorage from 200GB to
100GB on a one-node-at-a-time maintenance rollout:

`json
{
  "builder": {
    "gc": {
      "enabled": true,
      "defaultKeepStorage": "100GB"
    }
  }
}
`

Before changing daemon.json:

- back it up;
- validate the complete file with dockerd --validate;
- drain only the affected build workload;
- wait for or deliberately release active PGMQ deliveries;
- restart only one Docker daemon at a time;
- verify the node before proceeding.

### Quiet-period explicit prune

Use an explicit builder name because .78's host root buildx selection points at
the unhealthy harbor-builder:

`bash
docker buildx prune \
  --builder default \
  --force \
  --filter 'until=24h' \
  --reserved-space 20gb \
  --max-used-space 100gb \
  --min-free-space 2tb
`

Run this manually on a canary node first. If automated, use a systemd timer and
flock, and skip the run whenever any SWE-gen build slot is held. Active
BuildKit records are protected, but running GC concurrently with dozens of
solves adds metadata and I/O contention.

### Project-scoped runtime cleanup

Continue the existing process-group cancellation and task-container reaper.
Extend cleanup carefully to tracked task resources:

- known Compose project containers;
- project networks;
- task-created volumes only after the project is terminal;
- known task image IDs/tags after durable handoff;
- stopped task containers after an age threshold.

Do not infer ownership merely from age. Prefer Compose project labels and the
task IDs captured by the worker.

### Longer-term isolation

The host Docker daemons contain unrelated workloads. For safe automated
retention and predictable performance, move SWE-gen to either:

- dedicated Docker/K3s build nodes; or
- a dedicated per-node SWE-gen Docker daemon with its own socket, data root,
  exec root, bridge/address pool, and containerd namespaces, tested first on a
  canary.

A separate subdirectory on the same busy disk improves cleanup ownership but
does not eliminate physical I/O contention. .78 needs storage/workload
isolation before it can be treated like the other nodes.

## Optional standalone BuildKit experiment

Do this only after the embedded builder plus node admission is stable.

Use one shared standalone builder per node, not one per worker:

`toml
root = "/data/swegen-buildkit"

[worker.oci]
  gc = true
  snapshotter = "overlayfs"
  max-parallelism = 16
  reservedSpace = "20GB"
  maxUsedSpace = "100GB"
  minFreeSpace = "2TB"
`

For the docker-container driver, configure default-load=true and prove that
Compose can immediately run the resulting image in the same host Docker
engine. Image loading still passes through dockerd/containerd, so standalone
BuildKit may not improve throughput. Its advantage is configurable
max-parallelism and isolated builder metadata; its disadvantage is another
cache/store and a load boundary.

Do not create a multi-node buildx builder for the Harbor path without a design
for image locality. If Buildx chooses another node, the Kubernetes-scheduled
worker cannot compose-up that image locally unless it is pushed/pulled through
a registry.

## Staged test plan

### Phase 0: establish a reproducible canary

Create a separate canary queue and use 50-100 stored tasks spanning:

- Node/npm/pnpm/yarn;
- Python/pip/uv;
- Go;
- Java/Gradle/Maven;
- Rust/Cargo;
- both small and large Docker contexts.

Do not consume the production Repair/Validate queue for destructive A/B cache
tests.

Capture at least 30 minutes of baseline metrics before changing anything.

### Phase 1: environment propagation

On canary workers:

1. Set DOCKER_BUILDKIT=1.
2. Set BUILDX_BUILDER=default.
3. Set COMPOSE_BAKE=false.
4. Verify these reach both normal Validate Harbor calls and Harbor calls made
   from the Repair Claude session.
5. Verify no new Repair-attributed docker-buildx bake clients appear.

Rollback: restore the previous canary image/Deployment environment. No cache
deletion is required.

### Phase 2: admission sweep

Use .110 as the first canary because it had low CPU/memory pressure and
manageable I/O PSI despite its large cache.

Test total node build slots at:

1. 8
2. 12
3. 16
4. 20
5. 24

Run each point for at least 30 minutes and preferably 100 completed NOP/Oracle
pairs. Test a warm-cache and isolated cold-cache run separately.

Do not test a higher point if the previous point breaches an error/pressure
gate.

### Metrics

Measure:

- completed Validate and Repair handoffs per minute;
- PGMQ visible and in-flight queue depth;
- p50/p95 build time;
- p50/p95 NOP/Oracle validation time;
- real task failure versus infrastructure failure;
- "only one connection allowed" per 100 builds;
- fatal BuildKit healthchecks per 100 builds;
- canceled solves per 100 builds;
- active Compose clients and BuildKit executors;
- executor age distribution;
- Docker API latency for docker info and docker ps;
- orphan containers/networks/volumes five minutes after completion;
- CPU, memory, and I/O PSI;
- iowait, disk await, queue depth, and utilization;
- dockerd threads, file descriptors, RSS;
- embedded BuildKit cache growth;
- Docker image and volume growth;
- K3s/containerd image growth separately.

### Acceptance gates

Use a concurrency point only if:

- infrastructure build failure is below 1%;
- no monotonically growing orphan-resource count is observed;
- Docker API p95 latency is below 2 seconds;
- I/O PSI full avg60 remains below 10%;
- memory PSI remains near zero;
- disk await is below approximately 30ms;
- fatal session healthchecks are near zero;
- throughput improves by at least 10% over the previous point.

If 20 slots provides less than 5-10% improvement over 16, keep 16.

### Rollout order

1. .110
2. .209
3. .200
4. .78 only after external I/O contention is resolved

Roll one node at a time. Keep the other nodes on the last known-good
configuration. Observe at least one full validation/repair cycle before moving
to the next node.

### Rollback

Rollback must be possible without deleting cache:

1. Stop routing canary work to the changed node.
2. Restore the previous worker image/environment or bypass the Docker wrapper.
3. Release/wait for active PGMQ deliveries.
4. If daemon.json was changed, restore its backup and restart Docker only after
   build work is drained.
5. Confirm docker info, docker ps, the default builder, and a one-task smoke
   test.
6. Re-enable the prior replica placement.

If testing a standalone builder, set BUILDX_BUILDER=default to roll back.
Remove the standalone builder only later, after confirming it has no active
users and its cache is not needed.

## Safe diagnostic commands

Run from the K3s control host/worktree unless an SSH target is shown.

### Kubernetes placement and versions

`bash
kubectl get nodes -o wide
kubectl get nodes -o custom-columns=NAME:.metadata.name,CPU:.status.allocatable.cpu,MEM:.status.allocatable.memory,KUBELET:.status.nodeInfo.kubeletVersion,RUNTIME:.status.nodeInfo.containerRuntimeVersion
kubectl -n swegen-pipeline get deploy,pods -o wide
kubectl -n swegen-pipeline get pods -l swegen.pgcode/stage=validate -o wide
kubectl -n swegen-pipeline get pods -l swegen.pgcode/stage=repair -o wide
`

### Deployment image and environment

`bash
kubectl -n swegen-pipeline get deploy swegen-validate swegen-repair swegen-push -o yaml
kubectl -n swegen-pipeline get deploy swegen-repair -o json | jq '.spec.template.spec.containers[0] | {image,env,envFrom,resources}'
`

Never print Secret values or run env without filtering inside a worker Pod.

### Bounded Docker inventory

Use timeout because .78 has demonstrated slow Docker API responses:

`bash
timeout 15 docker info
timeout 15 docker ps -a
timeout 30 docker system df
timeout 30 docker builder du
timeout 15 docker buildx ls
`

If a command times out, record the node and timestamp. Do not immediately
restart Docker or kill clients.

### Build client/executor pressure

`bash
ps -eo pid,ppid,etimes,stat,wchan:24,args --sort=-etimes \
  | grep -E '[d]ocker compose .* build|[d]ocker-buildx bake|[d]ata/docker/buildkit/executor'
`

To inspect only safe build-selection environment values:

`bash
tr '\0' '\n' < /proc/PID/environ \
  | grep -E '^(DOCKER_BUILDKIT|BUILDX_BUILDER|COMPOSE_BAKE|DOCKER_HOST)='
`

Do not dump complete /proc/PID/environ; it may contain credentials.

### Dockerd errors

`bash
journalctl -u docker --since '-15 minutes' --no-pager \
  | grep -E 'only one connection allowed|healthcheck failed fatally|Control/Solve error|failed to exit within|taking a long time to exit|failed to kill process'
`

### Host pressure

`bash
uptime
cat /proc/pressure/cpu
cat /proc/pressure/io
cat /proc/pressure/memory
vmstat 1 5
iostat -dx 1 5
pidstat -d 1 5
`

### Docker storage

`bash
findmnt -T /data/docker
df -hT /data/docker
timeout 30 docker system df
timeout 30 docker builder du
docker volume ls
docker ps -a --filter label=com.docker.compose.project
`

docker volume ls is safe; docker volume prune is not.

### K3s/containerd storage

`bash
findmnt -T /data/k3s
df -hT /data/k3s
k3s crictl images
k3s crictl ps -a
timeout 30 du -sh /data/k3s/agent/containerd
cat /data/k3s/agent/etc/kubelet.conf.d/10-swegen-storage.conf
`

### Safe PostgreSQL/PGMQ checks

Use a secure environment or an existing worker connection. Never put the
database password on a command line or into this document.

`sql
SELECT * FROM pgmq.metrics('swegen_validate');
SELECT * FROM pgmq.metrics('swegen_repair');

SELECT stage, node_name, count(*), min(started_at), min(heartbeat_at)
FROM public.pipeline_stage_activity
WHERE stage IN ('validate', 'repair')
GROUP BY stage, node_name
ORDER BY stage, node_name;

SELECT stage, status, count(*)
FROM public.pipeline_stage_results
WHERE finished_at >= now() - interval '1 hour'
  AND stage IN ('validate', 'repair')
GROUP BY stage, status
ORDER BY stage, status;
`

## Destructive-command warnings

Do **not** run any of the following as an exploratory debugging step:

`bash
docker system prune -a --volumes
docker volume prune
docker image prune -a
docker builder prune -a
docker buildx prune -a
rm -rf /data/docker
rm -rf /data/k3s
k3s crictl rmi --prune
ctr --namespace k8s.io images rm ...
systemctl restart docker
systemctl restart k3s
kill -9 BUILD_PROCESS_PID
`

Specific risks:

- These Docker daemons contain unrelated images, volumes, and builder state.
- A global volume prune can destroy non-SWE-gen databases or builder caches.
- Killing a Compose client does not guarantee its BuildKit solve, executor, or
  task container is safely reconciled.
- Restarting Docker interrupts all host Docker workloads, not only K3s Pods.
- Manual K3s containerd deletion can break running Pods and fight kubelet GC.
- Deleting active BuildKit state can corrupt or invalidate builds.
- Recursive deletion under /data can destroy unrelated multi-terabyte data.

Any approved cleanup should first resolve exact resource ownership, drain the
relevant node-wide build slots, preserve/release PGMQ leases, and use
project-scoped labels or recorded task image IDs.

## First actions for the next debug agent

1. Do not scale Repair above 128.
2. Confirm no other agent is changing the same Deployment or worker image.
3. Implement/test build environment propagation on a canary only.
4. Implement a node-wide build-slot wrapper on .110 with 16 slots.
5. Drive a separate canary queue and compare error rate and throughput.
6. Keep .78 at no more than 8 build slots until its I/O PSI is understood.
7. Only after stable admission, test the 100GB embedded cache policy and
   quiet-period explicit prune on one node.
8. Treat standalone BuildKit as a later A/B experiment, not the immediate fix.

The central hypothesis to test is:

> A bounded number of local embedded BuildKit solves, with Repair forced off
> Compose Bake delegation, will produce higher completed tasks per minute than
> the current unbounded 24-37-client regime, even though fewer builds are
> simultaneously admitted.
