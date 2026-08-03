# SWE-gen K3s validation throughput: config + testing plan

Snapshot window: 2026-07-30 13:39–16:10 CST (+08:00). Read-only investigation;
no cluster, daemon, cache, deployment, or queue state was mutated.

This plan revises `buildkit-k3s-debug-handoff.md`. That document's operational
inventory (paths, versions, storage-system distinctions, destructive-command
warnings) is accurate and still authoritative. Its **central hypothesis is not
supported by the outcome data**, and following it would likely reduce throughput.

---

## 1. What changed vs. the handoff doc

| Claim in handoff | Measured now | Verdict |
| --- | --- | --- |
| CPU is not the bottleneck | Confirmed: 86–91% idle, load 30–76 of 192 | ✅ agree |
| Bottleneck is BuildKit admission (24–37 clients too many) | Success rate is **uniform ~8% across all 3 clean nodes** despite different cache sizes and client counts | ❌ not supported |
| `only one connection allowed` = daemon contention | Healthchecks fail in **189 µs**. Concurrent real `Solve` errors are genuine build failures (`yarn install … exit code 1`) | ❌ it is noise |
| .78 has "unrelated host disk load" | Confirmed and **identified**: `postprocess-panguml-by-platform` + 7 `tar/unzstd`, ~178 MB/s r + 200 MB/s w, under `/data/work/alex/temp_work` | ✅ agree, now actionable |
| Repair bypasses `COMPOSE_BAKE=false` | Confirmed empirically at the process level | ✅ agree |
| Cache above 200 GB target on .110 | Now **238.9 GB** (194.4 GB reclaimable) | ✅ agree, worse |
| `next_attempt` bug fixed | Fixed in validate at ~15:00 (`validate-next-attempt-20260730`); **still absent in `swegen-push:e2e-ca-20260729`** | ⚠️ partially |

### The reframe

The pipeline is **network-bound and task-quality-bound**, not BuildKit-parallelism
bound. Validate outcomes, last 6 h (n = 1076):

| Class | Count | Share | Nature |
| --- | ---: | ---: | --- |
| `succeeded` | 81 | 7.5% | — |
| `rejected` (unexpected nop/oracle reward) | 265 | 24.6% | task quality — expected attrition |
| build step failed inside Dockerfile | 347 | 32.3% | mostly `npm ci`, `curl`, `git clone` |
| timeout at 3600 s | 141 | 13.1% | wall-clock |
| network/proxy (`ECONNRESET`, `EAI_AGAIN`) | 75 | 7.0% | egress |
| `next_attempt` code bug | 97 | 9.0% | **fixed 15:00** |
| patch apply failed | 43 | 4.0% | task quality |
| other | 25 | 2.3% | — |

Top failing build steps are all network fetches: `./docker/bootstrap && npm ci`
(25), `curl -fsSLO` (18), `npm install` (14), `npm ci` (14), `git clone` (10).
There is **no package or registry mirror** configured — no `registry-mirrors`,
no `npm_config_registry`, no `PIP_INDEX_URL` — so all 224 build pods fetch
through one shared corporate proxy (`proxysg-spl.huawei.com:8080`). Single
requests through it are healthy (~0.45 s), so the loss is concurrency-dependent.

Lowering build admission to 8–16 slots/node would idle ~90% of CPU further
without touching any of the top three failure classes.

---

## 2. The real capacity ceiling: per-task container limits

Harbor caps every task container. Verified live on .200:

```
NanoCpus=1000000000   → 1 CPU
Mem=2147483648        → 2 GiB
MemSwap=4294967296    → 4 GiB
```

Source: `harbor/environments/docker/docker.py` env model — `cpus: int = 1`,
`memory: str = "1G"`; `docker-compose-build.yaml` applies them as
`deploy.resources.limits`. SWE-gen never overrides them
(`src/swegen/tools/suffixed_docker.py` sets only `main_image_name`).

Consequences:

1. **Throughput ceiling.** 56 concurrent tasks/node × 1 CPU = 56 of 192 CPUs.
   This, not BuildKit, is why the cluster looks idle.
2. **A failure cause.** 51 OOM kills in 2 h on .200, 30 on .110, 15 on .209 —
   all `CONSTRAINT_MEMCG` against a 2 GiB container scope, while the node had
   295 GB+ available. Victims are `bun`/`node`, which reserve large virtual
   address space. This produces exit 137/152 and some `npm` failures.
3. **A timeout cause.** 141 timeouts/6 h at exactly 3600 s. A 1-CPU test phase
   makes the 3600 s budget marginal for large repos.

Note `deploy.resources.limits` constrains the **run/test phase only** — image
build steps are unconstrained. That is consistent with the observation that
builds fail on network, not CPU.

Actual mean memory use is ~1.3 GB/task (73 GB used at 56 concurrent), so raising
the *limit* costs little; limits only bite the outliers we want to stop killing.

---

## 3. Configuration changes, ranked by expected payoff

### C1 — Raise per-task container limits (highest payoff)

Hook: `SwegenDockerEnvironment.__init__` in `src/swegen/tools/suffixed_docker.py`,
alongside the existing `main_image_name` mutation:

```python
self._env_vars.cpus = int(os.environ.get("SWEGEN_TASK_CPUS", "4"))
self._env_vars.memory = os.environ.get("SWEGEN_TASK_MEMORY", "8G")
```

Caveat to verify first: live containers show 2 GiB, not the model default 1 G, so
a per-task `EnvironmentConfig` may already supply `memory`. Decide precedence
deliberately — prefer `max(task_value, floor)` so rich task configs are not
downgraded. Add a unit test pinning the resolved `CPUS`/`MEMORY`.

Budget at 64 workers/node: 64 × 4 = 256 CPU (1.33× oversubscribed on a 90%-idle
node — acceptable, and limits are not reservations); 64 × 8 G = 512 G limit
against 369 G RAM, but ~1.3 G actual mean ⇒ ~83 G expected. Watch memory PSI.

### C2 — Add a local package/registry mirror (highest reliability payoff)

Addresses ~39% of all outcomes (build-step + network classes). Options, in order:

1. Node-local pull-through caches for npm / PyPI / apt, injected as
   `npm_config_registry`, `PIP_INDEX_URL`, apt sources into the build env.
2. `"registry-mirrors"` and `"max-concurrent-downloads"` in `daemon.json` for
   Docker base-image pulls.
3. Retry-with-backoff on the fetch-heavy `RUN` steps (task-template change).

This is the only item that needs new infrastructure; stage it after C1/C3/C4.

### C3 — Fix Repair build-env propagation (confirmed defect)

`repair_action()` → `run_claude_code_session()` passes only
`claude_session_env(task_id)`, so Repair's Compose clients have no
`COMPOSE_BAKE`/`DOCKER_BUILDKIT` and delegate to unbounded `docker buildx bake`.
Verified: repair compose client `/proc/<pid>/environ` lacks both; validate's has both.

Apply in **both** places:

- ConfigMap `swegen-pipeline-config` (reaches all stages via `envFrom`):
  ```yaml
  DOCKER_BUILDKIT: "1"
  BUILDX_BUILDER: default
  COMPOSE_BAKE: "false"
  ```
- Explicitly in `claude_session_env()` / the repair session env, because the
  prompt instructs `sg docker` / `newgrp docker`, which re-execs the shell.

Also add `BUILDX_BUILDER=default` to `harbor_runner.py`'s `child_env`, which
currently sets only two of the three vars (`slurm_validation_worker.py:636-638`
sets all three — follow that precedent).

### C4 — Rebuild push image (confirmed defect)

`swegen-push:e2e-ca-20260729` lacks the `next_attempt` alias fix that validate
and repair now carry. Rebuild push from current `HEAD` before trusting push results.

### C5 — Cache GC

`.110` is at 238.9 GB against a 200 GB `defaultKeepStorage`, which is a periodic
policy, not a hard cap. Prefer the explicit, bounded form over lowering
`defaultKeepStorage` blind:

```bash
docker buildx prune --builder default --force \
  --filter 'until=24h' --reserved-space 20gb \
  --max-used-space 100gb --min-free-space 2tb
```

Run on .110 first, during a quiet period, guarded by `flock`. Cache is **not**
a current failure cause — this is disk hygiene, so schedule it, don't rush it.

### C6 — Admission control: keep, but as a safety valve, set high

Still worth building — it bounds the Repair bake fan-out and cold-cache
thundering herd — but the evidence does not justify 8–16 slots. Implement the
handoff doc's PATH-wrapper + `flock` design, and start at **48 slots/node**
(≈ current peak, i.e. non-binding), then tighten only if a gate trips.

Implementation constraints established by code review:

- Wrapper at `/usr/local/bin/docker`; PATH is
  `/app/.venv/bin:/usr/local/bin:…:/usr/bin`, so it shadows `/usr/bin/docker`
  on every traced path, including Claude's Bash tool (its shell snapshot
  re-exports PATH verbatim and defines no `docker` alias).
- **`exec` the real docker last** so `argv[0]` stays `docker` — otherwise
  `_compose_build_processes()` (`harbor_runner.py:178-183`) stops recognising
  duplicate compose clients and the existing reaper silently dies.
- Compose execs `docker-buildx` by **absolute path**
  (`/usr/libexec/docker/cli-plugins/docker-buildx`), so the wrapper cannot gate
  bake fan-out. C3 is a prerequisite, not an alternative.
- The wait happens in a child process, so `ClaimHeartbeat` (a daemon thread,
  `worker.py:315-324`) keeps extending the PGMQ lease. Do **not** put a slot
  wait in `run_once`/pre-heartbeat `_process_claim`.
- Slot wait is charged against `SWEGEN_HARBOR_TIMEOUT_SECONDS` (3600) and
  `SWEGEN_REPAIR_TIMEOUT_SECONDS` (14400). Budget it, or timeouts will rise.
- `SWEGEN_PG_POOL_MAX` is 3 with a 2.0 s acquire timeout; a heartbeat DB failure
  is **fatal to the delivery** (`worker.py:587-596`). Longer waits ⇒ more
  concurrent long-lived deliveries ⇒ raise the pool before raising wait times.

### C7 — Do not schedule builds on .78 until its I/O owner is resolved

.78 is the only genuinely degraded node: 3.0% success vs ~8% elsewhere, 24.2%
timeouts, load 690, I/O PSI full 65%, `vdb` at 97.7% util / `aqu-sz` 144 /
await ~50–61 ms, 1008 processes in D-state. Cause is identified and is **not**
SWE-gen. Either stop/`ionice` that job or cordon .78 for build stages. Raising
or lowering build slots there changes little while the disk is saturated.

---

## 4. Testing plan

### Phase 0 — Baseline (no changes)

Capture ≥ 30 min on .110/.209/.200 with .78 excluded. The handoff doc's metric
list is good; **add** these, which turned out to be the discriminating ones:

- outcome mix by class (the §1 taxonomy), not just pass/fail;
- **OOM kills/hour** (`dmesg -T | grep -c CONSTRAINT_MEMCG`) — the missing signal;
- CPU idle % and run-queue depth, to prove utilisation is the goal;
- per-node success rate, to detect skew (uniformity falsifies contention theories);
- `docker inspect` of a live task container, to confirm applied limits.

Drop or de-prioritise `only one connection allowed` and fatal-healthcheck rates:
at 189 µs they are structural noise and will mislead the gate.

### Phase 1 — C3 + C4 (correctness, no capacity change)

Canary queue only. Verify: Repair compose clients now carry `COMPOSE_BAKE=false`
and `DOCKER_BUILDKIT=1` in `/proc/<pid>/environ`; zero Repair-attributed
`docker-buildx bake` clients; push records terminal results.
Rollback: restore prior image/env. No cache deletion.

### Phase 2 — C1 sweep (the main experiment)

On .110 only, ≥ 30 min or 100 completed nop/oracle pairs per point, warm and
cold cache separately:

| Point | `SWEGEN_TASK_CPUS` | `SWEGEN_TASK_MEMORY` |
| ---: | ---: | --- |
| 1 | 1 | 2G (baseline) |
| 2 | 2 | 4G |
| 3 | 4 | 8G |
| 4 | 8 | 16G |

Primary metric: **completed validate handoffs/min**. Secondary: OOM kills/hour
(expect → 0), timeout share (expect ↓ from 13%), p50/p95 validate duration.

### Phase 3 — Worker count sweep to the 64/node target

Only after Phase 2 picks a limit. Sweep validate replicas/node 24 → 40 → 56 → 64
(96 → 256 total), holding Repair at 128. Watch memory PSI and run-queue depth.

### Phase 4 — C6 admission wrapper

Install at 48 slots/node (non-binding), confirm zero throughput regression and
that slot-wait p95 ≈ 0. Only then sweep down 48 → 32 → 24 and stop at the first
point that loses > 5% throughput.

### Phase 5 — C2 mirror, then C5 cache GC

C2 is the largest remaining reliability lever but needs new infrastructure; C5 is
hygiene. Neither blocks the capacity work.

### Acceptance gates

Keep the handoff doc's gates, with these corrections:

- I/O PSI full avg60 < 10% — keep (clean nodes are at 0.2–8.9%, .78 at 65%).
- Docker API p95 < 2 s — keep.
- disk await < ~30 ms — keep.
- OOM kills/hour **= 0** — new, and the primary Phase 2 gate.
- infra build failure < 1% — **unreachable today** (~39% of outcomes are
  build-step/network). Re-baseline this after C2; do not block Phase 2 on it.
- fatal session healthchecks near zero — **drop**, it is noise.
- throughput +10% per point — keep for Phase 2/3; for Phase 4 invert it
  (accept the lowest slot count that does *not* lose > 5%).

### Rollout order

.110 → .209 → .200 → .78 (last, and only after C7). One node at a time; hold the
others on last-known-good.

---

## 5. Sequencing summary

1. **C4** rebuild push image — trivial, unblocks trustworthy push data.
2. **C3** Repair env propagation — small, confirmed defect, bounds bake fan-out.
3. **C1** raise task container limits — the capacity unlock; sweep in Phase 2.
4. **C7** resolve or cordon .78 — removes the worst outlier.
5. **Phase 3** scale to 64 workers/node.
6. **C6** admission wrapper as a safety valve at 48 slots.
7. **C2** package/registry mirror — largest remaining reliability win.
8. **C5** cache GC — hygiene.

Revised central hypothesis, to replace the handoff doc's:

> Throughput is limited by a 1-CPU/2 GiB per-task container cap and by
> unmirrored network egress, not by BuildKit admission. Raising per-task limits
> and adding a package mirror will increase completed tasks per minute, while
> node-wide build admission is needed only as a safety valve against Repair's
> unbounded Bake fan-out.
