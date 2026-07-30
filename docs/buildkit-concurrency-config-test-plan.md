# BuildKit concurrency + cache control: config & test plan

## Goal

Raise **completed Validate (and Repair handoff) throughput** on the 4-node k3s
cluster without:

1. daemon thrash (`only one connection allowed`, fatal session healthchecks,
   canceled solves, Docker API stalls);
2. uncontrolled BuildKit cache growth that forces emergency `docker builder
   prune` when the daemon is already crawling.

**Central hypothesis (from handoff):** a *bounded* number of concurrent
embedded-BuildKit solves per node will finish more tasks/minute than the
current unbounded 24–37 client regime, even though fewer builds run at once.

**Throughput model to optimize for:**

```text
node_throughput ≈ build_slots × (1 / p50_build_time) × (1 − infra_fail_rate)
```

Not:

```text
node_throughput ≈ num_worker_pods
```

Worker Pod count can (and should) exceed build slots so PGMQ claim/heartbeat,
LLM Repair thinking, and post-build test phases stay pipelined while Docker is
the scarce resource.

---

## Constraints (do not fight these)

| Fact | Implication |
| --- | --- |
| Workers mount host `/var/run/docker.sock` | K8s CPU/memory requests do **not** bound Docker/BuildKit work |
| Builder is embedded `docker` driver | One BuildKit inside dockerd; no `worker.oci.max-parallelism` via daemon.json |
| Validate, Repair, and Push all build | Admission must be **node-wide**, not per Deployment |
| Repair Claude path bypasses `run_harbor_agent` child_env | Bake clients still appear unless env is Pod-level **and** propagated into Claude/Harbor |
| `.78` has severe I/O PSI from unrelated host load | Cap that node lower; do not treat 192 CPUs as 192 builds |
| Host Docker has non-SWE-gen images/volumes | No global `system prune --volumes` / blind `image prune -a` |
| Live replica counts drift from git manifests | Patch live objects carefully; never unqualified `kubectl apply` for BuildKit work |
| PGMQ visibility leases | Drain/wait deliveries before Docker restarts or node drains |

---

## Architecture target (steady state)

```text
┌─────────────────────────────────────────────────────────────┐
│ Per node (e.g. .110)                                        │
│                                                             │
│  Validate Pods (24+) ──┐                                    │
│  Repair Pods  (20–32) ─┼── docker CLI wrapper ── flock ──┐  │
│  Push Pods    (0–16)  ─┘   intercepts build forms only   │  │
│                              │                            │  │
│                              ▼                            │  │
│                     /run/swegen-build-slots/{0..N-1}      │  │
│                              │                            │  │
│                              ▼                            │  │
│                     host dockerd + embedded BuildKit      │  │
│                     GC: defaultKeepStorage=100GB          │  │
│                     quiet-period prune (timer + flock)    │  │
└─────────────────────────────────────────────────────────────┘
```

**Pod count vs slots:** keep high Validate replica counts for queue drain and
CPU-bound NOP/Oracle *after* build; limit only simultaneous *builds*.

| Node | Initial build slots | Validate pods (suggest) | Repair pods (suggest) | Notes |
| --- | ---: | ---: | ---: | --- |
| `.110` canary | 16 | 24 | ≤32 | First admission + GC canary |
| `.209` | 16 | 24 | ≤32 | Second rollout |
| `.200` | 16 | 24 | ≤31 | Shares with Push |
| `.78` | **8** | 16–24 | ≤16 | Until I/O PSI full avg60 ≪ 10% |

Do **not** scale Repair above live 128 until admission is stable and terminal
Repair→Validate handoffs are proven.

---

## Phase 0 — Safety + baseline (no mutations)

**Duration:** ≥30 min under normal production load (or canary queue if already
isolated).

### 0.1 Coordinate

- Confirm no other agent is rolling worker images / Deployments.
- Prefer a **canary PGMQ queue** (50–100 tasks) for A/B later; do not burn
  production leases on destructive cache experiments.

### 0.2 Capture baseline metrics (per node)

Record for 30+ minutes:

| Metric | How |
| --- | --- |
| Completions/min | `pipeline_stage_results` for validate/repair last 1h |
| Queue depth | `pgmq.metrics('swegen_validate')`, `swegen_repair` |
| Build clients / executors | `ps` grep for compose build, buildx bake, buildkit/executor |
| Session errors / 15m | journalctl grep (see handoff) |
| Docker API latency | `time timeout 15 docker info`; `docker ps` |
| Cache size | `timeout 30 docker builder du` |
| Image/volume size | `timeout 30 docker system df` |
| Host PSI | `/proc/pressure/{cpu,io,memory}`, `vmstat`, `iostat` |
| Orphans | stopped containers with compose project labels; volume growth |

**Acceptance for “we understand baseline”:** numbers written down with
timestamps; `.78` I/O PSI explicitly noted.

### 0.3 Canary task set

50–100 stored tasks covering Node, Python, Go, Java, Rust; mix small and large
contexts. Split into:

- **Warm-cache** run (same tasks twice, or reuse tags),
- **Cold-cache** run (fresh builder state *only* on canary node after
  deliberate drain — never on all four nodes at once).

---

## Phase 1 — Environment propagation (cheap, high ROI)

**Why first:** Repair currently spawns `docker-buildx bake` clients that
multiply contention without adding useful parallelism. Validate already sets
`COMPOSE_BAKE=false` inside `run_harbor_agent` only.

### 1.1 Config change (all Docker-capable Deployments)

On **Validate, Repair, Push** (canary image first):

```yaml
env:
  - name: DOCKER_BUILDKIT
    value: "1"
  - name: BUILDX_BUILDER
    value: "default"
  - name: COMPOSE_BAKE
    value: "false"
```

Also ensure Claude/Harbor subprocesses inherit these (Pod env alone is
necessary but not always sufficient if login shells reset env). Mirror the
explicit child_env pattern used in `harbor_runner.run_harbor_agent` into the
Repair Claude session launcher if needed.

Keep worker Compose at **v2.40.3** (Compose v5 ignores `COMPOSE_BAKE=false`).

### 1.2 Verification

```bash
# Sample a Repair pod process tree after a build starts:
tr '\0' '\n' < /proc/PID/environ \
  | grep -E '^(DOCKER_BUILDKIT|BUILDX_BUILDER|COMPOSE_BAKE|DOCKER_HOST)='

# Expect zero new Repair-attributed bake clients:
ps -eo pid,etimes,args | grep -E '[d]ocker-buildx bake'
```

### 1.3 Gate to proceed

- No new Repair bake clients for 30 min under load.
- Session error rate does not worsen (may improve slightly).
- Rollback: previous image/env only; no cache delete.

---

## Phase 2 — Node-wide build admission (the concurrency lever)

### 2.1 Design rules

1. **One pool per node**, shared by Validate + Repair + Push.
2. Acquire slot **only around build-producing CLI invocations**, not for entire
   multi-hour Repair LLM sessions.
3. Use **hostPath flock files** (`/run/swegen-build-slots` or
   `/data/swegen-k3s/build-slots`) so locks are cross-Pod and release on crash.
4. Wrapper earlier in `PATH` than `/usr/bin/docker`.
5. Intercept only:
   - `docker build`
   - `docker buildx build` / `docker buildx bake`
   - `docker compose … build` (and `docker-compose` if present)
6. Non-build commands (`ps`, `rm`, `compose up/down`, `pull`, `push`) pass
   through without a slot.
7. While waiting for a slot: continue PGMQ visibility heartbeats; export wait
   time + slot id as metrics/logs.
8. Configure N via file or ConfigMap mounted read-only, e.g.
   `/etc/swegen/build-slots.count` → `16`, so N can change without image rebuild.

### 2.2 Suggested wrapper sketch

```bash
#!/bin/bash
# /usr/local/bin/docker  (wrapper; real binary at /usr/bin/docker.real)
REAL=/usr/bin/docker.real
SLOT_DIR=${SWEGEN_BUILD_SLOT_DIR:-/run/swegen-build-slots}
N=$(cat "${SLOT_DIR}/count" 2>/dev/null || echo 16)

needs_slot=false
case "$1" in
  build) needs_slot=true ;;
  buildx)
    case "$2" in build|bake) needs_slot=true ;; esac
    ;;
  compose|compose-plugin)
    # parse remaining args for "build" subcommand
    for a in "$@"; do [[ "$a" == "build" ]] && needs_slot=true; done
    ;;
esac

if ! $needs_slot; then
  exec "$REAL" "$@"
fi

# Try slots 0..N-1; block with flock; keep FD open for child lifetime
# (use a small helper in Python/Go if bash FD bookkeeping is fragile)
```

Prefer a small tested Python/Go helper in the worker image over a fragile bash
parser for `compose` flag order.

### 2.3 Deployment wiring

- hostPath volume `build-slots` → `/run/swegen-build-slots`
- Init or DaemonSet on each node: create slot files `0..N-1`, write `count`
- Optional: node label `swegen.pgcode/build-slots=16` for observability only
  (labels do not enforce; flock does)

### 2.4 Admission sweep (canary node `.110` first)

Hold Phase-1 env fixed. Sweep total node slots:

| Step | Slots | Min duration | Min completions |
| ---: | ---: | --- | --- |
| A | 8 | 30 min | ~100 NOP/Oracle pairs if canary allows |
| B | 12 | 30 min | same |
| C | 16 | 30 min | same |
| D | 20 | 30 min | same |
| E | 24 | 30 min | same |

**Stop escalating** if the previous point fails any acceptance gate.

Warm-cache and cold-cache: run separately at the winning slot count.

### 2.5 Metrics (every sweep point)

| Category | Metrics |
| --- | --- |
| Throughput | validate + repair handoffs/min; queue depth |
| Latency | p50/p95 image build; p50/p95 full NOP/Oracle |
| Correctness | real task fail vs infra fail; infra fail **&lt; 1%** |
| BuildKit health | `only one connection` /100 builds; fatal healthchecks /100; canceled solves /100 → **near zero** |
| Concurrency | active compose clients ≤ slots (+ small lag); executor age p95 |
| Daemon | docker info/ps p95 **&lt; 2s** |
| Host | I/O PSI full avg60 **&lt; 10%**; mem PSI ~0; disk await **≲ 30 ms** |
| Cache | `docker builder du` slope (GB/h); image/volume growth rate |
| Cleanup | orphan containers/networks 5 min after task end → non-monotonic |

### 2.6 Acceptance gates (slot selection)

Use a slot count only if:

1. Infra build failure &lt; 1%.
2. Fatal session healthchecks ≈ 0; connection-allowed storms gone.
3. Docker API p95 &lt; 2s.
4. I/O PSI full avg60 &lt; 10%.
5. Throughput **≥ +10%** vs previous lower slot (else stop; prefer lower).
6. If 20 slots gains **&lt; 5–10%** over 16 → **keep 16**.
7. Cache growth rate not climbing unboundedly during the window (see Phase 3).

### 2.7 Rollout order

1. `.110` (canary) → pick N\*
2. `.209` at N\*
3. `.200` at N\* (account for Push competing for slots)
4. `.78` at **min(8, N\*/2)** until external I/O fixed

One node at a time; ≥1 full validate/repair cycle observation between nodes.

### 2.8 Rollback

- Bypass wrapper (`PATH` or symlink to real docker) — **no cache wipe**.
- Or set `count` to previous N.
- Wait for PGMQ deliveries; do not restart Docker for wrapper-only rollback.

---

## Phase 3 — Maintainable cache control (replace emergency prune)

Emergency prune when the daemon crawls is a **symptom** of (a) too many
concurrent solves + (b) soft GC that runs late. Fix order: admission first,
then hard budgets, then scheduled quiet prune, then project-scoped reaping.

### 3.1 Embedded builder GC (daemon.json) — after admission stable

Current:

```json
{
  "builder": {
    "gc": {
      "enabled": true,
      "defaultKeepStorage": "200GB"
    }
  }
}
```

Target (one node at a time, canary `.110`):

```json
{
  "builder": {
    "gc": {
      "enabled": true,
      "defaultKeepStorage": "100GB"
    }
  }
}
```

**Procedure per node:**

1. Backup `/etc/docker/daemon.json`.
2. `dockerd --validate` on the edited file.
3. Drain build work (scale or pause Validate/Repair/Push routing to that node;
   release/wait PGMQ deliveries).
4. Restart **only that node’s** Docker.
5. Smoke: `docker info`, `docker buildx ls`, one canary task.
6. Watch cache for 24h; then next node.

`defaultKeepStorage` is **periodic**, not a hard synchronous cap. Expect overshoot
under load; Phase 3.2 handles the cap.

### 3.2 Quiet-period explicit prune (systemd timer)

**Never** prune while dozens of solves run. Gate on idle slots:

```bash
# Pseudocode for /usr/local/sbin/swegen-buildkit-gc
flock -n /run/swegen-buildkit-gc.lock || exit 0
# Skip if any build slot is held:
for s in /run/swegen-build-slots/[0-9]*; do
  flock -n "$s" true || exit 0
done

docker buildx prune \
  --builder default \
  --force \
  --filter 'until=24h' \
  --reserved-space 20gb \
  --max-used-space 100gb \
  --min-free-space 2tb
```

- Timer: e.g. every 6h, or hourly with the idle gate.
- Always `--builder default` (`.78` host root may point at broken
  `harbor-builder`).
- Log start/end size from `docker builder du`.
- Alert if size stays &gt; 120GB after successful prune.

### 3.3 Project-scoped runtime cleanup (not global prune)

Continue / extend existing Harbor reaper:

| Resource | Policy |
| --- | --- |
| Task containers | Force-rm by compose project prefix `task_id__` (already in harbor_runner) |
| Project networks | Remove when project terminal |
| Task volumes | Only after project terminal; **never** global volume prune |
| Task images | Delete known tags/IDs after durable Push handoff (or age + label) |
| Stopped containers | Age threshold + compose label |

Do **not** use age alone for ownership. Prefer labels + worker-recorded IDs.

### 3.4 Separate image store pressure

After BuildKit is bounded, watch `docker system df` **Images** separately.
If reclaimable images dominate:

```bash
# Safer than -a with no filter — still review first on a quiet node
docker image prune --all --force --filter 'until=168h'
```

Exclude worker production tags from any automated prune list.

### 3.5 Success criteria for “maintainable cache”

| Signal | Target |
| --- | --- |
| Embedded BuildKit size | Steady band ~20–100GB; rare overshoot &lt; 130GB |
| Emergency prune | **Zero** ops-driven crawls for 7 days |
| Prune jobs | Run on schedule; skip when slots busy; no daemon restart |
| Disk | `/data` not filled by builder growth |

---

## Phase 4 — Raise *effective* concurrency (only after 2+3 stable)

Once slots and cache are stable, increase **useful** parallelism without
reintroducing thrash:

### 4.1 Prefer more pods, not more simultaneous builds

| Knob | Direction | Why |
| --- | --- | --- |
| Validate replicas | Keep high (e.g. 24/node) | Claim queue, run tests after image ready |
| Build slots | Hold at N\* from Phase 2 | Protect dockerd |
| Repair replicas | Cap so peak build demand ≤ slots + waiting queue is OK | LLM holds pods without holding slots |

**Effective concurrency** = slots for builds + many pods waiting on LLM/tests.

### 4.2 Optional second sweep of slots

If Phase-2 N\* was conservative (gates green with headroom on I/O PSI and API
latency), re-sweep **N\* → N\*+4 → N\*+8** on `.110` only with production-like
mix. Same gates. Do not jump to 64.

### 4.3 Push co-location

On `.200`, Push rebuilds compete for the same 16 slots. Options:

- Schedule Push on a node with spare capacity, or
- Give Push lower priority in the wrapper (optional fair-share / weight later),
  or
- Time-box Push to lower-traffic windows.

Do not give Push a private builder without a load path design.

### 4.4 Fix `.78` I/O before equalizing slots

Until I/O PSI full avg60 is low:

- slots ≤ 8;
- reduce Validate/Repair placement if needed;
- move or throttle unrelated host disk jobs under `/data`.

---

## Phase 5 — Optional A/B: one standalone BuildKit per node

**Only after** embedded builder + admission + GC are stable for several days.

| Do | Do not |
| --- | --- |
| One shared `buildkitd` per node | One builder per Pod |
| `max-parallelism = N*` | Multi-node buildx without image locality design |
| Isolated GC root `/data/swegen-buildkit` | Assume auto-load into Docker without testing |
| `default-load=true` for docker-container driver | Drop embedded admission “because BuildKit is separate” |

Config sketch:

```toml
root = "/data/swegen-buildkit"

[worker.oci]
  gc = true
  snapshotter = "overlayfs"
  max-parallelism = 16
  reservedSpace = "20GB"
  maxUsedSpace = "100GB"
  minFreeSpace = "2TB"
```

**Prove:** Compose can immediately `up` the loaded image on the same host
Docker. Measure whether throughput beats embedded+admission; image load may
erase gains.

**Rollback:** `BUILDX_BUILDER=default`.

---

## Phase 6 — Longer-term isolation (strategic)

Host Docker mixes SWE-gen with unrelated workloads → unsafe automated prune and
noisy-neighbor I/O.

Preferred end states:

1. **Dedicated build nodes** for Validate/Repair/Push only, or
2. **Dedicated SWE-gen dockerd** per node: own socket, data-root, bridge,
   containerd namespace — canary one node first.

Same-disk subdirectory isolation helps ownership, not physical I/O contention.

---

## Recommended Validate-stage utilization strategy (summary)

To **fully utilize the cluster for validation** without 64-way BuildKit fights:

1. **Admit ~16 concurrent builds/node** (8 on `.78`), shared with Repair/Push.
2. **Keep ~24 Validate pods/node** so the queue stays claimed and post-build
   work uses the other CPUs.
3. **Stop Bake proliferation** via env on all Docker stages + Repair
   propagation.
4. **Cap cache at 100GB** with idle-gated scheduled prune; delete task
   resources by label, not global prune-on-crawl.
5. **Scale slots only when gates stay green**; never equate “192 CPUs” with
   “192 builds.”
6. Treat **completed tasks/min + infra fail rate + cache slope** as the
   optimization objective, not peak client count.

### Rough capacity planning (order of magnitude)

Assume p50 build wall time ~3–8 min under healthy daemon (measure for real):

| Slots/node | Nodes at full | Concurrent builds cluster-wide | If p50=5 min → builds/hour |
| ---: | ---: | ---: | ---: |
| 16 | 3 clean + 8 on .78 | 56 | ~672 |
| 16 | 4 equal (after .78 fixed) | 64 | ~768 |
| 24 | 4 (only if gates pass) | 96 | ~1152 |

64 **workers** per node with **unbounded** builds is worse than 24 workers with
16 slotted builds if each unbounded build is 3–10× slower due to thrash.

---

## Implementation checklist (ordered)

| # | Item | Risk | Mutates prod? |
| ---: | --- | --- | --- |
| 1 | Baseline metrics dump | None | No |
| 2 | Canary queue + task set | Low | No (if separate queue) |
| 3 | Pod env COMPOSE_BAKE/DOCKER_BUILDKIT/BUILDX_BUILDER on canary | Low | Canary only |
| 4 | Repair subprocess env propagation code | Low–med | Canary image |
| 5 | Docker wrapper + hostPath slots on `.110` | Med | Canary node |
| 6 | Admission sweep 8→24; pick N\* | Med | Canary load |
| 7 | Roll N\* to `.209`, `.200`; `.78` at 8 | Med | Gradual |
| 8 | daemon.json 100GB GC one node | Med (Docker restart) | One node drained |
| 9 | systemd quiet prune + idle flock | Low | Timer only |
| 10 | Extend project-scoped reaper | Low–med | Worker image |
| 11 | Optional slot re-sweep / standalone BuildKit | Med–high | Canary A/B |

---

## Explicit non-goals (this plan)

- Scaling Repair above 128 before admission.
- 64 concurrent BuildKit solves per node.
- Global `docker system prune --volumes` or recursive deletes under `/data`.
- Multi-node remote buildx without registry locality design.
- Restarting Docker/K3s as a debug step.
- Using K3s containerd GC to clean host Docker cache (different store).

---

## One-page operator runbook (steady state)

**Daily / automated**

- Timer: idle-gated `buildx prune` to 100GB band.
- Metrics: builder du, session-error rate, docker API latency, slot wait p95.

**When throughput drops**

1. Check I/O PSI and docker API latency — not CPU idle.
2. Count compose build clients vs configured slots (wrapper bug if clients ≫ N).
3. Check bake clients (env regression).
4. Check cache size (GC timer skipped too long?).
5. Only then consider lowering slots, not raising them.

**When cache grows past 120GB**

1. Confirm no builds holding all slots for hours.
2. Run one **idle-gated** prune with `--builder default`.
3. If still high, inspect image store vs BuildKit store separately.
4. Do not restart dockerd unless drained and planned.

---

## References

- Live snapshot and safe diagnostics:
  `docs/buildkit-k3s-debug-handoff.md`
- Deploy notes (Compose pin, prune budgets):
  `deploy/k3s/README.md`
- Validate child_env:
  `src/swegen/tools/harbor_runner.py`
- Manifests:
  `deploy/k3s/swegen-pipeline.yaml`
