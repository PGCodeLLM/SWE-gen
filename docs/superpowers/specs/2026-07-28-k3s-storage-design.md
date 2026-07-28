# K3s Cluster and Build Storage Design

## Scope

Rebuild the interrupted four-node K3s installation as one cluster and establish
storage boundaries suitable for running many short-lived SWE-gen/Harbor build
jobs. This design does not migrate the Slurm pipeline yet and does not deploy a
message queue, BuildKit service, or registry.

## Current state and root cause

All four nodes run K3s `v1.36.2+k3s1`. The interrupted installation created a
standalone server and then an agent on every node. Each server already owns the
local API load-balancer port `127.0.0.1:6444`, so every agent exits and restarts
with `bind: address already in use`. Each server currently contains only the
default `kube-system` workloads, so rebuilding does not remove application
workloads.

The Slurm pipeline uses Docker rooted at `/data/docker`; K3s uses its separate
embedded containerd runtime. Docker cache, K3s/containerd images, BuildKit
cache, and registry blobs are independent stores and require independent
policies.

## Cluster topology

- `7.244.3.200`: initial K3s server, embedded-etcd bootstrap, temporary fixed
  registration address.
- `7.244.3.78`: K3s server joined to embedded etcd.
- `7.244.2.110`: K3s server joined to embedded etcd.
- `7.244.1.209`: K3s agent.
- Server nodes remain schedulable so all four nodes can run pipeline workloads.
- K3s is pinned to `v1.36.2+k3s1` on every node.
- Traefik and ServiceLB are disabled because this batch pipeline does not need
  an ingress controller or host-port load-balancer pods.
- The three server IPs are certificate SANs. A kube-vip or external load
  balancer can later replace `7.244.3.200` as the fixed registration address;
  that requires an unused IP and is outside this bootstrap.

## Node storage layout

All four machines have a local 32 TB ext4 filesystem mounted at `/data`.

- `/data/k3s`: K3s server/agent state and embedded containerd.
- `/data/kubelet`: kubelet root, pod sandboxes, and ephemeral volumes.
- `/data/k3s-storage`: local-path PersistentVolume data.
- `/data/k3s-etcd-snapshots`: embedded-etcd snapshots on server nodes.
- `/data/buildkit`: reserved future root for a bounded BuildKit worker.
- `/etc/rancher/k3s/cluster-token`: root-only persistent join token on the two
  joining servers and the agent, required for service restarts.

K3s state is kept separate from the existing `/data/docker` Slurm runtime.

## Containerd and kubelet garbage collection

Kubernetes owns container/image cleanup through kubelet; external `ctr` or
`crictl` pruning timers are not used because they can race kubelet and remove
objects it expects.

Each node receives a kubelet drop-in with:

- image GC high threshold: 70 percent;
- image GC low threshold: 55 percent;
- minimum unused age: 10 minutes;
- maximum unused age: 6 hours;
- eviction hard limits: 15 percent image filesystem free, 10 percent node
  filesystem free, and 5 percent free inodes;
- minimum reclaim targets of 5 percent for imagefs and nodefs.

The six-hour age limit is the primary bound on the 32 TB data disks. Percentage
thresholds remain a final disk-pressure guard. The age timer resets when
kubelet restarts, which is acceptable because the pressure thresholds continue
to operate.

## BuildKit cache policy

Future Kubernetes workers must not use one unbounded shared daemon. The
preferred execution model is a small BuildKit pool (one daemon per node) or
job-scoped builders, with `/data/buildkit` mounted as the cache root and
BuildKit-native GC enabled.

Recommended per-node BuildKit settings:

```toml
[worker.oci]
  enabled = true
  gc = true
  reservedSpace = "20GB"
  maxUsedSpace = "200GB"
  minFreeSpace = "2TB"
  max-parallelism = 8

  [[worker.oci.gcpolicy]]
    filters = ["type==source.local", "type==exec.cachemount", "type==source.git.checkout"]
    keepDuration = "6h"
    maxUsedSpace = "40GB"

  [[worker.oci.gcpolicy]]
    all = true
    keepDuration = "24h"
    reservedSpace = "20GB"
    maxUsedSpace = "200GB"
    minFreeSpace = "2TB"
```

Every task Job must also set `ephemeral-storage` requests/limits and an
`emptyDir.sizeLimit`, and must use `ttlSecondsAfterFinished` so completed Jobs
and their pods disappear automatically.

## Registry policy

No cluster registry is deployed during bootstrap. Prefer the existing external
registry with repository lifecycle/retention rules because it avoids a new
single point of failure and supports quota enforcement.

If an in-cluster CNCF Distribution registry is introduced later:

1. Put blobs on a dedicated, quota-enforced volume rather than an unconstrained
   local-path directory.
2. Delete expired tags/manifests using a retention controller.
3. Put the registry in read-only mode or stop it before running
   `registry garbage-collect --delete-untagged`.
4. Run a dry-run first and alert on both logical registry bytes and underlying
   filesystem bytes.

## Recovery and verification

Before uninstalling, each node stores a root-only archive under
`/data/k3s-recovery/20260728T163006+0800/`. The old clusters contain only default
workloads, but the archive preserves their configuration and datastore.

Completion requires:

- exactly three Ready control-plane nodes and one Ready agent;
- no duplicate `k3s-agent` service on server nodes and no `k3s` server service
  on the agent node;
- embedded-etcd health from all three members;
- all system pods healthy with no Traefik or ServiceLB pods;
- effective kubelet configuration showing the configured image GC values;
- K3s/containerd data rooted under `/data/k3s`;
- existing Slurm and Docker services still active.
