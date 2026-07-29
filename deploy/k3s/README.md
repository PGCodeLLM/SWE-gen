# SWE-gen PGMQ + K3s deployment guide

This guide builds the distributed pipeline from an empty cluster and documents
the information needed to expand, rebuild, or migrate it later.

The pipeline is:

```text
PGMQ swegen_generate -> Generate -> PGMQ swegen_validate -> NOP/Oracle
  -> PGMQ swegen_reward -> reward-hack check -> PGMQ swegen_push -> SWR
```

PostgreSQL is authoritative for task state, stage results, queue state, and the
contents of generated Harbor tasks. Node filesystems contain caches and
temporary materializations only.

## Reviewed version matrix

The production cluster was verified with:

| Component | Reviewed version |
| --- | --- |
| OS | Ubuntu 24.04, amd64 |
| K3s | `v1.36.2+k3s1` |
| Bundled containerd | `v2.3.2-k3s2` |
| Docker Engine | `29.6.x` |
| PostgreSQL | `16.13` |
| PGMQ | official `1.12.0` SQL objects, installed without `CREATE EXTENSION` |
| Python | 3.12 in the worker image |
| Worker packaging | `uv sync --frozen --no-dev` |

Pin K3s exactly during installation. Test PostgreSQL, Docker, K3s, and PGMQ
upgrades on a staging queue before upgrading production.

## Recommended topology

Use three K3s servers for embedded-etcd quorum and any number of agents:

| Role | Current node | Notes |
| --- | --- | --- |
| Initial server | `7.244.3.200` | `cluster-init: true` |
| Server | `7.244.3.78` | joins embedded etcd |
| Server | `7.244.2.110` | joins embedded etcd |
| Agent | `7.244.1.209` | worker only |

K3s servers are schedulable by default. Keep an odd number of servers. A
single-server cluster is acceptable for a disposable test environment, but it
does not survive loss of that server.

## Network and host prerequisites

Each node needs:

- root or passwordless-sudo SSH access from the deployment host;
- outbound access to GitHub, language package registries, model endpoints, and
  the configured SWR registry, directly or through the approved proxy;
- synchronized time;
- Docker Engine with Buildx and Compose plugins;
- local SSD storage for `/data/k3s`, `/data/kubelet`, `/data/docker`,
  `/data/swegen-k3s/workspaces`, and `/data/swegen-k3s/cache`;
- the organization proxy CA installed on the host and available as a combined
  PEM bundle for worker containers.

Permit at least these paths between cluster nodes:

| Port/protocol | Source | Destination | Purpose |
| --- | --- | --- | --- |
| TCP 6443 | all nodes/operators | K3s servers | Kubernetes API |
| TCP 2379-2380 | K3s servers | K3s servers | embedded etcd |
| UDP 8472 | all nodes | all nodes | default Flannel VXLAN |
| TCP 10250 | all nodes/servers | all nodes | kubelet and metrics |

Also allow PostgreSQL TCP 5432 from every worker node. Add the node, Pod CIDR
`10.42.0.0/16`, and Service CIDR `10.43.0.0/16` to `NO_PROXY`. Do not expose
UDP 8472 publicly.

Install the basic host tools and verify Docker:

```bash
sudo apt-get update
sudo apt-get install --yes \
  ca-certificates curl git jq openssh-client postgresql-client unzip
docker version
docker buildx version
docker compose version
```

Create the local data roots on every node:

```bash
sudo install -d -m 0755 \
  /data/k3s \
  /data/kubelet \
  /data/k3s-storage \
  /data/k3s-etcd-snapshots \
  /data/docker \
  /data/swegen-k3s/workspaces \
  /data/swegen-k3s/cache
```

Configure Docker to use `/data/docker`. Do this before starting production
builds; moving an existing Docker root requires a separate migration procedure.

## Install K3s

### Common kubelet image-GC policy

Before installing K3s, create
`/data/k3s/agent/etc/kubelet.conf.d/10-swegen-storage.conf` on every node:

```yaml
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
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
```

This controls K3s/containerd images. It does not control Docker BuildKit cache;
Docker is covered separately below.

### Bootstrap the first server

Create root-only `/etc/rancher/k3s/config.yaml` on the first server:

```yaml
cluster-init: true
data-dir: /data/k3s
node-ip: 7.244.3.200
flannel-iface: eth0
disable:
  - traefik
  - servicelb
tls-san:
  - 7.244.3.200
  - 7.244.3.78
  - 7.244.2.110
default-local-storage-path: /data/k3s-storage
etcd-snapshot-dir: /data/k3s-etcd-snapshots
etcd-snapshot-retention: 28
etcd-snapshot-schedule-cron: "0 */6 * * *"
kubelet-arg:
  - root-dir=/data/kubelet
node-label:
  - swegen.pgcode/node-role=control-plane
  - swegen.pgcode/node-ip=7.244.3.200
```

Install the pinned release:

```bash
curl -sfL https://get.k3s.io | \
  INSTALL_K3S_VERSION='v1.36.2+k3s1' sh -s - server
sudo k3s kubectl get nodes -o wide
```

Copy `/data/k3s/server/node-token` to
`/etc/rancher/k3s/cluster-token` on joining nodes using a secure channel. Set
mode `0600`; never print or commit the token.

### Join additional servers

On each additional server, use this root-only config with its own `node-ip` and
label value:

```yaml
server: https://7.244.3.200:6443
token-file: /etc/rancher/k3s/cluster-token
data-dir: /data/k3s
node-ip: REPLACE_WITH_THIS_NODE_IP
flannel-iface: eth0
disable:
  - traefik
  - servicelb
tls-san:
  - 7.244.3.200
  - 7.244.3.78
  - 7.244.2.110
default-local-storage-path: /data/k3s-storage
etcd-snapshot-dir: /data/k3s-etcd-snapshots
etcd-snapshot-retention: 28
etcd-snapshot-schedule-cron: "0 */6 * * *"
kubelet-arg:
  - root-dir=/data/kubelet
node-label:
  - swegen.pgcode/node-role=control-plane
  - swegen.pgcode/node-ip=REPLACE_WITH_THIS_NODE_IP
```

Then run the same pinned server installation command. Wait for the server to
be Ready and for etcd health to recover before adding or restarting another
server.

### Join worker agents

Use this root-only config on an agent:

```yaml
server: https://7.244.3.200:6443
token-file: /etc/rancher/k3s/cluster-token
data-dir: /data/k3s
node-ip: REPLACE_WITH_THIS_NODE_IP
flannel-iface: eth0
kubelet-arg:
  - root-dir=/data/kubelet
node-label:
  - swegen.pgcode/node-role=worker
  - swegen.pgcode/node-ip=REPLACE_WITH_THIS_NODE_IP
```

Install it with:

```bash
curl -sfL https://get.k3s.io | \
  INSTALL_K3S_VERSION='v1.36.2+k3s1' sh -s - agent
```

Verify the cluster:

```bash
sudo k3s kubectl get nodes -o wide --show-labels
sudo k3s kubectl get pods -A -o wide
sudo k3s etcd-snapshot ls
```

Copy `/etc/rancher/k3s/k3s.yaml` to an operator-only location if remote
`kubectl` access is required, replace its loopback server address with a K3s
server address, and keep the file mode `0600`. Never add kubeconfigs to Git.

## Provision PostgreSQL and SQL-only PGMQ

The current deployment uses database `swegen_distributed`, schema `public`,
and SQL-only PGMQ 1.12.0. The PGMQ objects are installed in schema `pgmq`; there
is intentionally no `pg_extension` row.

Use a dedicated login role that owns `swegen_distributed`. The current code
expects `SWEGEN_PG_HOST`, `SWEGEN_PG_PORT`, `SWEGEN_PG_USER`,
`SWEGEN_PG_PASSWORD`, and `SWEGEN_PG_DB`. Do not place the password in a shell
command, repository file, or kubeconfig.

Create the database once while connected to the administrative `postgres`
database:

```bash
db_host='REPLACE_WITH_POSTGRES_HOST'
db_user='REPLACE_WITH_DATABASE_OWNER'
psql -X "host=${db_host} port=5432 dbname=postgres user=${db_user}" \
  -v ON_ERROR_STOP=1 \
  -c 'CREATE DATABASE swegen_distributed;'
```

Install the reviewed upstream SQL-only distribution and create the five fixed
queues. `psql` should prompt for the password interactively:

```bash
pgmq_temp_dir="$(mktemp -d)"
curl -fsSL https://api.pgxn.org/dist/pgmq/1.12.0/pgmq-1.12.0.zip \
  -o "${pgmq_temp_dir}/pgmq-1.12.0.zip"
printf '%s  %s\n' \
  e8b2eafe878e3b68cba92b874452d6da01d2c19b \
  "${pgmq_temp_dir}/pgmq-1.12.0.zip" | sha1sum --check -
unzip -q "${pgmq_temp_dir}/pgmq-1.12.0.zip" -d "${pgmq_temp_dir}"

psql -X -1 \
  "host=${db_host} port=5432 dbname=swegen_distributed user=${db_user}" \
  -v ON_ERROR_STOP=1 \
  -f "${pgmq_temp_dir}/pgmq-1.12.0/sql/pgmq.sql" \
  -f src/swegen/queueing/bootstrap.sql

psql -X \
  "host=${db_host} port=5432 dbname=swegen_distributed user=${db_user}" \
  -v ON_ERROR_STOP=1 \
  -f src/swegen/schema.sql

rm -rf -- "${pgmq_temp_dir}"
```

The checksum is for the reviewed PGXN 1.12.0 archive. Stop if it does not
match. SQL-only PGMQ upgrades are manual: back up the database, review the
upstream migration SQL and required function signatures, then test the upgrade
before applying it to production.

Verify the installation:

```sql
SELECT current_setting('server_version');
SELECT extversion FROM pg_extension WHERE extname = 'pgmq'; -- zero rows is expected
SELECT to_regprocedure('pgmq.send(text,jsonb,integer)');
SELECT to_regprocedure('pgmq.read_with_poll(text,integer,integer,integer,integer,jsonb)');
SELECT queue_name FROM pgmq.meta ORDER BY queue_name;
SELECT * FROM pgmq.metrics('swegen_generate');
```

Expected queues are `swegen_generate`, `swegen_validate`, `swegen_reward`,
`swegen_push`, and `swegen_dead`.

## Prepare the repository and worker image

On the deployment host:

```bash
git clone https://github.com/PGCodeLLM/SWE-gen.git
cd SWE-gen
git switch swegen-k3s
uv sync --frozen
```

Choose one immutable worker tag for a clean deployment and set all four
Deployments in `swegen-pipeline.yaml` to that tag. The production manifest can
temporarily contain different debugging tags; a rebuild should converge them.

Build once, checksum the archive, import it through K3s containerd on every
node, and retain exactly one current archive under the node's configured K3s
data directory:

```bash
SWEGEN_WORKER_IMAGE='swegen-worker:REPLACE_WITH_IMMUTABLE_TAG' \
SWEGEN_K3S_NODES='7.244.3.200 7.244.3.78 7.244.2.110 7.244.1.209' \
SWEGEN_K3S_SSH_USER='root' \
SWEGEN_BUILD_CA='/path/to/combined-ca.crt' \
  ./deploy/k3s/build-import-worker.sh
```

The helper discovers `data-dir` from `/etc/rancher/k3s/config.yaml`, verifies
the archive checksum, labels the image for CRI, and confirms `crictl` can see
the tag. With `imagePullPolicy: Never`, every schedulable node must have the
image before a rollout. This includes nodes that are currently full but may
become schedulable later.

## Create Kubernetes secrets

The helper requires these root-readable source files:

- model credentials for Generate;
- reward-checker credentials;
- `swegen.toml`;
- the combined proxy CA bundle;
- an HTTP/HTTPS proxy environment file;
- Docker `config.json` containing registry credentials.

Run it without printing secrets:

```bash
SWEGEN_SECRET_ROOT='/secure/swegen-secrets' \
SWEGEN_PROXY_ENV='/secure/swegen-secrets/proxy.env' \
SWEGEN_DOCKER_CONFIG='/root/.docker/config.json' \
  ./deploy/k3s/create-secrets.sh
```

The script prompts for the PostgreSQL password unless
`SWEGEN_PG_PASSWORD` is already supplied by a secure process environment. It
creates only Kubernetes Secret objects and never writes plaintext credentials
to the repository.

## Configure and deploy the pipeline

Review `deploy/k3s/swegen-pipeline.yaml` before applying it:

1. Set the PostgreSQL host, port, user, and database.
2. Update proxy and `NO_PROXY` values for the new network.
3. Replace worker image tags with the tag imported above.
4. Update or remove node selectors for Generate, Reward, and Push.
5. Keep Validate without a node selector so the scheduler can use any node
   with sufficient requested resources.
6. For the first smoke test, set every Deployment to one replica. Do not apply
   large production replica counts to an unverified cluster.
7. Confirm CPU and memory requests reflect observed usage. Kubernetes schedules
   against requests, not live utilization.

Validate and apply:

```bash
kubectl apply --dry-run=server -f deploy/k3s/swegen-pipeline.yaml
kubectl apply -f deploy/k3s/swegen-pipeline.yaml
kubectl -n swegen-pipeline get deploy,pods -o wide
kubectl -n swegen-pipeline rollout status deploy/swegen-generate --timeout=10m
kubectl -n swegen-pipeline rollout status deploy/swegen-validate --timeout=10m
kubectl -n swegen-pipeline rollout status deploy/swegen-reward --timeout=10m
kubectl -n swegen-pipeline rollout status deploy/swegen-push --timeout=10m
```

Do not force-delete workers performing long Harbor jobs. The Deployments use
long termination grace periods so claimed work can finish or safely become
visible again.

## End-to-end smoke test

Use one previously unseen merged pull request:

```bash
read -r -s -p 'PostgreSQL password: ' swegen_pg_password
printf '\n'
SWEGEN_PG_HOST='REPLACE_WITH_POSTGRES_HOST' \
SWEGEN_PG_PORT='5432' \
SWEGEN_PG_USER='REPLACE_WITH_DATABASE_USER' \
SWEGEN_PG_DB='swegen_distributed' \
SWEGEN_PG_PASSWORD="${swegen_pg_password}" \
  uv run swegen-pipeline enqueue --repo OWNER/REPO --pr PR_NUMBER
unset swegen_pg_password
```

Prefer injecting the password from a secret manager rather than entering it in
shell history. Monitor the task:

```bash
uv run swegen-pipeline status --task-id OWNER__REPO-PR_NUMBER
kubectl -n swegen-pipeline logs -l swegen.pgcode/stage=generate --tail=100
kubectl -n swegen-pipeline logs -l swegen.pgcode/stage=validate --tail=100
kubectl -n swegen-pipeline logs -l swegen.pgcode/stage=reward --tail=100
kubectl -n swegen-pipeline logs -l swegen.pgcode/stage=push --tail=100
```

Success requires one durable result for each stage, stored task files in
`public.pipeline_task_files`, empty or decreasing stage queues, and a recorded
SWR push.

## Dashboard

Run the distributed dashboard independently from the legacy Slurm dashboard:

```bash
uv run python src/run_pipeline_dashboard.py --host 0.0.0.0 --port 8766
```

For production, place it behind access control or a trusted network boundary
and run it under systemd with:

- `WorkingDirectory` set to the checked-out `swegen-k3s` worktree;
- a root-only `EnvironmentFile` containing the `SWEGEN_PG_*` values;
- `KUBECONFIG=/etc/rancher/k3s/k3s.yaml`;
- automatic restart on failure.

Verify `http://SERVER:8766/healthz` and `/api/pipeline/status`. The dashboard
worker limit is derived from total cluster allocatable CPUs, so it increases
when nodes are added.

## Storage model and backups

`/data/swegen-k3s/workspaces` is a node-local `hostPath`, not `emptyDir`, but
each delivery is intentionally created with `TemporaryDirectory` and removed
after the stage completes. Completed Harbor task files are stored in
`swegen_distributed.public.pipeline_task_files` and rematerialized on demand.

Consequences:

- deleting a worker Pod does not delete authoritative task contents;
- losing PostgreSQL can lose the pipeline and generated task contents;
- identical workspace paths on different nodes are unrelated local disks;
- use a shared RWX PVC only for durable exported ZIPs or operator-visible
  directories, not as the primary task ledger.

Back up:

1. PostgreSQL with regular tested `pg_dump`/physical backups;
2. K3s embedded etcd snapshots in `/data/k3s-etcd-snapshots`;
3. encrypted copies of secret source files and registry credentials;
4. this Git branch and immutable worker-image tags;
5. SWR registry data according to the registry's backup policy.

Test restoration, including a generated task export, before relying on the
backup.

## Bound containerd, BuildKit, Docker, and registry growth

There are separate stores with separate cleanup controls:

| Store | Typical path | Control |
| --- | --- | --- |
| K3s/containerd images | `/data/k3s` | kubelet image GC thresholds |
| Host Docker images and layers | `/data/docker` | Docker image/container prune |
| BuildKit cache | `/data/docker` | `docker buildx prune` budgets |
| Current local worker archive | `/data/k3s/agent/images/swegen-worker-current.tar` | overwritten by build helper |
| Remote SWR/Harbor registry | external | retention policy plus registry GC |

Inspect each node before pruning:

```bash
df -h /data
docker system df
docker buildx du
sudo k3s crictl images
sudo du -sh /data/k3s /data/kubelet /data/docker
```

A conservative BuildKit cleanup command for a large worker node is:

```bash
docker buildx prune --force \
  --filter 'until=24h' \
  --reserved-space 20gb \
  --max-used-space 100gb \
  --min-free-space 100gb
```

Tune those budgets to disk size and concurrent build volume. Run the command
manually first, then automate it with a systemd timer and `flock` so only one
cleanup runs at a time. BuildKit prunes unused cache records; it should not
need to stop the daemon.

Periodically remove stopped Harbor containers and old unused images:

```bash
docker container prune --force --filter 'until=24h'
docker image prune --all --force --filter 'until=168h'
```

Do not run `docker system prune --volumes` on these nodes. Do not prune while a
manual recovery depends on an untagged image. Keep immutable production worker
tags protected until all Pods have rolled forward.

For SWR/Harbor, configure retention before garbage collection: keep release
tags and the newest required task images, expire superseded/debug tags by age,
then run the registry's supported GC. Registry retention does not clean local
Docker or containerd stores.

Install the orphaned-CRI-container reconciler on every node if runtime
containers have previously outlived deleted Pods:

Before enabling it, provision `/etc/swegen/kubeconfig` as a root-only
kubeconfig that can list Pods in `swegen-pipeline`. Prefer a dedicated
least-privilege service account; if an administrative kubeconfig is used for
initial recovery, treat it as a secret and replace it afterward.

```bash
sudo install -m 0755 deploy/k3s/reconcile-orphaned-workers.py \
  /usr/local/sbin/swegen-reconcile-orphaned-workers
sudo install -m 0644 deploy/k3s/systemd/swegen-orphan-reconciler.service \
  /etc/systemd/system/
sudo install -m 0644 deploy/k3s/systemd/swegen-orphan-reconciler.timer \
  /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now swegen-orphan-reconciler.timer
```

The reconciler checks Kubernetes Pod UIDs before deleting anything. It is not a
replacement for Docker/BuildKit cleanup because K3s containerd and host Docker
are separate runtimes.

## Add or remove nodes

To add capacity:

1. prepare storage, Docker, proxy CA, firewall, and kubelet GC on the new node;
2. join it as a K3s agent with the pinned version and a unique node IP label;
3. add its IP to cluster and proxy `NO_PROXY` values;
4. import the current worker image before making the node schedulable;
5. verify Docker, `crictl`, registry authentication, and proxy connectivity;
6. uncordon the node and watch a one-Pod canary before increasing replicas.

Import only to the new node with:

```bash
SWEGEN_K3S_NODES='NEW_NODE_IP' \
SWEGEN_WORKER_IMAGE='swegen-worker:CURRENT_TAG' \
SWEGEN_BUILD_CA='/path/to/combined-ca.crt' \
  ./deploy/k3s/build-import-worker.sh
```

Validate has no node selector and can use new free capacity immediately. Other
stages remain pinned until their selectors are updated or removed. Kubernetes
does not automatically rebalance already-running Pods when a node is added;
use a controlled rollout or a reviewed descheduler policy if redistribution is
required.

Before removing a node, cordon it, allow long workers to finish, drain it with
an appropriate timeout, confirm PGMQ claims are visible or completed, and only
then remove the K3s member. Remove an etcd server one at a time and retain an
odd healthy quorum.

## Migrate the cluster or database

Use a controlled queue-preserving cutover:

1. stop new enqueues;
2. wait for `pipeline_stage_activity` to become empty, or explicitly release
   and account for every remaining claim;
3. scale the four Deployments to zero without deleting PGMQ queues;
4. take a full PostgreSQL backup and an etcd snapshot;
5. build the replacement cluster with the same reviewed versions;
6. restore the complete `swegen_distributed` database, including the SQL-only
   `pgmq` schema and queue tables;
7. run the PGMQ capability/queue verification and application schema bootstrap;
8. recreate Kubernetes Secrets from the secure source files;
9. import the exact worker image to every schedulable node;
10. start one worker per stage and complete a smoke task;
11. resume the preserved queues, then scale gradually while watching failure
    rate, queue age, PostgreSQL load, and Docker disk use.

Do not recreate queues after restoring their tables: `pgmq.create` is intended
for an empty bootstrap, while a migration must preserve queued messages and
archive history. Keep the old cluster stopped but recoverable until the new
pipeline has completed real tasks through SWR.

## Final verification checklist

- all nodes report Ready and the expected K3s version;
- embedded etcd has three healthy members and recent snapshots;
- all schedulable nodes have the exact worker image through CRI;
- PostgreSQL reports the five PGMQ queues and required function signatures;
- application schema and `pipeline_task_files` exist;
- secrets exist without plaintext copies in Git;
- one Pod per stage completes an unseen task end to end;
- dashboard health and cluster-capacity scaling controls work;
- containerd, Docker, BuildKit, workspace, PostgreSQL, and registry storage all
  have independent monitoring and cleanup/retention policies;
- PostgreSQL and etcd restore procedures have been tested.
