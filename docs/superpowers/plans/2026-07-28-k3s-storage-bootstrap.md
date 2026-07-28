# K3s Storage-Aware Cluster Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace four accidental standalone K3s servers plus duplicate agents with one three-server/one-agent cluster and bounded image-cache behavior.

**Architecture:** Bootstrap embedded etcd on `7.244.3.200`, join `7.244.3.78` and `7.244.2.110` as schedulable servers, then join `7.244.1.209` as an agent. Put K3s and kubelet state on local `/data`, use kubelet-native image GC, and reserve BuildKit/registry controls for their own stores.

**Tech Stack:** K3s v1.36.2+k3s1, embedded etcd, containerd, kubelet configuration drop-ins, systemd, ext4.

---

### Task 1: Capture recoverable pre-rebuild state

**Files:**
- Create remotely: `/data/k3s-recovery/20260728T163006+0800/k3s-pre-rebuild.tgz`
- Create remotely: `/data/k3s-recovery/20260728T163006+0800/sha256.txt`
- Create remotely: `/data/k3s-recovery/20260728T163006+0800/proxy.env`

- [ ] **Step 1: Confirm there are no non-system workloads**

Run on each node:

```bash
k3s kubectl get pods -A -o wide
```

Expected: only `kube-system` pods installed by a default K3s server.

- [ ] **Step 2: Archive configuration and server state**

Run on each node with a common timestamp:

```bash
install -d -m 0700 /data/k3s-recovery/20260728T163006+0800
sed -n -E '/^(HTTP_PROXY|HTTPS_PROXY|NO_PROXY|http_proxy|https_proxy|no_proxy)=/p' \
  /etc/systemd/system/k3s.service.env \
  > /data/k3s-recovery/20260728T163006+0800/proxy.env
chmod 0600 /data/k3s-recovery/20260728T163006+0800/proxy.env
tar -C / -czf /data/k3s-recovery/20260728T163006+0800/k3s-pre-rebuild.tgz \
  etc/rancher/k3s \
  etc/systemd/system/k3s.service \
  etc/systemd/system/k3s.service.env \
  etc/systemd/system/k3s-agent.service \
  etc/systemd/system/k3s-agent.service.env \
  var/lib/rancher/k3s/server
chmod 0600 /data/k3s-recovery/20260728T163006+0800/k3s-pre-rebuild.tgz
sha256sum /data/k3s-recovery/20260728T163006+0800/k3s-pre-rebuild.tgz \
  > /data/k3s-recovery/20260728T163006+0800/sha256.txt
```

Expected: archive and checksum exist and are root-only.

### Task 2: Remove the conflicting installations

- [ ] **Step 1: Remove the duplicate agent service first**

Run on all four nodes:

```bash
/usr/local/bin/k3s-agent-uninstall.sh
```

Expected: `k3s-agent.service` is absent while `k3s.service` remains.

- [ ] **Step 2: Remove each standalone server**

Run on all four nodes:

```bash
/usr/local/bin/k3s-uninstall.sh
```

Expected: both units are absent, ports 6443/6444 are closed, and Docker/Slurm remain active.

### Task 3: Bootstrap the first server

**Files:**
- Create remotely: `/etc/rancher/k3s/config.yaml`
- Create remotely: `/data/k3s/agent/etc/kubelet.conf.d/10-swegen-storage.conf`

- [ ] **Step 1: Write first-server configuration**

Write `/etc/rancher/k3s/config.yaml` with mode `0600`:

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

- [ ] **Step 2: Write kubelet storage configuration**

Write `/data/k3s/agent/etc/kubelet.conf.d/10-swegen-storage.conf`:

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

- [ ] **Step 3: Install the pinned server**

```bash
set -a
. /data/k3s-recovery/20260728T163006+0800/proxy.env
set +a
export NO_PROXY="${NO_PROXY:+$NO_PROXY,}127.0.0.1,localhost,7.244.3.200,7.244.3.78,7.244.2.110,7.244.1.209,10.42.0.0/16,10.43.0.0/16"
export no_proxy="$NO_PROXY"
curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=v1.36.2+k3s1 sh -s - server
```

Expected: `k3s.service` is active and the first node is Ready.

### Task 4: Join the remaining servers and agent

- [ ] **Step 1: Transfer the generated token without printing it**

Copy `/data/k3s/server/node-token` from `7.244.3.200` to
`/etc/rancher/k3s/cluster-token` on each joining node and set mode `0600`. Do
not print the token.

- [ ] **Step 2: Join two additional servers**

On `7.244.3.78`, write the following root-only config:

```yaml
server: https://7.244.3.200:6443
token-file: /etc/rancher/k3s/cluster-token
data-dir: /data/k3s
node-ip: 7.244.3.78
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
  - swegen.pgcode/node-ip=7.244.3.78
```

On `7.244.2.110`, write the following root-only config:

```yaml
server: https://7.244.3.200:6443
token-file: /etc/rancher/k3s/cluster-token
data-dir: /data/k3s
node-ip: 7.244.2.110
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
  - swegen.pgcode/node-ip=7.244.2.110
```

On both joining servers, write this kubelet drop-in:

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

On both joining servers, run:

```bash
set -a
. /data/k3s-recovery/20260728T163006+0800/proxy.env
set +a
export NO_PROXY="${NO_PROXY:+$NO_PROXY,}127.0.0.1,localhost,7.244.3.200,7.244.3.78,7.244.2.110,7.244.1.209,10.42.0.0/16,10.43.0.0/16"
export no_proxy="$NO_PROXY"
curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=v1.36.2+k3s1 sh -s - server
```

Expected: embedded etcd reports three healthy members and all three nodes are Ready.

- [ ] **Step 3: Join the agent**

On `7.244.1.209`, write `/etc/rancher/k3s/config.yaml`:

```yaml
server: https://7.244.3.200:6443
token-file: /etc/rancher/k3s/cluster-token
data-dir: /data/k3s
node-ip: 7.244.1.209
flannel-iface: eth0
kubelet-arg:
  - root-dir=/data/kubelet
node-label:
  - swegen.pgcode/node-role=worker
  - swegen.pgcode/node-ip=7.244.1.209
```

Write `/data/k3s/agent/etc/kubelet.conf.d/10-swegen-storage.conf`:

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

Install using:

```bash
set -a
. /data/k3s-recovery/20260728T163006+0800/proxy.env
set +a
export NO_PROXY="${NO_PROXY:+$NO_PROXY,}127.0.0.1,localhost,7.244.3.200,7.244.3.78,7.244.2.110,7.244.1.209,10.42.0.0/16,10.43.0.0/16"
export no_proxy="$NO_PROXY"
curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=v1.36.2+k3s1 sh -s - agent
```

Expected: the fourth node is Ready and has no control-plane role.

### Task 5: Verify storage and service behavior

- [ ] **Step 1: Verify nodes and pods**

```bash
k3s kubectl get nodes -o wide
k3s kubectl get pods -A -o wide
```

Expected: 4/4 Ready; only three control-plane nodes; all system pods healthy;
no Traefik or ServiceLB pods.

- [ ] **Step 2: Verify effective kubelet GC configuration**

Inspect each node's generated/default and SWE-gen kubelet drop-ins and query
the node config through the local filesystem. Expected values are 70/55,
10m/6h, and the configured eviction thresholds.

- [ ] **Step 3: Verify isolation from the Slurm runtime**

```bash
systemctl is-active slurmd
systemctl is-active docker
docker info --format '{{.DockerRootDir}}'
du -sh /data/k3s /data/kubelet /data/docker
```

Expected: Slurm and Docker remain active; Docker stays at `/data/docker`; K3s
uses `/data/k3s`.

- [ ] **Step 4: Verify persistent-token restart safety and remove local staging**

Restart `k3s` one at a time on `7.244.3.78` and `7.244.2.110`, then restart
`k3s-agent` on `7.244.1.209`. Confirm each node returns Ready before restarting
the next. Delete only the local `/tmp/swegen-k3s-join.*` staging directory;
retain `/etc/rancher/k3s/cluster-token` and the K3s-managed server token.
