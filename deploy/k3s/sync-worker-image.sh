#!/usr/bin/env bash
set -euo pipefail

# Ensure a worker image tag is present in every node's k3s containerd, so a
# Deployment using imagePullPolicy: Never never lands a Pod on a node that
# lacks the image (the ErrImageNeverPull failure mode). Given a tag that
# already exists on at least one node, this exports it from a node that has
# it and imports it onto every node that is missing it.
#
# Usage:
#   deploy/k3s/sync-worker-image.sh <image-tag>   # sync one tag
#   deploy/k3s/sync-worker-image.sh --all         # sync every tag referenced
#                                                 # by live swegen-pipeline Deployments
#   deploy/k3s/sync-worker-image.sh swegen-worker:generate-json-fence-fix-v2-20260804
#
# Run it after building/importing a new tag, and before scaling or applying a
# Deployment that references a tag which may only exist on some nodes. The
# --all mode is the "each deploy syncs across all nodes" guarantee: it reads
# the image of every Deployment in the namespace and ensures each is present
# on all nodes, so no scale-up or rollout can hit ErrImageNeverPull.
#
# Env:
#   SWEGEN_K3S_NODES     space-separated node IPs (default: the 4 cluster nodes)
#   SWEGEN_K3S_SSH_USER  ssh user (default: root)
#   SWEGEN_NAMESPACE     namespace for --all (default: swegen-pipeline)

image="${1:-}"
if [[ -z "${image}" ]]; then
    printf 'usage: %s <image-tag> | --all\n' "${0##*/}" >&2
    exit 2
fi

nodes_text="${SWEGEN_K3S_NODES:-7.244.3.200 7.244.3.78 7.244.2.110 7.244.1.209}"
read -r -a nodes <<< "${nodes_text}"
ssh_user="${SWEGEN_K3S_SSH_USER:-root}"
namespace="${SWEGEN_NAMESPACE:-swegen-pipeline}"

if ((${#nodes[@]} == 0)); then
    printf 'SWEGEN_K3S_NODES must contain at least one node.\n' >&2
    exit 1
fi

# --all: resolve every distinct worker image across the namespace's Deployments
# and sync each in turn. Requires kubectl access to the cluster.
if [[ "${image}" == "--all" ]]; then
    mapfile -t tags < <(
        kubectl get deploy -n "${namespace}" \
            -o jsonpath='{range .items[*]}{.spec.template.spec.containers[0].image}{"\n"}{end}' \
            | sed 's#^docker.io/library/##' | sort -u | grep .
    )
    if ((${#tags[@]} == 0)); then
        printf 'no Deployment images found in namespace %s\n' "${namespace}" >&2
        exit 1
    fi
    printf 'syncing %d distinct deployment image(s): %s\n' "${#tags[@]}" "${tags[*]}"
    rc=0
    for tag in "${tags[@]}"; do
        "${BASH_SOURCE[0]}" "${tag}" || rc=1
    done
    exit "${rc}"
fi

# containerd stores images under docker.io/library/<tag> when imported from a
# bare "name:tag". Match either the bare tag or that canonical reference.
ctr_ref="docker.io/library/${image#docker.io/library/}"

ssh_run() {
    ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${ssh_user}@${1}" "${2}"
}

node_has_image() {
    ssh_run "${1}" \
        "k3s ctr -n k8s.io images ls 2>/dev/null | grep -qF '${ctr_ref}'"
}

# Classify nodes into haves and needs.
haves=()
needs=()
for node in "${nodes[@]}"; do
    if node_has_image "${node}"; then
        haves+=("${node}")
    else
        needs+=("${node}")
    fi
done

if ((${#haves[@]} == 0)); then
    printf 'No node has %s; build/import it first.\n' "${image}" >&2
    exit 1
fi

if ((${#needs[@]} == 0)); then
    printf 'ok: %s already present on all %d nodes\n' "${image}" "${#nodes[@]}"
    exit 0
fi

# The cluster's node-to-node SSH mesh is not fully connected, so we relay the
# image tarball through the host running this script (which can reach every
# node over scp) instead of assuming source->target node SSH works. Pull once
# from any image-bearing node, then push to each needing node.
source_node="${haves[0]}"
printf 'syncing %s from %s to: %s\n' "${image}" "${source_node}" "${needs[*]}"

local_archive="$(mktemp -t swegen-worker-sync.XXXXXX.tar)"
remote_archive="/tmp/swegen-worker-sync.$$.tar"

cleanup() {
    rm -f "${local_archive}" >/dev/null 2>&1 || true
    ssh_run "${source_node}" "rm -f '${remote_archive}'" >/dev/null 2>&1 || true
    for node in "${needs[@]}"; do
        ssh_run "${node}" "rm -f '${remote_archive}'" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT

# Export once on the source node, then pull the tarball to this host.
ssh_run "${source_node}" \
    "k3s ctr -n k8s.io images export '${remote_archive}' '${ctr_ref}'"
scp -o BatchMode=yes -o StrictHostKeyChecking=no \
    "${ssh_user}@${source_node}:${remote_archive}" "${local_archive}"

# Push to every needing node from here and import.
for node in "${needs[@]}"; do
    scp -o BatchMode=yes -o StrictHostKeyChecking=no \
        "${local_archive}" "${ssh_user}@${node}:${remote_archive}"
    ssh_run "${node}" "
        set -e
        k3s ctr -n k8s.io images import '${remote_archive}'
        k3s ctr -n k8s.io images label '${ctr_ref}' io.cri-containerd.image=managed >/dev/null
        k3s crictl inspecti '${ctr_ref}' >/dev/null
    "
    printf '%s: %s ready via CRI\n' "${node}" "${image}"
done

printf 'done: %s now present on all %d nodes\n' "${image}" "${#nodes[@]}"
