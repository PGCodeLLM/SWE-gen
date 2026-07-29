#!/usr/bin/env bash
set -euo pipefail

image="${SWEGEN_WORKER_IMAGE:-swegen-worker:e2e}"
nodes_text="${SWEGEN_K3S_NODES:-7.244.3.200 7.244.3.78 7.244.2.110 7.244.1.209}"
read -r -a nodes <<< "${nodes_text}"
ssh_user="${SWEGEN_K3S_SSH_USER:-root}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
build_ca="${SWEGEN_BUILD_CA:-/etc/ssl/certs/ca-certificates.crt}"
temporary_directory="$(mktemp -d)"
archive="${temporary_directory}/swegen-worker.tar"
remote_staging_archive="/tmp/swegen-worker-current.tar"
remote_archive_name="swegen-worker-current.tar"

if ((${#nodes[@]} == 0)); then
    printf 'SWEGEN_K3S_NODES must contain at least one node.\n' >&2
    exit 1
fi

normalize_image_reference() {
    local reference="$1"
    local first_component

    if [[ "${reference}" != */* ]]; then
        printf 'docker.io/library/%s\n' "${reference}"
        return
    fi

    first_component="${reference%%/*}"
    if [[ "${first_component}" == *.* || "${first_component}" == *:* || "${first_component}" == localhost ]]; then
        printf '%s\n' "${reference}"
    else
        printf 'docker.io/%s\n' "${reference}"
    fi
}

runtime_image="$(normalize_image_reference "${image}")"

cleanup() {
    rm -rf -- "${temporary_directory}"
}
trap cleanup EXIT

if [[ ! -r "${build_ca}" ]]; then
    printf 'Build CA bundle is not readable: %s\n' "${build_ca}" >&2
    exit 1
fi

docker build \
    --build-arg HTTP_PROXY \
    --build-arg HTTPS_PROXY \
    --build-arg NO_PROXY \
    --build-arg http_proxy \
    --build-arg https_proxy \
    --build-arg no_proxy \
    --secret "id=combined_ca,src=${build_ca}" \
    --file "${repo_root}/deploy/k3s/Dockerfile.worker" \
    --tag "${image}" \
    "${repo_root}"
docker save --output "${archive}" "${image}"
archive_sha256="$(sha256sum "${archive}" | awk '{print $1}')"

for node in "${nodes[@]}"; do
    remote="${ssh_user}@${node}"
    remote_data_dir="$(
        ssh -o BatchMode=yes "${remote}" \
            "sudo sed -n -E 's|^[[:space:]]*data-dir:[[:space:]]*([^[:space:]#]+).*$|\\1|p' /etc/rancher/k3s/config.yaml 2>/dev/null | head -n 1"
    )"
    remote_data_dir="${remote_data_dir:-/var/lib/rancher/k3s}"
    if [[ "${remote_data_dir}" != /* || "${remote_data_dir}" == *$'\n'* ]]; then
        printf '%s reported an invalid K3s data directory: %q\n' \
            "${node}" "${remote_data_dir}" >&2
        exit 1
    fi
    remote_archive="${remote_data_dir}/agent/images/${remote_archive_name}"

    scp -q -o BatchMode=yes "${archive}" "${remote}:${remote_staging_archive}"
    ssh -o BatchMode=yes "${remote}" \
        "set -e; test \"\$(sha256sum '${remote_staging_archive}' | awk '{print \$1}')\" = '${archive_sha256}'; sudo install -d -m 0755 \"\$(dirname '${remote_archive}')\"; sudo mv -f -- '${remote_staging_archive}' '${remote_archive}'; sudo k3s ctr -n k8s.io images import '${remote_archive}'; sudo k3s ctr -n k8s.io images label '${runtime_image}' io.cri-containerd.image=managed >/dev/null; sudo k3s crictl inspecti '${runtime_image}' >/dev/null; printf '%s: %s ready via CRI\\n' '${node}' '${runtime_image}'"
done
