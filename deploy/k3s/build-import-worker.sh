#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd -- "${script_dir}/../.." && pwd)"
swegen_config="${SWEGEN_CONFIG_SOURCE:-${repository_root}/swegen.toml}"
temporary_directory="$(mktemp -d)"
archive="${temporary_directory}/swegen-worker.tar"
remote_staging_archive="/tmp/swegen-worker-current.tar"
remote_archive_name="swegen-worker-current.tar"

cleanup() {
    rm -rf -- "${temporary_directory}"
}
trap cleanup EXIT

if [[ ! -r "${swegen_config}" ]]; then
    printf 'SWE-gen configuration is not readable: %s\n' "${swegen_config}" >&2
    exit 1
fi
if ! command -v uv >/dev/null 2>&1; then
    printf 'uv is required to load swegen.toml.\n' >&2
    exit 1
fi

mapfile -d '' -t build_settings < <(
    SWEGEN_CONFIG="${swegen_config}" \
        uv --directory "${repository_root}" run python - <<'PY'
import sys

from swegen.model_settings import load_pipeline_settings

pipeline = load_pipeline_settings()
for value in (
    pipeline.worker_image,
    pipeline.k3s_ssh_user,
    str(pipeline.build_ca_path),
    *pipeline.k3s_nodes,
):
    sys.stdout.write(value)
    sys.stdout.write("\0")
PY
)
if [[ "${#build_settings[@]}" -lt 4 ]]; then
    printf 'Could not load worker image build settings from %s.\n' "${swegen_config}" >&2
    exit 1
fi

image="${build_settings[0]}"
ssh_user="${build_settings[1]}"
build_ca="${build_settings[2]}"
nodes=("${build_settings[@]:3}")
unset build_settings

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
    --file "${repository_root}/deploy/k3s/Dockerfile.worker" \
    --tag "${image}" \
    "${repository_root}"
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
