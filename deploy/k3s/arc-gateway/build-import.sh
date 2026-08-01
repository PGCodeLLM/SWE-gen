#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
image="${SWEGEN_ARC_GATEWAY_IMAGE:-swegen-worker:arc-pool-litellm-20260801}"
base_image="${SWEGEN_WORKER_IMAGE:-swegen-worker:proxy-noproxy-fix-20260801-0356}"
target_node="${SWEGEN_ARC_GATEWAY_NODE:-7.244.3.200}"
build_proxy="${SWEGEN_ARC_BUILD_PROXY:-${HTTPS_PROXY:-}}"

build_args=(--build-arg "SWEGEN_BASE_IMAGE=${base_image}")
if [[ -n "${build_proxy}" ]]; then
  build_args+=(
    --build-arg "HTTP_PROXY=${build_proxy}"
    --build-arg "HTTPS_PROXY=${build_proxy}"
    --build-arg "NO_PROXY=127.0.0.1,localhost"
  )
fi

docker build \
  "${build_args[@]}" \
  -f "${script_dir}/Dockerfile" \
  -t "${image}" \
  "${script_dir}"

docker save "${image}" \
  | ssh -o BatchMode=yes -o ConnectTimeout=10 "root@${target_node}" \
      'k3s ctr -n k8s.io images import -'

kubectl apply -k "${script_dir}"
kubectl -n swegen-pipeline rollout status deployment/swegen-arc-gateway --timeout=5m
