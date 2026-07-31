#!/usr/bin/env bash

set -Eeuo pipefail

usage() {
    cat <<'EOF'
Usage: ./master_switch.sh [start|stop|pause|resume|status]

Commands:
  start   Refresh Secrets, apply the K3s deployment, start workers, then start
          autoqueue. This is the default command.
  stop    Stop autoqueue first, then scale all workers to zero.
  pause   Stop autoqueue while allowing already queued work to continue.
  resume  Start autoqueue without changing worker replica counts.
  status  Show Deployments, Pods, and recent autoqueue logs.

Runtime sizing is read exclusively from [pipeline] in swegen.toml:
  namespace, worker_image, storage paths, K3s nodes, worker counts, and
  rollout_timeout_seconds.

Other useful overrides:
  SWEGEN_CONFIG_SOURCE       default <repository>/swegen.toml
  SWEGEN_SKIP_SECRET_REFRESH set to 1 when Kubernetes Secrets already exist
  SWEGEN_FORCE_SECRET_REFRESH set to 1 to rebuild every Secret from source files
  SWEGEN_KUBECTL             for example "sudo k3s kubectl"
EOF
}

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
namespace=""
worker_image=""
workspace_host_path=""
repo_cache_host_path=""
successful_tasks_host_path=""
build_worker_image_on_start=""
manifest="${SWEGEN_PIPELINE_MANIFEST:-${repository_root}/deploy/k3s/swegen-pipeline_tester.yaml}"
config_source="${SWEGEN_CONFIG_SOURCE:-${repository_root}/swegen.toml}"
action="${1:-start}"
generate_replicas=""
validate_replicas=""
reward_replicas=""
push_replicas=""
autoqueue_replicas=""
rollout_timeout=""
temporary_directory=""

cleanup() {
    if [[ -n "${temporary_directory}" && -d "${temporary_directory}" ]]; then
        rm -rf -- "${temporary_directory}"
    fi
}
trap cleanup EXIT

if [[ "$#" -gt 1 ]]; then
    usage >&2
    exit 2
fi

case "${action}" in
    start|stop|pause|resume|status) ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac

if [[ -n "${SWEGEN_KUBECTL:-}" ]]; then
    read -r -a kubectl_command <<<"${SWEGEN_KUBECTL}"
elif command -v kubectl >/dev/null 2>&1; then
    kubectl_command=(kubectl)
elif command -v k3s >/dev/null 2>&1 && [[ "$(id -u)" -eq 0 ]]; then
    kubectl_command=(k3s kubectl)
elif command -v k3s >/dev/null 2>&1 && command -v sudo >/dev/null 2>&1; then
    kubectl_command=(sudo k3s kubectl)
else
    echo "Could not find kubectl or k3s. Set SWEGEN_KUBECTL explicitly." >&2
    exit 1
fi

kube() {
    "${kubectl_command[@]}" "$@"
}

scale_deployment() {
    local name="$1"
    local replicas="$2"
    kube -n "${namespace}" scale "deployment/${name}" --replicas="${replicas}"
}

show_status() {
    kube -n "${namespace}" get deployments,pods -o wide
}

wait_for_deployment() {
    local name="$1"
    local replicas="$2"
    if [[ "${replicas}" -gt 0 ]]; then
        kube -n "${namespace}" rollout status "deployment/${name}" \
            --timeout="${rollout_timeout}"
    fi
}

load_runtime_settings() {
    local -a settings

    if [[ ! -r "${config_source}" ]]; then
        echo "SWE-gen configuration is not readable: ${config_source}" >&2
        exit 1
    fi
    if ! command -v uv >/dev/null 2>&1; then
        echo "uv is required to load swegen.toml." >&2
        exit 1
    fi

    mapfile -d '' -t settings < <(
        SWEGEN_CONFIG="${config_source}" \
            uv --directory "${repository_root}" run python - <<'PY'
import sys

from swegen.model_settings import load_pipeline_settings

pipeline = load_pipeline_settings()
for value in (
    pipeline.namespace,
    pipeline.worker_image,
    str(pipeline.workspace_host_path),
    str(pipeline.repo_cache_host_path),
    str(pipeline.successful_tasks_host_path),
    "1" if pipeline.build_worker_image_on_start else "0",
    pipeline.autoqueue_workers,
    pipeline.generate_workers,
    pipeline.validate_workers,
    pipeline.reward_workers,
    pipeline.push_workers,
    pipeline.rollout_timeout_seconds,
):
    sys.stdout.write(str(value))
    sys.stdout.write("\0")
PY
    )
    if [[ "${#settings[@]}" -ne 12 ]]; then
        echo "Could not load [pipeline] settings from ${config_source}." >&2
        exit 1
    fi

    namespace="${settings[0]}"
    worker_image="${settings[1]}"
    workspace_host_path="${settings[2]}"
    repo_cache_host_path="${settings[3]}"
    successful_tasks_host_path="${settings[4]}"
    build_worker_image_on_start="${settings[5]}"
    autoqueue_replicas="${settings[6]}"
    generate_replicas="${settings[7]}"
    validate_replicas="${settings[8]}"
    reward_replicas="${settings[9]}"
    push_replicas="${settings[10]}"
    rollout_timeout="${settings[11]}s"
}

validate_start_settings() {
    SWEGEN_CONFIG="${config_source}" \
        uv --directory "${repository_root}" run python - <<'PY'
from swegen.model_settings import load_swr_target

minddistiller = load_swr_target("minddistiller")
for key, value in (
    ("host", minddistiller.host),
    ("repository", minddistiller.repository),
    ("username", minddistiller.username),
    ("password", minddistiller.password),
):
    if not isinstance(value, str) or not value.strip() or value.strip() == "replace-me":
        raise SystemExit(f"[swr.minddistiller].{key} must be configured")
PY
}

runtime_secrets_exist() {
    local secret_name
    for secret_name in \
        swegen-runtime-proxy \
        swegen-private-files \
        swegen-docker-config \
        swegen-database
    do
        if ! kube -n "${namespace}" get "secret/${secret_name}" >/dev/null 2>&1; then
            return 1
        fi
    done
}

refresh_config_secret() {
    local patch_file

    if [[ -z "${temporary_directory}" ]]; then
        temporary_directory="$(mktemp -d)"
    fi
    patch_file="${temporary_directory}/swegen-private-files-patch.json"
    python3 - "${config_source}" "${patch_file}" <<'PY'
import base64
import json
import sys
from pathlib import Path

config_path = Path(sys.argv[1])
patch_path = Path(sys.argv[2])
encoded = base64.b64encode(config_path.read_bytes()).decode("ascii")
patch_path.write_text(
    json.dumps({"data": {"swegen.toml": encoded}}, separators=(",", ":")),
    encoding="utf-8",
)
patch_path.chmod(0o600)
PY
    kube -n "${namespace}" patch secret/swegen-private-files \
        --type=merge --patch-file "${patch_file}" >/dev/null
    echo "Updated swegen-private-files from ${config_source}; existing CA was preserved."
}

refresh_runtime_secrets() {
    if [[ "${SWEGEN_SKIP_SECRET_REFRESH:-0}" == "1" ]]; then
        echo "Using existing Kubernetes Secrets without refreshing swegen.toml."
        return
    fi
    if [[ "${SWEGEN_FORCE_SECRET_REFRESH:-0}" != "1" ]] && runtime_secrets_exist; then
        refresh_config_secret
        return
    fi
    SWEGEN_CONFIG_SOURCE="${config_source}" \
    SWEGEN_KUBECTL="${SWEGEN_KUBECTL:-}" \
        "${repository_root}/deploy/k3s/create-secrets.sh"
}

render_runtime_manifest() {
    local output_path="$1"

    uv --directory "${repository_root}" run python - \
        "${manifest}" \
        "${output_path}" \
        "${namespace}" \
        "${worker_image}" \
        "${workspace_host_path}" \
        "${repo_cache_host_path}" \
        "${successful_tasks_host_path}" \
        "${generate_replicas}" \
        "${validate_replicas}" \
        "${reward_replicas}" \
        "${push_replicas}" <<'PY'
import sys
from pathlib import Path

import yaml

source = Path(sys.argv[1])
destination = Path(sys.argv[2])
namespace = sys.argv[3]
worker_image = sys.argv[4]
host_paths = {
    "workspace": sys.argv[5],
    "repo-cache": sys.argv[6],
    "successful-harbor-tasks": sys.argv[7],
}
replicas = {
    "swegen-autoqueue": 0,
    "swegen-generate": int(sys.argv[8]),
    "swegen-validate": int(sys.argv[9]),
    "swegen-reward": int(sys.argv[10]),
    "swegen-push": int(sys.argv[11]),
}
documents = list(yaml.safe_load_all(source.read_text(encoding="utf-8")))
seen: set[str] = set()
for document in documents:
    if not isinstance(document, dict):
        continue
    metadata = document.setdefault("metadata", {})
    if document.get("kind") == "Namespace":
        metadata["name"] = namespace
    else:
        metadata["namespace"] = namespace
    if document.get("kind") != "Deployment":
        continue
    name = document.get("metadata", {}).get("name")
    if name in replicas:
        document.setdefault("spec", {})["replicas"] = replicas[name]
        pod_spec = document["spec"]["template"]["spec"]
        pod_spec["containers"][0]["image"] = worker_image
        for volume in pod_spec.get("volumes", []):
            volume_name = volume.get("name")
            if volume_name in host_paths and "hostPath" in volume:
                volume["hostPath"]["path"] = host_paths[volume_name]
        seen.add(name)
missing = set(replicas) - seen
if missing:
    raise SystemExit("pipeline manifest is missing Deployments: " + ", ".join(sorted(missing)))
destination.write_text(
    yaml.safe_dump_all(documents, sort_keys=False),
    encoding="utf-8",
)
PY
}

start_pipeline() {
    local runtime_manifest

    load_runtime_settings
    validate_start_settings
    if [[ ! -r "${manifest}" ]]; then
        echo "Pipeline manifest is not readable: ${manifest}" >&2
        exit 1
    fi

    if [[ "${build_worker_image_on_start}" == "1" ]]; then
        SWEGEN_CONFIG_SOURCE="${config_source}" \
            "${repository_root}/deploy/k3s/build-import-worker.sh"
    fi

    refresh_runtime_secrets

    if [[ -z "${temporary_directory}" ]]; then
        temporary_directory="$(mktemp -d)"
    fi
    runtime_manifest="${temporary_directory}/swegen-pipeline-runtime.yaml"
    render_runtime_manifest "${runtime_manifest}"

    kube apply --dry-run=server -f "${runtime_manifest}"
    kube apply -f "${runtime_manifest}"

    scale_deployment swegen-generate "${generate_replicas}"
    scale_deployment swegen-validate "${validate_replicas}"
    scale_deployment swegen-reward "${reward_replicas}"
    scale_deployment swegen-push "${push_replicas}"

    wait_for_deployment swegen-generate "${generate_replicas}"
    wait_for_deployment swegen-validate "${validate_replicas}"
    wait_for_deployment swegen-reward "${reward_replicas}"
    wait_for_deployment swegen-push "${push_replicas}"

    scale_deployment swegen-autoqueue "${autoqueue_replicas}"
    wait_for_deployment swegen-autoqueue "${autoqueue_replicas}"
    show_status
    echo "SWE-gen pipeline is running. Autoqueue was started last."
}

stop_pipeline() {
    scale_deployment swegen-autoqueue 0
    scale_deployment swegen-generate 0
    scale_deployment swegen-validate 0
    scale_deployment swegen-reward 0
    scale_deployment swegen-push 0
    show_status
    echo "SWE-gen pipeline shutdown requested; PostgreSQL and PGMQ state were preserved."
}

case "${action}" in
    start)
        start_pipeline
        ;;
    stop)
        load_runtime_settings
        stop_pipeline
        ;;
    pause)
        load_runtime_settings
        scale_deployment swegen-autoqueue 0
        show_status
        echo "Autoqueue is paused; existing queued work may continue."
        ;;
    resume)
        load_runtime_settings
        scale_deployment swegen-autoqueue "${autoqueue_replicas}"
        wait_for_deployment swegen-autoqueue "${autoqueue_replicas}"
        show_status
        ;;
    status)
        load_runtime_settings
        show_status
        kube -n "${namespace}" logs deployment/swegen-autoqueue --tail=30 || true
        ;;
esac
