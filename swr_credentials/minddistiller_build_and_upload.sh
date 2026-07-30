#!/usr/bin/env bash

set -Eeuo pipefail

usage() {
    cat <<'EOF'
Usage: minddistiller_build_and_upload.sh <instance_ids.txt> <harbor_task_directory>

Select the listed Harbor tasks and invoke build_5k_js_ts_original_images.sh.
Only each selected task's environment directory is copied to a temporary root.
JOBS, LOG_DIR, REPORT_FILE, SWR_RETRIES, BUILD_TIMEOUT_SECONDS, and
PUSH_TIMEOUT_SECONDS may be set in the environment.
EOF
}

if [[ "$#" -ne 2 ]]; then
    usage >&2
    exit 2
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
instance_ids_file="$1"
task_root="$2"
build_script="${script_dir}/build_5k_js_ts_original_images.sh"
credentials_file="${SWR_CREDENTIALS_FILE:-${script_dir}/minddistiller_swr.csv}"

if [[ ! -f "${instance_ids_file}" || ! -r "${instance_ids_file}" ]]; then
    echo "Instance ID file is not readable: ${instance_ids_file}" >&2
    exit 1
fi
if [[ ! -d "${task_root}" ]]; then
    echo "Harbor task directory does not exist: ${task_root}" >&2
    exit 1
fi
if [[ ! -x "${build_script}" ]]; then
    echo "Build/upload script is not executable: ${build_script}" >&2
    exit 1
fi

filtered_root="$(mktemp -d)"
trap 'rm -rf -- "${filtered_root}"' EXIT

declare -A seen=()
selected=0
missing=0
invalid=0
duplicates=0

while IFS= read -r raw_line || [[ -n "${raw_line}" ]]; do
    instance_id="${raw_line%$'\r'}"
    instance_id="${instance_id#"${instance_id%%[![:space:]]*}"}"
    instance_id="${instance_id%"${instance_id##*[![:space:]]}"}"
    [[ -z "${instance_id}" ]] && continue

    if [[ ! "${instance_id}" =~ ^[a-z0-9_][a-z0-9_.-]{0,127}$ ]]; then
        invalid=$((invalid + 1))
        continue
    fi
    if [[ -n "${seen[${instance_id}]:-}" ]]; then
        duplicates=$((duplicates + 1))
        continue
    fi
    seen["${instance_id}"]=1

    source_environment="${task_root}/${instance_id}/environment"
    if [[ ! -f "${source_environment}/Dockerfile" ]]; then
        missing=$((missing + 1))
        continue
    fi

    mkdir -p -- "${filtered_root}/${instance_id}"
    cp -a -- "${source_environment}" "${filtered_root}/${instance_id}/environment"
    selected=$((selected + 1))
done <"${instance_ids_file}"

echo "Selected Harbor tasks: ${selected}"
echo "Missing tasks or Dockerfiles: ${missing}"
echo "Invalid IDs ignored: ${invalid}"
echo "Duplicate IDs ignored: ${duplicates}"

if [[ "${selected}" -eq 0 ]]; then
    echo "No buildable Harbor tasks were selected." >&2
    exit 1
fi

TASK_ROOT="${filtered_root}" \
SWR_CREDENTIALS_FILE="${credentials_file}" \
    "${build_script}"
