#!/usr/bin/env bash

set -Eeuo pipefail

# Usage:
#   ./build_5k_js_ts_original_images.sh
#   JOBS=8 ./build_5k_js_ts_original_images.sh
#
# Optional environment variables:
#   TASK_ROOT  Directory containing the Harbor task subfolders.
#   JOBS       Number of concurrent Docker builds (default: 1).
#   LOG_DIR    Directory for per-task build logs.
#   REPORT_FILE Path for the final build report.
#   DOCKER_BIN Docker command to invoke (default: docker).
#   LOCAL_IMAGE_PREFIX Prefix for images retained locally (default: ea_sz_).
#   SWR_CREDENTIALS_FILE Credential CSV also used by upload_built_images_to_swr.py.
#   SWR_RETRIES Number of login/check/push attempts (default: 3).
#   BUILD_TIMEOUT_SECONDS Maximum time for each Docker build (default: 7200).
#   PUSH_TIMEOUT_SECONDS Maximum time for each SWR push (default: 7200).
#   PROGRESS_EVERY Number of completions between non-interactive updates (default: 25).

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TASK_ROOT="${TASK_ROOT:-${SCRIPT_DIR}/5k_js_ts_original}"
JOBS="${JOBS:-1}"
LOG_DIR="${LOG_DIR:-${SCRIPT_DIR}/docker-build-logs/5k_js_ts_original}"
REPORT_FILE="${REPORT_FILE:-${LOG_DIR}/build-report.txt}"
DOCKER_BIN="${DOCKER_BIN:-docker}"
LOCAL_IMAGE_PREFIX="${LOCAL_IMAGE_PREFIX:-ea_sz_}"
SWR_REGISTRY="swr-coder-data-trajectory-o84wch.swr-pro.myhuaweicloud.com"
SWR_REMOTE_REPOSITORY="${SWR_REGISTRY}/aifm.coder.exp/swegen/generated"
SWR_CREDENTIALS_FILE="${SWR_CREDENTIALS_FILE:-${SCRIPT_DIR}/minddistiller_swr.csv}"
SWR_RETRIES="${SWR_RETRIES:-3}"
BUILD_TIMEOUT_SECONDS="${BUILD_TIMEOUT_SECONDS:-7200}"
PUSH_TIMEOUT_SECONDS="${PUSH_TIMEOUT_SECONDS:-7200}"
PROGRESS_EVERY="${PROGRESS_EVERY:-25}"
PROGRESS_WIDTH=40

if [[ ! -d "${TASK_ROOT}" ]]; then
    echo "Task root does not exist: ${TASK_ROOT}" >&2
    exit 1
fi

if [[ ! "${JOBS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "JOBS must be a positive integer; received: ${JOBS}" >&2
    exit 1
fi

if [[ ! "${PROGRESS_EVERY}" =~ ^[1-9][0-9]*$ ]]; then
    echo "PROGRESS_EVERY must be a positive integer; received: ${PROGRESS_EVERY}" >&2
    exit 1
fi

if [[ ! "${SWR_RETRIES}" =~ ^[1-9][0-9]*$ ]]; then
    echo "SWR_RETRIES must be a positive integer; received: ${SWR_RETRIES}" >&2
    exit 1
fi

if [[ ! "${BUILD_TIMEOUT_SECONDS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "BUILD_TIMEOUT_SECONDS must be a positive integer; received: ${BUILD_TIMEOUT_SECONDS}" >&2
    exit 1
fi

if [[ ! "${PUSH_TIMEOUT_SECONDS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "PUSH_TIMEOUT_SECONDS must be a positive integer; received: ${PUSH_TIMEOUT_SECONDS}" >&2
    exit 1
fi

if [[ ! -f "${SWR_CREDENTIALS_FILE}" ]]; then
    echo "SWR credential CSV does not exist: ${SWR_CREDENTIALS_FILE}" >&2
    exit 1
fi

if ! command -v "${DOCKER_BIN}" >/dev/null 2>&1; then
    echo "Docker command not found: ${DOCKER_BIN}" >&2
    exit 1
fi

if ! command -v timeout >/dev/null 2>&1; then
    echo "The timeout command is required to limit individual Docker builds." >&2
    exit 1
fi

if ! command -v flock >/dev/null 2>&1; then
    echo "The flock command is required for synchronized progress reporting." >&2
    exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo "Python 3 is required to read the SWR credential CSV." >&2
    exit 1
fi

mapfile -d '' -t swr_credentials < <(
    python3 - "${SWR_CREDENTIALS_FILE}" <<'PY'
import csv
import sys
from pathlib import Path

path = Path(sys.argv[1])
with path.open(newline="", encoding="utf-8-sig") as handle:
    rows = list(csv.reader(handle))
values = {
    row[0].strip(): row[1].strip()
    for row in rows[1:]
    if len(row) >= 2 and row[0].strip()
}
for key in ("用户名", "密码"):
    value = values.get(key, "")
    if not value:
        raise SystemExit(f"credential CSV is missing {key}")
    sys.stdout.write(value)
    sys.stdout.write("\0")
PY
)
if [[ "${#swr_credentials[@]}" -ne 2 \
    || -z "${swr_credentials[0]}" \
    || -z "${swr_credentials[1]}" ]]; then
    echo "Could not load 用户名 and 密码 from ${SWR_CREDENTIALS_FILE}" >&2
    exit 1
fi
SWR_USERNAME="${swr_credentials[0]}"
SWR_PASSWORD="${swr_credentials[1]}"
unset swr_credentials

mkdir -p "${LOG_DIR}"
mkdir -p "$(dirname -- "${REPORT_FILE}")"
STATUS_DIR="$(mktemp -d)"
DOCKER_CONFIG_DIR="$(mktemp -d)"
export DOCKER_CONFIG="${DOCKER_CONFIG_DIR}"
export no_proxy="${no_proxy:-},*.myhuaweicloud.com"
export NO_PROXY="${NO_PROXY:-},*.myhuaweicloud.com"
trap 'rm -rf -- "${STATUS_DIR}" "${DOCKER_CONFIG_DIR}"' EXIT

format_duration() {
    local total_seconds="$1"
    local hours minutes seconds

    ((total_seconds < 0)) && total_seconds=0
    hours=$((total_seconds / 3600))
    minutes=$(((total_seconds % 3600) / 60))
    seconds=$((total_seconds % 60))
    printf '%02d:%02d:%02d' "${hours}" "${minutes}" "${seconds}"
}

update_progress() {
    local outcome="$1"
    local completed succeeded_so_far skipped_so_far failed_so_far
    local percent filled empty bar spaces now elapsed eta elapsed_text eta_text

    {
        flock -x 9
        read -r completed succeeded_so_far skipped_so_far failed_so_far \
            <"${PROGRESS_STATE}"
        completed=$((completed + 1))
        case "${outcome}" in
            ok)
                succeeded_so_far=$((succeeded_so_far + 1))
                ;;
            skipped)
                skipped_so_far=$((skipped_so_far + 1))
                ;;
            failed)
                failed_so_far=$((failed_so_far + 1))
                ;;
        esac
        printf '%d %d %d %d\n' \
            "${completed}" "${succeeded_so_far}" "${skipped_so_far}" \
            "${failed_so_far}" \
            >"${PROGRESS_STATE}"

        percent=$((completed * 100 / TASK_COUNT))
        now="$(date +%s)"
        elapsed=$((now - START_TIME))
        eta=$((elapsed * (TASK_COUNT - completed) / completed))
        elapsed_text="$(format_duration "${elapsed}")"
        eta_text="$(format_duration "${eta}")"
        if [[ -t 1 ]]; then
            filled=$((completed * PROGRESS_WIDTH / TASK_COUNT))
            empty=$((PROGRESS_WIDTH - filled))
            printf -v bar '%*s' "${filled}" ''
            bar="${bar// /#}"
            printf -v spaces '%*s' "${empty}" ''
            printf '\r[%s%s] %3d%%  %d/%d  Built: %d  Skipped: %d  Failed: %d  Elapsed: %s  ETA: %s' \
                "${bar}" "${spaces}" "${percent}" "${completed}" \
                "${TASK_COUNT}" "${succeeded_so_far}" "${skipped_so_far}" \
                "${failed_so_far}" "${elapsed_text}" "${eta_text}"
        elif ((completed % PROGRESS_EVERY == 0 || completed == TASK_COUNT)) \
            || [[ "${outcome}" == "failed" ]]; then
            printf '[PROGRESS] %d%%  %d/%d  Built: %d  Skipped: %d  Failed: %d  Elapsed: %s  ETA: %s\n' \
                "${percent}" "${completed}" "${TASK_COUNT}" \
                "${succeeded_so_far}" "${skipped_so_far}" "${failed_so_far}" \
                "${elapsed_text}" "${eta_text}"
        fi
    } 9>"${PROGRESS_LOCK}"
}

retry_delay() {
    local attempt="$1"
    local delay=$((2 ** attempt))
    ((delay > 30)) && delay=30
    sleep "${delay}"
}

docker_login() {
    local attempt login_output=""

    for ((attempt = 1; attempt <= SWR_RETRIES; attempt++)); do
        if login_output="$(
            printf '%s\n' "${SWR_PASSWORD}" \
                | timeout --signal=TERM --kill-after=30s 300s \
                    "${DOCKER_BIN}" login "${SWR_REGISTRY}" \
                    --username "${SWR_USERNAME}" --password-stdin 2>&1
        )"; then
            echo "Authenticated to ${SWR_REGISTRY}."
            return 0
        fi
        if [[ "${login_output,,}" == *"unauthorized"* \
            || "${login_output,,}" == *"incorrect username or password"* ]]; then
            break
        fi
        if ((attempt < SWR_RETRIES)); then
            retry_delay "${attempt}"
        fi
    done

    echo "Docker login to ${SWR_REGISTRY} failed:" >&2
    tail -n 10 <<<"${login_output}" >&2
    return 1
}

is_missing_manifest_error() {
    local lowered="${1,,}"

    if [[ "${lowered}" == *"unknown: artifact "* \
        && "${lowered}" == *" not found"* ]]; then
        return 0
    fi
    [[ "${lowered}" == *"no such manifest"* \
        || "${lowered}" == *"manifest unknown"* \
        || "${lowered}" == *"name unknown"* \
        || "${lowered}" == *"repository does not exist"* \
        || "${lowered}" == *"status code: 404"* ]]
}

remote_image_exists() {
    local remote_image="$1"
    local log_file="$2"
    local attempt manifest_output manifest_status

    for ((attempt = 1; attempt <= SWR_RETRIES; attempt++)); do
        if manifest_output="$(
            timeout --signal=TERM --kill-after=30s 300s \
                "${DOCKER_BIN}" manifest inspect "${remote_image}" 2>&1
        )"; then
            echo "Remote image already exists: ${remote_image}" >>"${log_file}"
            return 0
        else
            manifest_status=$?
        fi

        if is_missing_manifest_error "${manifest_output}"; then
            return 1
        fi
        if [[ "${manifest_output,,}" == *"unauthorized"* \
            || "${manifest_output,,}" == *"denied"* ]]; then
            printf 'SWR rejected manifest check for %s:\n%s\n' \
                "${remote_image}" "${manifest_output}" >>"${log_file}"
            return 2
        fi
        if ((attempt < SWR_RETRIES)); then
            printf 'Manifest check attempt %d/%d failed with status %d; retrying.\n' \
                "${attempt}" "${SWR_RETRIES}" "${manifest_status}" \
                >>"${log_file}"
            retry_delay "${attempt}"
        fi
    done

    printf 'Could not determine whether remote image exists: %s\n%s\n' \
        "${remote_image}" "${manifest_output}" >>"${log_file}"
    return 2
}

verify_remote_image() {
    local remote_image="$1"
    local log_file="$2"
    local attempt status=1

    for ((attempt = 1; attempt <= SWR_RETRIES; attempt++)); do
        if remote_image_exists "${remote_image}" "${log_file}"; then
            echo "Verified remote image after push: ${remote_image}" >>"${log_file}"
            return 0
        else
            status=$?
        fi
        if ((attempt < SWR_RETRIES)); then
            printf 'Post-push manifest verification %d/%d failed; retrying.\n' \
                "${attempt}" "${SWR_RETRIES}" >>"${log_file}"
            retry_delay "${attempt}"
        fi
    done

    printf 'Remote image was not visible after a successful push: %s\n' \
        "${remote_image}" >>"${log_file}"
    return "${status}"
}

push_image() {
    local local_image="$1"
    local remote_image="$2"
    local log_file="$3"
    local attempt push_status=1

    {
        echo
        echo "Tagging ${local_image} as ${remote_image}"
    } >>"${log_file}"

    if ! "${DOCKER_BIN}" tag "${local_image}" "${remote_image}" \
        >>"${log_file}" 2>&1; then
        echo "Failed to tag image for SWR upload." >>"${log_file}"
        return 1
    fi

    for ((attempt = 1; attempt <= SWR_RETRIES; attempt++)); do
        printf 'Pushing %s (attempt %d/%d)\n' \
            "${remote_image}" "${attempt}" "${SWR_RETRIES}" >>"${log_file}"
        if timeout --signal=TERM --kill-after=30s "${PUSH_TIMEOUT_SECONDS}s" \
            "${DOCKER_BIN}" push "${remote_image}" >>"${log_file}" 2>&1; then
            echo "Successfully pushed ${remote_image}" >>"${log_file}"
            "${DOCKER_BIN}" image rm "${remote_image}" \
                >>"${log_file}" 2>&1 || true
            return 0
        else
            push_status=$?
        fi

        if [[ "${push_status}" -eq 124 || "${push_status}" -eq 137 ]]; then
            printf 'SWR push attempt timed out after %s seconds.\n' \
                "${PUSH_TIMEOUT_SECONDS}" >>"${log_file}"
        else
            printf 'SWR push attempt failed with exit status %s.\n' \
                "${push_status}" >>"${log_file}"
        fi
        if ((attempt < SWR_RETRIES)); then
            retry_delay "${attempt}"
        fi
    done
    return "${push_status}"
}

build_task() {
    local task_dir="$1"
    local instance_id environment_dir dockerfile image_name local_image
    local remote_image log_file
    local build_outcome
    local proxy_var build_status remote_status
    local -a proxy_args=()

    instance_id="$(basename -- "${task_dir}")"
    environment_dir="${task_dir}/environment"
    dockerfile="${environment_dir}/Dockerfile"
    image_name="${LOCAL_IMAGE_PREFIX}${instance_id}"
    local_image="${image_name}:latest"
    remote_image="${SWR_REMOTE_REPOSITORY}:${instance_id}"
    log_file="${LOG_DIR}/${instance_id}.log"

    if "${DOCKER_BIN}" image inspect "${local_image}" >/dev/null 2>&1; then
        echo "Skipped build: local image ${local_image} already exists." >"${log_file}"
        build_outcome="skipped"
    else
        for proxy_var in \
            HTTP_PROXY HTTPS_PROXY NO_PROXY \
            http_proxy https_proxy no_proxy; do
            if [[ -n "${!proxy_var:-}" ]]; then
                proxy_args+=(--build-arg "${proxy_var}")
            fi
        done

        if [[ ! -f "${dockerfile}" ]]; then
            echo "Missing Dockerfile: ${dockerfile}" >"${log_file}"
            : >"${STATUS_DIR}/${instance_id}.failed"
            update_progress failed
            return 0
        fi

        if timeout --signal=TERM --kill-after=30s "${BUILD_TIMEOUT_SECONDS}s" \
            "${DOCKER_BIN}" build \
            "${proxy_args[@]}" \
            --file "${dockerfile}" \
            --tag "${local_image}" \
            "${environment_dir}" >"${log_file}" 2>&1; then
            if "${DOCKER_BIN}" image inspect "${local_image}" \
                >/dev/null 2>&1; then
                printf '\nSaved local image: %s\n' "${local_image}" \
                    >>"${log_file}"
                build_outcome="ok"
            else
                printf '\nDocker reported a successful build, but local image %s was not found.\n' \
                    "${local_image}" >>"${log_file}"
                : >"${STATUS_DIR}/${instance_id}.failed"
                update_progress failed
                return 0
            fi
        else
            build_status=$?
            if [[ "${build_status}" -eq 124 || "${build_status}" -eq 137 ]]; then
                printf '\nBuild timed out after %s seconds.\n' \
                    "${BUILD_TIMEOUT_SECONDS}" >>"${log_file}"
            fi
            : >"${STATUS_DIR}/${instance_id}.failed"
            update_progress failed
            return 0
        fi
    fi

    if remote_image_exists "${remote_image}" "${log_file}"; then
        if "${DOCKER_BIN}" image inspect "${local_image}" >/dev/null 2>&1; then
            echo "Local image retained: ${local_image}" >>"${log_file}"
            : >"${STATUS_DIR}/${instance_id}.${build_outcome}"
            update_progress "${build_outcome}"
        else
            echo "Local image disappeared before upload check completed: ${local_image}" \
                >>"${log_file}"
            : >"${STATUS_DIR}/${instance_id}.failed"
            update_progress failed
        fi
    else
        remote_status=$?
        if [[ "${remote_status}" -eq 1 ]] \
            && push_image "${local_image}" "${remote_image}" "${log_file}" \
            && verify_remote_image "${remote_image}" "${log_file}"; then
            if "${DOCKER_BIN}" image inspect "${local_image}" >/dev/null 2>&1; then
                echo "Local image retained: ${local_image}" >>"${log_file}"
                : >"${STATUS_DIR}/${instance_id}.${build_outcome}"
                update_progress "${build_outcome}"
            else
                echo "Local image disappeared after push: ${local_image}" \
                    >>"${log_file}"
                : >"${STATUS_DIR}/${instance_id}.failed"
                update_progress failed
            fi
        else
            : >"${STATUS_DIR}/${instance_id}.failed"
            : >"${STATUS_DIR}/${instance_id}.push_failed"
            update_progress failed
        fi
    fi
}

docker_login
unset SWR_USERNAME SWR_PASSWORD

task_count="$(find "${TASK_ROOT}" -mindepth 1 -maxdepth 1 -type d -printf '.\n' | wc -l)"
if [[ "${task_count}" -eq 0 ]]; then
    echo "No task directories found under ${TASK_ROOT}" >&2
    exit 1
fi

TASK_COUNT="${task_count}"
START_TIME="$(date +%s)"
PROGRESS_STATE="${STATUS_DIR}/progress.state"
PROGRESS_LOCK="${STATUS_DIR}/progress.lock"
printf '0 0 0 0\n' >"${PROGRESS_STATE}"
: >"${PROGRESS_LOCK}"

export -f format_duration update_progress retry_delay is_missing_manifest_error
export -f remote_image_exists verify_remote_image push_image build_task
export LOG_DIR STATUS_DIR DOCKER_BIN TASK_COUNT PROGRESS_STATE PROGRESS_LOCK
export BUILD_TIMEOUT_SECONDS PUSH_TIMEOUT_SECONDS SWR_RETRIES
export SWR_REMOTE_REPOSITORY LOCAL_IMAGE_PREFIX
export PROGRESS_EVERY PROGRESS_WIDTH START_TIME

echo "Building ${task_count} task images from ${TASK_ROOT}"
echo "Parallel builds: ${JOBS}"
echo "Per-build timeout: ${BUILD_TIMEOUT_SECONDS} seconds"
echo "Per-push timeout: ${PUSH_TIMEOUT_SECONDS} seconds"
echo "SWR retries: ${SWR_RETRIES}"
echo "Local images: ${LOCAL_IMAGE_PREFIX}<instance_id>:latest"
echo "SWR destination: ${SWR_REMOTE_REPOSITORY}:<instance_id>"
echo "Build logs: ${LOG_DIR}"
echo "Build report: ${REPORT_FILE}"

set +e
find "${TASK_ROOT}" -mindepth 1 -maxdepth 1 -type d -print0 \
    | sort -z \
    | xargs -0 -r -n 1 -P "${JOBS}" bash -c 'build_task "$1"' _
xargs_status=$?
set -e

if [[ -t 1 ]]; then
    echo
fi

succeeded="$(find "${STATUS_DIR}" -type f -name '*.ok' | wc -l)"
skipped="$(find "${STATUS_DIR}" -type f -name '*.skipped' | wc -l)"
failed="$(find "${STATUS_DIR}" -type f -name '*.failed' | wc -l)"
push_failed="$(find "${STATUS_DIR}" -type f -name '*.push_failed' | wc -l)"
elapsed_seconds=$(($(date +%s) - START_TIME))
elapsed_text="$(format_duration "${elapsed_seconds}")"
failed_list="${STATUS_DIR}/failed-instances.txt"
push_failed_list="${STATUS_DIR}/push-failed-instances.txt"
find "${STATUS_DIR}" -type f -name '*.failed' -printf '%f\n' \
    | sed 's/\.failed$//' \
    | sort >"${failed_list}"
find "${STATUS_DIR}" -type f -name '*.push_failed' -printf '%f\n' \
    | sed 's/\.push_failed$//' \
    | sort >"${push_failed_list}"

{
    echo
    echo "Docker build report"
    echo "  Generated:  $(date --iso-8601=seconds)"
    echo "  Task root:  ${TASK_ROOT}"
    echo "  Total:      ${task_count}"
    echo "  Built and uploaded/present:    ${succeeded}"
    echo "  Existing locally and uploaded/present: ${skipped}"
    echo "  Failed:     ${failed}"
    echo "  Push failures: ${push_failed}"
    echo "  Timeout:    ${BUILD_TIMEOUT_SECONDS} seconds per build"
    echo "  Push timeout: ${PUSH_TIMEOUT_SECONDS} seconds per image"
    echo "  SWR retries: ${SWR_RETRIES}"
    echo "  Local images: ${LOCAL_IMAGE_PREFIX}<instance_id>:latest"
    echo "  SWR destination: ${SWR_REMOTE_REPOSITORY}:<instance_id>"
    echo "  Elapsed:    ${elapsed_text}"
    echo "  Log folder: ${LOG_DIR}"
    echo
    echo "Failed instances:"
    if [[ "${failed}" -gt 0 ]]; then
        sed 's/^/  /' "${failed_list}"
    else
        echo "  (none)"
    fi
    echo
    echo "SWR push failures:"
    if [[ "${push_failed}" -gt 0 ]]; then
        sed 's/^/  /' "${push_failed_list}"
    else
        echo "  (none)"
    fi
} | tee "${REPORT_FILE}"

if [[ "${xargs_status}" -ne 0 ]]; then
    echo "The parallel build runner exited with status ${xargs_status}." >&2
    exit "${xargs_status}"
fi
if [[ "${failed}" -gt 0 ]]; then
    echo "${failed} image build/upload operation(s) failed; see ${REPORT_FILE}." >&2
    exit 1
fi

[[ "${failed}" -eq 0 ]]
