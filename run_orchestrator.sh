#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd -- "${SCRIPT_DIR}"

# Network clients do not consistently agree on uppercase versus lowercase
# proxy variables. Mirror whichever form is present so uv, Python SDKs, git,
# Docker builds, and Slurm child jobs all inherit the same proxy configuration.
mirror_proxy_variable() {
    local upper_name="$1"
    local lower_name="$2"
    local proxy_value=""

    if [[ -v "${upper_name}" && -n "${!upper_name}" ]]; then
        proxy_value="${!upper_name}"
    elif [[ -v "${lower_name}" && -n "${!lower_name}" ]]; then
        proxy_value="${!lower_name}"
    else
        return 0
    fi

    printf -v "${upper_name}" '%s' "${proxy_value}"
    printf -v "${lower_name}" '%s' "${proxy_value}"
    export "${upper_name}" "${lower_name}"
}

mirror_proxy_variable HTTP_PROXY http_proxy
mirror_proxy_variable HTTPS_PROXY https_proxy
mirror_proxy_variable ALL_PROXY all_proxy
mirror_proxy_variable NO_PROXY no_proxy

# The restricted-network proxy re-signs HTTPS traffic with the Huawei proxy CA.
# Requests uses certifi instead of the operating-system trust store by default,
# so build a combined bundle and point the common Python, git, curl, pip, and
# Node TLS variables at it before starting uv or any worker process.
SYSTEM_CA_BUNDLE="${SWEGEN_SYSTEM_CA_BUNDLE:-/etc/ssl/certs/ca-certificates.crt}"
PROXY_CA_CERT="${SWEGEN_PROXY_CA_CERT:-${SWEGEN_PROXY_CA_BUNDLE:-${CA_CERT_REMOTE:-${SCRIPT_DIR}/src/swegen/assets/swegen-proxy-ca.crt}}}"
COMBINED_CA_BUNDLE="${SWEGEN_CA_BUNDLE_OUTPUT:-${SCRIPT_DIR}/.swegen/combined-ca-bundle.pem}"

if [[ ! -r "${SYSTEM_CA_BUNDLE}" ]]; then
    echo "System CA bundle is not readable: ${SYSTEM_CA_BUNDLE}" >&2
    exit 1
fi
if [[ ! -r "${PROXY_CA_CERT}" ]]; then
    echo "Huawei proxy CA certificate is not readable: ${PROXY_CA_CERT}" >&2
    exit 1
fi

mkdir -p -- "$(dirname -- "${COMBINED_CA_BUNDLE}")"
ca_bundle_tmp="$(mktemp "${COMBINED_CA_BUNDLE}.tmp.XXXXXX")"
cleanup_ca_bundle() {
    if [[ -n "${ca_bundle_tmp:-}" ]]; then
        rm -f -- "${ca_bundle_tmp}"
    fi
}
trap cleanup_ca_bundle EXIT
{
    cat -- "${SYSTEM_CA_BUNDLE}"
    printf '\n'
    cat -- "${PROXY_CA_CERT}"
    printf '\n'
} >"${ca_bundle_tmp}"
chmod 0644 "${ca_bundle_tmp}"
mv -f -- "${ca_bundle_tmp}" "${COMBINED_CA_BUNDLE}"
ca_bundle_tmp=""
trap - EXIT

export REQUESTS_CA_BUNDLE="${COMBINED_CA_BUNDLE}"
export SSL_CERT_FILE="${COMBINED_CA_BUNDLE}"
export CURL_CA_BUNDLE="${COMBINED_CA_BUNDLE}"
export GIT_SSL_CAINFO="${COMBINED_CA_BUNDLE}"
export PIP_CERT="${COMBINED_CA_BUNDLE}"
export NODE_EXTRA_CA_CERTS="${COMBINED_CA_BUNDLE}"
export UV_SYSTEM_CERTS=true

WORKERS="${WORKERS:-64}"
CC_TIMEOUT="${CC_TIMEOUT:-12800}"
TRANSIENT_ATTEMPTS="${TRANSIENT_ATTEMPTS:-3}"

# Additional orchestrator flags can be supplied directly to this script, for
# example: ./run_orchestrator.sh --slurm --include-obs-missing
exec uv run python src/orchestrator.py \
    --workers "${WORKERS}" \
    --cc-timeout "${CC_TIMEOUT}" \
    --transient-attempts "${TRANSIENT_ATTEMPTS}" \
    "$@"
