#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd -- "${script_dir}/../.." && pwd)"
swegen_config="${SWEGEN_CONFIG_SOURCE:-${repository_root}/swegen.toml}"
secret_root="${SWEGEN_SECRET_ROOT:-${repository_root}/.secrets}"
proxy_env="${SWEGEN_PROXY_ENV:-${secret_root}/proxy.env}"
docker_config="${SWEGEN_DOCKER_CONFIG:-/root/.docker/config.json}"
combined_ca="${secret_root}/combined-ca.crt"

if [[ ! -r "${swegen_config}" ]]; then
    printf 'SWE-gen configuration is not readable: %s\n' "${swegen_config}" >&2
    exit 1
fi

mapfile -d '' -t namespace_settings < <(
    python3 - "${swegen_config}" <<'PY'
import sys
import tomllib
from pathlib import Path

with Path(sys.argv[1]).open("rb") as handle:
    pipeline = tomllib.load(handle).get("pipeline", {})
if not isinstance(pipeline, dict):
    raise SystemExit("[pipeline] must be a TOML table")
for key in ("namespace", "secret_source_namespace"):
    value = pipeline.get(key)
    if not isinstance(value, str) or not value.strip():
        raise SystemExit(f"[pipeline].{key} must be configured")
    sys.stdout.write(value.strip())
    sys.stdout.write("\0")
PY
)
if [[ "${#namespace_settings[@]}" -ne 2 ]]; then
    echo "Could not load pipeline namespaces from ${swegen_config}." >&2
    exit 1
fi
namespace="${namespace_settings[0]}"
source_namespace="${namespace_settings[1]}"
unset namespace_settings

if [[ -n "${SWEGEN_KUBECTL:-}" ]]; then
    read -r -a kubectl <<<"${SWEGEN_KUBECTL}"
elif command -v kubectl >/dev/null 2>&1; then
    kubectl=(kubectl)
elif command -v k3s >/dev/null 2>&1 && [[ "$(id -u)" -eq 0 ]]; then
    kubectl=(k3s kubectl)
elif command -v k3s >/dev/null 2>&1 && command -v sudo >/dev/null 2>&1; then
    kubectl=(sudo k3s kubectl)
else
    echo "Could not find kubectl or k3s. Set SWEGEN_KUBECTL explicitly." >&2
    exit 1
fi

umask 077
temporary_directory="$(mktemp -d)"
cleanup() {
    rm -rf -- "${temporary_directory}"
}
trap cleanup EXIT

"${kubectl[@]}" create namespace "${namespace}" \
    --dry-run=client -o yaml | "${kubectl[@]}" apply -f -

required_secret_names=(
    swegen-runtime-proxy
    swegen-private-files
    swegen-docker-config
    swegen-database
)

source_secrets_exist() {
    local secret_name
    for secret_name in "${required_secret_names[@]}"; do
        if ! "${kubectl[@]}" -n "${source_namespace}" get "secret/${secret_name}" \
            >/dev/null 2>&1; then
            return 1
        fi
    done
}

clone_secret() {
    local secret_name="$1"
    "${kubectl[@]}" -n "${source_namespace}" get "secret/${secret_name}" -o json \
        | python3 -c '
import json
import sys

target_namespace = sys.argv[1]
source = json.load(sys.stdin)
secret = {
    "apiVersion": source.get("apiVersion", "v1"),
    "kind": source.get("kind", "Secret"),
    "metadata": {
        "name": source["metadata"]["name"],
        "namespace": target_namespace,
    },
}
for key in ("type", "data", "stringData", "immutable"):
    if key in source:
        secret[key] = source[key]
json.dump(secret, sys.stdout, separators=(",", ":"))
' "${namespace}" \
        | "${kubectl[@]}" apply -f -
}

patch_config_secret() {
    local patch_file="${temporary_directory}/swegen-private-files-patch.json"
    python3 - "${swegen_config}" "${patch_file}" <<'PY'
import base64
import json
import sys
from pathlib import Path

config_path = Path(sys.argv[1])
patch_path = Path(sys.argv[2])
patch_path.write_text(
    json.dumps(
        {"data": {"swegen.toml": base64.b64encode(config_path.read_bytes()).decode("ascii")}},
        separators=(",", ":"),
    ),
    encoding="utf-8",
)
patch_path.chmod(0o600)
PY
    "${kubectl[@]}" -n "${namespace}" patch secret/swegen-private-files \
        --type=merge --patch-file "${patch_file}" >/dev/null
}

local_sources_available=true
for required_file in "${combined_ca}" "${proxy_env}" "${docker_config}"; do
    if [[ ! -r "${required_file}" ]]; then
        local_sources_available=false
    fi
done

if [[ "${local_sources_available}" == "false" ]]; then
    if [[ "${namespace}" == "${source_namespace}" ]] || ! source_secrets_exist; then
        printf '%s\n' \
            "Local proxy/CA/Docker secret sources are incomplete and the source namespace" \
            "${source_namespace} does not contain all required SWE-gen Secrets." >&2
        exit 1
    fi
    for secret_name in "${required_secret_names[@]}"; do
        clone_secret "${secret_name}"
    done
    patch_config_secret
else
    normalize_env_file() {
        local source_file="$1"
        local destination_file="$2"
        sed -E 's/^export[[:space:]]+//' "${source_file}" >"${destination_file}"
        chmod 0600 "${destination_file}"
    }

    normalized_proxy="${temporary_directory}/proxy.env"
    merged_docker_config="${temporary_directory}/docker-config.json"
    normalize_env_file "${proxy_env}" "${normalized_proxy}"

    python3 - "${docker_config}" "${normalized_proxy}" "${merged_docker_config}" <<'PY'
import json
import sys
from pathlib import Path

docker_config_path, proxy_env_path, destination_path = map(Path, sys.argv[1:])
config = json.loads(docker_config_path.read_text(encoding="utf-8"))
if not isinstance(config, dict):
    raise SystemExit("Docker config must contain a JSON object")

proxy_env: dict[str, str] = {}
for raw_line in proxy_env_path.read_text(encoding="utf-8").splitlines():
    line = raw_line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    key, value = line.split("=", 1)
    proxy_env[key.strip()] = value.strip()

http_proxy = proxy_env.get("HTTP_PROXY") or proxy_env.get("http_proxy")
https_proxy = proxy_env.get("HTTPS_PROXY") or proxy_env.get("https_proxy")
no_proxy = proxy_env.get("NO_PROXY") or proxy_env.get("no_proxy")
if not http_proxy or not https_proxy:
    raise SystemExit("Proxy env must define both HTTP_PROXY and HTTPS_PROXY")

default_proxy = {"httpProxy": http_proxy, "httpsProxy": https_proxy}
if no_proxy:
    default_proxy["noProxy"] = no_proxy
proxies = config.setdefault("proxies", {})
if not isinstance(proxies, dict):
    raise SystemExit("Docker config proxies entry must contain a JSON object")
proxies["default"] = default_proxy
destination_path.write_text(
    json.dumps(config, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
PY
    chmod 0600 "${merged_docker_config}"

    "${kubectl[@]}" -n "${namespace}" create secret generic swegen-runtime-proxy \
        --from-env-file="${normalized_proxy}" \
        --dry-run=client -o yaml | "${kubectl[@]}" apply -f -
    "${kubectl[@]}" -n "${namespace}" create secret generic swegen-private-files \
        --from-file=swegen.toml="${swegen_config}" \
        --from-file=combined-ca.crt="${combined_ca}" \
        --dry-run=client -o yaml | "${kubectl[@]}" apply -f -
    "${kubectl[@]}" -n "${namespace}" create secret generic swegen-docker-config \
        --from-file=config.json="${merged_docker_config}" \
        --dry-run=client -o yaml | "${kubectl[@]}" apply -f -

    postgres_password="${SWEGEN_PG_PASSWORD:-}"
    if [[ -z "${postgres_password}" ]]; then
        read -r -s -p "PostgreSQL password: " postgres_password
        printf '\n' >&2
    fi
    if [[ -z "${postgres_password}" ]]; then
        printf 'PostgreSQL password must not be empty.\n' >&2
        exit 1
    fi
    password_file="${temporary_directory}/database.env"
    printf 'SWEGEN_PG_PASSWORD=%s\n' "${postgres_password}" >"${password_file}"
    unset postgres_password
    "${kubectl[@]}" -n "${namespace}" create secret generic swegen-database \
        --from-env-file="${password_file}" \
        --dry-run=client -o yaml | "${kubectl[@]}" apply -f -
fi

"${kubectl[@]}" -n "${namespace}" get secret "${required_secret_names[@]}" -o name
