#!/usr/bin/env bash
set -euo pipefail

namespace="${SWEGEN_K3S_NAMESPACE:-swegen-pipeline}"
model_secret_name="${SWEGEN_MODEL_SECRET_NAME:-swegen-model-credentials-v2}"
generate_glm_secret_name="${SWEGEN_GENERATE_GLM_SECRET_NAME:-swegen-model-credentials-glm52-thinking-npu-20260804}"
reward_model_secret_name="${SWEGEN_REWARD_MODEL_SECRET_NAME:-swegen-reward-credentials-gpt56sol-20260803}"
script_directory="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_directory}/../.." && pwd)"
runtime_root="${SWEGEN_RUNTIME_ROOT:-/data/work/slurm-swegen/slurm-runtime/20260716-sol-max-full-16w/workspace}"
secret_root="${SWEGEN_SECRET_ROOT:-${runtime_root}/.slurm-secrets}"
proxy_env="${SWEGEN_PROXY_ENV:-/data/work/slurm-swegen/.env}"
docker_config="${SWEGEN_DOCKER_CONFIG:-/root/.docker/config.json}"
remote_buildkit_registry="${SWEGEN_REMOTE_BUILDKIT_REGISTRY:-swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com}"
models_yaml="${SWEGEN_MODELS_YAML:-/data/work/alex/SWE-gen/models.yaml}"

credentials_env="${secret_root}/credentials.env"
reward_env="${secret_root}/reward-credentials.env"
swegen_config="${secret_root}/swegen.toml"
combined_ca="${secret_root}/combined-ca.crt"

for required_file in \
    "${credentials_env}" \
    "${reward_env}" \
    "${swegen_config}" \
    "${combined_ca}" \
    "${proxy_env}" \
    "${docker_config}" \
    "${models_yaml}"
do
    if [[ ! -r "${required_file}" ]]; then
        printf 'Required secret source is not readable: %s\n' "${required_file}" >&2
        exit 1
    fi
done

umask 077
temporary_directory="$(mktemp -d)"
cleanup() {
    rm -rf -- "${temporary_directory}"
}
trap cleanup EXIT

normalize_env_file() {
    local source_file="$1"
    local destination_file="$2"
    sed -E 's/^export[[:space:]]+//' "${source_file}" > "${destination_file}"
    chmod 0600 "${destination_file}"
}

normalized_credentials="${temporary_directory}/credentials.env"
normalized_proxy="${temporary_directory}/proxy.env"
normalized_reward="${temporary_directory}/reward.env"
repair_model_env="${temporary_directory}/repair-model.env"
reward_model_env="${temporary_directory}/reward-model.env"
merged_docker_config="${temporary_directory}/docker-config.json"
normalize_env_file "${credentials_env}" "${normalized_credentials}"
normalize_env_file "${proxy_env}" "${normalized_proxy}"
normalize_env_file "${reward_env}" "${normalized_reward}"

# Model credentials have their own immutable Secret. Keep them out of the
# generic proxy Secret so envFrom ordering cannot silently replace the model
# endpoint or API key in newly deployed workers.
python3 - "${normalized_proxy}" <<'PY'
import os
import sys
from pathlib import Path

path = Path(sys.argv[1])
blocked_prefixes = ("ANTHROPIC_", "CLAUDE_", "OPENAI_", "SWEGEN_CLAUDE_")
kept: list[str] = []
for raw_line in path.read_text(encoding="utf-8").splitlines():
    stripped = raw_line.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        kept.append(raw_line)
        continue
    key = stripped.split("=", 1)[0].strip()
    if key.startswith(blocked_prefixes):
        continue
    kept.append(raw_line)
path.write_text("\n".join(kept) + "\n", encoding="utf-8")
os.chmod(path, 0o600)
PY

python3 - "${docker_config}" "${normalized_proxy}" "${remote_buildkit_registry}" <<'PY'
import base64
import json
import os
import sys
from pathlib import Path

docker_config_path = Path(sys.argv[1])
proxy_env_path = Path(sys.argv[2])
registry = sys.argv[3].strip().rstrip("/")
config = json.loads(docker_config_path.read_text(encoding="utf-8"))
auths = config.get("auths") if isinstance(config, dict) else None
entry = auths.get(registry) if isinstance(auths, dict) else None
encoded = entry.get("auth") if isinstance(entry, dict) else None
if not isinstance(encoded, str) or not encoded:
    raise SystemExit(f"Docker config has no inline auth for {registry}")
try:
    decoded = base64.b64decode(encoded, validate=True).decode("utf-8")
    username, password = decoded.split(":", 1)
except (ValueError, UnicodeDecodeError) as error:
    raise SystemExit(f"Docker config auth for {registry} is invalid") from error
if not username or not password or any(character in username + password for character in "\r\n\0"):
    raise SystemExit(f"Docker config auth for {registry} is incomplete or unsafe")
values = {
    "SWEGEN_REMOTE_BUILDKIT_REGISTRY_USERNAME": username,
    "SWEGEN_REMOTE_BUILDKIT_REGISTRY_PASSWORD": password,
    "SWEGEN_REMOTE_BUILDKIT_PULL_USERNAME": username,
    "SWEGEN_REMOTE_BUILDKIT_PULL_PASSWORD": password,
}
with proxy_env_path.open("a", encoding="utf-8") as destination:
    for key, value in values.items():
        destination.write(f"{key}={value}\n")
os.chmod(proxy_env_path, 0o600)
PY

uv run --project "${repo_root}" python - "${models_yaml}" "${repair_model_env}" <<'PY'
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit

import yaml

source, destination = map(Path, sys.argv[1:])
document = yaml.safe_load(source.read_text(encoding="utf-8"))
entries = document.get("model_list") if isinstance(document, dict) else None
model = os.environ.get("SWEGEN_REPAIR_MODEL_NAME", "glm-5.2-moedsa-512k-thinking").strip()
if not model:
    raise SystemExit("SWEGEN_REPAIR_MODEL_NAME must not be blank")
matches = [
    entry for entry in entries or []
    if isinstance(entry, dict) and entry.get("model_name") == model
]
if len(matches) != 1:
    raise SystemExit(f"models.yaml must contain exactly one {model} entry")
params = matches[0].get("litellm_params")
if not isinstance(params, dict):
    raise SystemExit(f"{model} requires litellm_params")
api_base = str(params.get("api_base") or "").rstrip("/")
api_key = str(params.get("api_key") or "")
parsed = urlsplit(api_base)
if parsed.scheme not in {"http", "https"} or not parsed.netloc or not api_key:
    raise SystemExit(f"{model} requires a valid api_base and non-empty api_key")
anthropic_base = api_base.removesuffix("/v1")
values = {
    "OPENAI_API_KEY": api_key,
    "OPENAI_BASE_URL": api_base,
    "OPENAI_MODEL": model,
    "ANTHROPIC_API_KEY": api_key,
    "ANTHROPIC_AUTH_TOKEN": api_key,
    "ANTHROPIC_BASE_URL": anthropic_base,
    "ANTHROPIC_MODEL": model,
    "ANTHROPIC_DEFAULT_OPUS_MODEL": model,
    "ANTHROPIC_DEFAULT_SONNET_MODEL": model,
    "ANTHROPIC_DEFAULT_HAIKU_MODEL": model,
    "ANTHROPIC_SMALL_FAST_MODEL": model,
    "SWEGEN_CLAUDE_FAST_MODEL": model,
    "CLAUDE_CODE_MAX_CONTEXT_TOKENS": os.environ.get(
        "CLAUDE_CODE_MAX_CONTEXT_TOKENS", "160000"
    ),
    "CLAUDE_CODE_AUTO_COMPACT_WINDOW": os.environ.get(
        "CLAUDE_CODE_AUTO_COMPACT_WINDOW", "150000"
    ),
}
destination.write_text(
    "".join(f"{key}={value}\n" for key, value in values.items()),
    encoding="utf-8",
)
os.chmod(destination, 0o600)
PY

uv run --project "${repo_root}" python - "${models_yaml}" "${reward_model_env}" <<'PY'
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit

import yaml

source, destination = map(Path, sys.argv[1:])
document = yaml.safe_load(source.read_text(encoding="utf-8"))
entries = document.get("model_list") if isinstance(document, dict) else None
model = os.environ.get("SWEGEN_REWARD_MODEL_NAME", "gpt-5.6-sol").strip()
if not model:
    raise SystemExit("SWEGEN_REWARD_MODEL_NAME must not be blank")
matches = [
    entry for entry in entries or []
    if isinstance(entry, dict) and entry.get("model_name") == model
]
if len(matches) != 1:
    raise SystemExit(f"models.yaml must contain exactly one {model} entry")
params = matches[0].get("litellm_params")
if not isinstance(params, dict):
    raise SystemExit(f"{model} requires litellm_params")
api_base = str(params.get("api_base") or "").rstrip("/")
api_key = str(params.get("api_key") or "")
parsed = urlsplit(api_base)
if parsed.scheme not in {"http", "https"} or not parsed.netloc or not api_key:
    raise SystemExit(f"{model} requires a valid api_base and non-empty api_key")
destination.write_text(f"SWEGEN_REWARD_API_KEY={api_key}\n", encoding="utf-8")
os.chmod(destination, 0o600)
PY

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


def first_value(*names: str) -> str:
    return next((proxy_env[name] for name in names if proxy_env.get(name)), "")


http_proxy = first_value("HTTP_PROXY", "http_proxy")
https_proxy = first_value("HTTPS_PROXY", "https_proxy")
no_proxy = first_value("NO_PROXY", "no_proxy")
if not http_proxy or not https_proxy:
    raise SystemExit("Proxy env must define both HTTP_PROXY and HTTPS_PROXY")

default_proxy = {
    "httpProxy": http_proxy,
    "httpsProxy": https_proxy,
}
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

if [[ "$(id -u)" -eq 0 ]]; then
    kubectl=(k3s kubectl)
else
    kubectl=(sudo k3s kubectl)
fi

ensure_immutable_env_secret() {
    local secret_name="$1"
    local env_file="$2"
    local desired_json="${temporary_directory}/${secret_name}.desired.json"
    local existing_json="${temporary_directory}/${secret_name}.existing.json"

    "${kubectl[@]}" -n "${namespace}" create secret generic "${secret_name}" \
        --from-env-file="${env_file}" \
        --dry-run=client -o json > "${desired_json}"
    python3 - "${desired_json}" <<'PY'
import json
import os
import sys
from pathlib import Path

path = Path(sys.argv[1])
document = json.loads(path.read_text(encoding="utf-8"))
document["immutable"] = True
metadata = document.setdefault("metadata", {})
metadata["labels"] = {
    **metadata.get("labels", {}),
    "app.kubernetes.io/managed-by": "swegen-platform",
    "swegen.pgcode/credential-scope": "model",
}
path.write_text(json.dumps(document, separators=(",", ":")), encoding="utf-8")
os.chmod(path, 0o600)
PY

    if "${kubectl[@]}" -n "${namespace}" get secret "${secret_name}" \
        -o json > "${existing_json}" 2>/dev/null
    then
        python3 - "${existing_json}" "${desired_json}" "${secret_name}" <<'PY'
import json
import sys
from pathlib import Path

existing_path, desired_path = map(Path, sys.argv[1:3])
secret_name = sys.argv[3]
existing = json.loads(existing_path.read_text(encoding="utf-8"))
desired = json.loads(desired_path.read_text(encoding="utf-8"))
existing_data = existing.get("data", {})
desired_data = desired.get("data", {})
if existing_data != desired_data:
    differing = sorted(
        key
        for key in set(existing_data) | set(desired_data)
        if existing_data.get(key) != desired_data.get(key)
    )
    raise SystemExit(
        f"Immutable Secret {secret_name} differs for keys: {', '.join(differing)}. "
        "Use the documented credential-rotation procedure instead of overwriting it."
    )
PY
        "${kubectl[@]}" -n "${namespace}" patch secret "${secret_name}" \
            --type=merge \
            -p '{"metadata":{"labels":{"app.kubernetes.io/managed-by":"swegen-platform","swegen.pgcode/credential-scope":"model"},"annotations":{"kubectl.kubernetes.io/last-applied-configuration":null}},"immutable":true}'
    else
        "${kubectl[@]}" -n "${namespace}" create -f "${desired_json}"
    fi
}

"${kubectl[@]}" create namespace "${namespace}" \
    --dry-run=client -o yaml | "${kubectl[@]}" apply -f -

ensure_immutable_env_secret "${model_secret_name}" "${normalized_credentials}"
ensure_immutable_env_secret "${generate_glm_secret_name}" "${repair_model_env}"
ensure_immutable_env_secret "${reward_model_secret_name}" "${reward_model_env}"

"${kubectl[@]}" -n "${namespace}" create secret generic swegen-repair-model-credentials \
    --from-env-file="${repair_model_env}" \
    --dry-run=client -o yaml | "${kubectl[@]}" apply -f -

"${kubectl[@]}" -n "${namespace}" create secret generic swegen-runtime-proxy \
    --from-env-file="${normalized_proxy}" \
    --dry-run=client -o yaml | "${kubectl[@]}" apply -f -

"${kubectl[@]}" -n "${namespace}" create secret generic swegen-reward-credentials \
    --from-env-file="${normalized_reward}" \
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
printf 'SWEGEN_PG_PASSWORD=%s\n' "${postgres_password}" > "${password_file}"
unset postgres_password

"${kubectl[@]}" -n "${namespace}" create secret generic swegen-database \
    --from-env-file="${password_file}" \
    --dry-run=client -o yaml | "${kubectl[@]}" apply -f -

"${kubectl[@]}" -n "${namespace}" get secret \
    "${model_secret_name}" \
    "${generate_glm_secret_name}" \
    "${reward_model_secret_name}" \
    swegen-repair-model-credentials \
    swegen-runtime-proxy \
    swegen-reward-credentials \
    swegen-private-files \
    swegen-docker-config \
    swegen-database \
    -o name
