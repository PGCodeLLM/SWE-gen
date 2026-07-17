#!/bin/bash
set -euo pipefail
umask 0077

workspace=""
run_name=""
node_index=""
initial_delay=0
shard_dir=""
preflight=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workspace) workspace="$2"; shift 2 ;;
    --run-name) run_name="$2"; shift 2 ;;
    --node-index) node_index="$2"; shift 2 ;;
    --initial-delay) initial_delay="$2"; shift 2 ;;
    --shard-dir) shard_dir="$2"; shift 2 ;;
    --preflight) preflight=1; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$workspace" || -z "$run_name" || -z "$node_index" || -z "$shard_dir" ]]; then
  echo "--workspace, --run-name, --node-index, and --shard-dir are required" >&2
  exit 2
fi
if [[ "$node_index" != "1" && "$node_index" != "2" ]]; then
  echo "node index must be 1 or 2" >&2
  exit 2
fi
if ! [[ "$initial_delay" =~ ^[0-9]+$ ]]; then
  echo "initial delay must be a non-negative integer" >&2
  exit 2
fi

cd "$workspace"
export HOME="$workspace/.home"
export PATH="$workspace/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
mkdir -p "$HOME" "runs/$run_name"

route_env() {
  case "$1" in
    sg) printf '%s\n' "$workspace/.slurm-secrets/env/.env" ;;
    hk) printf '%s\n' "$workspace/.slurm-secrets/env/.env_hk" ;;
    de) printf '%s\n' "$workspace/.slurm-secrets/env/.env_de" ;;
    *) return 2 ;;
  esac
}

normalize_proxy_env() {
  local proxy_url env_no_proxy internal_no_proxy
  proxy_url="${HTTPS_PROXY:-${https_proxy:-${HTTP_PROXY:-${http_proxy:-}}}}"
  [[ -n "$proxy_url" ]] || return 1
  export http_proxy="${http_proxy:-$proxy_url}"
  export https_proxy="${https_proxy:-$proxy_url}"
  export HTTP_PROXY="${HTTP_PROXY:-$http_proxy}"
  export HTTPS_PROXY="${HTTPS_PROXY:-$https_proxy}"
  unset all_proxy ALL_PROXY
  env_no_proxy="${no_proxy:-${NO_PROXY:-}}"
  internal_no_proxy="*.huaweicloud.com,100.*,10.*,.huawei.com,127.0.0.1,7.244.3.251,10.170.22.223,10.170.22.98"
  export no_proxy="${env_no_proxy:+$env_no_proxy,}$internal_no_proxy"
  export NO_PROXY="$no_proxy"
}

write_docker_proxy_config() {
  local destination="$1"
  install -d -m 0700 "$destination"
  jq -n \
    '{proxies:{default:{httpProxy:env.HTTP_PROXY,httpsProxy:env.HTTPS_PROXY,noProxy:env.NO_PROXY}}}' \
    >"$destination/config.json"
  chmod 0600 "$destination/config.json"
}

probe_arcyleung() {
  SWEGEN_PREFLIGHT_CA_BUNDLE="$1" .venv/bin/python - <<'PY'
import os

import requests

try:
    response = requests.get(
        "https://arcyleung-ubuntu.tailb940e6.ts.net/v1/models",
        headers={"Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}"},
        timeout=(15, 45),
        verify=os.environ["SWEGEN_PREFLIGHT_CA_BUNDLE"],
    )
except requests.RequestException:
    print("000")
else:
    print(response.status_code)
PY
}

probe_github_quota() {
  SWEGEN_PREFLIGHT_CA_BUNDLE="$1" .venv/bin/python - <<'PY'
import os
import sys

import requests

from swegen.model_settings import load_github_tokens

tokens = load_github_tokens()
ca_bundle = os.environ["SWEGEN_PREFLIGHT_CA_BUNDLE"]
for token in tokens:
    try:
        response = requests.get(
            "https://api.github.com/rate_limit",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=(15, 45),
            verify=ca_bundle,
        )
    except requests.RequestException:
        continue
    if response.status_code != 200:
        continue
    remaining = response.json().get("resources", {}).get("core", {}).get("remaining", 0)
    if isinstance(remaining, int) and remaining > 0:
        print(len(tokens), remaining)
        raise SystemExit(0)

print("no configured GitHub token is authenticated with remaining core quota", file=sys.stderr)
raise SystemExit(1)
PY
}

probe_docker_build_proxy() {
  local route="$1" docker_config="$2" image_id
  image_id=$(
    DOCKER_CONFIG="$docker_config" docker build --quiet --no-cache \
      --build-arg "SWEGEN_PREFLIGHT_ROUTE=$route" - <<'EOF'
FROM ubuntu:24.04
ARG SWEGEN_PREFLIGHT_ROUTE
RUN test -n "$SWEGEN_PREFLIGHT_ROUTE" && apt-get update >/dev/null
EOF
  )
  [[ -n "$image_id" ]] || return 1
  docker image rm --force "$image_id" >/dev/null 2>&1 || true
}

run_preflight() {
  local command_name route env_file proxy api_key status attempt ca_bundle
  local github_status github_tokens github_remaining docker_config
  for command_name in bash curl docker git jq; do
    command -v "$command_name" >/dev/null || {
      echo "missing command on $(hostname): $command_name" >&2
      return 1
    }
  done
  [[ -x .venv/bin/python ]] || { echo "missing node-local .venv" >&2; return 1; }
  [[ -s "$workspace/.slurm-secrets/credentials.env" ]] || {
    echo "missing private runtime credentials" >&2
    return 1
  }
  ca_bundle="$workspace/.slurm-secrets/combined-ca.crt"
  [[ -s "$ca_bundle" ]] || { echo "missing combined CA bundle" >&2; return 1; }
  export SWEGEN_CONFIG="$workspace/.slurm-secrets/swegen.toml"
  [[ -s "$SWEGEN_CONFIG" ]] || { echo "missing private SWE-gen config" >&2; return 1; }
  .venv/bin/python -c 'import claude_agent_sdk, harbor, swegen' >/dev/null
  docker info >/dev/null
  github_tokens=0
  github_remaining=0

  for route in sg hk de; do
    env_file=$(route_env "$route")
    [[ -s "$env_file" ]] || { echo "missing $route environment file" >&2; return 1; }
    set -a
    # shellcheck disable=SC1090
    source "$env_file"
    source "$workspace/.slurm-secrets/credentials.env"
    set +a
    normalize_proxy_env || {
      echo "$route route lacks proxy configuration" >&2
      return 1
    }
    proxy="${HTTPS_PROXY:-${https_proxy:-${HTTP_PROXY:-${http_proxy:-}}}}"
    api_key="${OPENAI_API_KEY:-}"
    [[ -n "$proxy" && -n "$api_key" ]] || {
      echo "$route route lacks proxy/API configuration" >&2
      return 1
    }
    status="000"
    for attempt in 1 2 3; do
      status=$(probe_arcyleung "$ca_bundle" || true)
      [[ "$status" == "200" ]] && break
      sleep "$attempt"
    done
    if [[ "$status" != "200" ]]; then
      echo "$route Arcyleung preflight failed with HTTP $status" >&2
      return 1
    fi
    if [[ "$route" == "sg" ]]; then
      github_status=$(probe_github_quota "$ca_bundle") || return 1
      read -r github_tokens github_remaining <<<"$github_status"
      if ! [[ "$github_tokens" =~ ^[1-9][0-9]*$ ]] \
        || ! [[ "$github_remaining" =~ ^[1-9][0-9]*$ ]]; then
        echo "invalid GitHub token quota preflight result" >&2
        return 1
      fi
    fi
    GIT_SSL_CAINFO="$ca_bundle" git ls-remote \
      https://github.com/octocat/Hello-World.git HEAD >/dev/null
    docker_config="$workspace/.slurm-secrets/preflight-docker/$route"
    write_docker_proxy_config "$docker_config"
    probe_docker_build_proxy "$route" "$docker_config"
    find "$docker_config" -maxdepth 1 -type f -name 'config.json*' -delete
    echo "preflight route=${route^^} endpoint=arcyleung-ubuntu status=200 docker_build_proxy=ok"
    unset OPENAI_API_KEY ANTHROPIC_API_KEY ANTHROPIC_AUTH_TOKEN GITHUB_TOKEN
    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy
    unset no_proxy NO_PROXY
  done
  echo "preflight node=$(hostname) docker=ok runtime=ok github_tokens=$github_tokens github_remaining=$github_remaining"
}

if [[ "$preflight" == "1" ]]; then
  run_preflight
  exit 0
fi

run_preflight

set -a
# shellcheck disable=SC1091
source "$workspace/.slurm-secrets/credentials.env"
set +a

if (( initial_delay > 0 )); then
  echo "initial stagger: ${initial_delay}s"
  sleep "$initial_delay"
fi

routes=(sg hk de sg hk de)
suffixes=(a a a b b b)
pids=()
groups=()

# Give every orchestrator its own token-pool file. All six processes are
# started immediately so they can load and delete these files before any
# Claude session begins; processing remains staggered inside orchestrator.py.
master_config="$workspace/.slurm-secrets/swegen.toml"
[[ -s "$master_config" ]] || { echo "missing private SWE-gen config" >&2; exit 1; }
for index in "${!routes[@]}"; do
  route="${routes[$index]}"
  suffix="${suffixes[$index]}"
  shard_name="r6-${route}-n${node_index}-${suffix}"
  config_dir="$workspace/.slurm-secrets/config/$shard_name"
  install -d -m 0700 "$config_dir"
  cp "$master_config" "$config_dir/swegen.toml"
  chmod 0600 "$config_dir/swegen.toml"
done
find "$master_config" -maxdepth 0 -type f -delete

terminate_children() {
  local pid
  trap - TERM INT
  for pid in "${pids[@]:-}"; do
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
  done
  wait || true
}
trap terminate_children TERM INT

for index in "${!routes[@]}"; do
  route="${routes[$index]}"
  suffix="${suffixes[$index]}"
  shard_name="r6-${route}-n${node_index}-${suffix}"
  input="$workspace/$shard_dir/${shard_name}.jsonl"
  env_file=$(route_env "$route")
  log_dir="$workspace/runs/$run_name/orchestrator-logs-${route}-n${node_index}-${suffix}-r6-4w"
  launch_log="$workspace/runs/$run_name/orchestrator-${route}-n${node_index}-${suffix}-r6-4w-launch.log"
  progress="$workspace/runs/$run_name/orchestrator-progress-${route}-n${node_index}-${suffix}-r6.jsonl"
  status="$workspace/runs/$run_name/orchestrator-instance-status-${route}-n${node_index}-${suffix}-r6.jsonl"
  [[ -s "$input" ]] || { echo "missing shard: $input" >&2; terminate_children; exit 1; }
  mkdir -p "$log_dir"

  echo "launching $shard_name with 4 workers via ${route^^}"
  setsid env \
    SWEGEN_PROXY_ENV_FILE="$env_file" \
    SWEGEN_WORKERS=4 \
    SWEGEN_RUN_NAME="$run_name" \
    SWEGEN_INPUT_JSONL="$input" \
    SWEGEN_ORCHESTRATOR_LOG_DIR="$log_dir" \
    SWEGEN_PROGRESS_JSONL="$progress" \
    SWEGEN_INSTANCE_STATUS_JSONL="$status" \
    SWEGEN_CONFIG="$workspace/.slurm-secrets/config/$shard_name/swegen.toml" \
    SWEGEN_DELETE_CONFIG_AFTER_LOAD=1 \
    SWEGEN_ORCHESTRATOR_START_DELAY_SECONDS="$((index * 60))" \
    SWEGEN_DOCKER_CONFIG_DIR="$workspace/.slurm-secrets/docker/$shard_name" \
    SWEGEN_CLAUDE_CONFIG_DIR="$workspace/.slurm-secrets/claude/$shard_name" \
    SWEGEN_SLURM_NODE="${SLURMD_NODENAME:-$(hostname)}" \
    SWEGEN_SLURM_ROUTE="${route^^}" \
    SWEGEN_SLURM_GROUP="$shard_name" \
    bash "$workspace/run_orchestrator.sh" >"$launch_log" 2>&1 &
  pids+=("$!")
  groups+=("$shard_name")
done

rc=0
for index in "${!pids[@]}"; do
  if wait "${pids[$index]}"; then
    echo "completed ${groups[$index]}"
  else
    child_rc=$?
    echo "failed ${groups[$index]} rc=$child_rc" >&2
    rc=1
  fi
done
exit "$rc"
