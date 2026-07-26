#!/bin/bash
set -euo pipefail
umask 0077

workspace=""
run_name=""
node_index=""
initial_delay=0
shard_dir=""
groups_per_route=3
workers_per_group=4
route_workers=""
shard_revision="r6"
preserve_master_config=0
preflight=0
skip_preflight=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workspace) workspace="$2"; shift 2 ;;
    --run-name) run_name="$2"; shift 2 ;;
    --node-index) node_index="$2"; shift 2 ;;
    --initial-delay) initial_delay="$2"; shift 2 ;;
    --shard-dir) shard_dir="$2"; shift 2 ;;
    --groups-per-route) groups_per_route="$2"; shift 2 ;;
    --workers-per-group) workers_per_group="$2"; shift 2 ;;
    --route-workers) route_workers="$2"; shift 2 ;;
    --shard-revision) shard_revision="$2"; shift 2 ;;
    --preserve-master-config) preserve_master_config=1; shift ;;
    --preflight) preflight=1; shift ;;
    --skip-preflight) skip_preflight=1; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$workspace" || -z "$run_name" || -z "$node_index" || -z "$shard_dir" ]]; then
  echo "--workspace, --run-name, --node-index, and --shard-dir are required" >&2
  exit 2
fi
if ! [[ "$node_index" =~ ^[1-9][0-9]*$ ]]; then
  echo "node index must be a positive integer" >&2
  exit 2
fi
if ! [[ "$initial_delay" =~ ^[0-9]+$ ]]; then
  echo "initial delay must be a non-negative integer" >&2
  exit 2
fi
if [[ "$groups_per_route" != "1" && "$groups_per_route" != "2" && "$groups_per_route" != "3" ]]; then
  echo "groups per route must be 1, 2, or 3" >&2
  exit 2
fi
if [[ "$workers_per_group" != "1" && "$workers_per_group" != "2" && "$workers_per_group" != "3" && "$workers_per_group" != "4" ]]; then
  echo "workers per group must be 1, 2, 3, or 4" >&2
  exit 2
fi
if [[ -n "$route_workers" ]]; then
  IFS=',' read -r sg_workers hk_workers de_workers extra_workers <<<"$route_workers"
  if [[ -n "${extra_workers:-}" || -z "${sg_workers:-}" || -z "${hk_workers:-}" || -z "${de_workers:-}" ]]; then
    echo "route workers must contain exactly SG,HK,DE counts" >&2
    exit 2
  fi
  for route_count in "$sg_workers" "$hk_workers" "$de_workers"; do
    if ! [[ "$route_count" =~ ^[0-9]+$ ]] || (( route_count > 12 )); then
      echo "each route worker count must be between 0 and 12" >&2
      exit 2
    fi
  done
  if (( sg_workers + hk_workers + de_workers == 0 )); then
    echo "at least one route must have a worker" >&2
    exit 2
  fi
fi
if ! [[ "$shard_revision" =~ ^r[0-9]+$ ]]; then
  echo "shard revision must look like r7" >&2
  exit 2
fi
if [[ "$preflight" == "1" && "$skip_preflight" == "1" ]]; then
  echo "--preflight and --skip-preflight are mutually exclusive" >&2
  exit 2
fi

cd "$workspace"
export HOME="$workspace/.home"
export PATH="$workspace/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export TMPDIR="$workspace/.tmp"
export TMP="$TMPDIR"
export TEMP="$TMPDIR"
export CLAUDE_CODE_TMPDIR="$workspace/.claude-tmp"
install -d -m 0700 "$HOME" "$TMPDIR" "$CLAUDE_CODE_TMPDIR"
mkdir -p "runs/$run_name"

credentials_fallback="$workspace/.slurm-secrets/credentials.env"
model_profile_dir="$workspace/.slurm-secrets/model-profiles"
model_profile_manifest="$model_profile_dir/profiles.list"
runtime_profile_files=()
if [[ -s "$model_profile_manifest" ]]; then
  while IFS= read -r profile_name; do
    [[ -n "$profile_name" ]] || continue
    if ! [[ "$profile_name" =~ ^backend-[0-9]{3,}\.env$ ]]; then
      echo "invalid model profile name: $profile_name" >&2
      exit 1
    fi
    profile_file="$model_profile_dir/$profile_name"
    [[ -s "$profile_file" ]] || {
      echo "missing model profile: $profile_file" >&2
      exit 1
    }
    runtime_profile_files+=("$profile_file")
  done <"$model_profile_manifest"
elif [[ -d "$model_profile_dir" ]]; then
  mapfile -d '' -t runtime_profile_files \
    < <(find "$model_profile_dir" -maxdepth 1 -type f -name 'backend-*.env' -print0 | sort -z)
fi
if (( ${#runtime_profile_files[@]} == 0 )); then
  [[ -s "$credentials_fallback" ]] || {
    echo "missing private runtime credentials" >&2
    exit 1
  }
  runtime_profile_files=("$credentials_fallback")
fi

model_profile_name() {
  local filename
  filename=$(basename "$1")
  if [[ "$filename" == "credentials.env" ]]; then
    printf '%s\n' "default"
  else
    printf '%s\n' "${filename%.env}"
  fi
}

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

probe_model_backend() {
  SWEGEN_PREFLIGHT_CA_BUNDLE="$1" .venv/bin/python - <<'PY'
import os

import requests

def status_code(response):
    return str(response.status_code)


try:
    openai_base_url = os.environ.get(
        "OPENAI_BASE_URL", "https://arcyleung-ubuntu.tailb940e6.ts.net/v1"
    ).rstrip("/")
    anthropic_base_url = os.environ.get(
        "ANTHROPIC_BASE_URL", "https://arcyleung-ubuntu.tailb940e6.ts.net"
    ).rstrip("/")
    api_key = os.environ["OPENAI_API_KEY"]
    model = os.environ["ANTHROPIC_MODEL"]
    verify = os.environ["SWEGEN_PREFLIGHT_CA_BUNDLE"]
    models_response = requests.get(
        openai_base_url + "/models",
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=(15, 45),
        verify=verify,
    )
    anthropic_headers = {
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
        "x-api-key": api_key,
    }
    message = {"role": "user", "content": "Reply with OK."}
    count_response = requests.post(
        anthropic_base_url + "/v1/messages/count_tokens",
        headers=anthropic_headers,
        json={"model": model, "messages": [message]},
        timeout=(15, 45),
        verify=verify,
    )
    message_response = requests.post(
        anthropic_base_url + "/v1/messages",
        headers=anthropic_headers,
        json={"model": model, "max_tokens": 1, "messages": [message]},
        timeout=(15, 90),
        verify=verify,
    )
except requests.RequestException:
    print("000/000/000")
else:
    print(
        "/".join(
            status_code(response)
            for response in (models_response, count_response, message_response)
        )
    )
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
  # A busy Docker daemon can leave image cleanup waiting indefinitely even
  # after the preflight build itself succeeded. Cleanup is best-effort, so
  # bound it rather than blocking every subsequent node stage.
  timeout --kill-after=5s 15s docker image rm --force "$image_id" \
    >/dev/null 2>&1 || true
}

run_preflight() {
  local command_name route env_file proxy api_key status attempt ca_bundle
  local credential_file profile_name
  local github_status github_tokens github_remaining docker_config
  for command_name in bash curl docker git jq timeout; do
    command -v "$command_name" >/dev/null || {
      echo "missing command on $(hostname): $command_name" >&2
      return 1
    }
  done
  [[ -x .venv/bin/python ]] || { echo "missing node-local .venv" >&2; return 1; }
  [[ -s "$credentials_fallback" ]] || {
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
    set +a
    normalize_proxy_env || {
      echo "$route route lacks proxy configuration" >&2
      return 1
    }
    proxy="${HTTPS_PROXY:-${https_proxy:-${HTTP_PROXY:-${http_proxy:-}}}}"
    [[ -n "$proxy" ]] || {
      echo "$route route lacks proxy configuration" >&2
      return 1
    }
    for credential_file in "${runtime_profile_files[@]}"; do
      set -a
      # shellcheck disable=SC1090
      source "$credential_file"
      set +a
      api_key="${OPENAI_API_KEY:-}"
      [[ -n "$api_key" ]] || {
        echo "$route model profile lacks API credentials" >&2
        return 1
      }
      status="000"
      for attempt in 1 2 3; do
        status=$(probe_model_backend "$ca_bundle" || true)
        [[ "$status" =~ ^200/(200|404)/200$ ]] && break
        sleep "$attempt"
      done
      profile_name=$(model_profile_name "$credential_file")
      if ! [[ "$status" =~ ^200/(200|404)/200$ ]]; then
        echo "$route model-backend profile=$profile_name preflight failed " \
          "(models/count_tokens/messages HTTP $status)" >&2
        return 1
      fi
      if [[ "$status" == "200/404/200" ]]; then
        echo "preflight route=${route^^} profile=$profile_name endpoint=model-backend " \
          "models=200 count_tokens=unsupported messages=200"
      else
        echo "preflight route=${route^^} profile=$profile_name endpoint=model-backend " \
          "models=200 count_tokens=200 messages=200"
      fi
    done
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
    echo "preflight route=${route^^} docker_build_proxy=ok"
    unset OPENAI_API_KEY ANTHROPIC_API_KEY ANTHROPIC_AUTH_TOKEN GITHUB_TOKEN
    unset OPENAI_BASE_URL ANTHROPIC_BASE_URL OPENAI_MODEL ANTHROPIC_MODEL
    unset ANTHROPIC_DEFAULT_OPUS_MODEL ANTHROPIC_DEFAULT_SONNET_MODEL
    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy
    unset no_proxy NO_PROXY
  done
  echo "preflight node=$(hostname) docker=ok runtime=ok github_tokens=$github_tokens github_remaining=$github_remaining"
}

if [[ "$preflight" == "1" ]]; then
  run_preflight
  exit 0
fi

if [[ "$skip_preflight" == "1" ]]; then
  echo "preflight skipped by explicit operator request"
else
  run_preflight
fi

set -a
# shellcheck disable=SC1091
source "$credentials_fallback"
set +a

if (( initial_delay > 0 )); then
  echo "initial stagger: ${initial_delay}s"
  sleep "$initial_delay"
fi

route_names=(sg hk de)
group_suffixes=(a b c)
routes=()
suffixes=()
group_workers=()
if [[ -n "$route_workers" ]]; then
  route_worker_totals=("$sg_workers" "$hk_workers" "$de_workers")
  for ((group_index = 0; group_index < ${#group_suffixes[@]}; group_index++)); do
    for route_index in "${!route_names[@]}"; do
      remaining_workers=$((route_worker_totals[route_index] - group_index * 4))
      if (( remaining_workers > 0 )); then
        worker_count="$remaining_workers"
        if (( worker_count > 4 )); then
          worker_count=4
        fi
        routes+=("${route_names[$route_index]}")
        suffixes+=("${group_suffixes[$group_index]}")
        group_workers+=("$worker_count")
      fi
    done
  done
else
  for ((group_index = 0; group_index < groups_per_route; group_index++)); do
    for route in "${route_names[@]}"; do
      routes+=("$route")
      suffixes+=("${group_suffixes[$group_index]}")
      group_workers+=("$workers_per_group")
    done
  done
fi
pids=()
groups=()

# Give every orchestrator its own token-pool file. All selected processes are
# started immediately so they can load and delete these files before any
# Claude session begins; processing remains staggered inside orchestrator.py.
master_config="$workspace/.slurm-secrets/swegen.toml"
[[ -s "$master_config" ]] || { echo "missing private SWE-gen config" >&2; exit 1; }
for index in "${!routes[@]}"; do
  route="${routes[$index]}"
  suffix="${suffixes[$index]}"
  shard_name="${shard_revision}-${route}-n${node_index}-${suffix}"
  config_dir="$workspace/.slurm-secrets/config/$shard_name"
  install -d -m 0700 "$config_dir"
  cp "$master_config" "$config_dir/swegen.toml"
  chmod 0600 "$config_dir/swegen.toml"
done
if [[ "$preserve_master_config" != "1" ]]; then
  find "$master_config" -maxdepth 0 -type f -delete
fi

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
  worker_count="${group_workers[$index]}"
  shard_name="${shard_revision}-${route}-n${node_index}-${suffix}"
  global_group_index=$(( (node_index - 1) * ${#routes[@]} + index ))
  profile_index=$(( global_group_index % ${#runtime_profile_files[@]} ))
  credential_file="${runtime_profile_files[$profile_index]}"
  profile_name=$(model_profile_name "$credential_file")
  input="$workspace/$shard_dir/${shard_name}.jsonl"
  env_file=$(route_env "$route")
  log_dir="$workspace/runs/$run_name/orchestrator-logs-${route}-n${node_index}-${suffix}-${shard_revision}-${worker_count}w"
  launch_log="$workspace/runs/$run_name/orchestrator-${route}-n${node_index}-${suffix}-${shard_revision}-${worker_count}w-launch.log"
  progress="$workspace/runs/$run_name/orchestrator-progress-${route}-n${node_index}-${suffix}-${shard_revision}.jsonl"
  status="$workspace/runs/$run_name/orchestrator-instance-status-${route}-n${node_index}-${suffix}-${shard_revision}.jsonl"
  [[ -s "$input" ]] || { echo "missing shard: $input" >&2; terminate_children; exit 1; }
  mkdir -p "$log_dir"

  echo "launching $shard_name with $worker_count workers via ${route^^} profile=$profile_name"
  setsid env \
    SWEGEN_PROXY_ENV_FILE="$env_file" \
    SWEGEN_WORKERS="$worker_count" \
    SWEGEN_RUN_NAME="$run_name" \
    SWEGEN_INPUT_JSONL="$input" \
    SWEGEN_ORCHESTRATOR_LOG_DIR="$log_dir" \
    SWEGEN_PROGRESS_JSONL="$progress" \
    SWEGEN_INSTANCE_STATUS_JSONL="$status" \
    SWEGEN_CONFIG="$workspace/.slurm-secrets/config/$shard_name/swegen.toml" \
    SWEGEN_RUNTIME_CREDENTIALS_FILE="$credential_file" \
    SWEGEN_MODEL_PROFILE="$profile_name" \
    SWEGEN_MODEL_PROFILE_DIR="$model_profile_dir" \
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

declare -A group_by_pid=()
for index in "${!pids[@]}"; do
  group_by_pid["${pids[$index]}"]="${groups[$index]}"
done

# A failed orchestrator group must fail the allocation promptly. Otherwise the
# Slurm job remains RUNNING while one route silently loses all of its workers,
# and the controller cannot distinguish that from the requested topology.
while (( ${#pids[@]} > 0 )); do
  finished_pid=""
  if wait -n -p finished_pid "${pids[@]}"; then
    child_rc=0
  else
    child_rc=$?
  fi
  group="${group_by_pid[$finished_pid]:-unknown-group}"
  remaining_pids=()
  for pid in "${pids[@]}"; do
    if [[ "$pid" != "$finished_pid" ]]; then
      remaining_pids+=("$pid")
    fi
  done
  pids=("${remaining_pids[@]}")
  unset "group_by_pid[$finished_pid]"

  if (( child_rc == 0 )); then
    echo "completed $group"
    continue
  fi
  echo "failed $group rc=$child_rc; terminating remaining orchestrators" >&2
  terminate_children
  exit "$child_rc"
done
exit 0
