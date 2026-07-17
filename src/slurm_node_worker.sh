#!/bin/bash
set -euo pipefail

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

run_preflight() {
  local command_name route env_file proxy api_key status attempt ca_bundle
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
  .venv/bin/python -c 'import claude_agent_sdk, harbor, swegen' >/dev/null
  docker info >/dev/null

  for route in sg hk de; do
    env_file=$(route_env "$route")
    [[ -s "$env_file" ]] || { echo "missing $route environment file" >&2; return 1; }
    set -a
    # shellcheck disable=SC1090
    source "$env_file"
    source "$workspace/.slurm-secrets/credentials.env"
    set +a
    proxy="${HTTPS_PROXY:-${https_proxy:-${HTTP_PROXY:-${http_proxy:-}}}}"
    api_key="${OPENAI_API_KEY:-}"
    [[ -n "$proxy" && -n "$api_key" ]] || {
      echo "$route route lacks proxy/API configuration" >&2
      return 1
    }
    status="000"
    for attempt in 1 2 3; do
      status=$(curl --cacert "$ca_bundle" --silent --show-error --output /dev/null \
        --write-out '%{http_code}' --connect-timeout 15 --max-time 45 \
        --proxy "$proxy" -H "Authorization: Bearer $api_key" \
        "https://arcyleung-ubuntu.tailb940e6.ts.net/v1/models" || true)
      [[ "$status" == "200" ]] && break
      sleep "$attempt"
    done
    if [[ "$status" != "200" ]]; then
      echo "$route Arcyleung preflight failed with HTTP $status" >&2
      return 1
    fi
    GIT_SSL_CAINFO="$ca_bundle" git ls-remote \
      https://github.com/octocat/Hello-World.git HEAD >/dev/null
    echo "preflight route=${route^^} endpoint=arcyleung-ubuntu status=200"
    unset OPENAI_API_KEY ANTHROPIC_API_KEY ANTHROPIC_AUTH_TOKEN GITHUB_TOKEN
    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy
    unset no_proxy NO_PROXY
  done
  echo "preflight node=$(hostname) docker=ok runtime=ok"
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
    SWEGEN_SLURM_NODE="${SLURMD_NODENAME:-$(hostname)}" \
    SWEGEN_SLURM_ROUTE="${route^^}" \
    SWEGEN_SLURM_GROUP="$shard_name" \
    bash "$workspace/run_orchestrator.sh" >"$launch_log" 2>&1 &
  pids+=("$!")
  groups+=("$shard_name")

  if (( index + 1 < ${#routes[@]} )); then
    sleep 60
  fi
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
