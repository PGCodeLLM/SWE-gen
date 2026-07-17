#!/usr/bin/env bash
set -euo pipefail

workspace=/data/work/alex/SWE-gen
run_name=20260716-sol-max-full-16w
run_dir="$workspace/runs/$run_name"

if [[ "$(id -un)" != "alex" ]]; then
  echo "This launcher must run as alex." >&2
  exit 2
fi

cd "$workspace"
mkdir -p "$run_dir"

exec 9>"$run_dir/r5-launch.lock"
if ! flock -n 9; then
  echo "r5 launcher already active"
  exit 0
fi

for session in \
  swegen-sg-a-r4 swegen-hk-a-r4 swegen-de-a-r4 \
  swegen-sg-b-r4 swegen-hk-b-r4 swegen-de-b-r4; do
  if tmux has-session -t "=$session" 2>/dev/null; then
    echo "refusing launch: overlapping r4 session active: $session" >&2
    exit 3
  fi
done

launch_wave() {
  local session=$1 env_file=$2 shard=$3 stem=$4
  local input="$workspace/data_cache/orchestrator_shards/$shard"
  local logs="$run_dir/orchestrator-logs-$stem-4w"
  local progress="$run_dir/orchestrator-progress-$stem.jsonl"
  local status="$run_dir/orchestrator-instance-status-$stem.jsonl"
  local launch_log="$run_dir/orchestrator-$stem-4w-launch.log"
  local shell_command

  if [[ ! -r "$input" || ! -r "$workspace/$env_file" ]]; then
    echo "missing input or environment for $session" >&2
    return 2
  fi
  if tmux has-session -t "=$session" 2>/dev/null; then
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $session already active"
    return 0
  fi

  printf -v shell_command \
    'exec env -u no_proxy -u NO_PROXY -u all_proxy -u ALL_PROXY SWEGEN_PROXY_ENV_FILE=%q SWEGEN_WORKERS=4 SWEGEN_RUN_NAME=%q SWEGEN_INPUT_JSONL=%q SWEGEN_ORCHESTRATOR_LOG_DIR=%q SWEGEN_PROGRESS_JSONL=%q SWEGEN_INSTANCE_STATUS_JSONL=%q %q >> %q 2>&1' \
    "$env_file" "$run_name" "$input" "$logs" "$progress" "$status" \
    "$workspace/run_orchestrator.sh" "$launch_log"
  tmux new-session -d -s "$session" -c "$workspace" "$shell_command"
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) launched $session"
}

sessions=(swegen-sg-a-r5 swegen-hk-a-r5 swegen-de-a-r5 swegen-sg-b-r5 swegen-hk-b-r5 swegen-de-b-r5)
env_files=(.env .env_hk .env_de .env .env_hk .env_de)
shards=(r5-sg-a.jsonl r5-hk-a.jsonl r5-de-a.jsonl r5-sg-b.jsonl r5-hk-b.jsonl r5-de-b.jsonl)
stems=(sg-a-r5 hk-a-r5 de-a-r5 sg-b-r5 hk-b-r5 de-b-r5)

for index in "${!sessions[@]}"; do
  launch_wave "${sessions[$index]}" "${env_files[$index]}" "${shards[$index]}" "${stems[$index]}"
  if (( index + 1 < ${#sessions[@]} )); then
    sleep 60
  fi
done

echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) all r5 waves launched"
