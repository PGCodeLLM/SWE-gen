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

exec 9>"$run_dir/r3-launch.lock"
if ! flock -n 9; then
  echo "r3 launcher already active"
  exit 0
fi

legacy_sessions=(
  swegen-sg-r2
  swegen-hk-a-r2
  swegen-hk-b-r2
  swegen-wg-a-r2
  swegen-wg-b-r2
  swegen-sg-extra-r2
)
for session in "${legacy_sessions[@]}"; do
  if tmux has-session -t "=$session" 2>/dev/null; then
    echo "refusing launch: legacy session still active: $session" >&2
    exit 3
  fi
done

launch_wave() {
  local session=$1
  local env_file=$2
  local shard=$3
  local stem=$4
  local workers=$5
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
    'exec env -u no_proxy -u NO_PROXY -u all_proxy -u ALL_PROXY SWEGEN_PROXY_ENV_FILE=%q SWEGEN_WORKERS=%q SWEGEN_RUN_NAME=%q SWEGEN_INPUT_JSONL=%q SWEGEN_ORCHESTRATOR_LOG_DIR=%q SWEGEN_PROGRESS_JSONL=%q SWEGEN_INSTANCE_STATUS_JSONL=%q %q >> %q 2>&1' \
    "$env_file" "$workers" "$run_name" "$input" "$logs" "$progress" \
    "$status" "$workspace/run_orchestrator.sh" "$launch_log"

  tmux new-session -d -s "$session" -c "$workspace" "$shell_command"
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) launched $session"
}

sessions=(
  swegen-sg-a-r3
  swegen-hk-a-r3
  swegen-sg-b-r3
  swegen-hk-b-r3
  swegen-sg-c-r3
  swegen-hk-c-r3
)
env_files=(.env .env_hk .env .env_hk .env .env_hk)
shards=(
  r3-sg-a.jsonl
  r3-hk-a.jsonl
  r3-sg-b.jsonl
  r3-hk-b.jsonl
  r3-sg-c.jsonl
  r3-hk-c.jsonl
)
stems=(sg-a-r3 hk-a-r3 sg-b-r3 hk-b-r3 sg-c-r3 hk-c-r3)
workers=(4 4 4 4 4 4)

for index in "${!sessions[@]}"; do
  launch_wave \
    "${sessions[$index]}" \
    "${env_files[$index]}" \
    "${shards[$index]}" \
    "${stems[$index]}" \
    "${workers[$index]}"
  if (( index + 1 < ${#sessions[@]} )); then
    sleep 60
  fi
done

echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) all r3 waves launched"
