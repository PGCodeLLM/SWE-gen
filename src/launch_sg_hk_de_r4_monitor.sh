#!/usr/bin/env bash
set -euo pipefail

workspace=/data/work/alex/SWE-gen
run_name=20260716-sol-max-full-16w
run_dir="$workspace/runs/$run_name"
session=swegen-combined-monitor-r4
status="$run_dir/multi-proxy-health-r4.json"
pid_file="$run_dir/multi-proxy-health-r4.pid"
log="$run_dir/multi-proxy-health-r4-monitor.log"

if [[ "$(id -un)" != "alex" ]]; then
  echo "This monitor launcher must run as alex." >&2
  exit 2
fi

cd "$workspace"

groups=(
  "name=SG-A,session=swegen-sg-a-r4,input=r4-sg-a.jsonl,logs=$run_dir/orchestrator-logs-sg-a-r4-4w,workers=4,launch=$run_dir/orchestrator-sg-a-r4-4w-launch.log,progress=$run_dir/orchestrator-progress-sg-a-r4.jsonl"
  "name=HK-A,session=swegen-hk-a-r4,input=r4-hk-a.jsonl,logs=$run_dir/orchestrator-logs-hk-a-r4-4w,workers=4,launch=$run_dir/orchestrator-hk-a-r4-4w-launch.log,progress=$run_dir/orchestrator-progress-hk-a-r4.jsonl"
  "name=DE-A,session=swegen-de-a-r4,input=r4-de-a.jsonl,logs=$run_dir/orchestrator-logs-de-a-r4-4w,workers=4,launch=$run_dir/orchestrator-de-a-r4-4w-launch.log,progress=$run_dir/orchestrator-progress-de-a-r4.jsonl"
  "name=SG-B,session=swegen-sg-b-r4,input=r4-sg-b.jsonl,logs=$run_dir/orchestrator-logs-sg-b-r4-4w,workers=4,launch=$run_dir/orchestrator-sg-b-r4-4w-launch.log,progress=$run_dir/orchestrator-progress-sg-b-r4.jsonl"
  "name=HK-B,session=swegen-hk-b-r4,input=r4-hk-b.jsonl,logs=$run_dir/orchestrator-logs-hk-b-r4-4w,workers=4,launch=$run_dir/orchestrator-hk-b-r4-4w-launch.log,progress=$run_dir/orchestrator-progress-hk-b-r4.jsonl"
  "name=DE-B,session=swegen-de-b-r4,input=r4-de-b.jsonl,logs=$run_dir/orchestrator-logs-de-b-r4-4w,workers=4,launch=$run_dir/orchestrator-de-b-r4-4w-launch.log,progress=$run_dir/orchestrator-progress-de-b-r4.jsonl"
)

for group in "${groups[@]}"; do
  group_session=${group#*session=}
  group_session=${group_session%%,*}
  if ! tmux has-session -t "=$group_session" 2>/dev/null; then
    echo "waiting for worker session $group_session"
  fi
done

if tmux has-session -t "=$session" 2>/dev/null; then
  echo "$session already active"
  exit 0
fi

command=(
  "$workspace/.venv/bin/python"
  "$workspace/src/multi_proxy_health_monitor.py"
  --run-dir "$run_dir"
  --interval 30
  --max-log-age 1800
  --status-file "$status"
  --pid-file "$pid_file"
)
for group in "${groups[@]}"; do
  command+=(--group "$group")
done

shell_command=exec
for argument in "${command[@]}"; do
  printf -v quoted '%q' "$argument"
  shell_command+=" $quoted"
done
printf -v quoted_log '%q' "$log"
shell_command+=" >> $quoted_log 2>&1"

tmux new-session -d -s "$session" -c "$workspace" "$shell_command"
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) launched $session"
