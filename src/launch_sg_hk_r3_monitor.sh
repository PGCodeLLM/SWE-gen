#!/usr/bin/env bash
set -euo pipefail

workspace=/data/work/alex/SWE-gen
run_name=20260716-sol-max-full-16w
run_dir="$workspace/runs/$run_name"
session=swegen-combined-monitor-r3
status="$run_dir/multi-proxy-health-r3.json"
pid_file="$run_dir/multi-proxy-health-r3.pid"
log="$run_dir/multi-proxy-health-r3-monitor.log"

if [[ "$(id -un)" != "alex" ]]; then
  echo "This monitor launcher must run as alex." >&2
  exit 2
fi

cd "$workspace"

groups=(
  "name=SG-A,session=swegen-sg-a-r3,input=r3-sg-a.jsonl,logs=$run_dir/orchestrator-logs-sg-a-r3-4w,workers=4,launch=$run_dir/orchestrator-sg-a-r3-4w-launch.log,progress=$run_dir/orchestrator-progress-sg-a-r3.jsonl"
  "name=HK-A,session=swegen-hk-a-r3,input=r3-hk-a.jsonl,logs=$run_dir/orchestrator-logs-hk-a-r3-4w,workers=4,launch=$run_dir/orchestrator-hk-a-r3-4w-launch.log,progress=$run_dir/orchestrator-progress-hk-a-r3.jsonl"
  "name=SG-B,session=swegen-sg-b-r3,input=r3-sg-b.jsonl,logs=$run_dir/orchestrator-logs-sg-b-r3-4w,workers=4,launch=$run_dir/orchestrator-sg-b-r3-4w-launch.log,progress=$run_dir/orchestrator-progress-sg-b-r3.jsonl"
  "name=HK-B,session=swegen-hk-b-r3,input=r3-hk-b.jsonl,logs=$run_dir/orchestrator-logs-hk-b-r3-4w,workers=4,launch=$run_dir/orchestrator-hk-b-r3-4w-launch.log,progress=$run_dir/orchestrator-progress-hk-b-r3.jsonl"
  "name=SG-C,session=swegen-sg-c-r3,input=r3-sg-c.jsonl,logs=$run_dir/orchestrator-logs-sg-c-r3-4w,workers=4,launch=$run_dir/orchestrator-sg-c-r3-4w-launch.log,progress=$run_dir/orchestrator-progress-sg-c-r3.jsonl"
  "name=HK-C,session=swegen-hk-c-r3,input=r3-hk-c.jsonl,logs=$run_dir/orchestrator-logs-hk-c-r3-4w,workers=4,launch=$run_dir/orchestrator-hk-c-r3-4w-launch.log,progress=$run_dir/orchestrator-progress-hk-c-r3.jsonl"
)

for group in "${groups[@]}"; do
  group_session=${group#*session=}
  group_session=${group_session%%,*}
  if ! tmux has-session -t "=$group_session" 2>/dev/null; then
    echo "refusing monitor launch: missing worker session $group_session" >&2
    exit 3
  fi
done

if tmux has-session -t "=$session" 2>/dev/null; then
  start_command=$(tmux display-message -p -t "=$session:0.0" '#{pane_start_command}')
  for marker in \
    r3-sg-a.jsonl r3-hk-a.jsonl r3-sg-b.jsonl \
    r3-hk-b.jsonl r3-sg-c.jsonl r3-hk-c.jsonl; do
    if [[ "$start_command" != *"$marker"* ]]; then
      echo "existing $session is stale or incomplete: missing $marker" >&2
      exit 4
    fi
  done
  echo "$session already active and complete"
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
