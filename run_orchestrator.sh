#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f ".venv/bin/activate" ]]; then
  echo "Missing .venv/bin/activate. Run: uv venv && uv pip install -e ." >&2
  exit 2
fi

source ".venv/bin/activate"

SWEGEN_PROXY_ENV_FILE="${SWEGEN_PROXY_ENV_FILE:-.env}"
if [[ ! -f "$SWEGEN_PROXY_ENV_FILE" ]]; then
  echo "Missing proxy configuration: $SWEGEN_PROXY_ENV_FILE" >&2
  exit 2
fi

# Export the shared proxy configuration for the orchestrator, OpenAI clients,
# Claude Code SDK processes, and any tools/subagents they spawn.
set -a
source "$SWEGEN_PROXY_ENV_FILE"
set +a

# Keep shared cache files writable by the cache group, including repositories
# created by a launcher invoked through sudo.
umask 0002

# Normalize the four common HTTP proxy names in case .env only defines one
# casing. The model hostname is deliberately absent from NO_PROXY because
# direct TCP/TLS access from this host is unavailable.
PROXY_URL="${HTTPS_PROXY:-${https_proxy:-${HTTP_PROXY:-${http_proxy:-}}}}"
if [[ -z "$PROXY_URL" ]]; then
  echo "Missing http_proxy/https_proxy configuration in $SWEGEN_PROXY_ENV_FILE." >&2
  exit 2
fi
export http_proxy="${http_proxy:-$PROXY_URL}"
export https_proxy="${https_proxy:-$PROXY_URL}"
export HTTP_PROXY="${HTTP_PROXY:-$http_proxy}"
export HTTPS_PROXY="${HTTPS_PROXY:-$https_proxy}"
unset all_proxy ALL_PROXY

ENV_NO_PROXY="${no_proxy:-${NO_PROXY:-}}"
INTERNAL_NO_PROXY="*.huaweicloud.com,100.*,10.*,.huawei.com,127.0.0.1,7.244.3.251,10.170.22.223,10.170.22.98"
export no_proxy="${ENV_NO_PROXY:+$ENV_NO_PROXY,}$INTERNAL_NO_PROXY"
export NO_PROXY="$no_proxy"

export SWEGEN_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
export SWEGEN_PROXY_CA_BUNDLE=/data/work/alex/ProxyCA260122.crt
export REQUESTS_CA_BUNDLE="$SWEGEN_CA_BUNDLE"
export CURL_CA_BUNDLE="$SWEGEN_CA_BUNDLE"
export SSL_CERT_FILE="$SWEGEN_CA_BUNDLE"
export GIT_SSL_CAINFO="$SWEGEN_CA_BUNDLE"

# The shared repository cache may contain repositories cloned by an earlier
# invocation under a different account. Trust only repositories beneath this
# specific cache root; do not disable Git's ownership protection globally.
export SWEGEN_REPO_CACHE_DIR="$PWD/data_cache/repos"

# Use the same .env proxy for Git, GitHub REST, OpenAI, Claude Code, and every
# Claude-spawned agent. Git/GitHub retain explicit settings because their
# clients do not all consume environment proxy variables consistently.
export GIT_PROXY="$HTTPS_PROXY"
export SWEGEN_CLAUDE_PROXY="$HTTPS_PROXY"
export SWEGEN_CLAUDE_HTTP_PROXY="$HTTP_PROXY"
export GIT_CONFIG_COUNT=3
export GIT_CONFIG_KEY_0=http.proxy
export GIT_CONFIG_VALUE_0="$GIT_PROXY"
export GIT_CONFIG_KEY_1=http.version
export GIT_CONFIG_VALUE_1=HTTP/1.1
export GIT_CONFIG_KEY_2=safe.directory
export GIT_CONFIG_VALUE_2="$SWEGEN_REPO_CACHE_DIR/*"
# GitHub REST calls are made by Python requests, not git.
export SWEGEN_GITHUB_PROXY="$GIT_PROXY"
# Prevent GitHub rate limits/outages from turning the remaining queue into
# thousands of immediate false failures. The orchestrator preflights the token
# pool, and individual API calls wait/retry with reset-aware backoff.
export SWEGEN_GITHUB_PREFLIGHT="${SWEGEN_GITHUB_PREFLIGHT:-1}"
export SWEGEN_GITHUB_API_ATTEMPTS="${SWEGEN_GITHUB_API_ATTEMPTS:-6}"
export SWEGEN_GITHUB_RETRY_BASE_SECONDS="${SWEGEN_GITHUB_RETRY_BASE_SECONDS:-10}"
export SWEGEN_GITHUB_MAX_WAIT_SECONDS="${SWEGEN_GITHUB_MAX_WAIT_SECONDS:-3600}"

# Clear stale per-worker proxy assignments if the launcher is sourced from a
# shell that previously ran the SOCKS-backed configuration.
unset SWEGEN_WORKER_PROXY_POOL SWEGEN_CLAUDE_PROXY_POOL
unset SWEGEN_PROXY_WORKERS_PER_ENDPOINT SWEGEN_ASSIGNED_SOCKS_PROXY
unset SWEGEN_PROXY_ENDPOINT_INDEX

SWEGEN_WORKERS="${SWEGEN_WORKERS:-4}"
SWEGEN_RUN_NAME="${SWEGEN_RUN_NAME:-20260716-sol-max-full-16w}"
SWEGEN_ORCHESTRATOR_LOG_DIR="${SWEGEN_ORCHESTRATOR_LOG_DIR:-runs/20260716-sol-max-full-16w/orchestrator-logs-4w-env-proxy}"
SWEGEN_INPUT_JSONL="${SWEGEN_INPUT_JSONL:-data_cache/pr_tasks_export_ts_js_12122_removed.jsonl}"
SWEGEN_PROGRESS_JSONL="${SWEGEN_PROGRESS_JSONL:-}"
SWEGEN_INSTANCE_STATUS_JSONL="${SWEGEN_INSTANCE_STATUS_JSONL:-}"

# Credentials must come from the selected proxy environment file. Keeping
# them out of this tracked launcher lets the stable topology be deployed to
# Slurm nodes without baking secrets into Git or deployment bundles.
if [[ -z "${OPENAI_API_KEY:-}" ]]; then
  echo "Missing OPENAI_API_KEY in $SWEGEN_PROXY_ENV_FILE" >&2
  exit 2
fi
if [[ -z "${ANTHROPIC_API_KEY:-}" && -z "${ANTHROPIC_AUTH_TOKEN:-}" ]]; then
  echo "Missing ANTHROPIC_API_KEY/ANTHROPIC_AUTH_TOKEN in $SWEGEN_PROXY_ENV_FILE" >&2
  exit 2
fi
export ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-$ANTHROPIC_AUTH_TOKEN}"
export ANTHROPIC_AUTH_TOKEN="${ANTHROPIC_AUTH_TOKEN:-$ANTHROPIC_API_KEY}"

# 2. Updated Base URLs: Pointing to your Tailscale endpoint
export OPENAI_BASE_URL="https://arcyleung-ubuntu.tailb940e6.ts.net/v1"
# Claude Code appends /v1/messages itself, unlike the OpenAI client above.
export ANTHROPIC_BASE_URL="https://arcyleung-ubuntu.tailb940e6.ts.net"

# 3. Use the flagship model served by the Tailscale endpoint for both clients.
export OPENAI_MODEL="gpt-5.6-sol"
export ANTHROPIC_MODEL="$OPENAI_MODEL"

# Claude Code can independently select its Opus and Sonnet tiers for spawned
# Task agents even when the top-level SDK session uses ANTHROPIC_MODEL.
export ANTHROPIC_DEFAULT_OPUS_MODEL="gpt-5.6-sol"
export ANTHROPIC_DEFAULT_SONNET_MODEL="gpt-5.6-terra"

# Claude Code otherwise selects claude-haiku-4-5 for built-in Explore
# subagents and lightweight helpers such as Bash command-path extraction.
# Prefer Spark when the endpoint advertises it, but fall back to Terra when
# Spark is unavailable or the catalog probe itself fails.
SWEGEN_CLAUDE_FAST_MODEL="${SWEGEN_CLAUDE_FAST_MODEL:-gpt-5.3-codex-spark}"
SWEGEN_CLAUDE_FAST_FALLBACK_MODEL="${SWEGEN_CLAUDE_FAST_FALLBACK_MODEL:-gpt-5.6-terra}"
if [[ "$SWEGEN_CLAUDE_FAST_MODEL" != "$SWEGEN_CLAUDE_FAST_FALLBACK_MODEL" ]]; then
  fast_model_catalog=$(
    curl --silent --show-error --connect-timeout 10 --max-time 20 \
      -H "Authorization: Bearer $OPENAI_API_KEY" \
      "$OPENAI_BASE_URL/models" 2>/dev/null || true
  )
  if ! jq --exit-status --arg model "$SWEGEN_CLAUDE_FAST_MODEL" \
    'any(.data[]?; .id == $model) or any(.models[]?; .id == $model)' \
    <<<"$fast_model_catalog" >/dev/null 2>&1; then
    SWEGEN_CLAUDE_FAST_MODEL="$SWEGEN_CLAUDE_FAST_FALLBACK_MODEL"
  fi
  unset fast_model_catalog
fi
export SWEGEN_CLAUDE_FAST_MODEL
export ANTHROPIC_DEFAULT_HAIKU_MODEL="$SWEGEN_CLAUDE_FAST_MODEL"
export ANTHROPIC_SMALL_FAST_MODEL="$SWEGEN_CLAUDE_FAST_MODEL"
echo "Claude fast/small model: $SWEGEN_CLAUDE_FAST_MODEL"

# Use adaptive thinking at high session-wide effort for the agentic
# task-completion stage.
export SWEGEN_AGENT_REASONING_EFFORT="high"

# Allow individual Docker builds and Harbor validation commands to run for up
# to 30 minutes. The generated task.toml uses the same agent/verifier/build
# timeout so neither Claude's Bash tool nor Harbor cuts the other off early.
export BASH_DEFAULT_TIMEOUT_MS=1800000
export BASH_MAX_TIMEOUT_MS=1800000

# Do not allow ~/.claude/settings.json to override the endpoint or model for
# batch workers. Use a per-user temporary directory so a prior sudo invocation
# cannot leave root-owned Claude state in the shared worktree.
export CLAUDE_CONFIG_DIR="${TMPDIR:-/tmp}/swegen-claude-${UID}"
mkdir -p "$CLAUDE_CONFIG_DIR"

export SWEGEN_SSL_NO_VERIFY=1

orchestrator_args=(
  "$SWEGEN_INPUT_JSONL"
  --workers "$SWEGEN_WORKERS"
  --run-name "$SWEGEN_RUN_NAME"
  --log-dir "$SWEGEN_ORCHESTRATOR_LOG_DIR"
  --repo-cache-dir "$SWEGEN_REPO_CACHE_DIR"
  --cc-timeout 10800
)

if [[ -n "$SWEGEN_PROGRESS_JSONL" ]]; then
  orchestrator_args+=(--progress-jsonl "$SWEGEN_PROGRESS_JSONL")
fi
if [[ -n "$SWEGEN_INSTANCE_STATUS_JSONL" ]]; then
  orchestrator_args+=(--instance-status-jsonl "$SWEGEN_INSTANCE_STATUS_JSONL")
fi

# Secrets and endpoint settings stay in the inherited environment instead of
# being exposed in the orchestrator process command line.
python src/orchestrator.py "${orchestrator_args[@]}"
