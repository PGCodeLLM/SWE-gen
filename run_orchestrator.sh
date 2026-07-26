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

# Slurm bundles keep model credentials separate from route proxy files.  Load
# them after the selected route so a newly staged backend/key cannot be
# overwritten by legacy values retained in .env, .env_hk, or .env_de.
SWEGEN_RUNTIME_CREDENTIALS_FILE="${SWEGEN_RUNTIME_CREDENTIALS_FILE:-$PWD/.slurm-secrets/credentials.env}"
if [[ -s "$SWEGEN_RUNTIME_CREDENTIALS_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$SWEGEN_RUNTIME_CREDENTIALS_FILE"
  set +a
fi

# Slurm run artifacts may contain verbose SDK output, so keep them private.
# The controller's long-lived local farm still uses the shared cache group.
if [[ -n "${SWEGEN_SLURM_NODE:-}" ]]; then
  umask 0077
else
  umask 0002
fi

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

# Docker does not automatically inherit shell proxy variables into Dockerfile
# RUN steps. Slurm supplies a unique directory per orchestrator so concurrent
# SG/HK/DE groups cannot overwrite one shared Docker client configuration.
cleanup_docker_proxy_config() {
  if [[ -n "${SWEGEN_DOCKER_CONFIG_DIR:-}" ]]; then
    find "$SWEGEN_DOCKER_CONFIG_DIR" -maxdepth 1 -type f -name 'config.json*' -delete \
      2>/dev/null || true
  fi
}
if [[ -n "${SWEGEN_DOCKER_CONFIG_DIR:-}" ]]; then
  export DOCKER_CONFIG="$SWEGEN_DOCKER_CONFIG_DIR"
  install -d -m 0700 "$DOCKER_CONFIG"
  docker_config_tmp=$(mktemp "$DOCKER_CONFIG/config.json.tmp.XXXXXX")
  jq -n \
    '{proxies:{default:{httpProxy:env.HTTP_PROXY,httpsProxy:env.HTTPS_PROXY,noProxy:env.NO_PROXY}}}' \
    >"$docker_config_tmp"
  chmod 0600 "$docker_config_tmp"
  mv "$docker_config_tmp" "$DOCKER_CONFIG/config.json"
  unset docker_config_tmp
  trap cleanup_docker_proxy_config EXIT
fi

# Force harbor's `docker compose build` to use the Docker daemon's built-in
# BuildKit (the "default"/`docker` buildx driver) instead of the
# `docker-container` driver, which spawns a separate `buildx_buildkit_*`
# container (a full buildkitd, ~60-75 threads) PER concurrent build. At
# validation/generation concurrency, dozens of those buildkitd instances
# collectively deadlock the shared host dockerd/containerd on futexes,
# stalling all builds. One shared daemon BuildKit removes the multiplication.
export DOCKER_BUILDKIT=1
export BUILDX_BUILDER=default
export COMPOSE_BAKE=false

default_ca_bundle=/etc/ssl/certs/ca-certificates.crt
if [[ -f "$PWD/.slurm-secrets/combined-ca.crt" ]]; then
  default_ca_bundle="$PWD/.slurm-secrets/combined-ca.crt"
fi
export SWEGEN_CA_BUNDLE="${SWEGEN_CA_BUNDLE:-$default_ca_bundle}"
unset default_ca_bundle
default_proxy_ca=/data/work/alex/ProxyCA260122.crt
if [[ -f "$PWD/.slurm-secrets/ProxyCA260122.crt" ]]; then
  default_proxy_ca="$PWD/.slurm-secrets/ProxyCA260122.crt"
fi
export SWEGEN_PROXY_CA_BUNDLE="${SWEGEN_PROXY_CA_BUNDLE:-$default_proxy_ca}"
unset default_proxy_ca
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

# The staged runtime credentials may select a different OpenAI/Anthropic-
# compatible backend.  Keep the former endpoint as a local-run fallback only.
export OPENAI_BASE_URL="${OPENAI_BASE_URL:-https://arcyleung-ubuntu.tailb940e6.ts.net/v1}"
# Claude Code appends /v1/messages itself, unlike the OpenAI client above.
export ANTHROPIC_BASE_URL="${ANTHROPIC_BASE_URL:-https://arcyleung-ubuntu.tailb940e6.ts.net}"

# 3. Preserve the model role selected by a staged endpoint profile.  These
# defaults keep local single-endpoint launches backward compatible.
export OPENAI_MODEL="${OPENAI_MODEL:-gpt-5.6-sol}"
export ANTHROPIC_MODEL="${ANTHROPIC_MODEL:-$OPENAI_MODEL}"

# Claude Code can independently select its Opus and Sonnet tiers for spawned
# Task agents even when the top-level SDK session uses ANTHROPIC_MODEL.
export ANTHROPIC_DEFAULT_OPUS_MODEL="${ANTHROPIC_DEFAULT_OPUS_MODEL:-gpt-5.6-sol}"
export ANTHROPIC_DEFAULT_SONNET_MODEL="${ANTHROPIC_DEFAULT_SONNET_MODEL:-gpt-5.6-terra}"

# Claude Code otherwise selects claude-haiku-4-5 for built-in Explore
# subagents and lightweight helpers such as Bash command-path extraction.
# Prefer Spark when the endpoint advertises it, but fall back to Terra when
# Spark is unavailable or the catalog probe itself fails.
SWEGEN_CLAUDE_FAST_MODEL="${SWEGEN_CLAUDE_FAST_MODEL:-gpt-5.3-codex-spark}"
SWEGEN_CLAUDE_FAST_FALLBACK_MODEL="${SWEGEN_CLAUDE_FAST_FALLBACK_MODEL:-gpt-5.6-terra}"
if [[ "$SWEGEN_CLAUDE_FAST_MODEL" != "$SWEGEN_CLAUDE_FAST_FALLBACK_MODEL" ]]; then
  fast_model_catalog=$(
    python - <<'PY' 2>/dev/null || true
import os

import requests

ca_bundle = os.environ.get("SWEGEN_CA_BUNDLE") or True
response = requests.get(
    os.environ["OPENAI_BASE_URL"].rstrip("/") + "/models",
    headers={"Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}"},
    timeout=(10, 20),
    verify=ca_bundle,
)
response.raise_for_status()
print(response.text)
PY
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
export CLAUDE_CONFIG_DIR="${SWEGEN_CLAUDE_CONFIG_DIR:-${TMPDIR:-/tmp}/swegen-claude-${UID}}"
install -d -m 0700 "$CLAUDE_CONFIG_DIR"

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
