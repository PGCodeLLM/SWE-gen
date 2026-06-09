#!/usr/bin/env bash
set -euo pipefail
rm -rf orchestrator-logs
rm -rf tasks

python src/orchestrator.py 19576_repo_pr_pairs.jsonl --workers 16 \
  --anthropic-api-key "$ANTHROPIC_API_KEY" \
  --anthropic-auth-token "$ANTHROPIC_AUTH_TOKEN" \
  --anthropic-base-url "$ANTHROPIC_BASE_URL" \
  --openai-base-url "$OPENAI_BASE_URL" \
  --cc-timeout 6400 \
  --slurm
