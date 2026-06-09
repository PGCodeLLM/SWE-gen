#!/usr/bin/env bash
set -euo pipefail

python src/orchestrator.py 19576_repo_pr_pairs.jsonl --workers 24 \
  --github-token "$GITHUB_TOKEN" \
  --anthropic-api-key "$ANTHROPIC_API_KEY" \
  --anthropic-auth-token "$ANTHROPIC_AUTH_TOKEN" \
  --anthropic-base-url "$ANTHROPIC_BASE_URL" \
  --openai-base-url "$OPENAI_BASE_URL" \
  --cc-timeout 6400 \
  --slurm
