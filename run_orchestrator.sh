#!/usr/bin/env bash
set -euo pipefail

python src/orchestrator.py 1000_from_khaled_4k_for_boyuan_comparison.jsonl --workers 32 \
  --anthropic-api-key "$ANTHROPIC_API_KEY" \
  --anthropic-auth-token "$ANTHROPIC_AUTH_TOKEN" \
  --anthropic-base-url "$ANTHROPIC_BASE_URL" \
  --openai-base-url "$OPENAI_BASE_URL" \
  --cc-timeout 6400 