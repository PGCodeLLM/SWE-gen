ARG SWEGEN_BASE_IMAGE=swegen-worker:proxy-noproxy-fix-20260801-0356
FROM ${SWEGEN_BASE_IMAGE}

# The normal worker image contains LiteLLM's client library, but not the proxy
# server extras needed for Anthropic <-> OpenAI protocol translation.
RUN uv pip install --python /app/.venv/bin/python 'litellm[proxy]'
