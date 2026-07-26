"""Manually test an LLM endpoint against the API formats used by SWE-gen.

Edit ENDPOINT, API_KEY, and MODEL below, then run:

    python tests/check_endpoint_compatibility.py

The API key is never printed. This is deliberately a standalone manual tool,
not a pytest test, because it sends real requests to the configured endpoint.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import requests

# ---------------------------------------------------------------------------
# Edit these values before running the script.
# ---------------------------------------------------------------------------
ENDPOINT = "http://1.95.77.23:3000"
API_KEY = "sk-GlQn5Ov7O1qK1nKfrEpMqgpLek0vxygRvSLdPp5BTLc39aqv"
MODEL = "qwen3.5-397b-a17b"

# Select any subset of: "model", "openai", "hacking".
FORMATS_TO_TEST = ("model", "openai", "hacking")
TIMEOUT_SECONDS = 60.0
VERIFY_TLS = True


@dataclass(frozen=True)
class ProbeResult:
    name: str
    config_field: str
    url: str
    compatible: bool
    status_code: int | None
    elapsed_seconds: float
    detail: str


def main() -> int:
    endpoint_root = validate_configuration()
    probes = {
        "model": probe_anthropic_messages,
        "openai": probe_openai_structured_output,
        "hacking": probe_hacking_chat_completion,
    }

    unknown = sorted(set(FORMATS_TO_TEST) - probes.keys())
    if unknown:
        raise ValueError(f"Unknown FORMATS_TO_TEST values: {', '.join(unknown)}")

    results = [probes[name](endpoint_root) for name in FORMATS_TO_TEST]
    print("\nSWE-gen endpoint compatibility\n")
    for result in results:
        status = "PASS" if result.compatible else "FAIL"
        print(f"[{status}] {result.config_field}: {result.name}")
        print(f"  URL: {result.url}")
        print(f"  Result: {result.detail}")
        print(f"  Time: {result.elapsed_seconds:.2f}s")

    passed = sum(result.compatible for result in results)
    print(f"\nCompatible formats: {passed}/{len(results)}")
    return 0 if passed == len(results) else 1


def validate_configuration() -> str:
    endpoint = ENDPOINT.strip().rstrip("/")
    parsed = urlsplit(endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("ENDPOINT must be an absolute http:// or https:// URL")
    if parsed.username or parsed.password:
        raise ValueError("ENDPOINT must not contain embedded credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("ENDPOINT must not contain a query string or fragment")
    if API_KEY.strip() in {"", "replace_me"}:
        raise ValueError("Set API_KEY at the top of this file before running it")
    if MODEL.strip() in {"", "replace-with-model-id"}:
        raise ValueError("Set MODEL at the top of this file before running it")

    # Accept either https://host or https://host/v1 as the manually entered base.
    return endpoint[:-3] if endpoint.endswith("/v1") else endpoint


def probe_anthropic_messages(endpoint_root: str) -> ProbeResult:
    url = f"{endpoint_root}/v1/messages"
    headers = {
        "Content-Type": "application/json",
        "x-api-key": API_KEY,
        "anthropic-version": "2023-06-01",
    }
    payload = {
        "model": MODEL,
        "max_tokens": 16,
        "messages": [{"role": "user", "content": "Reply with exactly OK."}],
    }
    return run_probe(
        name="Anthropic Messages API",
        config_field="[model]",
        url=url,
        headers=headers,
        payload=payload,
        validator=validate_anthropic_response,
    )


def probe_openai_structured_output(endpoint_root: str) -> ProbeResult:
    url = f"{endpoint_root}/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": MODEL,
        "messages": [
            {
                "role": "user",
                "content": 'Return a JSON object with the single field "ok" set to true.',
            }
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "swegen_endpoint_probe",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": {"ok": {"type": "boolean"}},
                    "required": ["ok"],
                    "additionalProperties": False,
                },
            },
        },
        "max_completion_tokens": 64,
    }
    return run_probe(
        name="OpenAI structured chat completions",
        config_field="[openai]",
        url=url,
        headers=headers,
        payload=payload,
        validator=validate_openai_structured_response,
    )


def probe_hacking_chat_completion(endpoint_root: str) -> ProbeResult:
    url = f"{endpoint_root}/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": MODEL,
        "messages": [
            {
                "role": "user",
                "content": (
                    "Return only this JSON object: "
                    '{"is_hacking":false,"test_framework":"pytest",'
                    '"reason":"compatibility probe"}'
                ),
            }
        ],
    }
    return run_probe(
        name="OpenAI chat completions with hacking-check JSON",
        config_field="[[hacking.llm]]",
        url=url,
        headers=headers,
        payload=payload,
        validator=validate_hacking_response,
    )


def run_probe(
    *,
    name: str,
    config_field: str,
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    validator: Callable[[Any], str],
) -> ProbeResult:
    started = time.monotonic()
    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=TIMEOUT_SECONDS,
            verify=VERIFY_TLS,
        )
        elapsed = time.monotonic() - started
        if not 200 <= response.status_code < 300:
            return ProbeResult(
                name,
                config_field,
                url,
                False,
                response.status_code,
                elapsed,
                http_error_detail(response),
            )

        try:
            detail = validator(response.json())
        except (KeyError, TypeError, ValueError) as exc:
            return ProbeResult(
                name,
                config_field,
                url,
                False,
                response.status_code,
                elapsed,
                f"HTTP {response.status_code}, but response format was invalid: {sanitize(str(exc))}",
            )
        return ProbeResult(
            name,
            config_field,
            url,
            True,
            response.status_code,
            elapsed,
            detail,
        )
    except requests.RequestException as exc:
        return ProbeResult(
            name,
            config_field,
            url,
            False,
            None,
            time.monotonic() - started,
            f"Request failed: {sanitize(str(exc))}",
        )


def validate_anthropic_response(data: Any) -> str:
    content = data["content"]
    if not isinstance(content, list):
        raise ValueError("'content' is not a list")
    text_blocks = [
        block.get("text", "")
        for block in content
        if isinstance(block, dict) and block.get("type") == "text"
    ]
    if not any(isinstance(text, str) and text.strip() for text in text_blocks):
        raise ValueError("no non-empty Anthropic text block was returned")
    return "Valid Anthropic Messages response"


def validate_openai_structured_response(data: Any) -> str:
    content = openai_message_content(data)
    parsed = json.loads(content)
    if not isinstance(parsed, dict) or not isinstance(parsed.get("ok"), bool):
        raise ValueError('structured content did not match {"ok": boolean}')
    return "Valid OpenAI JSON Schema structured-output response"


def validate_hacking_response(data: Any) -> str:
    content = openai_message_content(data).strip()
    start = content.find("{")
    end = content.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("message content did not contain a JSON object")
    parsed = json.loads(content[start : end + 1])
    if not isinstance(parsed, dict) or "is_hacking" not in parsed:
        raise ValueError("JSON response did not contain 'is_hacking'")
    return "Valid OpenAI chat response containing hacking-check JSON"


def openai_message_content(data: Any) -> str:
    content = data["choices"][0]["message"]["content"]
    if not isinstance(content, str) or not content.strip():
        raise ValueError("no non-empty choices[0].message.content was returned")
    return content


def http_error_detail(response: requests.Response) -> str:
    text = response.text
    try:
        data = response.json()
        if isinstance(data, dict):
            error = data.get("error", data)
            if isinstance(error, dict):
                text = str(error.get("message") or error.get("detail") or error)
            else:
                text = str(error)
    except (TypeError, ValueError):
        pass
    clean = sanitize(text)
    return f"HTTP {response.status_code}" + (f": {clean}" if clean else "")


def sanitize(value: str, limit: int = 500) -> str:
    clean = " ".join(value.replace(API_KEY, "[REDACTED]").split())
    return clean if len(clean) <= limit else clean[:limit] + "..."


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as exc:
        print(f"Configuration error: {exc}")
        raise SystemExit(2) from None
