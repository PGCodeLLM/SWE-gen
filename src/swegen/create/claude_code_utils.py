from __future__ import annotations

import re
from typing import Any

from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    SystemMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)

URL_CREDENTIAL_RE = re.compile(r"(?i)\b((?:https?|socks5h?)://)[^/@\s'\"<>]+@")
TOKEN_RE = re.compile(
    r"(?i)\b(?:ghp_[A-Za-z0-9_-]{10,}|github_pat_[A-Za-z0-9_]{10,}|sk-[A-Za-z0-9_-]{10,})"
)
BEARER_RE = re.compile(r"(?i)(\bAuthorization[\"']?\s*[:=]\s*[\"']?Bearer\s+)[A-Za-z0-9._~+/=-]+")
QUOTED_SECRET_RE = re.compile(
    r"(?i)(\b(?:[A-Z][A-Z0-9_]*(?:API_KEY|AUTH_TOKEN|ACCESS_TOKEN)|GITHUB_TOKEN)\s*=\s*[\"'])[^\"']+([\"'])"
)
UNQUOTED_SECRET_RE = re.compile(
    r"(?i)(\b(?:[A-Z][A-Z0-9_]*(?:API_KEY|AUTH_TOKEN|ACCESS_TOKEN)|GITHUB_TOKEN)\s*=\s*)[^\s\"'`]+"
)
MAPPING_SECRET_RE = re.compile(
    r"(?i)([\"'](?:[A-Z][A-Z0-9_]*(?:API_KEY|AUTH_TOKEN|ACCESS_TOKEN)|GITHUB_TOKEN)[\"']\s*:\s*[\"'])[^\"']+([\"'])"
)


def redact_sensitive_text(text: str) -> str:
    """Remove proxy userinfo and common API-token forms from persisted output."""
    text = URL_CREDENTIAL_RE.sub(r"\1<REDACTED>@", text)
    text = BEARER_RE.sub(r"\1<REDACTED>", text)
    text = QUOTED_SECRET_RE.sub(r"\1<REDACTED>\2", text)
    text = UNQUOTED_SECRET_RE.sub(r"\1<REDACTED>", text)
    text = MAPPING_SECRET_RE.sub(r"\1<REDACTED>\2", text)
    return TOKEN_RE.sub("<REDACTED>", text)


def _redact_value(value: Any) -> Any:
    if isinstance(value, str):
        return redact_sensitive_text(value)
    if isinstance(value, dict):
        return {key: _redact_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact_value(item) for item in value)
    return value


# ANSI color codes for verbose output
class Colors:
    BLUE = "\033[94m"  # Assistant messages
    CYAN = "\033[96m"  # Tool use
    MAGENTA = "\033[95m"  # Tool results
    GREEN = "\033[92m"  # Success/final result
    YELLOW = "\033[93m"  # System messages
    RED = "\033[91m"  # Errors
    BOLD = "\033[1m"
    RESET = "\033[0m"


def print_sdk_message(message: object) -> None:
    """Print SDK messages with colored formatting.

    Message types include:
    - AssistantMessage: Claude's text responses and tool uses
    - UserMessage: User messages (for tool results in SDK)
    - ResultMessage: Final result message
    - SystemMessage: System notifications
    """
    if isinstance(message, AssistantMessage):
        # Assistant message content
        for block in message.content:
            if isinstance(block, TextBlock):
                text = redact_sensitive_text(block.text)
                if text.strip():
                    print(f"\n{Colors.BLUE}[Assistant]{Colors.RESET} {text}", flush=True)
            elif isinstance(block, ToolUseBlock):
                tool_name = block.name.upper()
                tool_input = _redact_value(block.input)
                # Less aggressive truncation - commands are important!
                summary: dict | str
                if isinstance(tool_input, dict):
                    # For bash commands, show up to 2000 chars; for other inputs, 1000 chars
                    max_len = 2000 if tool_name.lower() == "bash" else 1000
                    summary = {
                        k: (v[:max_len] + "..." if isinstance(v, str) and len(v) > max_len else v)
                        for k, v in tool_input.items()
                    }
                else:
                    summary = redact_sensitive_text(str(tool_input))[:2000]
                print(
                    f"\n{Colors.CYAN}{Colors.BOLD}{tool_name}{Colors.RESET}: {summary}",
                    flush=True,
                )

    elif isinstance(message, UserMessage):
        # In SDK mode, tool results come as UserMessage with ToolResultBlock
        for block in message.content:
            if isinstance(block, ToolResultBlock):
                content = block.content if hasattr(block, "content") else str(block)
                content = _redact_value(content)
                # Less aggressive truncation - show up to 2000 chars
                if isinstance(content, str) and len(content) > 2000:
                    content = content[:2000] + f"... ({len(content)} chars total)"
                print(f"{Colors.MAGENTA}[Tool Result]{Colors.RESET} {content}", flush=True)
            elif isinstance(block, TextBlock):
                text = redact_sensitive_text(block.text)
                if text.strip():
                    print(f"{Colors.MAGENTA}[Tool Result]{Colors.RESET} {text}", flush=True)

    elif isinstance(message, ResultMessage):
        # Final result message
        result_text = redact_sensitive_text(str(getattr(message, "text", message)))
        if result_text and result_text.strip():
            if len(result_text) > 3000:
                result_text = result_text[:3000] + f"... ({len(result_text)} chars total)"
            print(
                f"\n{Colors.GREEN}{Colors.BOLD}[Final Result]{Colors.RESET}\n{result_text}",
                flush=True,
            )

    elif isinstance(message, SystemMessage):
        # System messages
        msg_text = redact_sensitive_text(str(getattr(message, "text", message)))
        if msg_text:
            print(f"{Colors.YELLOW}[System]{Colors.RESET} {msg_text}", flush=True)
