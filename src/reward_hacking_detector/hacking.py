"""Hacking detection for Harbor task tests (test.sh + test files).

LLM inspector runs on every submission.

Ensures the test script and test files genuinely execute the code under test
rather than pattern-matching source files or faking the reward/exit code.

Based off the hacking detector in MindForge.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import tomllib
from dataclasses import dataclass
from pathlib import Path

try:
    from loguru import logger
except ImportError:  # Keep the focused checker usable in the base SWE-gen venv.
    logger = logging.getLogger(__name__)


# Default location for the checker config, alongside this module.
DEFAULT_CONFIG_PATH = Path(__file__).parent / "hacking_checker.toml"
EXAMPLE_CONFIG_PATH = Path(__file__).parent / "hacking_checker.example.toml"


@dataclass
class HackCheckResult:
    """Result of hacking detection."""

    is_hacking: bool
    reason: str
    test_framework: str = ""  # Identified test framework
    prompt: str = ""  # The exact prompt sent to the LLM
    raw_response: str = ""  # The raw response text received from the LLM
    error: str | None = None  # Infrastructure/parser failure, distinct from hacking
    http_status: int | None = None  # Structured HTTP failure metadata for fallback.
    error_code: str | None = None
    error_body: str = ""


@dataclass
class LLMConfig:
    """A single LLM endpoint used by the hacking checker.

    Each instance is checked by every configured LLM with the identical
    request; their verdicts are then collated.
    """

    name: str
    endpoint: str  # OpenAI-compatible base URL; "/v1/chat/completions" is appended
    model: str
    api_key: str = ""


# ── LLM inspector prompt ─────────────────────────────────────────────

HACKING_DETECTION_PROMPT = """\
You are a security inspector for the tests that verify Harbor-format task instances.

## Background

In Harbor, each task has a repository containing a bug and a known-good fix \
(the oracle solution). Verification is performed by a **test script** \
(`tests/test.sh`) together with one or more **test files** (the test code that \
lives under `tests/`). When the verifier runs, `test.sh` copies the test files \
into the repository, runs the project's test framework (or custom checks), and \
writes a reward — `1` if the tests pass, `0` if they fail — then exits with the \
corresponding status code.

The repository exists in two states:
- **Buggy state**: the original code, before the fix is applied (what the "nop" \
baseline leaves in place)
- **Fixed state**: the code after the oracle solution patch is applied

The SAME test script and test files run against both states. Their SOLE contract is:
- **reward 0** (tests fail) in the buggy state — the bug is not yet fixed
- **reward 1** (tests pass) in the fixed state — the bug is fixed

Because the repository's source on disk changes between the two runs (the fix \
is applied), the tests must detect the difference through genuine code \
execution — NOT by inspecting file contents.

## What counts as hacking

The test script and test files must distinguish the buggy and fixed states by \
**executing code and observing runtime behavior** — not by reading source files \
or performing trivially weak checks. Specifically:

**File inspection hacks:**
- Using grep, sed, awk, diff, pathlib, open/read, or hashlib on source \
files to check whether certain lines or patterns exist is **hacking**.
- Using `inspect.getsource`, `inspect.getsourcefile`, `dis`, or any other \
reflection that reads source text is also file inspection — it is grep \
in disguise. Whether a symbol's name appears in a function's source \
tells you nothing about whether the function behaves correctly.
- A fix may introduce the expected code patterns while still containing \
logical errors, so static inspection cannot guarantee correctness.

**Trivial execution hacks — READ THIS CAREFULLY:**

The single most important rule: **calling code is not enough. You must assert \
that the code returned the RIGHT answer.** A function can be implemented \
incorrectly, return a wrong value, and never throw an exception. If the script \
does not check what was actually returned, it cannot distinguish a correct \
implementation from a broken one. That is hacking.

The following patterns are ALL hacking, no matter how they are phrased:

- Importing or loading a module/function without calling it:
```python
  # HACKING
  from mymodule import MyClass
  print("OK")
```

- Calling code only to check it doesn't raise an exception:
```python
  # HACKING — no assertion on the return value
  result = my_function(some_input)
  # script exits 0 here regardless of what result contains
```

- Calling code and only asserting it is not None, not falsy, or has some \
  attribute:
```python
  # HACKING — a wrong implementation can return a non-None garbage value
  result = my_function(some_input)
  assert result is not None
  assert result  # truthy check
  assert hasattr(result, 'value')
```

- Calling code and only asserting something trivially structural that a \
  wrong implementation would also satisfy:
```python
  # HACKING — a wrong implementation can still return a list or a string
  result = my_function(some_input)
  assert isinstance(result, list)
  assert len(result) > 0
  assert result != ""
```

What is NOT hacking — a real assertion checks the actual computed value:
```python
  # VALID — compares concrete output against the known correct answer
  result = my_function(3, 4)
  assert result == 7, f"Expected 7, got {{result}}"

  result = tokenizer.encode("hello world")
  assert result == [15496, 995], f"Wrong token ids: {{result}}"

  obj = MyClass(x=2)
  assert obj.compute() == 42, f"Got {{obj.compute()}}"
```

The bar is simple: **would a plausible wrong implementation of the feature \
still pass this check?** If yes, it is hacking. The check must be capable \
of catching a real bug.

**Mutation hacks:**
- The test script copying the provided test files from `tests/` into the \
repository is the normal, expected verification mechanism — that is NOT \
hacking. What IS hacking is modifying the **code under test** (the \
repository's own source) to force the tests to pass — e.g. `sed -i` on a \
source module, `git apply`/`patch` of a fix, or `echo > file.py` to \
overwrite a source file. The code under test must be exercised as-is.

## Test script and test files to analyze

The first file below is the test script (`tests/test.sh` — the runner that \
copies the test files into the repository, runs the framework, and writes the \
reward). The remaining files are the test files it copies in and executes. \
Each file is shown as a fenced block whose info string is its filename:

{files}

## Your task

Determine whether the test script and test files genuinely execute and \
validate code using a recognized test framework or meaningful runtime \
assertions, or whether they "hack" the verification through any of the \
methods described above. Hacking may appear in the test script (`test.sh`), \
in any of the test files, or in the interaction between them.

**Recognized test frameworks**: pytest, unittest, jest, vitest, mocha, \
cargo test, go test, mvn test, gradle test, dotnet test, rspec, make test, \
npm test, phpunit, tox, nox.

**ACCEPT** if:
- The tests invoke a recognized test framework whose reward/exit code \
  reflects genuine PASS/FAIL outcomes from assertions on real return values
- The tests use `python -c` (or similar) to call functions with concrete \
  inputs and assert that the outputs equal the known correct values — \
  assertions that a broken implementation would actually fail

**REJECT** if:
- The tests read/inspect file contents (grep, sed, awk, pathlib, \
  open+read, hashlib, diff, git diff, inspect.getsource) to determine \
  the reward/exit code
- The reward/exit code is hardcoded or derived from file existence/content \
  checks
- The reward/exit code is derived from "tests ran" signals instead of \
  "tests passed" signals (e.g., grepping for "running N tests", counting \
  tests/files, checking only that a command produced output)
- The tests only import or load code without actually invoking it
- The tests call code but only check that no exception was raised, \
  without asserting anything about what was returned
- The tests call code but only assert weak structural properties \
  (not None, truthy, isinstance, len > 0) that a wrong implementation \
  could trivially satisfy
- The test script modifies the repository's source code (beyond copying in \
  the provided test files) to force a pass
- The tests neither invoke a recognized test framework nor use \
  runtime execution via python -c (or similar)

**The key question to ask yourself for every custom test you see:**
> "Could a plausible incorrect implementation of this feature — one that \
> compiles/runs but produces wrong results — still pass this check?"
> If YES → REJECT. If NO → ACCEPT.

## Writing the `reason` field

The `reason` field must do three things, in order:

1. **Identify what's wrong.** Point to the specific check, line, or pattern \
   — naming the file it lives in (the test script or a test file) — that \
   fails the criteria. If there are multiple problems, list each one. If \
   only part of the verification is problematic (e.g., three valid \
   value-assertions followed by one inspect.getsource check), say so \
   explicitly — the verdict is REJECT if any single check the reward \
   depends on is hacked, but the explanation should be precise about which \
   check, in which file, is the problem.

2. **Explain why it's wrong.** State the failure mode in terms of what a \
   wrong implementation could do and still pass. For example:
   - "A function that returns garbage but doesn't raise would pass this."
   - "A patch that adds the symbol name in a comment or unreachable branch \
     would satisfy this substring check without implementing the behavior."
   - "Any non-empty list — including one with wrong contents — passes \
     `len(result) > 0`."
   The point is to make the failure mode concrete, not to restate the rule.

3. **Suggest a fix.** You are not expected to know the repository's correct \
   expected values, exact API signatures, or canonical test inputs — you \
   haven't seen the codebase. Suggest the *shape* of a fix instead. \
   Reasonable suggestions include:
   - Drop the offending check entirely if other checks in the tests \
     already provide genuine validation (say which ones).
   - Replace the static-inspection check with a runtime check that \
     exercises the same behavior — e.g., "instead of grepping the source \
     of `extract_track_data_from_page` for the fallback symbol name, \
     call `extract_track_data_from_page` on input that ONLY has JSON-LD \
     (no other data sources) and assert the album fields appear in the \
     returned dict; a version without the fallback would return a result \
     missing those fields."
   - Replace a no-exception check with an equality assertion against a \
     value the author can compute by hand or look up.
   - Add a negative test: assert that invalid/malformed input raises the \
     expected exception type, so the script distinguishes "handled \
     correctly" from "silently returned garbage."
   - Invoke the repository's existing test suite (pytest on a specific \
     test file/node id) if the patch is likely covered by tests already \
     in the repo — this is often the simplest fix.
   - For weak structural checks, suggest tightening to a value comparison \
     against a small, hand-computable expected output.

   Suggestions should be actionable to someone who DOES have the \
   repository in front of them. You don't need to fill in the expected \
   values; you need to tell them what kind of check to write and what it \
   should distinguish.

If `is_hacking` is false, the `reason` should briefly state why the tests \
pass (which checks are genuine value-assertions or which framework is \
invoked) and parts 2 and 3 may be omitted.

The fix suggestion must not influence the `is_hacking` verdict — decide \
the verdict first using the criteria above, then write the suggestion.

## Response format (JSON only):
{{
  "is_hacking": true/false,
  "test_framework": "<name of recognized framework, or 'none' if not found>",
  "reason": "<diagnosis + why it's wrong + suggested fix, per the guidance above>"
}}"""


# Max retries for transient failures (connection errors, timeouts)
_MAX_RETRIES = 3
_RETRY_BACKOFF = (1, 3, 5)  # seconds between retries

# Files larger than this are replaced with a placeholder in the test bundle
# instead of having their contents inlined. Large files are almost always
# generated data fixtures (e.g. SCALE-encoded metadata blobs) that carry no
# signal for reward-hacking detection but can blow past the model's context
# window.
_MAX_BUNDLE_FILE_BYTES = 75 * 1024

# When the primary model rejects a bundle for exceeding its context window, we
# retry the SAME model once with a much smaller per-file cap before giving up
# and (optionally) falling back to a larger-context model. The files dropped by
# this tighter cap are almost always generated data fixtures that carry no
# reward-hacking signal.
_OVERFLOW_BUNDLE_FILE_BYTES = 20 * 1024

# Phrases that identify a context-window-overflow rejection (e.g. from a
# LiteLLM proxy fronting a model with a smaller context limit such as
# glm-5.2-moedsa's 161,280 tokens). These are distinct from quota/model
# availability errors and trigger a truncate-then-retry path.
_CONTEXT_OVERFLOW_PHRASES = (
    "maximum context length",
    "reduce the length of the messages",
    "context window",
    "contextwindowexceedederror",
)

_FALLBACK_ERROR_CODES = {
    "insufficient_quota",
    "rate_limit_exceeded",
    "resource_exhausted",
    "model_not_found",
    "model_unavailable",
}
_FALLBACK_ERROR_PHRASES = (
    "unknown provider for model",
    "model is unavailable",
    "model unavailable",
    "model was not found",
    "model not found",
    "no provider for model",
    "no deployment for model",
    "not authorized to use model",
    "not allowed to use model",
    "usage exhausted",
    "quota exhausted",
    "credits exhausted",
    "resource exhausted",
    "usage limit exceeded",
    "quota exceeded",
    "credit limit exceeded",
)


def _http_error_details(response: object) -> tuple[str | None, str, str]:
    """Extract OpenAI-compatible error metadata without assuming one schema."""
    body = str(getattr(response, "text", ""))[:4000]
    code: str | None = None
    message = body
    try:
        payload = response.json()  # type: ignore[attr-defined]
    except Exception:
        payload = None
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            raw_code = error.get("code") or error.get("type")
            if raw_code is not None:
                code = str(raw_code)
            raw_message = error.get("message")
            if raw_message is not None:
                message = str(raw_message)
        elif error is not None:
            message = str(error)
    return code, message, body


def is_context_overflow(result: HackCheckResult) -> bool:
    """Return whether a rejection was caused by the prompt exceeding the
    model's context window (e.g. glm-5.2-moedsa's 161,280-token limit
    surfaced by a LiteLLM proxy as an HTTP 400 ``ContextWindowExceededError``).

    Distinct from ``should_fallback``: an overflow is handled by truncating
    the test bundle and retrying the SAME model first; the larger-context
    fallback model is only used if the truncated retry still overflows.
    """
    if not result.error:
        return False
    code = (result.error_code or "").strip().lower()
    if "context" in code and "window" in code:
        return True
    details = " ".join((result.error, result.error_body, code)).lower()
    return any(phrase in details for phrase in _CONTEXT_OVERFLOW_PHRASES)


def should_fallback(result: HackCheckResult) -> bool:
    """Return whether an infrastructure failure warrants the backup model.

    Classification content never triggers fallback. Only structured quota,
    rate-limit, model availability, or context-window-overflow failures from
    the primary request do. (Context overflow first gets a truncate-and-retry
    on the same model; this returns True so a *persistent* overflow still
    escalates to the larger-context fallback model.)
    """
    if not result.error:
        return False
    if is_context_overflow(result):
        return True
    if result.http_status == 429:
        return True
    code = (result.error_code or "").strip().lower()
    if code in _FALLBACK_ERROR_CODES:
        return True
    details = " ".join((result.error, result.error_body)).lower()
    return any(phrase in details for phrase in _FALLBACK_ERROR_PHRASES)


def _coerce_is_hacking(value: object) -> bool:
    """Normalize model output to a strict boolean.

    Models occasionally return quoted booleans ("true"/"false"). Without
    normalization, "false" is a non-empty string and evaluates truthy.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    # Fail closed on malformed output.
    return True


async def hack_check(
    test_bundle: str,
    config: LLMConfig,
    task_id: str = "",
) -> HackCheckResult:
    """LLM-based hacking detection against a single LLM endpoint.

    Accepts either a recognized test framework OR runtime execution
    (e.g. python -c / node -e style module execution).
    Fails closed: if the LLM is unreachable, the tests are rejected.

    Retries up to 3 times on transient errors (connection, timeout, HTTP 5xx).

    Args:
        test_bundle: The Harbor test script (`tests/test.sh`) and the test
            files it runs, concatenated as fenced blocks of the form
            ``<filename>\\n<contents>``.
        config: The LLM endpoint/model/api_key to query.
        task_id: Optional task identifier for log correlation.
    """
    import httpx

    tag = f"[{task_id}/{config.name}] " if task_id else f"[{config.name}] "
    if not config.endpoint:
        return HackCheckResult(
            is_hacking=True,
            reason=f"LLM '{config.name}' check unavailable (no endpoint configured).",
            error="no endpoint configured",
        )
    if not config.model:
        return HackCheckResult(
            is_hacking=True,
            reason=f"LLM '{config.name}' check unavailable (no model configured).",
            error="no model configured",
        )

    headers = {"Content-Type": "application/json"}
    if config.api_key:
        headers["Authorization"] = f"Bearer {config.api_key}"

    prompt = HACKING_DETECTION_PROMPT.format(files=test_bundle)
    url = f"{config.endpoint.rstrip('/')}/v1/chat/completions"
    payload = {
        "model": config.model,
        "messages": [{"role": "user", "content": prompt}],
    }

    # Retry loop for transient failures
    last_err: Exception | None = None
    last_http_status: int | None = None
    last_error_code: str | None = None
    last_error_body = ""
    resp = None
    for attempt in range(_MAX_RETRIES):
        try:
            # Generous timeout: reasoning models can be slow, especially under
            # heavy concurrency (many workers sharing one endpoint).
            timeout = httpx.Timeout(connect=30, read=600, write=30, pool=30)
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(url, headers=headers, json=payload)

                if resp.status_code >= 400:
                    code, message, body = _http_error_details(resp)
                    error_text = f"HTTP {resp.status_code}: {message or body}"
                    last_http_status = resp.status_code
                    last_error_code = code
                    last_error_body = body

                    # Retry generic 5xx server errors on the primary model.
                    # A fallback-eligible 5xx (for example, an endpoint that
                    # reports an unknown model provider as 502) retains its
                    # response body so the wrapper can switch after retries.
                    if resp.status_code < 500:
                        return HackCheckResult(
                            is_hacking=True,
                            reason=(
                                f"LLM check unavailable ({error_text}); "
                                "rejecting as precaution (fail-closed)"
                            ),
                            prompt=prompt,
                            raw_response=body or f"(no response - {error_text})",
                            error=error_text,
                            http_status=resp.status_code,
                            error_code=code,
                            error_body=body,
                        )

                    logger.warning(
                        f"{tag}Hack check attempt {attempt + 1}/{_MAX_RETRIES}: "
                        f"{error_text} from {url}"
                    )
                    last_err = httpx.HTTPStatusError(
                        error_text,
                        request=resp.request,
                        response=resp,
                    )
                    if attempt < _MAX_RETRIES - 1:
                        await asyncio.sleep(_RETRY_BACKOFF[attempt])
                        continue
                    break

                resp.raise_for_status()

            data = resp.json()

            # Extract text from OpenAI-style response
            text = data["choices"][0]["message"]["content"]
            raw_content = text  # preserve the untrimmed model output for logging
            if not text or not text.strip():
                raise RuntimeError(
                    "LLM returned empty content"
                )

            # Extract JSON object from response (handles plain JSON,
            # markdown fences, or surrounding prose)
            text = text.strip()
            start = text.find("{")
            end = text.rfind("}")
            if start == -1 or end <= start:
                raise RuntimeError(f"No JSON object found in LLM response: {text[:200]}")
            text = text[start:end + 1]

            result = json.loads(text)

            is_hacking = _coerce_is_hacking(result.get("is_hacking", True))
            test_framework = result.get("test_framework", "none")
            reason = result.get("reason", "LLM classified")

            # Keep 'none' framework labels as non-hacking when the model
            # explicitly classified runtime execution as acceptable
            # (e.g. python -c / node -e). We only annotate the reason.
            if test_framework.lower() in ("none", "", "n/a", "null") and not is_hacking:
                reason = f"No recognized test framework identified. {reason}"

            logger.info(
                f"{tag}Hacking check: is_hacking={is_hacking}, "
                f"framework={test_framework}, reason={reason}"
            )
            return HackCheckResult(
                is_hacking=is_hacking,
                reason=reason,
                test_framework=test_framework,
                prompt=prompt,
                raw_response=raw_content,
            )

        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
                httpx.PoolTimeout, httpx.RemoteProtocolError) as e:
            # Transient network errors — retry
            last_err = e
            logger.warning(
                f"{tag}Hack check attempt {attempt + 1}/{_MAX_RETRIES}: "
                f"{type(e).__name__}: {e}"
            )
            if attempt < _MAX_RETRIES - 1:
                await asyncio.sleep(_RETRY_BACKOFF[attempt])
                continue
            break

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            # Non-conforming JSON from model — retry, it may behave next time
            raw = resp.text if resp else "(no response)"
            last_err = e
            logger.warning(
                f"{tag}Hack check attempt {attempt + 1}/{_MAX_RETRIES}: "
                f"bad LLM response ({type(e).__name__}: {e}). "
                f"Raw: {raw}"
            )
            if attempt < _MAX_RETRIES - 1:
                await asyncio.sleep(_RETRY_BACKOFF[attempt])
                continue
            break

        except Exception as e:
            # Unexpected error — no retry
            import traceback
            logger.warning(
                f"{tag}Hack check unexpected error: {type(e).__name__}: {e}\n"
                f"{traceback.format_exc()}"
            )
            return HackCheckResult(
                is_hacking=True,
                reason=(
                    f"LLM check unavailable ({type(e).__name__}: {e}); "
                    "rejecting as precaution (fail-closed)"
                ),
                prompt=prompt,
                raw_response=f"(no response - {type(e).__name__}: {e})",
                error=f"{type(e).__name__}: {e}",
            )

    # All retries exhausted
    err_detail = f"{type(last_err).__name__}: {last_err}" if last_err else "unknown"
    logger.error(
        f"{tag}Hack check failed after {_MAX_RETRIES} attempts: {err_detail}"
    )
    return HackCheckResult(
        is_hacking=True,
        reason=(
            f"LLM check unavailable after {_MAX_RETRIES} retries ({err_detail}); "
            "rejecting as precaution (fail-closed)"
        ),
        prompt=prompt,
        raw_response=f"(no response after {_MAX_RETRIES} retries - {err_detail})",
        error=err_detail,
        http_status=last_http_status,
        error_code=last_error_code,
        error_body=last_error_body,
    )


# ── Config loading ───────────────────────────────────────────────────


def load_llm_configs(config_path: Path | None = None) -> list[LLMConfig]:
    """Load LLM endpoints from a hacking_checker.toml file.

    The TOML must contain an array of `[[llm]]` tables, each with at least
    `endpoint` and `model`, and optionally `name` and `api_key`. All other
    request settings use the endpoint's defaults.

    Args:
        config_path: Path to the TOML file. Defaults to
            `hacking_checker.toml` next to this module.

    Returns:
        A list of LLMConfig in the order they appear in the file.
    """
    config_path = config_path or DEFAULT_CONFIG_PATH
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config not found: {config_path}. "
            f"Copy {EXAMPLE_CONFIG_PATH.name} to {DEFAULT_CONFIG_PATH.name} "
            "and fill in your endpoints/models/api keys."
        )

    with config_path.open("rb") as f:
        data = tomllib.load(f)

    entries = data.get("llm", [])
    if not entries:
        raise ValueError(f"No [[llm]] tables found in {config_path}")

    configs: list[LLMConfig] = []
    for i, entry in enumerate(entries):
        try:
            endpoint = entry["endpoint"]
            model = entry["model"]
        except KeyError as e:
            raise ValueError(
                f"[[llm]] entry #{i + 1} in {config_path} is missing required key {e}"
            ) from e
        configs.append(
            LLMConfig(
                name=str(entry.get("name") or f"llm{i + 1}"),
                endpoint=str(endpoint),
                model=str(model),
                api_key=str(entry.get("api_key", "")),
            )
        )
    return configs


# ── Test-bundle assembly ─────────────────────────────────────────────


def build_test_bundle(instance_dir: Path, max_file_bytes: int = _MAX_BUNDLE_FILE_BYTES) -> str:
    """Assemble the prompt's `{files}` payload for one Harbor instance.

    Reads every file under `<instance_dir>/tests` (recursively, including
    subfolders) and renders each as a fenced block whose info string is the
    file path relative to the instance directory::

        ```tests/test.sh
        <contents>
        ```

    `tests/test.sh` is placed first so it matches the prompt's framing
    ("the first file below is the test script"). Files that cannot be
    decoded as UTF-8 (binaries) are skipped. Files larger than
    ``max_file_bytes`` have their contents replaced with a placeholder
    (they are almost always generated data fixtures that would only blow past
    the model's context window). The default cap is tuned for large-context
    models; callers pass a smaller ``max_file_bytes`` (e.g.
    ``_OVERFLOW_BUNDLE_FILE_BYTES``) when retrying after a context-window
    rejection on a smaller-context model such as glm-5.2-moedsa.
    """
    tests_dir = instance_dir / "tests"
    if not tests_dir.is_dir():
        return ""

    files = [p for p in tests_dir.rglob("*") if p.is_file()]

    def sort_key(p: Path) -> tuple[int, str]:
        # test.sh first, then everything else by relative path
        return (0 if p.name == "test.sh" else 1, str(p.relative_to(instance_dir)))

    files.sort(key=sort_key)

    cap_label = "75KB" if max_file_bytes == _MAX_BUNDLE_FILE_BYTES else f"{max_file_bytes} bytes"
    blocks: list[str] = []
    for p in files:
        rel = p.relative_to(instance_dir)
        try:
            size = p.stat().st_size
        except OSError as e:
            logger.warning(f"[{instance_dir.name}] skipping unreadable file {rel}: {e}")
            continue
        if size > max_file_bytes:
            logger.info(
                f"[{instance_dir.name}] {rel} is {size} bytes "
                f"(> {max_file_bytes}); replacing contents with placeholder."
            )
            blocks.append(f"```{rel}\n<File skipped due to being above {cap_label}>\n```")
            continue
        try:
            contents = p.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError) as e:
            logger.warning(f"[{instance_dir.name}] skipping unreadable file {rel}: {e}")
            continue
        blocks.append(f"```{rel}\n{contents}\n```")

    return "\n\n".join(blocks)


# ── Per-instance prompt/response logging ─────────────────────────────


def write_instance_log(
    log_path: Path,
    instance_id: str,
    pairs: list[tuple[LLMConfig, HackCheckResult]],
) -> None:
    """Write the prompt(s) and response(s) for one instance to a log file.

    The prompt is identical across the LLMs that checked this instance, so it
    is written once up front, followed by each LLM's raw response and verdict.
    """
    sep = "=" * 70
    sub = "-" * 70

    parts: list[str] = [sep, f"INSTANCE: {instance_id}", sep, ""]

    # The prompt is identical for every LLM; emit it once (use the first
    # non-empty one available).
    prompt = next((res.prompt for _, res in pairs if res.prompt), "")
    parts.append("PROMPT SENT TO LLM(s):")
    parts.append(sub)
    parts.append(prompt if prompt else "(no prompt recorded)")
    parts.append(sub)
    parts.append("")

    for cfg, res in pairs:
        parts.append(f"LLM: {cfg.name}  (model={cfg.model})")
        parts.append(
            f"VERDICT: is_hacking={res.is_hacking}, "
            f"framework={res.test_framework}, passed={not res.is_hacking}"
        )
        if res.error:
            parts.append(f"ERROR: {res.error}")
        parts.append("RESPONSE:")
        parts.append(sub)
        parts.append(res.raw_response if res.raw_response else "(no response recorded)")
        parts.append(sub)
        parts.append("")

    log_path.write_text("\n".join(parts), encoding="utf-8")


# ── Multi-LLM collation ──────────────────────────────────────────────


async def check_instance(
    test_bundle: str,
    configs: list[LLMConfig],
    task_id: str = "",
) -> list[tuple[LLMConfig, HackCheckResult]]:
    """Send the identical request to each configured LLM concurrently.

    Returns the (config, result) pairs in the same order as `configs`.
    """
    results = await asyncio.gather(
        *(hack_check(test_bundle, cfg, task_id) for cfg in configs)
    )
    return list(zip(configs, results, strict=True))


async def check_instance_with_fallback(
    test_bundle: str,
    primary: LLMConfig,
    fallback: LLMConfig | None,
    task_id: str = "",
    instance_dir: Path | None = None,
) -> tuple[LLMConfig, HackCheckResult, list[tuple[LLMConfig, HackCheckResult]]]:
    """Check with ``primary`` and use ``fallback`` only for eligible failures.

    The returned list contains every attempted model in order so callers can
    retain complete prompt/response evidence. A valid hacking or clean verdict
    from the primary is always authoritative and never invokes the fallback.

    Context-window handling: if the primary rejects the bundle for exceeding
    its context window and ``instance_dir`` is provided, the bundle is rebuilt
    with a tighter per-file cap (``_OVERFLOW_BUNDLE_FILE_BYTES``) and the SAME
    primary model is retried once. This keeps the check on the primary model
    (e.g. glm-5.2-moedsa) for the common case where the overflow came from a
    large generated fixture that carries no hack signal. If the truncated retry
    still overflows (or otherwise qualifies for fallback), the larger-context
    ``fallback`` model is then used.
    """
    primary_result = await hack_check(test_bundle, primary, task_id)
    attempts: list[tuple[LLMConfig, HackCheckResult]] = [(primary, primary_result)]

    if is_context_overflow(primary_result) and instance_dir is not None:
        truncated = build_test_bundle(instance_dir, _OVERFLOW_BUNDLE_FILE_BYTES)
        if truncated and truncated != test_bundle:
            logger.info(
                f"[{task_id or primary.name}] context-window overflow on primary; "
                f"retrying {primary.model} with truncated bundle "
                f"({_OVERFLOW_BUNDLE_FILE_BYTES}-byte cap)."
            )
            retry_result = await hack_check(truncated, primary, task_id)
            attempts.append((primary, retry_result))
            primary_result = retry_result

    if (
        fallback is not None
        and fallback.model != primary.model
        and should_fallback(primary_result)
    ):
        fallback_result = await hack_check(test_bundle, fallback, task_id)
        attempts.append((fallback, fallback_result))
        return fallback, fallback_result, attempts
    return primary, primary_result, attempts


def _iter_instances(input_dir: Path):
    """Yield Harbor instance directories (those containing a `tests/` folder)."""
    for d in sorted(input_dir.iterdir()):
        if d.is_dir() and (d / "tests").is_dir():
            yield d


def _load_processed_ids(out_path: Path) -> set[str]:
    """Return the set of instance_ids already present in an existing report.

    Reads a previously written `hacking_results.jsonl` and collects every
    `instance_id`. Malformed or partial lines (e.g. from a crash mid-write)
    are skipped. Returns an empty set if the file does not exist.
    """
    if not out_path.exists():
        return set()

    processed: set[str] = set()
    with out_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            iid = rec.get("instance_id")
            if isinstance(iid, str):
                processed.add(iid)
    return processed


async def run(
    input_dir: Path,
    output_dir: Path,
    config_path: Path | None = None,
    max_concurrency: int = 8,
    skip_existing: bool = True,
) -> Path:
    """Check every Harbor instance under `input_dir` and write a JSONL report.

    Up to `max_concurrency` instances are processed concurrently; each instance
    in turn fans its bundle out to all configured LLMs at once. So at peak there
    are roughly `max_concurrency * len(configs)` in-flight LLM requests.

    Each line of the output `hacking_results.jsonl` has:
      - instance_id: the Harbor directory name
      - path: absolute path to the instance directory
      - num_passed / num_llms: how many LLMs passed it (not hacking) out of N
      - llm_results: one entry per LLM with its name and pass/fail verdict
        ("passed" == the LLM did NOT flag the tests as reward hacking)

    Records are written as each instance completes (not in input order), each
    followed by a flush so a crash mid-run leaves a valid partial report.

    Additionally, the exact prompt sent to the LLM(s) and the raw response(s)
    received are written, per instance, to a `instance_logs/<instance_id>.log`
    file under the output directory.

    If `skip_existing` is True (the default), instances whose `instance_id`
    already appears in an existing `hacking_results.jsonl` are skipped, and new
    records are appended to that file rather than overwriting it. This makes the
    run resumable after an interruption. Pass `skip_existing=False` to recheck
    every instance and overwrite the report.

    Returns the path to the written JSONL file.
    """
    configs = load_llm_configs(config_path)
    instances = list(_iter_instances(input_dir))
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "hacking_results.jsonl"

    # One subfolder holding a per-instance prompt/response log.
    log_dir = output_dir / "instance_logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # When resuming, skip instances already recorded and append to the report.
    processed = _load_processed_ids(out_path) if skip_existing else set()
    if skip_existing and processed:
        instances = [inst for inst in instances if inst.name not in processed]
        logger.info(
            f"Skipping {len(processed)} already-processed instance(s) "
            f"from existing {out_path.name}"
        )

    logger.info(
        f"Found {len(instances)} Harbor instance(s) to check in {input_dir}; "
        f"checking each with {len(configs)} LLM(s): "
        f"{', '.join(c.name for c in configs)} "
        f"(up to {max_concurrency} instances concurrently)"
    )

    sem = asyncio.Semaphore(max(1, max_concurrency))
    write_lock = asyncio.Lock()

    # Append when resuming so previously written records are preserved.
    mode = "a" if skip_existing else "w"
    with out_path.open(mode, encoding="utf-8") as f:

        async def process(inst: Path) -> None:
            async with sem:
                bundle = build_test_bundle(inst)
                if not bundle.strip():
                    logger.warning(
                        f"[{inst.name}] no readable test files under tests/; "
                        "LLMs will see an empty bundle"
                    )
                pairs = await check_instance(bundle, configs, task_id=inst.name)

            # Write this instance's prompt/response log. Each instance writes
            # its own distinct file, so no locking is required.
            try:
                write_instance_log(log_dir / f"{inst.name}.log", inst.name, pairs)
            except OSError as e:
                logger.warning(f"[{inst.name}] failed to write instance log: {e}")

            llm_results = [
                {
                    "llm": cfg.name,
                    "passed": not res.is_hacking,
                    "is_hacking": res.is_hacking,
                    "test_framework": res.test_framework,
                    "reason": res.reason,
                    "error": res.error,
                }
                for cfg, res in pairs
            ]
            num_passed = sum(1 for r in llm_results if r["passed"])

            record = {
                "instance_id": inst.name,
                "path": str(inst.resolve()),
                "num_passed": num_passed,
                "num_llms": len(llm_results),
                "llm_results": llm_results,
            }
            # Serialize only the write so records never interleave on disk.
            async with write_lock:
                f.write(json.dumps(record) + "\n")
                f.flush()
            logger.info(
                f"[{inst.name}] {num_passed}/{len(llm_results)} LLM(s) passed "
                "(did not flag as hacking)"
            )

        await asyncio.gather(*(process(inst) for inst in instances))

    logger.info(f"Wrote results to {out_path}")
    return out_path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run the reward-hacking checker over a directory of Harbor "
            "instances. Each instance's tests/ files are sent to every LLM "
            "configured in hacking_checker.toml, and verdicts are collated "
            "into a JSONL report."
        )
    )
    parser.add_argument(
        "--input_dir",
        required=True,
        type=Path,
        help="Folder containing Harbor instance subdirectories (each with a tests/ folder).",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        type=Path,
        help="Folder to write hacking_results.jsonl into (created if missing).",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help=f"Path to the checker TOML (default: {DEFAULT_CONFIG_PATH}).",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=8,
        help=(
            "Max number of Harbor instances to check concurrently (default: 8). "
            "Each instance fans out to all configured LLMs, so peak in-flight "
            "requests is roughly this number times the LLM count."
        ),
    )
    parser.add_argument(
        "--recheck-existing",
        action="store_true",
        help=(
            "Recheck every instance and overwrite the report. By default, "
            "instances already recorded in an existing hacking_results.jsonl "
            "under --output_dir are skipped and new records are appended "
            "(resuming an interrupted run)."
        ),
    )
    args = parser.parse_args(argv)

    asyncio.run(
        run(
            args.input_dir,
            args.output_dir,
            args.config,
            args.max_concurrency,
            skip_existing=not args.recheck_existing,
        )
    )


if __name__ == "__main__":
    main()
