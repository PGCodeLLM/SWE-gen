import asyncio

from reward_hacking_detector import hacking
from reward_hacking_detector.hacking import HackCheckResult, LLMConfig

PRIMARY = LLMConfig("spark", "https://example.invalid", "gpt-5.3-codex-spark", "key")
FALLBACK = LLMConfig("terra", "https://example.invalid", "gpt-5.6-terra", "key")


def failure(
    error: str,
    *,
    status: int | None = None,
    code: str | None = None,
    body: str = "",
) -> HackCheckResult:
    return HackCheckResult(
        is_hacking=True,
        reason="infrastructure failure",
        error=error,
        http_status=status,
        error_code=code,
        error_body=body,
    )


def test_should_fallback_only_for_quota_or_model_availability_errors() -> None:
    assert hacking.should_fallback(failure("HTTP 429", status=429))
    assert hacking.should_fallback(
        failure("quota", status=400, code="insufficient_quota")
    )
    assert hacking.should_fallback(
        failure(
            "HTTP 502",
            status=502,
            body='{"error":{"message":"unknown provider for model spark"}}',
        )
    )
    assert not hacking.should_fallback(failure("HTTP 502: bad gateway", status=502))
    assert not hacking.should_fallback(failure("ConnectTimeout"))
    assert not hacking.should_fallback(failure("HTTP 401: invalid API key", status=401))


def test_valid_verdict_reason_never_triggers_fallback() -> None:
    result = HackCheckResult(
        is_hacking=False,
        reason="The tests cover a quota exhausted error path with real assertions.",
    )

    assert not hacking.should_fallback(result)


def test_fallback_wrapper_uses_terra_after_spark_exhaustion(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_hack_check(_bundle, config, _task_id=""):
        calls.append(config.model)
        if config.model == PRIMARY.model:
            return failure("HTTP 429: usage exhausted", status=429)
        return HackCheckResult(is_hacking=False, reason="real assertions", test_framework="pytest")

    monkeypatch.setattr(hacking, "hack_check", fake_hack_check)

    selected, result, attempts = asyncio.run(
        hacking.check_instance_with_fallback("tests", PRIMARY, FALLBACK, "task")
    )

    assert calls == [PRIMARY.model, FALLBACK.model]
    assert selected == FALLBACK
    assert result.error is None
    assert not result.is_hacking
    assert [config.model for config, _result in attempts] == calls


def test_fallback_wrapper_keeps_valid_spark_verdict(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_hack_check(_bundle, config, _task_id=""):
        calls.append(config.model)
        return HackCheckResult(
            is_hacking=True,
            reason="quota exhausted appears in the test fixture, but checks are weak",
        )

    monkeypatch.setattr(hacking, "hack_check", fake_hack_check)

    selected, result, attempts = asyncio.run(
        hacking.check_instance_with_fallback("tests", PRIMARY, FALLBACK, "task")
    )

    assert calls == [PRIMARY.model]
    assert selected == PRIMARY
    assert result.is_hacking
    assert len(attempts) == 1


def test_hack_check_preserves_structured_429_metadata(monkeypatch) -> None:
    import httpx

    class FakeResponse:
        status_code = 429
        text = (
            '{"error":{"message":"Spark usage exhausted",'
            '"type":"resource_exhausted","code":"insufficient_quota"}}'
        )
        request = httpx.Request("POST", "https://example.invalid/v1/chat/completions")

        def json(self):
            return {
                "error": {
                    "message": "Spark usage exhausted",
                    "type": "resource_exhausted",
                    "code": "insufficient_quota",
                }
            }

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, *_args, **_kwargs):
            return FakeResponse()

    monkeypatch.setattr(httpx, "AsyncClient", FakeClient)

    result = asyncio.run(hacking.hack_check("tests", PRIMARY, "task"))

    assert result.error is not None
    assert result.http_status == 429
    assert result.error_code == "insufficient_quota"
    assert "usage exhausted" in result.error_body.lower()
    assert hacking.should_fallback(result)


def test_instance_log_records_primary_and_fallback_attempts(tmp_path) -> None:
    primary_result = failure("HTTP 429: usage exhausted", status=429)
    fallback_result = HackCheckResult(
        is_hacking=False,
        reason="meaningful assertions",
        test_framework="pytest",
        raw_response='{"is_hacking":false}',
    )
    path = tmp_path / "task.log"

    hacking.write_instance_log(
        path,
        "owner__repo-1",
        [(PRIMARY, primary_result), (FALLBACK, fallback_result)],
    )

    contents = path.read_text()
    assert "model=gpt-5.3-codex-spark" in contents
    assert "ERROR: HTTP 429: usage exhausted" in contents
    assert "model=gpt-5.6-terra" in contents


# ── Context-window overflow handling (glm-5.2-moedsa, 161,280-token limit) ──

GLM = LLMConfig("glm", "https://example.invalid", "glm-5.2-moedsa", "key")


def _overflow_failure() -> HackCheckResult:
    return failure(
        "HTTP 400: This model's maximum context length is 161280 tokens. "
        "However, you requested 162557 tokens in the messages, "
        "Please reduce the length of the messages.",
        status=400,
        code="ContextWindowExceededError",
    )


def test_is_context_overflow_matches_litellm_context_error() -> None:
    assert hacking.is_context_overflow(_overflow_failure())
    assert hacking.is_context_overflow(
        failure("ContextWindowExceededError: too many tokens", status=400)
    )
    # Plain 400s (auth, bad request) are NOT context overflows.
    assert not hacking.is_context_overflow(failure("HTTP 400: invalid request", status=400))
    assert not hacking.is_context_overflow(failure("HTTP 401: invalid API key", status=401))
    assert not hacking.is_context_overflow(
        HackCheckResult(is_hacking=False, reason="clean verdict")
    )


def test_should_fallback_treats_context_overflow_as_eligible() -> None:
    assert hacking.should_fallback(_overflow_failure())


def test_context_overflow_truncates_and_retries_same_model(monkeypatch, tmp_path) -> None:
    """Primary overflows on the full bundle but succeeds once truncated.

    The SAME glm model is retried with a smaller bundle; terra is NOT called.
    """
    instance_dir = tmp_path / "owner__repo-1"
    (instance_dir / "tests").mkdir(parents=True)
    (instance_dir / "tests" / "test.sh").write_text("#!/bin/sh\nexit 0\n")

    bundles_seen: list[str] = []
    models_called: list[str] = []

    async def fake_hack_check(bundle, config, _task_id=""):
        bundles_seen.append(bundle)
        models_called.append(config.model)
        if "maximum context length" not in (bundle or ""):
            # The truncated retry (small bundle) succeeds.
            return HackCheckResult(
                is_hacking=False, reason="real assertions", test_framework="pytest"
            )
        return _overflow_failure()

    monkeypatch.setattr(hacking, "hack_check", fake_hack_check)

    selected, result, attempts = asyncio.run(
        hacking.check_instance_with_fallback(
            "maximum context length placeholder bundle",
            GLM,
            FALLBACK,
            "task",
            instance_dir=instance_dir,
        )
    )

    assert models_called == [GLM.model, GLM.model]  # glm retried, terra never called
    assert selected == GLM
    assert result.error is None
    assert not result.is_hacking
    # The retry used the rebuilt (truncated) bundle, which differs from the original.
    assert bundles_seen[1] != bundles_seen[0]
    assert [config.model for config, _r in attempts] == [GLM.model, GLM.model]


def test_context_overflow_falls_back_to_terra_when_still_overflows(
    monkeypatch, tmp_path
) -> None:
    """Truncated retry still overflows → escalate to the larger-context terra."""
    instance_dir = tmp_path / "owner__repo-2"
    (instance_dir / "tests").mkdir(parents=True)
    (instance_dir / "tests" / "test.sh").write_text("#!/bin/sh\nexit 0\n")

    models_called: list[str] = []

    async def fake_hack_check(_bundle, config, _task_id=""):
        models_called.append(config.model)
        if config.model == FALLBACK.model:
            return HackCheckResult(
                is_hacking=False, reason="clean on terra", test_framework="pytest"
            )
        return _overflow_failure()  # both glm attempts overflow

    monkeypatch.setattr(hacking, "hack_check", fake_hack_check)

    selected, result, attempts = asyncio.run(
        hacking.check_instance_with_fallback(
            "bundle",
            GLM,
            FALLBACK,
            "task",
            instance_dir=instance_dir,
        )
    )

    # glm tried twice (full + truncated), then terra once.
    assert models_called == [GLM.model, GLM.model, FALLBACK.model]
    assert selected == FALLBACK
    assert result.error is None
    assert [config.model for config, _r in attempts] == models_called


def test_context_overflow_without_instance_dir_skips_truncation_retry(monkeypatch) -> None:
    """No instance_dir → no truncation retry; overflow escalates straight to terra."""
    models_called: list[str] = []

    async def fake_hack_check(_bundle, config, _task_id=""):
        models_called.append(config.model)
        if config.model == FALLBACK.model:
            return HackCheckResult(is_hacking=False, reason="clean on terra")
        return _overflow_failure()

    monkeypatch.setattr(hacking, "hack_check", fake_hack_check)

    selected, result, attempts = asyncio.run(
        hacking.check_instance_with_fallback("bundle", GLM, FALLBACK, "task")
    )

    assert models_called == [GLM.model, FALLBACK.model]
    assert selected == FALLBACK
    assert result.error is None


def test_build_test_bundle_respects_smaller_cap(tmp_path) -> None:
    instance_dir = tmp_path / "owner__repo-3"
    tests = instance_dir / "tests"
    tests.mkdir(parents=True)
    (tests / "test.sh").write_text("#!/bin/sh\nexit 0\n")
    big = "x" * (30 * 1024)  # 30 KB: above overflow cap, below default cap
    (tests / "fixture.json").write_text(big)

    default_bundle = hacking.build_test_bundle(instance_dir)
    overflow_bundle = hacking.build_test_bundle(
        instance_dir, hacking._OVERFLOW_BUNDLE_FILE_BYTES
    )

    # Default 75KB cap inlines the fixture; 20KB overflow cap replaces it.
    assert big in default_bundle
    assert big not in overflow_bundle
    assert "skipped due to being above" in overflow_bundle
