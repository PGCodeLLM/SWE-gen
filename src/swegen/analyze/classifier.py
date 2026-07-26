from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    ResultMessage,
)
from harbor.models.trial.result import TrialResult
from openai import OpenAI
from rich.console import Console

from swegen.create.claude_code_utils import Colors, print_sdk_message
from swegen.model_settings import (
    claude_runtime_env,
    load_analysis_settings,
    load_openai_settings,
)

from .models import (
    BaselineResult,
    BaselineValidation,
    Classification,
    TaskVerdict,
    TaskVerdictModel,
    TrialClassification,
    TrialClassificationModel,
)

# OpenAI verdict synthesis constants
VERDICT_MODEL: str | None = None
VERDICT_TIMEOUT = 120.0
VERDICT_MAX_TOKENS = 4096


# Load prompt templates
_CLASSIFY_PROMPT_PATH = Path(__file__).parent / "classify_prompt.txt"
_CLASSIFY_PROMPT = _CLASSIFY_PROMPT_PATH.read_text()

_VERDICT_PROMPT_PATH = Path(__file__).parent / "verdict_prompt.txt"
_VERDICT_PROMPT = _VERDICT_PROMPT_PATH.read_text()


def classify_trial(
    trial_dir: str | Path,
    task_dir: str | Path,
    *,
    model: str | None = None,
    verbose: bool = False,
    timeout: int = 300,
) -> TrialClassification:
    """Classify a single trial outcome.

    Convenience function for classifying an existing trial without
    instantiating TrialClassifier. This is the simplest way to use
    the classification system.

    Example:
        >>> from swegen.analyze import classify_trial
        >>> classification = classify_trial(
        ...     "path/to/trial",
        ...     "path/to/task",
        ... )
        >>> print(classification.classification)  # GOOD_SUCCESS, BAD_FAILURE, etc.
        >>> print(classification.evidence)

    Args:
        trial_dir: Path to trial directory (contains result.json, agent/, verifier/)
        task_dir: Path to task directory (contains instruction.md, solution/, tests/)
        model: Optional override; defaults to [analysis].classifier_model
        verbose: If True, stream Claude Code output to console
        timeout: Maximum time for classification in seconds (default: 300 = 5 min)

    Returns:
        TrialClassification with classification, evidence, root_cause, and recommendation
    """
    classifier = TrialClassifier(model=model, verbose=verbose, timeout=timeout)
    return classifier.classify_trial_sync(Path(trial_dir), Path(task_dir))


class TrialClassifier:
    """Classifies trial outcomes using Claude Code to identify task quality issues.

    Uses Claude Agent SDK with file access to explore trial artifacts
    and classify whether outcomes reveal task problems.

    Authentication, endpoint, and model defaults are loaded from swegen.toml.
    """

    def __init__(
        self,
        model: str | None = None,
        verbose: bool = False,
        timeout: int = 300,  # 5 minutes per classification
    ):
        """Initialize the classifier.

        Args:
            model: Optional override; defaults to [analysis].classifier_model
            verbose: If True, stream Claude Code output to console
            timeout: Maximum time per classification in seconds (default: 300 = 5 min)
        """
        self._model = model or load_analysis_settings().classifier_model
        if not self._model:
            raise ValueError("[analysis].classifier_model must be set in swegen.toml")
        self._verbose = verbose
        self._timeout = timeout

    async def classify_trial(
        self,
        trial_dir: Path,
        task_dir: Path,
    ) -> TrialClassification:
        """Classify a single trial outcome using Claude Code.

        Args:
            trial_dir: Path to trial directory (contains result.json, agent/, verifier/)
            task_dir: Path to task directory (contains instruction.md, solution/, tests/)

        Returns:
            TrialClassification with classification, evidence, and recommendations
        """
        # Read trial result to get the verified outcome
        # Harbor job directories have structure:
        #   job_dir/result.json (job summary - wrong!)
        #   job_dir/task-xxx/result.json (trial result - correct!)
        # We need to find the nested trial result.json
        result_path = trial_dir / "result.json"

        # Check if root result.json is a job summary (has n_total_trials field)
        # If so, look for nested trial result.json in task-* subdirectory
        if result_path.exists():
            try:
                root_data = json.loads(result_path.read_text())
                if "n_total_trials" in root_data or "stats" in root_data:
                    # This is a job summary, look for nested trial result
                    for subdir in trial_dir.iterdir():
                        if subdir.is_dir() and subdir.name.startswith("task-"):
                            nested_result = subdir / "result.json"
                            if nested_result.exists():
                                result_path = nested_result
                                break
            except Exception:
                pass  # If parsing fails, continue with original path

        if not result_path.exists():
            return TrialClassification(
                trial_name=trial_dir.name,
                classification=Classification.HARNESS_ERROR,
                subtype="Missing Result",
                evidence="result.json not found in trial directory",
                root_cause="Trial did not complete - no result.json file",
                recommendation="Check Harbor logs for infrastructure issues",
                reward=None,
            )

        # Extract reward - try multiple formats for compatibility
        reward = None
        result_json_raw = None
        try:
            result_json_raw = json.loads(result_path.read_text())

            # Try parsing as Harbor's TrialResult first
            try:
                result = TrialResult.model_validate(result_json_raw)
                if result.verifier_result and result.verifier_result.rewards:
                    reward = result.verifier_result.rewards.get("reward")
            except Exception:
                # TrialResult parsing failed - try extracting reward from raw JSON
                # Common locations: verifier_result.rewards.reward, reward, result.reward
                if isinstance(result_json_raw, dict):
                    # Try nested verifier_result path
                    vr = result_json_raw.get("verifier_result", {})
                    if isinstance(vr, dict):
                        rewards = vr.get("rewards", {})
                        if isinstance(rewards, dict):
                            reward = rewards.get("reward")
                    # Try direct reward field
                    if reward is None:
                        reward = result_json_raw.get("reward")
        except Exception as e:
            # JSON parsing failed entirely
            return TrialClassification(
                trial_name=trial_dir.name,
                classification=Classification.HARNESS_ERROR,
                subtype="Invalid Result",
                evidence=f"Could not parse result.json as JSON: {e}",
                root_cause="Trial result file is corrupted or malformed",
                recommendation="Check Harbor logs for what went wrong",
                reward=None,
            )

        # Determine result string for prompt
        if reward == 1.0:
            result_str = "pass"
        elif reward == 0.0:
            result_str = "fail"
        else:
            result_str = f"unknown (reward={reward})"

        # Build prompt with paths for Claude to explore
        prompt = _CLASSIFY_PROMPT.format(
            result=result_str,
            task_dir=str(task_dir),
            trial_dir=str(trial_dir),
        )

        # Run Claude Code with file access
        options = ClaudeAgentOptions(
            permission_mode="bypassPermissions",
            allowed_tools=["Read", "Glob"],
            cwd=str(trial_dir),
            add_dirs=[str(task_dir)],
            model=self._model,
            # Pin all rounds for this task instance to one model (X-Session-ID)
            # so a multi-model router maximizes KV cache reuse across trials.
            env=claude_runtime_env(task_dir.name),
            # Prefer structured output when supported by the SDK/runtime.
            # This avoids brittle "parse JSON from text" logic entirely.
            output_format={
                "type": "json_schema",
                "schema": TrialClassificationModel.model_json_schema(),
            },
        )

        structured_output: Any = None
        try:
            auth_env = claude_runtime_env(task_dir.name)
            if not any(
                auth_env.get(name)
                for name in (
                    "CLAUDE_CODE_OAUTH_TOKEN",
                    "ANTHROPIC_API_KEY",
                    "ANTHROPIC_AUTH_TOKEN",
                )
            ):
                raise RuntimeError("No Claude authentication configured in swegen.toml [model]")

            if self._verbose:
                print(
                    f"{Colors.YELLOW}[Classifier] Running Claude Code classification (timeout: {self._timeout}s)...{Colors.RESET}",
                    flush=True,
                )
                print(
                    f"{Colors.YELLOW}[Classifier] Trial: {trial_dir.name}{Colors.RESET}", flush=True
                )
                print(
                    f"{Colors.YELLOW}[Classifier] Task: {task_dir.name}{Colors.RESET}", flush=True
                )
                print("-" * 60, flush=True)

            # Run with timeout
            try:
                async with asyncio.timeout(self._timeout):
                    async with ClaudeSDKClient(options=options) as client:
                        await client.query(prompt)

                        async for message in client.receive_response():
                            if self._verbose:
                                print_sdk_message(message)
                            if isinstance(message, ResultMessage):
                                structured_output = message.structured_output
            except TimeoutError:
                if self._verbose:
                    print(
                        f"{Colors.RED}[Classifier] Timed out after {self._timeout}s{Colors.RESET}",
                        flush=True,
                    )
                return TrialClassification(
                    trial_name=trial_dir.name,
                    classification=Classification.HARNESS_ERROR,
                    subtype="Timeout",
                    evidence=f"Classification timed out after {self._timeout} seconds",
                    root_cause="Claude Code classification exceeded time limit",
                    recommendation="Review trial manually or increase timeout",
                    reward=reward,
                )

            if structured_output is None:
                raise RuntimeError(
                    "Claude Agent SDK did not return structured_output for this request"
                )

            if self._verbose:
                print("-" * 60, flush=True)
                print(
                    f"{Colors.GREEN}[Classifier] Classification complete for {trial_dir.name}{Colors.RESET}",
                    flush=True,
                )

            return self._parse_trial_classification_structured(
                structured_output, trial_dir.name, reward
            )

        except Exception as e:
            # Classification failed - report the error, don't guess
            return TrialClassification(
                trial_name=trial_dir.name,
                classification=Classification.HARNESS_ERROR,
                subtype="Classification Failed",
                evidence=f"Claude Code classification failed: {e}",
                root_cause="Could not analyze trial with Claude Code",
                recommendation="Review trial manually or check authentication",
                reward=reward,
            )

    def _parse_trial_classification_structured(
        self,
        structured_output: Any,
        trial_name: str,
        reward: float | None,
    ) -> TrialClassification:
        """Parse and validate structured classification output (preferred path)."""
        try:
            data: Any = structured_output

            # Allow mild nesting from some SDK wrappers
            if isinstance(data, dict):
                if "structured_output" in data and isinstance(data["structured_output"], dict):
                    data = data["structured_output"]
                if "result" in data and isinstance(data["result"], dict):
                    data = data["result"]

            model = TrialClassificationModel.model_validate(data)
            classification = TrialClassification.from_model(
                trial_name=trial_name, model=model, reward=reward
            )

            # Enforce classification/result consistency (defensive)
            if reward == 1.0 and not classification.classification.is_success:
                classification.classification = Classification.BAD_SUCCESS
                classification.subtype = "Inconsistent Output"
                classification.evidence = (
                    f"Claude returned {model.classification} but verified result was pass (reward=1.0). "
                    + classification.evidence
                ).strip()
            if reward == 0.0 and classification.classification.is_success:
                classification.classification = Classification.HARNESS_ERROR
                classification.subtype = "Inconsistent Output"
                classification.evidence = (
                    f"Claude returned {model.classification} but verified result was fail (reward=0.0). "
                    + classification.evidence
                ).strip()

            return classification
        except Exception as e:
            return TrialClassification(
                trial_name=trial_name,
                classification=Classification.HARNESS_ERROR,
                subtype="Parse Error",
                evidence=f"Could not parse structured output: {e}",
                root_cause="Claude's structured output did not match expected schema",
                recommendation="Review trial manually",
                reward=reward,
            )

    def classify_trial_sync(
        self,
        trial_dir: Path,
        task_dir: Path,
    ) -> TrialClassification:
        """Synchronous wrapper for classify_trial."""
        return asyncio.run(self.classify_trial(trial_dir, task_dir))

    async def classify_trials(
        self,
        trial_dirs: list[Path],
        task_dir: Path,
        console: Console | None = None,
    ) -> list[TrialClassification]:
        """Classify multiple trials.

        Note: Runs sequentially to avoid overwhelming Claude Code.

        Args:
            trial_dirs: List of trial directories to classify
            task_dir: Path to task directory
            console: Optional console for progress output

        Returns:
            List of TrialClassification results
        """
        if console:
            console.print(f"  Classifying {len(trial_dirs)} trial(s) with Claude Code...")

        classifications = []
        for i, trial_dir in enumerate(trial_dirs):
            if console:
                console.print(f"    [{i + 1}/{len(trial_dirs)}] {trial_dir.name}...")

            try:
                classification = await self.classify_trial(trial_dir, task_dir)
                classifications.append(classification)
            except Exception as e:
                classifications.append(
                    TrialClassification(
                        trial_name=trial_dir.name,
                        classification=Classification.HARNESS_ERROR,
                        subtype="Classification Error",
                        evidence=str(e),
                        root_cause="Exception during classification",
                        recommendation="Review trial manually",
                        reward=None,
                    )
                )

        return classifications

    def classify_trials_sync(
        self,
        trial_dirs: list[Path],
        task_dir: Path,
        console: Console | None = None,
    ) -> list[TrialClassification]:
        """Synchronous wrapper for classify_trials."""
        return asyncio.run(self.classify_trials(trial_dirs, task_dir, console))


def _compute_task_verdict_openai(
    classifications: list[TrialClassification],
    baseline: BaselineValidation | None = None,
    quality_check_passed: bool = True,
    model: str | None = None,
    console: Console | None = None,
    verbose: bool = False,
    api_key: str | None = None,
    timeout: float | None = None,
) -> TaskVerdict:
    """Compute task verdict using OpenAI to synthesize trial analyses.

    Uses OpenAI's structured outputs for fast, reliable verdict synthesis.
    This is much simpler and faster than Claude Code since no file access is needed.

    Args:
        classifications: List of individual trial classifications
        baseline: Optional baseline validation results
        quality_check_passed: Whether static quality check passed
        model: Optional override; defaults to [openai].verdict_model
        console: Optional console for progress output
        verbose: If True, print progress messages
        api_key: Optional explicit API key override
        timeout: Optional OpenAI client timeout override (seconds)

    Returns:
        TaskVerdict with LLM-synthesized analysis

    Raises:
        RuntimeError: If the configured API key is missing or the call fails
    """
    if not classifications:
        return TaskVerdict(
            is_good=False,
            confidence="low",
            primary_issue="No trials to analyze",
            recommendations=["Run agent trials first"],
        )

    openai_settings = load_openai_settings()
    resolved_model = model or openai_settings.verdict_model

    # Format baseline summary
    if baseline:
        if baseline.is_valid:
            baseline_summary = "✓ Passed (nop failed as expected, oracle passed as expected)"
        else:
            baseline_summary = "✗ FAILED:\n" + "\n".join(
                f"  - {issue}" for issue in baseline.issues
            )
    else:
        baseline_summary = "Not run"

    # Format quality check summary
    quality_check_summary = "✓ Passed" if quality_check_passed else "✗ Failed"

    # Format trial classifications
    trial_lines = []
    for i, c in enumerate(classifications, 1):
        trial_lines.append(f"""Trial {i}: {c.trial_name}
  Classification: {c.classification.value}
  Subtype: {c.subtype}
  Reward: {c.reward}
  Evidence: {c.evidence}
  Root Cause: {c.root_cause}
  Recommendation: {c.recommendation}
""")
    trial_classifications = "\n".join(trial_lines)

    # Build prompt
    prompt = _VERDICT_PROMPT.format(
        num_trials=len(classifications),
        baseline_summary=baseline_summary,
        quality_check_summary=quality_check_summary,
        trial_classifications=trial_classifications,
    )

    if console:
        console.print("  [dim]Synthesizing verdict with OpenAI...[/dim]")

    if verbose:
        print(
            f"\n{Colors.YELLOW}[Verdict] Synthesizing task verdict with {resolved_model}...{Colors.RESET}",
            flush=True,
        )

    # Create OpenAI client and make structured output call
    client = OpenAI(
        api_key=api_key or openai_settings.api_key,
        base_url=openai_settings.base_url,
        timeout=timeout or VERDICT_TIMEOUT,
    )

    try:
        completion = client.beta.chat.completions.parse(
            model=resolved_model,
            messages=[
                {"role": "user", "content": prompt},
            ],
            response_format=TaskVerdictModel,
            max_completion_tokens=VERDICT_MAX_TOKENS,
        )

        verdict_model = completion.choices[0].message.parsed
        if verdict_model is None:
            raise RuntimeError("OpenAI returned no parsed result for verdict synthesis")

        if verbose:
            print(f"{Colors.GREEN}[Verdict] Verdict synthesis complete{Colors.RESET}\n", flush=True)

    except Exception as exc:
        exc_type = type(exc).__name__
        if verbose:
            print(f"{Colors.RED}[Verdict] Failed ({exc_type}): {exc}{Colors.RESET}\n", flush=True)
        raise RuntimeError(f"Verdict synthesis failed: {exc}") from exc

    # Build TaskVerdict from LLM response
    task_problem_count = sum(1 for c in classifications if c.is_task_problem)
    agent_problem_count = sum(
        1 for c in classifications if c.classification == Classification.GOOD_FAILURE
    )
    success_count = sum(
        1
        for c in classifications
        if c.classification in (Classification.GOOD_SUCCESS, Classification.BAD_SUCCESS)
    )
    harness_error_count = sum(
        1 for c in classifications if c.classification == Classification.HARNESS_ERROR
    )

    return TaskVerdict(
        is_good=verdict_model.is_good,
        confidence=verdict_model.confidence,
        primary_issue=verdict_model.primary_issue,
        recommendations=verdict_model.recommendations,
        task_problem_count=task_problem_count,
        agent_problem_count=agent_problem_count,
        success_count=success_count,
        harness_error_count=harness_error_count,
        classifications=classifications,
        baseline=baseline,
    )


def compute_task_verdict(
    classifications: list[TrialClassification],
    baseline: BaselineValidation | None = None,
    quality_check_passed: bool = True,
    model: str | None = None,
    console: Console | None = None,
    verbose: bool = False,
    api_key: str | None = None,
    timeout: float | None = None,
) -> TaskVerdict:
    """Compute overall task verdict from trial classifications using LLM synthesis.

    Uses OpenAI to intelligently synthesize individual trial analyses into a final verdict.
    Performs pattern recognition, root cause analysis, and generates actionable recommendations.

    Args:
        classifications: List of trial classifications
        baseline: Optional baseline validation results
        quality_check_passed: Whether static quality check passed
        model: Optional override; defaults to [openai].verdict_model
        console: Optional console for progress output
        verbose: If True, print progress messages
        api_key: Optional explicit API key override
        timeout: Optional OpenAI client timeout override (seconds)

    Returns:
        TaskVerdict with is_good, confidence, and recommendations

    Raises:
        RuntimeError: If the configured API key is missing
    """
    return _compute_task_verdict_openai(
        classifications,
        baseline,
        quality_check_passed,
        model,
        console,
        verbose,
        api_key,
        timeout,
    )


def classify_baseline_result(
    agent: str,
    reward: float | None,
    error: str | None = None,
) -> BaselineResult:
    """Create a BaselineResult from agent run outcome.

    Args:
        agent: "nop" or "oracle"
        reward: Reward value (1.0 = pass, 0.0 = fail)
        error: Optional error message if agent failed to run

    Returns:
        BaselineResult with pass/fail status
    """
    passed = reward == 1.0 if reward is not None else False
    return BaselineResult(
        agent=agent,  # type: ignore
        passed=passed,
        reward=reward,
        error=error,
    )
