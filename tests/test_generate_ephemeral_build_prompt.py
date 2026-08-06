"""Generate/repair prompts must tell the agent to build Dockerfiles ephemerally.

Regression guard: the generate + repair Claude Code agents iterate on the task
`environment/Dockerfile`. If told to `docker build -t <name>-debug .` on each
attempt, they leave orphaned `-debug`/`-debug2`/... images that are never pushed
to SWR and match neither pruner path, bloating the node Docker store (~200GB/node
observed). The prompts must instruct `--output type=cacheonly` iteration so no
image is persisted, and reserve the one persisted+pushed build for the sanctioned
SUGGESTED_IMAGE_REF base image.
"""
from __future__ import annotations

import swegen.create.claude_code_runner as runner


def _build_driving_prompts() -> dict[str, str]:
    out = {}
    for name in dir(runner):
        if not (name.startswith("CC_") and name.endswith("PROMPT")):
            continue
        value = getattr(runner, name)
        if not isinstance(value, str):
            continue
        if "docker build" in value or "SUGGESTED_IMAGE_REF" in value:
            out[name] = value
    return out


def test_build_driving_prompts_instruct_ephemeral_cacheonly_builds() -> None:
    prompts = _build_driving_prompts()
    # There must be at least the generate + repair prompts.
    assert {"CC_PROMPT", "CC_REPAIR_PROMPT"} <= set(prompts), prompts.keys()
    for name, value in prompts.items():
        assert "--output type=cacheonly" in value, f"{name} lacks cacheonly guidance"
        # It must warn against leaving debug-tagged orphan images.
        assert "-debug" in value, f"{name} does not mention the -debug orphan pattern"


def test_sanctioned_base_build_still_persists_and_pushes() -> None:
    # The one build that SHOULD persist (pushed to SWR) must remain intact in
    # whichever prompt carries the base-build protocol — the ephemeral guidance
    # must not have removed the SUGGESTED_IMAGE_REF build+push path.
    all_build_text = "\n".join(_build_driving_prompts().values())
    assert "SUGGESTED_IMAGE_REF" in all_build_text
    assert "docker push SUGGESTED_IMAGE_REF" in all_build_text
