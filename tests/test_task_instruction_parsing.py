import pytest

from swegen.create.task_instruction import _tolerant_parse_content

VALID_OBJ = (
    '{"is_substantial": true, "reason": "fixes a bug", '
    '"instruction": "x" * 120, "difficulty": "medium", '
    '"category": "bugfix", "tags": ["python", "backend", "http"], '
    '"task_name": null}'
).replace('"x" * 120', '"' + "x" * 120 + '"')


def test_clean_json_parses_via_fast_path() -> None:
    result = _tolerant_parse_content(VALID_OBJ)
    assert result.is_substantial is True
    assert result.tags == ["python", "backend", "http"]


def test_markdown_fenced_json_is_stripped_and_parsed() -> None:
    fenced = f"```json\n{VALID_OBJ}\n```"
    result = _tolerant_parse_content(fenced)
    assert result.is_substantial is True


def test_fenced_json_without_language_tag() -> None:
    fenced = f"```\n{VALID_OBJ}\n```"
    result = _tolerant_parse_content(fenced)
    assert result.is_substantial is True


def test_think_block_prefix_then_fenced_json() -> None:
    wrapped = f"<Thoughts>\nLet me analyze this PR...\n</Thoughts>\n\n```json\n{VALID_OBJ}\n```"
    result = _tolerant_parse_content(wrapped)
    assert result.is_substantial is True


def test_pure_markdown_prose_with_no_json_raises() -> None:
    prose = "## Task Instruction\n\nThis PR fixes a bug.\n\nIs Substantial\n\ntrue"
    with pytest.raises(ValueError):
        _tolerant_parse_content(prose)


def test_yaml_like_prose_with_no_json_raises() -> None:
    prose = "is_substantial: true\n\nreason: fixes a real bug\n\ntags: python, backend"
    with pytest.raises(ValueError):
        _tolerant_parse_content(prose)
