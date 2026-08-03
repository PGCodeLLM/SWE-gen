#!/usr/bin/env python3
"""Build skill-variant copies of the exported Harbor task archive.

The baseline zip is produced by ``export-pushed-harbor-tasks.py``. Each variant
is that same set of tasks with one subset of skills copied into every task
directory, plus a line appended to ``instruction.md`` naming the skills.

Variants are built by streaming the baseline archive rather than re-querying
PostgreSQL once per variant: the task bodies are over a gigabyte and identical
across variants, so re-reading them from the database five times would be far
slower and would put needless load on the pipeline's database.

Skill layout follows the platform's standard subdirectory format documented in
``agent-skills.md``: ``skills/<name>/SKILL.md`` with YAML frontmatter whose
``name`` matches the directory. The platform injects the COPY steps at image
build time, so no Dockerfile or task.toml edit is needed here -- and none is
made, since ``skills_dir`` defaults to ``/skills`` when a non-empty ``skills/``
is present.
"""

from __future__ import annotations

import argparse
import json
import shutil
import zipfile
from pathlib import Path

import yaml

_INSTRUCTION = "instruction.md"
_MANIFEST = "manifest.json"


def _load_subsets(skill_map: Path, min_enabled: int) -> list[list[str]]:
    """Enumerate skill subsets, mirroring ``skill_subset_variants.py``."""

    document = yaml.safe_load(skill_map.read_text())
    skills = list(document["skills"])
    always_on = set(document.get("always-on") or ())

    subsets = []
    for mask in range(1, 1 << len(skills)):
        chosen = {skills[index] for index in range(len(skills)) if mask & (1 << index)}
        subsets.append(chosen | always_on)
    # Sorted for stable variant numbering across runs; the raw powerset order
    # depends on bit positions and would shuffle if skill_map.yaml is reordered.
    return sorted((sorted(subset) for subset in subsets if len(subset) >= min_enabled))


def _instruction_addendum(subset: list[str]) -> str:
    names = ", ".join(subset)
    return (
        f"\n\nYou should use the following {len(subset)} skills to help you "
        f"accomplish the feature implementation: {names}\n"
    )


def _skill_payload(skills_root: Path, subset: list[str]) -> list[tuple[str, bytes, int]]:
    """Read one subset's skill files once, for reuse across every task."""

    payload: list[tuple[str, bytes, int]] = []
    for name in subset:
        directory = skills_root / name
        if not directory.is_dir():
            raise SystemExit(f"skill directory is missing: {directory}")
        entry = directory / "SKILL.md"
        # The platform only discovers this exact spelling; a rename would make
        # the skill silently invisible to OpenCode rather than fail loudly.
        if not entry.is_file():
            raise SystemExit(f"skill entry point is missing: {entry}")
        for path in sorted(directory.rglob("*")):
            # Symlinks are rejected by the platform, so drop them here rather
            # than shipping an archive that fails at build time.
            if path.is_symlink() or not path.is_file():
                continue
            relative = path.relative_to(skills_root)
            payload.append((f"skills/{relative.as_posix()}", path.read_bytes(), path.stat().st_mode))
    return payload


def build_variant(
    baseline: Path,
    destination: Path,
    subset: list[str],
    skills_root: Path,
) -> dict[str, object]:
    payload = _skill_payload(skills_root, subset)
    addendum = _instruction_addendum(subset).encode()
    directories: set[str] = set()
    task_count = 0

    with (
        zipfile.ZipFile(baseline) as source,
        zipfile.ZipFile(
            destination, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6
        ) as target,
    ):
        for info in source.infolist():
            if info.is_dir():
                continue
            body = source.read(info)
            if info.filename == _MANIFEST:
                manifest = json.loads(body)
                manifest["skill_variant"] = {"skills": subset, "skill_count": len(subset)}
                body = (json.dumps(manifest, indent=2) + "\n").encode()
            else:
                directory = info.filename.split("/", 1)[0]
                directories.add(directory)
                if info.filename == f"{directory}/{_INSTRUCTION}":
                    body += addendum
                    task_count += 1
            copied = zipfile.ZipInfo(info.filename, date_time=info.date_time)
            copied.external_attr = info.external_attr
            copied.compress_type = zipfile.ZIP_DEFLATED
            target.writestr(copied, body)

        for directory in sorted(directories):
            for relative, body, mode in payload:
                info = zipfile.ZipInfo(f"{directory}/{relative}")
                info.external_attr = (mode & 0o7777) << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                target.writestr(info, body)

    return {
        "skills": subset,
        "task_directories": len(directories),
        "instructions_updated": task_count,
        "skill_files_per_task": len(payload),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--prefix", default="harbor-tasks")
    parser.add_argument("--skill-map", type=Path, default=Path(__file__).parent / "skill_map.yaml")
    parser.add_argument("--skills-root", type=Path, default=Path(__file__).parent / "skills")
    parser.add_argument("--min-enabled", type=int, default=4)
    arguments = parser.parse_args()

    subsets = _load_subsets(arguments.skill_map, arguments.min_enabled)
    arguments.output_dir.mkdir(parents=True, exist_ok=True)
    print(f"variants: {len(subsets)}")

    for index, subset in enumerate(subsets, start=1):
        destination = arguments.output_dir / f"{arguments.prefix}-v{index}.zip"
        staging = destination.with_suffix(".zip.partial")
        summary = build_variant(
            arguments.baseline.resolve(), staging, subset, arguments.skills_root.resolve()
        )
        # Publish only once the archive is complete, so an interrupted run never
        # leaves a truncated zip sitting at the final path.
        shutil.move(staging, destination)
        size = destination.stat().st_size
        print(
            f"v{index}: {len(subset)} skills, "
            f"{summary['task_directories']} tasks, "
            f"{summary['instructions_updated']} instructions updated, "
            f"{size:,} bytes -> {destination}"
        )
        print(f"     skills: {', '.join(subset)}")


if __name__ == "__main__":
    main()
