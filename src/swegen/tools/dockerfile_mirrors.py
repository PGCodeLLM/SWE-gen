"""Apply the internal Huawei package mirrors to task Dockerfiles."""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from pathlib import Path

DEFAULT_UBUNTU_ARCHIVE_MIRROR = "http://mirrors.tools.huawei.com/ubuntu/"
DEFAULT_UBUNTU_CDIMAGE_MIRROR = "http://mirrors.tools.huawei.com/ubuntu-cdimage/"
DEFAULT_UBUNTU_CLOUD_IMAGES_MIRROR = "http://mirrors.tools.huawei.com/ubuntu-cloud-images/"
DEFAULT_UBUNTU_PORTS_MIRROR = "http://mirrors.tools.huawei.com/ubuntu-ports/"
DEFAULT_UBUNTU_RELEASES_MIRROR = "http://mirrors.tools.huawei.com/ubuntu-releases/"
DEFAULT_NPM_REGISTRY = "http://mirrors.tools.huawei.com/npm/"

_MIRROR_BLOCK_MARKER = "# SWEGEN_UBUNTU_MIRRORS"
_NPM_BLOCK_MARKER = "# SWEGEN_NPM_MIRROR"
_SAFE_MIRROR_URL = re.compile(r"https?://[A-Za-z0-9._:/-]+\Z")
_DOCKER_INSTRUCTION = re.compile(r"^[ \t]*([A-Za-z]+)\b")
_NPM_COMMAND = re.compile(
    r"\bnpm(?=\s+(?:access|adduser|audit|bugs|cache|ci|completion|config|dedupe|"
    r"deprecate|diff|dist-tag|docs|doctor|edit|exec|explain|explore|find-dupes|"
    r"fund|help|hook|init|install|install-ci-test|install-test|link|ll|login|"
    r"logout|ls|org|outdated|owner|pack|ping|pkg|prefix|profile|prune|publish|"
    r"query|rebuild|repo|restart|root|run|run-script|search|set|shrinkwrap|star|"
    r"stars|start|stop|team|test|token|uninstall|unpublish|unstar|update|version|"
    r"view|whoami|x|i|it|t|un|up)\b)",
    re.IGNORECASE,
)
_NPM_JSON_COMMAND = re.compile(r"\[\s*[\"']npm[\"']\s*,", re.IGNORECASE)


def _mirror_url(environment_name: str, default: str) -> str:
    value = os.environ.get(environment_name, "").strip() or default
    if _SAFE_MIRROR_URL.fullmatch(value) is None:
        raise ValueError(f"{environment_name} must be a simple HTTP(S) URL")
    return value.rstrip("/")


def _configured_mirrors() -> dict[str, str]:
    return {
        "archive": _mirror_url(
            "SWEGEN_UBUNTU_ARCHIVE_MIRROR",
            DEFAULT_UBUNTU_ARCHIVE_MIRROR,
        ),
        "cdimage": _mirror_url(
            "SWEGEN_UBUNTU_CDIMAGE_MIRROR",
            DEFAULT_UBUNTU_CDIMAGE_MIRROR,
        ),
        "cloud_images": _mirror_url(
            "SWEGEN_UBUNTU_CLOUD_IMAGES_MIRROR",
            DEFAULT_UBUNTU_CLOUD_IMAGES_MIRROR,
        ),
        "ports": _mirror_url(
            "SWEGEN_UBUNTU_PORTS_MIRROR",
            DEFAULT_UBUNTU_PORTS_MIRROR,
        ),
        "releases": _mirror_url(
            "SWEGEN_UBUNTU_RELEASES_MIRROR",
            DEFAULT_UBUNTU_RELEASES_MIRROR,
        ),
        "npm": _mirror_url(
            "SWEGEN_NPM_REGISTRY",
            DEFAULT_NPM_REGISTRY,
        ),
    }


def _direct_url_rewrites(mirrors: dict[str, str]) -> tuple[tuple[str, str], ...]:
    return (
        ("http://archive.ubuntu.com/ubuntu", mirrors["archive"]),
        ("https://archive.ubuntu.com/ubuntu", mirrors["archive"]),
        ("http://security.ubuntu.com/ubuntu", mirrors["archive"]),
        ("https://security.ubuntu.com/ubuntu", mirrors["archive"]),
        ("http://cn.archive.ubuntu.com/ubuntu", mirrors["archive"]),
        ("https://cn.archive.ubuntu.com/ubuntu", mirrors["archive"]),
        ("http://ports.ubuntu.com/ubuntu-ports", mirrors["ports"]),
        ("https://ports.ubuntu.com/ubuntu-ports", mirrors["ports"]),
        ("http://cdimage.ubuntu.com", mirrors["cdimage"]),
        ("https://cdimage.ubuntu.com", mirrors["cdimage"]),
        ("http://cloud-images.ubuntu.com", mirrors["cloud_images"]),
        ("https://cloud-images.ubuntu.com", mirrors["cloud_images"]),
        ("http://releases.ubuntu.com", mirrors["releases"]),
        ("https://releases.ubuntu.com", mirrors["releases"]),
    )


def _ubuntu_source_block(mirrors: dict[str, str]) -> str:
    replacements = _direct_url_rewrites(mirrors)[:8]
    sed_lines = " \\\n".join(
        f"                -e 's#{source}#{target}#g'" for source, target in replacements
    )
    return (
        f"{_MIRROR_BLOCK_MARKER}\n"
        "RUN set -eux; \\\n"
        "    for sources_file in /etc/apt/sources.list "
        "/etc/apt/sources.list.d/ubuntu.sources; do \\\n"
        '        if [ -f "$sources_file" ]; then \\\n'
        "            sed -i \\\n"
        f"{sed_lines} \\\n"
        '                "$sources_file"; \\\n'
        "        fi; \\\n"
        "    done\n"
    )


def _from_uses_ubuntu(line: str) -> bool:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return False
    fields = stripped.split()
    if not fields or fields[0].upper() != "FROM":
        return False
    for field in fields[1:]:
        if field.startswith("--"):
            continue
        return "ubuntu" in field.lower()
    return False


def _dockerfile_units(content: str) -> list[str]:
    """Split Dockerfile text while keeping continued instructions intact."""

    units: list[str] = []
    current: list[str] = []
    continued = False
    for line in content.splitlines(keepends=True):
        if not current:
            current.append(line)
        elif continued:
            current.append(line)
        else:
            units.append("".join(current))
            current = [line]
        continued = line.rstrip("\r\n").rstrip().endswith("\\")
    if current:
        units.append("".join(current))
    return units


def _instruction_name(unit: str) -> str | None:
    match = _DOCKER_INSTRUCTION.match(unit)
    if match is None:
        return None
    return match.group(1).upper()


def _strip_generated_ubuntu_blocks(content: str) -> str:
    units = _dockerfile_units(content)
    rendered: list[str] = []
    index = 0
    while index < len(units):
        if _MIRROR_BLOCK_MARKER in units[index]:
            index += 1
            if index < len(units) and _instruction_name(units[index]) == "RUN":
                index += 1
            continue
        rendered.append(units[index])
        index += 1
    return "".join(rendered)


def _npm_setup_command(registry: str) -> str:
    return (
        "npm config set strict-ssl false && "
        f"npm config set registry {registry}/ && "
        "npm cache clean -f"
    )


def _rewrite_npm_stage(stage_units: list[str], registry: str) -> list[str]:
    if any(_NPM_BLOCK_MARKER in unit for unit in stage_units):
        return stage_units

    setup = _npm_setup_command(registry)
    for index, unit in enumerate(stage_units):
        if _instruction_name(unit) != "RUN":
            continue
        command_match = _NPM_COMMAND.search(unit)
        if command_match is not None:
            indent = re.match(r"^[ \t]*", unit).group(0)
            rewritten = (
                unit[: command_match.start()] + setup + " && npm" + unit[command_match.end() :]
            )
            return [
                *stage_units[:index],
                f"{indent}{_NPM_BLOCK_MARKER}\n",
                rewritten,
                *stage_units[index + 1 :],
            ]
        if _NPM_JSON_COMMAND.search(unit) is not None:
            indent = re.match(r"^[ \t]*", unit).group(0)
            return [
                *stage_units[:index],
                f"{indent}{_NPM_BLOCK_MARKER}\n",
                f"{indent}RUN {setup}\n",
                unit,
                *stage_units[index + 1 :],
            ]
    return stage_units


def _rewrite_ubuntu_stage(stage_units: list[str], mirrors: dict[str, str]) -> list[str]:
    if not stage_units or _MIRROR_BLOCK_MARKER in "".join(stage_units):
        return stage_units
    from_unit = stage_units[0]
    if _instruction_name(from_unit) != "FROM" or not _from_uses_ubuntu(from_unit):
        return stage_units
    if not from_unit.endswith(("\n", "\r")):
        from_unit += "\n"
    return [from_unit, _ubuntu_source_block(mirrors), *stage_units[1:]]


def _rewrite_stages(
    content: str,
    rewrite_stage: Callable[[list[str]], list[str]],
) -> str:
    units = _dockerfile_units(content)
    rendered: list[str] = []
    stage: list[str] = []
    saw_from = False

    for unit in units:
        if _instruction_name(unit) == "FROM":
            if saw_from:
                rendered.extend(rewrite_stage(stage))
                stage = []
            else:
                rendered.extend(stage)
                stage = []
                saw_from = True
        stage.append(unit)
    if saw_from:
        rendered.extend(rewrite_stage(stage))
    else:
        rendered.extend(stage)
    return "".join(rendered)


def _rewrite_npm_mirror(content: str, registry: str) -> str:
    return _rewrite_stages(
        content,
        lambda stage: _rewrite_npm_stage(stage, registry),
    )


def rewrite_ubuntu_mirrors(dockerfile: Path) -> bool:
    """Rewrite Ubuntu URLs and configure apt and npm package mirrors."""

    if not dockerfile.is_file():
        return False
    mirrors = _configured_mirrors()
    original = dockerfile.read_text(encoding="utf-8")
    updated = _strip_generated_ubuntu_blocks(original)
    for source, target in _direct_url_rewrites(mirrors):
        updated = updated.replace(source, target)

    updated = _rewrite_stages(
        updated,
        lambda stage: _rewrite_ubuntu_stage(stage, mirrors),
    )

    updated = _rewrite_npm_mirror(updated, mirrors["npm"])

    if updated == original:
        return False
    dockerfile.write_text(updated, encoding="utf-8")
    return True


__all__ = [
    "DEFAULT_UBUNTU_ARCHIVE_MIRROR",
    "DEFAULT_UBUNTU_CDIMAGE_MIRROR",
    "DEFAULT_UBUNTU_CLOUD_IMAGES_MIRROR",
    "DEFAULT_UBUNTU_PORTS_MIRROR",
    "DEFAULT_UBUNTU_RELEASES_MIRROR",
    "DEFAULT_NPM_REGISTRY",
    "rewrite_ubuntu_mirrors",
]
