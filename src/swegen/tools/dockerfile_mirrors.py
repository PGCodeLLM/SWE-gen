"""Apply the configured package mirrors to task Dockerfiles."""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlsplit

DEFAULT_UBUNTU_ARCHIVE_MIRROR = "http://mirrors.tools.huawei.com/ubuntu/"
DEFAULT_UBUNTU_CDIMAGE_MIRROR = "http://mirrors.tools.huawei.com/ubuntu-cdimage/"
DEFAULT_UBUNTU_CLOUD_IMAGES_MIRROR = "http://mirrors.tools.huawei.com/ubuntu-cloud-images/"
DEFAULT_UBUNTU_PORTS_MIRROR = "http://mirrors.tools.huawei.com/ubuntu-ports/"
DEFAULT_UBUNTU_RELEASES_MIRROR = "http://mirrors.tools.huawei.com/ubuntu-releases/"
DEFAULT_NPM_REGISTRY = "https://registry.npmmirror.com/"
DEFAULT_GO_PROXY = "http://mirrors.tools.huawei.com/goproxy/"
# Stored bare so it passes the shared HTTP(S) validation; Cargo's required
# "sparse+" scheme prefix is added where the config is emitted.
DEFAULT_CARGO_REGISTRY = "http://mirrors.tools.huawei.com/cargo/"

_MIRROR_BLOCK_MARKER = "# SWEGEN_UBUNTU_MIRRORS"
_NPM_BLOCK_MARKER = "# SWEGEN_NPM_MIRROR"
_LEGACY_NPM_REGISTRIES = (
    "http://mirrors.tools.huawei.com/npm/",
    "http://mirrors.tools.huawei.com/npm",
)
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
_METEOR_NPM_COMMAND = re.compile(r"\bmeteor\s+npm(?=\s+)", re.IGNORECASE)
_BROKEN_METEOR_NPM_SETUP = re.compile(
    r"meteor\s+npm\s+config\s+set\s+strict-ssl\s+false\s+&&\s+"
    r"npm\s+config\s+set\s+registry\s+\S+\s+&&\s+"
    r"npm\s+cache\s+clean\s+-f\s+&&\s+npm\b",
    re.IGNORECASE,
)
_APT_GET_UPDATE = re.compile(r"\bapt-get\s+update\b")
_CURL_DOWNLOAD_COMMAND = re.compile(r"\bcurl(?P<space>[ \t]+)(?P<flags>-[A-Za-z]*f[A-Za-z]*)")
_DEBIAN_REPOSITORY = re.compile(
    r"(?<!\[trusted=yes\][ \t])deb[ \t]+(?P<url>https?://deb\.debian\.org/debian\b)"
)
_NODE_TARBALL_CHECKSUM_LINE = re.compile(
    r"(?m)^[ \t]*&&[ \t]+echo[^\r\n]*node-v\$\{NODE_VERSION\}[^\r\n]*"
    r"sha256sum[^\r\n]*(?:\r?\n)"
)
_COREPACK_PREPARE_PNPM = re.compile(
    r"corepack[ \t]+enable[ \t]*\\?[ \t]*(?:\r?\n[ \t]*)?&&[ \t]*"
    r"corepack[ \t]+prepare[ \t]+pnpm@(?P<version>[A-Za-z0-9_.-]+)"
    r"[ \t]+--activate",
    re.IGNORECASE,
)
_REDIS_STACK_NOBLE_REPOSITORY = re.compile(
    r"(packages\.redis\.io/deb\s+)noble(\s+main\b)",
    re.IGNORECASE,
)
_LEGACY_NODE_VERSION = re.compile(
    r"(?:^|\n)\s*(?:ARG|ENV)\s+NODE_VERSION(?:\s*=|\s+)\s*[\"']?"
    r"(?P<version>\d+(?:\.\d+){0,2})(?:[\"'\s]|$)",
    re.IGNORECASE,
)
_LEGACY_NODE_BASE_IMAGE = re.compile(
    r"(?:^|\n)\s*FROM\s+(?:--\S+\s+)*node:(?P<version>\d+(?:\.\d+){0,2})"
    r"(?:[-\s]|$)",
    re.IGNORECASE,
)
_OLD_NPM_PIN = re.compile(r"\bnpm@(?:[2-5])(?:\.\d+){0,2}\b", re.IGNORECASE)
_YARN_INSTALL_COMMAND = re.compile(r"(?<![/A-Za-z0-9_.-])yarn(?=[ \t]+install\b)")
_NODE_GYP_PYTHON_COMPAT_MARKER = "# SWEGEN_NODE_GYP_PYTHON_COMPAT"
_NPM_CI_FALLBACK_MARKER = "# SWEGEN_NPM_CI_FALLBACK"
_PNPM_FROZEN_LOCKFILE_FALLBACK_MARKER = "# SWEGEN_PNPM_FROZEN_LOCKFILE_FALLBACK"
_BUN_ENV_MARKER = "# SWEGEN_BUN_ENV"
_PNPM_REGISTRY_MARKER = "# SWEGEN_PNPM_REGISTRY"
_GO_PROXY_MARKER = "# SWEGEN_GO_PROXY"
_CARGO_MIRROR_MARKER = "# SWEGEN_CARGO_MIRROR"
_GO_COMMAND = re.compile(r"(?<![/A-Za-z0-9_.-])go[ \t]+(?:mod|get|build|install|test|run)\b")
_CARGO_COMMAND = re.compile(
    r"(?<![/A-Za-z0-9_.-])cargo[ \t]+(?:fetch|build|test|install|run|check|update|vendor)\b"
)
_BUN_COMMAND = re.compile(r"(?<![/A-Za-z0-9_.-])bun[ \t]+(?:install|add|ci)\b")
_PNPM_COMMAND = re.compile(r"(?<![/A-Za-z0-9_.-])pnpm[ \t]+(?:install|add|import)\b")
# Captures the trailing flags that belong to the pnpm invocation itself, so the
# fallback can repeat them in both branches. Stops at a shell operator or line
# continuation: anything after those is a separate command, not a pnpm flag.
_PNPM_FROZEN_LOCKFILE_COMMAND = re.compile(
    r"pnpm[ \t]+install[ \t]+--frozen-lockfile(?P<flags>(?:[ \t]+-{1,2}[A-Za-z0-9][\w.-]*)*)"
)
_GIT_HTTPS_DEPENDENCY_MARKER = "# SWEGEN_GIT_HTTPS_DEPENDENCIES"
_DOTNET_ICU_MARKER = "# SWEGEN_DOTNET_ICU"
_DENO_CA_MARKER = "# SWEGEN_DENO_CA"
_JAVA_CA_TRUST_ALIAS = "swegen-proxy-ca-explicit"
_NPM_CI_COMMAND = re.compile(
    r"\bnpm\s+ci(?P<args>(?:\s+--[A-Za-z0-9_.=/:-]+)*)",
    re.IGNORECASE,
)
_PLAYWRIGHT_INSTALL_COMMAND = re.compile(
    r"(?P<command>\bnpx[ \t]+playwright[ \t]+install"
    r"(?:(?:[ \t]+--[A-Za-z0-9_.-]+(?:=[^\s\\&;]+)?)|"
    r"(?:[ \t]+[A-Za-z0-9_.-]+))*)",
    re.IGNORECASE,
)
_YARN_CLASSIC_ENV = re.compile(r"^[ \t]*YARN_(?:NETWORK_TIMEOUT|HTTP_TIMEOUT|NETWORK_CONCURRENCY)=")
_PROXY_CA_INSTALL = re.compile(
    r"(?P<copy>cp[ \t]+/tmp/swegen-proxy-ca\.crt[ \t]+"
    r"/usr/local/share/ca-certificates/swegen-proxy-ca\.crt)"
    r"(?P<separator>[ \t]*\\?[ \t]*(?:\r?\n[ \t]*)?&&[ \t]*)"
    r"update-ca-certificates"
)


def _legacy_node_major(stage_content: str) -> int | None:
    """Return the pinned Node.js major version when the stage declares one."""

    for pattern in (_LEGACY_NODE_VERSION, _LEGACY_NODE_BASE_IMAGE):
        match = pattern.search(stage_content)
        if match is not None:
            return int(match.group("version").split(".", 1)[0])
    return None


def _upgrade_node4_runtime(stage_content: str) -> str:
    """Move unsupported Node 4 task images to the oldest maintained-compatible Node 6."""

    for pattern in (_LEGACY_NODE_VERSION, _LEGACY_NODE_BASE_IMAGE):
        match = pattern.search(stage_content)
        if match is None or int(match.group("version").split(".", 1)[0]) > 4:
            continue
        start, end = match.span("version")
        stage_content = stage_content[:start] + "6.17.1" + stage_content[end:]
    return stage_content


def _legacy_node_gyp_setup(registry: str) -> str:
    return (
        "if ! command -v python >/dev/null 2>&1; then "
        "apt-get -o Acquire::Retries=5 update && "
        "DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends python3 "
        "&& ln -sf /usr/bin/python3 /usr/local/bin/python "
        "&& rm -rf /var/lib/apt/lists/*; fi && "
        "npm config set strict-ssl false && "
        f"npm config set registry {registry}/ && "
        "npm install --global npm@6.14.18 && "
        "npm config set python /usr/bin/python3 && "
        "for input_py in "
        "/usr/local/lib/node_modules/npm/node_modules/node-gyp/gyp/pylib/gyp/input.py "
        "/usr/share/nodejs/node-gyp/gyp/pylib/gyp/input.py; do "
        'if [ -f "$input_py" ]; then sed -i "s/\'rU\'/\'r\'/g" "$input_py"; fi; '
        "done"
    )


def _git_https_dependency_setup() -> str:
    return (
        "if command -v git >/dev/null 2>&1; then "
        'git config --global url."https://github.com/".insteadOf git://github.com/; '
        "fi"
    )


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
        "go": _mirror_url(
            "SWEGEN_GO_PROXY",
            DEFAULT_GO_PROXY,
        ),
        "cargo": _mirror_url(
            "SWEGEN_CARGO_REGISTRY",
            DEFAULT_CARGO_REGISTRY,
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


def _mirror_no_proxy_hosts(mirrors: dict[str, str]) -> str:
    """List the mirror hosts that apt must reach without the corporate proxy."""

    hosts: list[str] = []
    for name, target in mirrors.items():
        # Only the apt mirrors are proxy-exempt; the npm registry is a public
        # HTTPS host that still needs the proxy to be reachable.
        if name == "npm":
            continue
        host = urlsplit(target).hostname
        if host and host not in hosts:
            hosts.append(host)
    return ",".join(hosts)


def _ubuntu_source_block(mirrors: dict[str, str]) -> str:
    replacements = _direct_url_rewrites(mirrors)[:8]
    sed_lines = " \\\n".join(
        f"                -e 's#{source}#{target}#g'" for source, target in replacements
    )
    # The build container inherits http_proxy/https_proxy from the daemon but
    # not no_proxy, so apt sends mirror traffic through the corporate proxy and
    # collects 504s. Pin the mirror hosts to a direct route for every later
    # stage instead of relying on the builder's environment.
    no_proxy_hosts = _mirror_no_proxy_hosts(mirrors)
    no_proxy_line = (
        f"ENV no_proxy={no_proxy_hosts} NO_PROXY={no_proxy_hosts}\n" if no_proxy_hosts else ""
    )
    return (
        f"{_MIRROR_BLOCK_MARKER}\n"
        f"{no_proxy_line}"
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
            # The generated block is an optional proxy-exemption ENV followed by
            # the sed RUN; drop both so repeated rewrites stay idempotent.
            if index < len(units) and _instruction_name(units[index]) == "ENV":
                index += 1
            if index < len(units) and _instruction_name(units[index]) == "RUN":
                index += 1
            continue
        rendered.append(units[index])
        index += 1
    return "".join(rendered)


def _npm_setup_command(registry: str, *, executable: str = "npm") -> str:
    return (
        f"{executable} config set strict-ssl false && "
        f"{executable} config set registry {registry}/ && "
        f"{executable} cache clean -f"
    )


def _rewrite_npm_stage(stage_units: list[str], registry: str) -> list[str]:
    stage_content = "".join(stage_units)
    original_node_major = _legacy_node_major(stage_content)
    if original_node_major is not None and original_node_major <= 12:
        stage_content = stage_content.replace("python2.7", "python3")
        stage_content = stage_content.replace("/usr/bin/python2", "/usr/bin/python3")
    stage_content = _upgrade_node4_runtime(stage_content)
    stage_content = _OLD_NPM_PIN.sub("npm@6.14.18", stage_content)
    if "/opt/package/bin/yarn.js" in stage_content:
        stage_content = _YARN_INSTALL_COMMAND.sub(
            "node /opt/package/bin/yarn.js",
            stage_content,
        )
    stage_units = _dockerfile_units(stage_content)
    npm_already_rewritten = _NPM_BLOCK_MARKER in stage_content
    git_https_dependencies = _GIT_HTTPS_DEPENDENCY_MARKER not in stage_content
    legacy_node_major = _legacy_node_major(stage_content)
    legacy_node_gyp = (
        legacy_node_major is not None
        and legacy_node_major <= 12
        and _NODE_GYP_PYTHON_COMPAT_MARKER not in stage_content
    )
    if npm_already_rewritten and not legacy_node_gyp and not git_https_dependencies:
        return stage_units

    for index, unit in enumerate(stage_units):
        if _instruction_name(unit) != "RUN":
            continue
        meteor_match = _METEOR_NPM_COMMAND.search(unit)
        if meteor_match is not None:
            indent = re.match(r"^[ \t]*", unit).group(0)
            setup_parts: list[str] = []
            if git_https_dependencies:
                setup_parts.append(_git_https_dependency_setup())
            if not npm_already_rewritten:
                setup_parts.append(_npm_setup_command(registry, executable="meteor npm"))
            setup = " && ".join(setup_parts)
            rewritten = (
                unit[: meteor_match.start()] + setup + " && meteor npm" + unit[meteor_match.end() :]
            )
            prefix = [*stage_units[:index]]
            if not npm_already_rewritten:
                prefix.append(f"{indent}{_NPM_BLOCK_MARKER}\n")
            if git_https_dependencies:
                prefix.append(f"{indent}{_GIT_HTTPS_DEPENDENCY_MARKER}\n")
            return [*prefix, rewritten, *stage_units[index + 1 :]]
        command_match = _NPM_COMMAND.search(unit)
        if command_match is not None:
            indent = re.match(r"^[ \t]*", unit).group(0)
            setup_parts: list[str] = []
            if git_https_dependencies:
                setup_parts.append(_git_https_dependency_setup())
            if legacy_node_gyp:
                setup_parts.append(_legacy_node_gyp_setup(registry))
            if not npm_already_rewritten:
                setup_parts.append(_npm_setup_command(registry))
            setup = " && ".join(setup_parts)
            rewritten = (
                unit[: command_match.start()] + setup + " && npm" + unit[command_match.end() :]
            )
            prefix = [*stage_units[:index]]
            if not npm_already_rewritten:
                prefix.append(f"{indent}{_NPM_BLOCK_MARKER}\n")
            if git_https_dependencies:
                prefix.append(f"{indent}{_GIT_HTTPS_DEPENDENCY_MARKER}\n")
            if legacy_node_gyp:
                prefix.append(f"{indent}{_NODE_GYP_PYTHON_COMPAT_MARKER}\n")
            return [*prefix, rewritten, *stage_units[index + 1 :]]
        if _NPM_JSON_COMMAND.search(unit) is not None:
            indent = re.match(r"^[ \t]*", unit).group(0)
            setup = _npm_setup_command(registry)
            setup_parts = []
            if git_https_dependencies:
                setup_parts.append(_git_https_dependency_setup())
            if not npm_already_rewritten:
                setup_parts.append(setup)
            prefix = [*stage_units[:index]]
            if not npm_already_rewritten:
                prefix.append(f"{indent}{_NPM_BLOCK_MARKER}\n")
            if git_https_dependencies:
                prefix.append(f"{indent}{_GIT_HTTPS_DEPENDENCY_MARKER}\n")
            prefix.append(f"{indent}RUN {' && '.join(setup_parts)}\n")
            return [*prefix, unit, *stage_units[index + 1 :]]
        if "node /opt/package/bin/yarn.js install" in unit and legacy_node_gyp:
            indent = re.match(r"^[ \t]*", unit).group(0)
            setup_parts = []
            if git_https_dependencies:
                setup_parts.append(_git_https_dependency_setup())
            setup_parts.append(_legacy_node_gyp_setup(registry))
            if not npm_already_rewritten:
                setup_parts.append(_npm_setup_command(registry))
            prefix = [*stage_units[:index]]
            if not npm_already_rewritten:
                prefix.append(f"{indent}{_NPM_BLOCK_MARKER}\n")
            if git_https_dependencies:
                prefix.append(f"{indent}{_GIT_HTTPS_DEPENDENCY_MARKER}\n")
            prefix.append(f"{indent}{_NODE_GYP_PYTHON_COMPAT_MARKER}\n")
            prefix.append(f"{indent}RUN {' && '.join(setup_parts)}\n")
            return [*prefix, unit, *stage_units[index + 1 :]]
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
    for legacy_registry in _LEGACY_NPM_REGISTRIES:
        content = content.replace(legacy_registry, f"{registry}/")
    content = _BROKEN_METEOR_NPM_SETUP.sub(
        _npm_setup_command(registry, executable="meteor npm") + " && meteor npm",
        content,
    )
    return _rewrite_stages(
        content,
        lambda stage: _rewrite_npm_stage(stage, registry),
    )


def _rewrite_apt_retries(content: str) -> str:
    """Retry transient proxy/mirror failures during apt index refreshes."""

    return _APT_GET_UPDATE.sub("apt-get -o Acquire::Retries=5 update", content)


def _rewrite_curl_download_retries(content: str) -> str:
    """Retry truncated and transient curl downloads used by generated runtimes."""

    return _CURL_DOWNLOAD_COMMAND.sub(
        r"curl --retry 5 --retry-all-errors --retry-delay 2\g<space>\g<flags>",
        content,
    )


def _rewrite_mixed_debian_repository(content: str) -> str:
    """Trust explicitly added Debian compatibility repositories in Ubuntu tasks."""

    return _DEBIAN_REPOSITORY.sub(r"deb [trusted=yes] \g<url>", content)


def _strip_stale_node_tarball_checksum(content: str) -> str:
    """Remove generated Node checksums that no longer match the selected runtime."""

    return _NODE_TARBALL_CHECKSUM_LINE.sub("", content)


def _rewrite_corepack_pnpm_bootstrap(content: str) -> str:
    """Install pnpm through the configured npm registry instead of Corepack CDN."""

    return _COREPACK_PREPARE_PNPM.sub(
        lambda match: f"npm install --global pnpm@{match.group('version')}",
        content,
    )


def _rewrite_npm_ci_fallback(content: str) -> str:
    """Fall back to ``npm install`` when a generated task has no usable lockfile."""

    def rewrite_stage(stage_units: list[str]) -> list[str]:
        if _NPM_CI_FALLBACK_MARKER in "".join(stage_units):
            return stage_units
        for index, unit in enumerate(stage_units):
            if _instruction_name(unit) != "RUN":
                continue
            rewritten, count = _NPM_CI_COMMAND.subn(
                lambda match: (
                    f"(npm ci{match.group('args')} || npm install{match.group('args')})"
                ),
                unit,
            )
            if count:
                indent = re.match(r"^[ \t]*", unit).group(0)
                return [
                    *stage_units[:index],
                    f"{indent}{_NPM_CI_FALLBACK_MARKER}\n",
                    rewritten,
                    *stage_units[index + 1 :],
                ]
        return stage_units

    return _rewrite_stages(content, rewrite_stage)


def _rewrite_pnpm_frozen_lockfile_fallback(content: str) -> str:
    """Retry pnpm with lockfile updates when generated metadata is inconsistent."""

    def rewrite_stage(stage_units: list[str]) -> list[str]:
        if _PNPM_FROZEN_LOCKFILE_FALLBACK_MARKER in "".join(stage_units):
            return stage_units
        for index, unit in enumerate(stage_units):
            if _instruction_name(unit) != "RUN":
                continue
            needle = "pnpm install --frozen-lockfile"
            if needle not in unit:
                continue
            indent = re.match(r"^[ \t]*", unit).group(0)
            # Trailing flags belong to the pnpm invocation, so they have to be
            # captured into both branches. Wrapping only the bare command left
            # them stranded after the closing parenthesis
            # ("(pnpm install -x || pnpm install -y) --ignore-scripts"), which
            # /bin/sh rejects with "word unexpected" before pnpm ever runs.
            match = _PNPM_FROZEN_LOCKFILE_COMMAND.search(unit)
            if match is None:
                continue
            flags = match.group("flags").rstrip()
            fallback = f"{needle}{flags} || pnpm install --no-frozen-lockfile{flags}"
            rewritten = f"{unit[: match.start()]}({fallback}){unit[match.end() :]}"
            return [
                *stage_units[:index],
                f"{indent}{_PNPM_FROZEN_LOCKFILE_FALLBACK_MARKER}\n",
                rewritten,
                *stage_units[index + 1 :],
            ]
        return stage_units

    return _rewrite_stages(content, rewrite_stage)


def _rewrite_proxy_ca_permissions(content: str) -> str:
    """Let package-manager lifecycle subprocesses read the injected proxy CA."""

    trusted_ca = "/usr/local/share/ca-certificates/swegen-proxy-ca.crt"
    if f"chmod 0644 {trusted_ca}" in content:
        return content

    def replacement(match: re.Match[str]) -> str:
        separator = match.group("separator")
        return (
            f"{match.group('copy')}{separator}chmod 0644 {trusted_ca}"
            f"{separator}update-ca-certificates"
        )

    return _PROXY_CA_INSTALL.sub(replacement, content, count=1)


def _rewrite_dotnet_icu(content: str) -> str:
    """Install ICU for older self-contained .NET SDKs on Ubuntu 24.04."""

    def rewrite_stage(stage_units: list[str]) -> list[str]:
        stage_content = "".join(stage_units)
        if (
            _DOTNET_ICU_MARKER in stage_content
            or "dotnet-install.sh" not in stage_content
            or "apt-get" not in stage_content
        ):
            return stage_units
        for index, unit in enumerate(stage_units):
            if _instruction_name(unit) == "RUN" and "dotnet-install.sh" in unit:
                indent = re.match(r"^[ \t]*", unit).group(0)
                return [
                    *stage_units[:index],
                    f"{indent}{_DOTNET_ICU_MARKER}\n",
                    f"{indent}ENV DOTNET_NUGET_SIGNATURE_VERIFICATION=false \\\n"
                    f"{indent}    NUGET_CERT_REVOCATION_MODE=offline\n",
                    f"{indent}RUN apt-get -o Acquire::Retries=5 update "
                    "&& apt-get install -y --no-install-recommends libicu-dev "
                    "&& rm -rf /var/lib/apt/lists/*\n",
                    *stage_units[index:],
                ]
        return stage_units

    return _rewrite_stages(content, rewrite_stage)


def _rewrite_deno_ca(content: str) -> str:
    """Point Deno at the injected corporate CA for HTTPS module downloads."""

    def rewrite_stage(stage_units: list[str]) -> list[str]:
        stage_content = "".join(stage_units)
        if (
            _DENO_CA_MARKER in stage_content
            or "/usr/local/share/ca-certificates/swegen-proxy-ca.crt" not in stage_content
            or "deno" not in stage_content.lower()
        ):
            return stage_units
        for index, unit in enumerate(stage_units):
            if _instruction_name(unit) == "RUN" and "deno" in unit.lower():
                indent = re.match(r"^[ \t]*", unit).group(0)
                return [
                    *stage_units[:index],
                    f"{indent}{_DENO_CA_MARKER}\n",
                    f"{indent}ENV DENO_TLS_CA_STORE=system \\\n"
                    f"{indent}    DENO_CERT=/usr/local/share/ca-certificates/"
                    "swegen-proxy-ca.crt\n",
                    *stage_units[index:],
                ]
        return stage_units

    return _rewrite_stages(content, rewrite_stage)


def _rewrite_bun_registry_and_ca(content: str, registry: str) -> str:
    """Point Bun at the npm mirror and the injected corporate CA.

    Bun reads neither ``npm config`` nor ``.npmrc`` for TLS, so the
    ``npm config set strict-ssl false`` emitted for npm leaves ``bun install``
    talking to the upstream registry over a chain it does not trust; it fails
    with ``SELF_SIGNED_CERT_IN_CHAIN``. ``NPM_CONFIG_REGISTRY`` redirects it to
    the mirror and ``NODE_EXTRA_CA_CERTS`` supplies the proxy CA.
    """

    def rewrite_stage(stage_units: list[str]) -> list[str]:
        stage_content = "".join(stage_units)
        if _BUN_ENV_MARKER in stage_content or not _BUN_COMMAND.search(stage_content):
            return stage_units
        has_proxy_ca = "/usr/local/share/ca-certificates/swegen-proxy-ca.crt" in stage_content
        for index, unit in enumerate(stage_units):
            if _instruction_name(unit) != "RUN" or not _BUN_COMMAND.search(unit):
                continue
            indent = re.match(r"^[ \t]*", unit).group(0)
            env_lines = [f"{indent}ENV NPM_CONFIG_REGISTRY={registry}/"]
            if has_proxy_ca:
                env_lines.append(
                    f"{indent}ENV NODE_EXTRA_CA_CERTS="
                    "/usr/local/share/ca-certificates/swegen-proxy-ca.crt"
                )
            return [
                *stage_units[:index],
                f"{indent}{_BUN_ENV_MARKER}\n",
                *(f"{line}\n" for line in env_lines),
                *stage_units[index:],
            ]
        return stage_units

    return _rewrite_stages(content, rewrite_stage)


def _rewrite_pnpm_registry(content: str, registry: str) -> str:
    """Configure the npm mirror for pnpm, which ignores ``npm config set``.

    pnpm keeps its own store and config, so without this it resolves against
    ``registry.npmjs.org`` and absorbs that registry's 429 rate limiting even
    though a working mirror is configured for npm.
    """

    def rewrite_stage(stage_units: list[str]) -> list[str]:
        stage_content = "".join(stage_units)
        if _PNPM_REGISTRY_MARKER in stage_content or not _PNPM_COMMAND.search(stage_content):
            return stage_units
        for index, unit in enumerate(stage_units):
            if _instruction_name(unit) != "RUN" or not _PNPM_COMMAND.search(unit):
                continue
            indent = re.match(r"^[ \t]*", unit).group(0)
            return [
                *stage_units[:index],
                f"{indent}{_PNPM_REGISTRY_MARKER}\n",
                f"{indent}ENV NPM_CONFIG_REGISTRY={registry}/\n",
                *stage_units[index:],
            ]
        return stage_units

    return _rewrite_stages(content, rewrite_stage)


def _rewrite_go_proxy(content: str, proxy: str) -> str:
    """Point the Go toolchain at the internal module proxy.

    Go does not read the npm or apt mirrors: it fetches from proxy.golang.org
    and sum.golang.org directly through the corporate proxy, which is the
    single largest source of validator build failures.

    GOSUMDB=off and GONOSUMDB disable checksum verification, which the mirror
    does not serve. GOPRIVATE is deliberately NOT set: it makes Go bypass the
    proxy and clone from the origin over git, which is exactly the unreachable
    path being routed around. A measured fetch took 61s and failed with
    "dial tcp ... i/o timeout" direct, 164s and failed with GOPRIVATE=*, and
    977ms through the proxy alone.
    """

    def rewrite_stage(stage_units: list[str]) -> list[str]:
        stage_content = "".join(stage_units)
        if _GO_PROXY_MARKER in stage_content or not _GO_COMMAND.search(stage_content):
            return stage_units
        for index, unit in enumerate(stage_units):
            if _instruction_name(unit) != "RUN" or not _GO_COMMAND.search(unit):
                continue
            indent = re.match(r"^[ \t]*", unit).group(0)
            return [
                *stage_units[:index],
                f"{indent}{_GO_PROXY_MARKER}\n",
                f"{indent}ENV GO111MODULE=on \\\n"
                f"{indent}    GOPROXY={proxy} \\\n"
                f"{indent}    GOSUMDB=off \\\n"
                f"{indent}    GONOSUMDB=* \\\n"
                f"{indent}    GONOSUMCHECK=1\n",
                *stage_units[index:],
            ]
        return stage_units

    return _rewrite_stages(content, rewrite_stage)


def _rewrite_cargo_mirror(content: str, registry: str) -> str:
    """Point Cargo at the internal sparse registry mirror.

    Like Go, Cargo bypasses every other mirror and reaches crates.io directly.
    The config is written to /usr/local/cargo/config.toml and ~/.cargo/config
    both: the former is what CARGO_HOME-based images read, the latter covers
    images that leave CARGO_HOME unset. The mirror publishes its own download
    endpoint in config.json, so only the registry needs replacing here.
    """

    def rewrite_stage(stage_units: list[str]) -> list[str]:
        stage_content = "".join(stage_units)
        if _CARGO_MIRROR_MARKER in stage_content or not _CARGO_COMMAND.search(stage_content):
            return stage_units
        for index, unit in enumerate(stage_units):
            if _instruction_name(unit) != "RUN" or not _CARGO_COMMAND.search(unit):
                continue
            indent = re.match(r"^[ \t]*", unit).group(0)
            # Cargo needs a trailing slash on a sparse index and the "sparse+"
            # scheme prefix to skip the git index protocol entirely. Both TOML
            # strings use double quotes: the printf body is single-quoted for
            # the shell, so a nested single quote would be eaten and yield an
            # unquoted, invalid `replace-with = mirror`.
            config = (
                "[source.crates-io]\\n"
                'replace-with = "mirror"\\n'
                "[source.mirror]\\n"
                f'registry = "sparse+{registry}/"\\n'
            )
            return [
                *stage_units[:index],
                f"{indent}{_CARGO_MIRROR_MARKER}\n",
                # Only config.toml is written. Cargo warns "both config and
                # config.toml exist" and then silently ignores config.toml,
                # so emitting both would leave the mirror unapplied on any
                # image that already ships a legacy ~/.cargo/config.
                f'{indent}RUN mkdir -p "${{CARGO_HOME:-/usr/local/cargo}}" "$HOME/.cargo" && '
                f"printf '{config}' | tee "
                f'"${{CARGO_HOME:-/usr/local/cargo}}/config.toml" '
                f'"$HOME/.cargo/config.toml" >/dev/null && '
                f'rm -f "${{CARGO_HOME:-/usr/local/cargo}}/config" "$HOME/.cargo/config"\n',
                *stage_units[index:],
            ]
        return stage_units

    return _rewrite_stages(content, rewrite_stage)


def _strip_unresolved_env_templates(content: str) -> str:
    """Remove unresolved template-valued ENV assignments that Docker cannot parse."""

    rendered: list[str] = []
    for unit in _dockerfile_units(content):
        if _instruction_name(unit) != "ENV" or "{{" not in unit:
            rendered.append(unit)
            continue
        lines = unit.splitlines(keepends=True)
        kept = [line for line in lines if "{{" not in line and "}}" not in line]
        if kept:
            last = kept[-1]
            newline = "\n" if last.endswith("\n") else ""
            body = last[:-1] if newline else last
            kept[-1] = re.sub(r"[ \t]*\\[ \t]*$", "", body) + newline
        rendered.append("".join(kept))
    return "".join(rendered)


def _rewrite_playwright_download_retry(content: str) -> str:
    """Retry Playwright browser downloads that intermittently time out in China."""

    if "PLAYWRIGHT_DOWNLOAD_CONNECTION_TIMEOUT=" in content:
        return content

    def replacement(match: re.Match[str]) -> str:
        command = match.group("command")
        timed = f"PLAYWRIGHT_DOWNLOAD_CONNECTION_TIMEOUT=300000 {command}"
        return f"({timed} || (sleep 10 && {timed}) || (sleep 20 && {timed}))"

    return _PLAYWRIGHT_INSTALL_COMMAND.sub(replacement, content)


def _strip_yarn_classic_env_for_berry(content: str) -> str:
    """Remove Yarn 1-only environment settings from Yarn Berry task images."""

    if "yarn install --immutable" not in content.lower():
        return content

    rendered: list[str] = []
    for unit in _dockerfile_units(content):
        if _instruction_name(unit) != "ENV" or "YARN_" not in unit:
            rendered.append(unit)
            continue
        lines = unit.splitlines(keepends=True)
        kept = [line for line in lines if _YARN_CLASSIC_ENV.match(line) is None]
        if len(kept) == len(lines):
            rendered.append(unit)
            continue
        if kept:
            last = kept[-1]
            newline = "\n" if last.endswith("\n") else ""
            body = last[:-1] if newline else last
            kept[-1] = re.sub(r"[ \t]*\\[ \t]*$", "", body) + newline
        rendered.append("".join(kept))
    return "".join(rendered)


def _rewrite_redis_stack_repository(content: str) -> str:
    """Use Redis Stack's supported Jammy repository on Ubuntu 24.04.

    packages.redis.io currently publishes Redis Stack packages for Jammy but
    not Noble.  Restrict the compatibility rewrite to Dockerfiles that
    explicitly install ``redis-stack-server`` so ordinary Redis repositories
    keep their requested suite.
    """

    if "redis-stack-server" not in content:
        return content
    return _REDIS_STACK_NOBLE_REPOSITORY.sub(r"\1jammy\2", content)


def _rewrite_java_ca_trust(content: str) -> str:
    """Explicitly import the proxy CA into the JVM truststore when Java is used."""

    lowered = content.lower()
    if (
        _JAVA_CA_TRUST_ALIAS in content
        or "/tmp/swegen-proxy-ca.crt" not in content
        or "update-ca-certificates" not in content
        or not any(
            package in lowered
            for package in ("openjdk", "default-jdk", "default-jre", "jre-headless")
        )
    ):
        return content

    trust_setup = (
        "update-ca-certificates \\\n"
        "    && if command -v keytool >/dev/null 2>&1; then \\\n"
        f"        keytool -delete -alias {_JAVA_CA_TRUST_ALIAS} "
        "-keystore /etc/ssl/certs/java/cacerts -storepass changeit "
        "2>/dev/null || true; \\\n"
        f"        keytool -importcert -noprompt -trustcacerts -alias {_JAVA_CA_TRUST_ALIAS} "
        "-file /usr/local/share/ca-certificates/swegen-proxy-ca.crt "
        "-keystore /etc/ssl/certs/java/cacerts -storepass changeit; \\\n"
        "    fi"
    )
    return content.replace("update-ca-certificates", trust_setup, 1)


def rewrite_ubuntu_mirrors(dockerfile: Path) -> bool:
    """Rewrite Ubuntu URLs and configure apt and npm package mirrors."""

    if not dockerfile.is_file():
        return False
    mirrors = _configured_mirrors()
    original = dockerfile.read_text(encoding="utf-8")
    updated = _strip_generated_ubuntu_blocks(original)
    for source, target in _direct_url_rewrites(mirrors):
        updated = updated.replace(source, target)

    updated = _strip_unresolved_env_templates(updated)
    updated = _rewrite_proxy_ca_permissions(updated)
    updated = _rewrite_mixed_debian_repository(updated)
    updated = _strip_stale_node_tarball_checksum(updated)
    updated = _rewrite_corepack_pnpm_bootstrap(updated)
    updated = _rewrite_curl_download_retries(updated)
    updated = _rewrite_apt_retries(updated)
    updated = _rewrite_redis_stack_repository(updated)
    updated = _rewrite_java_ca_trust(updated)
    updated = _rewrite_dotnet_icu(updated)
    updated = _rewrite_deno_ca(updated)
    updated = _strip_yarn_classic_env_for_berry(updated)

    updated = _rewrite_stages(
        updated,
        lambda stage: _rewrite_ubuntu_stage(stage, mirrors),
    )

    updated = _rewrite_npm_mirror(updated, mirrors["npm"])
    updated = _rewrite_npm_ci_fallback(updated)
    updated = _rewrite_bun_registry_and_ca(updated, mirrors["npm"])
    updated = _rewrite_pnpm_registry(updated, mirrors["npm"])
    updated = _rewrite_pnpm_frozen_lockfile_fallback(updated)
    updated = _rewrite_go_proxy(updated, mirrors["go"])
    updated = _rewrite_cargo_mirror(updated, mirrors["cargo"])
    updated = _rewrite_playwright_download_retry(updated)

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
