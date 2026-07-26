from __future__ import annotations

import re
import shutil
from pathlib import Path

CERTIFICATE_NAME = "swegen-proxy-ca.crt"
BEGIN_MARKER = "# BEGIN SWEGEN PROXY CA SETUP"
END_MARKER = "# END SWEGEN PROXY CA SETUP"
FROM_RE = re.compile(
    r"^\s*FROM\s+(?:--platform=\S+\s+)?\S+"
    r"(?:\s+AS\s+[A-Za-z0-9_.-]+)?\s*(?:#.*)?$",
    re.IGNORECASE,
)


def bundled_certificate_path() -> Path:
    return Path(__file__).parent / "assets" / CERTIFICATE_NAME


def proxy_setup_block(newline: str = "\n") -> str:
    return newline.join(
        [
            BEGIN_MARKER,
            f"COPY {CERTIFICATE_NAME} /tmp/{CERTIFICATE_NAME}",
            "",
            "RUN apt-get update \\",
            "    && apt-get install -y --no-install-recommends ca-certificates \\",
            "    && mkdir -p /usr/local/share/ca-certificates \\",
            f"    && cp /tmp/{CERTIFICATE_NAME} "
            f"/usr/local/share/ca-certificates/{CERTIFICATE_NAME} \\",
            "    && update-ca-certificates \\",
            f"    && rm /tmp/{CERTIFICATE_NAME} \\",
            "    && rm -rf /var/lib/apt/lists/*",
            END_MARKER,
        ]
    )


def add_proxy_setup(text: str) -> tuple[str, bool]:
    if BEGIN_MARKER in text:
        return text, False
    lines = text.splitlines(keepends=True)
    from_indices = [
        index for index, line in enumerate(lines) if FROM_RE.fullmatch(line.rstrip("\r\n"))
    ]
    if not from_indices:
        raise ValueError("Dockerfile has no recognizable FROM instruction")
    newline = "\r\n" if "\r\n" in text else "\n"
    insert_at = from_indices[-1] + 1
    lines.insert(insert_at, newline + proxy_setup_block(newline) + newline)
    return "".join(lines), True


def copy_proxy_certificate(environment_dir: Path) -> Path:
    source = bundled_certificate_path()
    if not source.is_file():
        raise FileNotFoundError(f"Bundled proxy certificate is missing: {source}")
    destination = environment_dir / CERTIFICATE_NAME
    environment_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return destination
