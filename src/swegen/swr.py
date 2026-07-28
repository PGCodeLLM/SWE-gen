from __future__ import annotations

import os
import re
import subprocess
import time
from dataclasses import dataclass

from swegen.model_settings import SWRSettings

PROXY_ENVIRONMENT_VARIABLES = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "DOCKER_HTTP_PROXY",
    "DOCKER_HTTPS_PROXY",
    "DOCKER_NO_PROXY",
)


@dataclass(frozen=True)
class SWRUploadResult:
    success: bool
    status: str
    remote_ref: str = ""


def _remote_image_name(instance_id: str, settings: SWRSettings) -> str:
    image = re.sub(r"[^a-z0-9._-]", "-", instance_id.lower())
    return f"{settings.registry}/{settings.repository}:{settings.tag_prefix}{image}"


def _proxy_free_environment() -> dict[str, str]:
    """Return the current environment with proxy routing disabled for SWR."""
    env = os.environ.copy()
    for name in PROXY_ENVIRONMENT_VARIABLES:
        env.pop(name, None)
    return env


def upload_image_to_swr(
    instance_id: str,
    local_image_refs: tuple[str, ...],
    settings: SWRSettings,
) -> SWRUploadResult:
    """Tag and push the retained Harbor image to Huawei SWR.

    Credentials are passed to ``docker login`` through stdin and are never
    included in argv or logs. The caller must retain the local image whenever
    this function returns ``success=False``.
    """
    if not settings.enabled:
        return SWRUploadResult(False, "SWR upload is disabled in swegen.toml")
    refs = tuple(dict.fromkeys(ref for ref in local_image_refs if ref))
    if not refs:
        return SWRUploadResult(False, "No retained Harbor image tag was found")

    swr_env = _proxy_free_environment()
    local_ref = ""
    for candidate in refs:
        inspect = subprocess.run(
            ["docker", "image", "inspect", candidate],
            check=False,
            capture_output=True,
            text=True,
            timeout=settings.push_timeout,
            env=swr_env,
        )
        if inspect.returncode == 0:
            local_ref = candidate
            break
    if not local_ref:
        return SWRUploadResult(False, "Retained Harbor image is no longer available")

    remote_ref = _remote_image_name(instance_id, settings)
    last_error = ""
    for attempt in range(1, settings.retries + 1):
        try:
            login = subprocess.run(
                [
                    "docker",
                    "login",
                    settings.registry,
                    "--username",
                    settings.username,
                    "--password-stdin",
                ],
                input=settings.password,
                check=False,
                capture_output=True,
                text=True,
                timeout=settings.push_timeout,
                env=swr_env,
            )
            if login.returncode != 0:
                last_error = (login.stderr or login.stdout or "docker login failed").strip()
                raise RuntimeError(last_error)

            tag = subprocess.run(
                ["docker", "tag", local_ref, remote_ref],
                check=False,
                capture_output=True,
                text=True,
                timeout=settings.push_timeout,
                env=swr_env,
            )
            if tag.returncode != 0:
                last_error = (tag.stderr or tag.stdout or "docker tag failed").strip()
                raise RuntimeError(last_error)

            push = subprocess.run(
                ["docker", "push", remote_ref],
                check=False,
                capture_output=True,
                text=True,
                timeout=settings.push_timeout,
                env=swr_env,
            )
            if push.returncode == 0:
                return SWRUploadResult(True, "uploaded", remote_ref)
            last_error = (push.stderr or push.stdout or "docker push failed").strip()
        except (OSError, subprocess.TimeoutExpired, RuntimeError) as exc:
            last_error = str(exc)

        if attempt < settings.retries:
            time.sleep(min(5 * attempt, 30))

    detail = last_error.splitlines()[-1] if last_error else "unknown error"
    return SWRUploadResult(
        False,
        f"SWR upload failed after {settings.retries} attempt(s): {detail}",
        remote_ref,
    )
