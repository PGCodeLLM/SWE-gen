from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path

from harbor.environments.docker.docker import DockerEnvironment
from harbor.models.task.config import EnvironmentConfig
from harbor.models.trial.paths import TrialPaths

from swegen.tools.remote_buildkit import (
    DatabaseBuildTracker,
    RemoteBuildkitClient,
    RemoteBuildkitConfig,
    RemoteBuildkitError,
    context_digest,
    create_context_archive,
    select_build_route,
)

DOCKER_IMAGE_SUFFIX = "-swegenimage"
_IMAGE_COMPOSE_TEMPLATE = """services:
  main:
    image: ${MAIN_IMAGE_NAME}
"""


def _append_suffix_once(name: str, suffix: str) -> str:
    return name if name.endswith(suffix) else f"{name}{suffix}"


def with_swegen_image_suffix(name: str) -> str:
    """Append the swegen Docker image suffix once."""
    return _append_suffix_once(name, DOCKER_IMAGE_SUFFIX)


class SwegenDockerEnvironment(DockerEnvironment):
    """Docker environment that tags SWE-gen-built images predictably."""

    def __init__(
        self,
        environment_dir: Path,
        environment_name: str,
        session_id: str,
        trial_paths: TrialPaths,
        task_env_config: EnvironmentConfig,
        *args,
        **kwargs,
    ):
        super().__init__(
            environment_dir,
            environment_name,
            session_id,
            trial_paths,
            task_env_config,
            *args,
            **kwargs,
        )
        self._env_vars.main_image_name = with_swegen_image_suffix(self._env_vars.main_image_name)
        self._swegen_compose_path = self.trial_paths.trial_dir / "docker-compose.swegen-image.yaml"

    @property
    def _docker_compose_paths(self) -> list[Path]:
        paths = list(super()._docker_compose_paths)
        if self._use_prebuilt:
            return paths

        self._swegen_compose_path.write_text(_IMAGE_COMPOSE_TEMPLATE)
        return [*paths, self._swegen_compose_path]

    async def start(self, force_build: bool):
        """Start with a deterministic local/remote BuildKit routing decision."""

        # Preserve Harbor's explicit prebuilt-image and force-build semantics.
        # Custom compose definitions cannot safely be rewritten to Harbor's
        # one-service prebuilt compose file, so they remain local too.
        if (
            self.task_env_config.docker_image is not None
            or force_build
            or self._environment_docker_compose_path.exists()
        ):
            await super().start(force_build)
            return

        try:
            config = RemoteBuildkitConfig.from_env()
        except ValueError as error:
            self.logger.warning("Remote BuildKit configuration is invalid: %s", error)
            await super().start(force_build)
            return
        if config is None:
            await super().start(force_build)
            return

        registry_rewrites = config.dockerfile_registry_rewrites
        digest = await asyncio.to_thread(
            context_digest,
            self.environment_dir,
            dockerfile_registry_rewrites=registry_rewrites,
        )
        if select_build_route(config, digest) == "local":
            await super().start(force_build)
            return

        image_ref = config.image_ref(digest)
        lock = self._image_build_locks.setdefault(image_ref, asyncio.Lock())
        tracker = DatabaseBuildTracker()
        request_id: str | None = None
        try:
            async with lock:
                if not await self._pull_image(image_ref, config.pull_timeout_seconds):
                    with tempfile.TemporaryDirectory(
                        prefix="swegen-remote-build-"
                    ) as temporary_directory:
                        archive_path = Path(temporary_directory) / "context.tar"
                        await asyncio.to_thread(
                            create_context_archive,
                            self.environment_dir,
                            archive_path,
                            dockerfile_registry_rewrites=registry_rewrites,
                        )
                        client = RemoteBuildkitClient(config)
                        result = await asyncio.to_thread(
                            client.build,
                            archive_path=archive_path,
                            context_digest=digest,
                            environment_name=self.environment_name,
                            tracker=tracker,
                            worker_id=os.environ.get("POD_NAME"),
                            node_name=os.environ.get("NODE_NAME"),
                        )
                        request_id = result.request_id
                        image_ref = result.image_ref
                    if not await self._pull_image(image_ref, config.pull_timeout_seconds):
                        tracker.update(
                            request_id,
                            status="pull_failed",
                            error=f"local Docker could not pull {image_ref}",
                            terminal=True,
                        )
                        raise RemoteBuildkitError(
                            f"remote image was built but could not be pulled: {image_ref}"
                        )

            self._env_vars.prebuilt_image_name = image_ref
            self._use_prebuilt = True
            await self._run_docker_compose_command(["up", "-d"])
        except RemoteBuildkitError as error:
            if not config.fallback_local:
                raise
            if request_id is not None:
                tracker.update(
                    request_id,
                    status="fallback_local",
                    error=str(error),
                    terminal=True,
                )
            self.logger.warning(
                "Remote BuildKit failed for %s; falling back to local slots: %s",
                self.environment_name,
                error,
            )
            await super().start(force_build)

    async def _pull_image(self, image_ref: str, timeout_seconds: float) -> bool:
        process = await asyncio.create_subprocess_exec(
            "docker",
            "pull",
            image_ref,
            env=os.environ.copy(),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
        except TimeoutError:
            process.terminate()
            try:
                await asyncio.wait_for(process.communicate(), timeout=5)
            except TimeoutError:
                process.kill()
                await process.communicate()
            return False
        return process.returncode == 0
