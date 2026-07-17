from __future__ import annotations

import fcntl
import json
import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import IO

logger = logging.getLogger("swegen")


@dataclass
class TaskReference:
    """Reference to a successful task that can be reused."""

    repo: str
    task_id: str
    pr_number: int
    created_at: str | None = None


class TaskReferenceStore:
    """Stores references to successful tasks for reuse across PRs."""

    def __init__(self, reference_file: Path | None = None):
        """
        Initialize task reference store.

        Args:
            reference_file: Path to JSON file storing references (default: .swegen/task_references.json)
        """
        self.reference_file = reference_file or Path(".swegen/task_references.json")
        self.lock_file = self.reference_file.with_name(f"{self.reference_file.name}.lock")
        self.reference_file.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _locked(self, *, exclusive: bool) -> Iterator[IO[str]]:
        """Hold a cross-process advisory lock for reference store access."""
        with self.lock_file.open("a+", encoding="utf-8") as lock_fh:
            operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
            fcntl.flock(lock_fh.fileno(), operation)
            try:
                yield lock_fh
            finally:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)

    def _load_references(self) -> dict[str, list[TaskReference]]:
        """Load all references from file."""
        if not self.reference_file.exists():
            return {}

        try:
            data = json.loads(self.reference_file.read_text())
            references: dict[str, list[TaskReference]] = {}
            for repo, ref_data in data.items():
                if isinstance(ref_data, list):
                    references[repo] = [
                        TaskReference(**item) for item in ref_data if isinstance(item, dict)
                    ]
                elif isinstance(ref_data, dict):
                    # Backward compatibility with the old one-reference-per-repo format.
                    references[repo] = [TaskReference(**ref_data)]
            return references
        except Exception as e:
            logger.warning(f"Failed to load task references: {e}")
            return {}

    def _save_references(self, references: dict[str, list[TaskReference]]) -> None:
        """Save all references to file."""
        data = {repo: [asdict(ref) for ref in refs] for repo, refs in references.items()}
        temporary = self.reference_file.with_name(f".{self.reference_file.name}.tmp.{os.getpid()}")
        try:
            with temporary.open("w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2)
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(temporary, self.reference_file)
        finally:
            temporary.unlink(missing_ok=True)

    def save(
        self,
        repo: str,
        task_id: str,
        pr_number: int,
    ) -> bool:
        """
        Save a reference to a successful task.

        Args:
            repo: Repository name (owner/repo)
            task_id: Task identifier of the successful task
            pr_number: PR number

        Returns:
            True if reference was saved successfully
        """
        try:
            # Create reference
            reference = TaskReference(
                repo=repo,
                task_id=task_id,
                pr_number=pr_number,
                created_at=datetime.now(UTC).isoformat(),
            )

            with self._locked(exclusive=True):
                references = self._load_references()
                repo_references = references.setdefault(repo, [])
                repo_references[:] = [
                    ref
                    for ref in repo_references
                    if not (ref.task_id == task_id or ref.pr_number == pr_number)
                ]
                repo_references.append(reference)
                repo_references.sort(key=lambda ref: ref.pr_number)
                self._save_references(references)

            logger.info(f"✓ Saved task reference for {repo} → {task_id}")
            return True

        except Exception as e:
            logger.warning(f"Failed to save task reference: {e}")
            return False

    def get(
        self,
        repo: str,
        current_pr_number: int | None = None,
        max_age_days: int = 180,
        tasks_root: Path | None = None,
    ) -> TaskReference | None:
        """
        Get reference to a successful task for reuse.

        Args:
            repo: Repository name (owner/repo)
            current_pr_number: Current PR number, used to choose the closest reference
            max_age_days: Maximum age of reference in days (default: 180)
            tasks_root: If provided, require the reference Dockerfile to exist

        Returns:
            TaskReference if valid reference exists, None otherwise
        """
        try:
            with self._locked(exclusive=False):
                references = self._load_references()

            if repo not in references:
                logger.debug(f"No task reference found for {repo}")
                return None

            candidates: list[TaskReference] = []
            for reference in references[repo]:
                if current_pr_number is not None and reference.pr_number == current_pr_number:
                    continue

                # Check age
                if reference.created_at:
                    created = datetime.fromisoformat(reference.created_at)
                    if created.tzinfo is None:
                        created = created.replace(tzinfo=UTC)
                    age_days = (datetime.now(UTC) - created).days
                    if age_days > max_age_days:
                        logger.debug(
                            f"Reference too old for {repo}: {age_days} days > {max_age_days}"
                        )
                        continue

                if tasks_root is not None:
                    dockerfile = tasks_root / reference.task_id / "environment" / "Dockerfile"
                    if not dockerfile.is_file():
                        logger.debug(
                            "Reference Dockerfile missing for %s: %s",
                            reference.task_id,
                            dockerfile,
                        )
                        continue

                candidates.append(reference)

            if not candidates:
                logger.debug(f"No usable task reference found for {repo}")
                return None

            if current_pr_number is None:
                reference = max(candidates, key=lambda ref: ref.created_at or "")
            else:
                reference = min(
                    candidates,
                    key=lambda ref: (
                        abs(ref.pr_number - current_pr_number),
                        ref.pr_number,
                    ),
                )

            logger.info(
                f"✓ Found task reference for {repo} → {reference.task_id} "
                f"(from PR #{reference.pr_number})"
            )
            return reference

        except Exception as e:
            logger.warning(f"Failed to get task reference for {repo}: {e}")
            return None
