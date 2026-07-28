from __future__ import annotations

import fcntl
import json
import os
import socket
import tempfile
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class QuotaSnapshot:
    limit: int
    successes: int
    reservations: int

    @property
    def reached(self) -> bool:
        return self.successes >= self.limit


class ProductionQuota:
    """Cross-process success quota backed by a locked run-state file.

    A reservation is acquired before a PR is claimed. Failed attempts release
    their reservation, while fully successful attempts convert it into a
    durable success count. The same state file is shared by local threads and
    Slurm workers using the same run directory.
    """

    def __init__(
        self,
        path: Path,
        limit: int,
        *,
        stale_after_seconds: int,
        poll_interval: float = 0.5,
    ) -> None:
        if limit < 1:
            raise ValueError("production quota limit must be >= 1")
        self.path = path
        self.limit = limit
        self.stale_after_seconds = max(1, stale_after_seconds)
        self.poll_interval = max(0.01, poll_interval)
        self.hostname = socket.gethostname()
        self.lock_path = path.with_name(f"{path.name}.lock")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._locked_state() as state:
            self._validate_limit(state)

    @contextmanager
    def _locked_state(self) -> Iterator[dict[str, Any]]:
        with self.lock_path.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            try:
                content = (
                    self.path.read_text(encoding="utf-8").strip() if self.path.exists() else ""
                )
                state = json.loads(content) if content else self._initial_state()
                if not isinstance(state, dict):
                    raise RuntimeError(f"Invalid production quota state in {self.path}")
                yield state
                temporary_path: Path | None = None
                try:
                    with tempfile.NamedTemporaryFile(
                        "w",
                        encoding="utf-8",
                        dir=self.path.parent,
                        prefix=f".{self.path.name}.",
                        delete=False,
                    ) as temporary:
                        temporary_path = Path(temporary.name)
                        json.dump(state, temporary, sort_keys=True)
                        temporary.write("\n")
                        temporary.flush()
                        os.fsync(temporary.fileno())
                    os.replace(temporary_path, self.path)
                    temporary_path = None
                finally:
                    if temporary_path is not None:
                        temporary_path.unlink(missing_ok=True)
            finally:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

    def _initial_state(self) -> dict[str, Any]:
        return {"limit": self.limit, "successes": 0, "reservations": {}}

    def _validate_limit(self, state: dict[str, Any]) -> None:
        state_limit = int(state.get("limit", self.limit))
        if state_limit != self.limit:
            raise ValueError(
                f"Production quota state {self.path} has limit {state_limit}, "
                f"but swegen.toml requests {self.limit}"
            )
        state["limit"] = self.limit
        state.setdefault("successes", 0)
        state.setdefault("reservations", {})
        if not isinstance(state["reservations"], dict):
            raise RuntimeError(f"Invalid reservations in production quota state {self.path}")

    @staticmethod
    def _pid_is_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def _remove_stale_reservations(self, state: dict[str, Any]) -> None:
        now = time.time()
        reservations = state["reservations"]
        stale: list[str] = []
        for token, reservation in reservations.items():
            if not isinstance(reservation, dict):
                stale.append(token)
                continue
            hostname = str(reservation.get("hostname", ""))
            pid = int(reservation.get("pid", 0) or 0)
            created_at = float(reservation.get("created_at", 0) or 0)
            owner_gone = hostname == self.hostname and pid > 0 and not self._pid_is_alive(pid)
            expired = created_at <= 0 or now - created_at > self.stale_after_seconds
            if owner_gone or expired:
                stale.append(token)
        for token in stale:
            reservations.pop(token, None)

    def acquire(self) -> str | None:
        """Reserve capacity for one PR, waiting until available or complete."""
        while True:
            with self._locked_state() as state:
                self._validate_limit(state)
                self._remove_stale_reservations(state)
                successes = int(state["successes"])
                reservations = state["reservations"]
                if successes >= self.limit:
                    return None
                if successes + len(reservations) < self.limit:
                    token = uuid.uuid4().hex
                    reservations[token] = {
                        "hostname": self.hostname,
                        "pid": os.getpid(),
                        "created_at": time.time(),
                    }
                    return token
            time.sleep(self.poll_interval)

    def complete(self, token: str, *, success: bool) -> QuotaSnapshot:
        """Release a reservation and optionally count a successful instance."""
        with self._locked_state() as state:
            self._validate_limit(state)
            reservation = state["reservations"].pop(token, None)
            if reservation is None:
                raise RuntimeError(f"Unknown or expired production quota reservation: {token}")
            if success:
                state["successes"] = int(state["successes"]) + 1
                if state["successes"] > self.limit:
                    raise RuntimeError("Production quota exceeded its configured limit")
            return self._snapshot_from_state(state)

    def snapshot(self) -> QuotaSnapshot:
        with self._locked_state() as state:
            self._validate_limit(state)
            self._remove_stale_reservations(state)
            return self._snapshot_from_state(state)

    def _snapshot_from_state(self, state: dict[str, Any]) -> QuotaSnapshot:
        return QuotaSnapshot(
            limit=self.limit,
            successes=int(state["successes"]),
            reservations=len(state["reservations"]),
        )
