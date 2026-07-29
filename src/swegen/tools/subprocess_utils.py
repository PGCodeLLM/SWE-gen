"""Bounded subprocess execution with complete process-group cleanup."""

from __future__ import annotations

import os
import signal
import subprocess
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from math import isfinite
from pathlib import Path

DEFAULT_READ_BYTES = 64 * 1024
DEFAULT_STOP_GRACE_SECONDS = 10.0


@dataclass(frozen=True, slots=True)
class BoundedCommandResult:
    returncode: int
    duration_seconds: float
    output_bytes: int
    log_bytes: int
    output_truncated: bool
    tail: str


class _BoundedTail:
    def __init__(self, max_bytes: int) -> None:
        self.max_bytes = max_bytes
        self.total_bytes = 0
        self._content = bytearray()
        self._lock = threading.Lock()

    def append(self, chunk: bytes) -> None:
        with self._lock:
            self.total_bytes += len(chunk)
            if len(chunk) >= self.max_bytes:
                self._content[:] = chunk[-self.max_bytes :]
                return
            self._content.extend(chunk)
            overflow = len(self._content) - self.max_bytes
            if overflow > 0:
                del self._content[:overflow]

    def snapshot(self) -> tuple[bytes, int]:
        with self._lock:
            return bytes(self._content), self.total_bytes


def _process_group_exists(process_group_id: int) -> bool:
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _terminate_process_group(
    process: subprocess.Popen[bytes],
    grace_seconds: float,
) -> str | None:
    errors: list[str] = []
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except OSError as error:
        errors.append(f"SIGTERM failed: {error}")

    if process.poll() is None:
        try:
            process.wait(timeout=grace_seconds)
        except subprocess.TimeoutExpired:
            pass
        except OSError as error:
            errors.append(f"wait after SIGTERM failed: {error}")
    else:
        time.sleep(min(grace_seconds, 0.05))

    if process.poll() is None or _process_group_exists(process.pid):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except OSError as error:
            errors.append(f"SIGKILL failed: {error}")

    if process.poll() is None:
        try:
            process.wait(timeout=grace_seconds)
        except (OSError, subprocess.TimeoutExpired) as error:
            errors.append(f"final wait failed: {error}")
    return "; ".join(errors) or None


def _safe_tail(
    raw_tail: bytes,
    *,
    max_bytes: int,
    redactor: Callable[[str], str],
) -> str:
    decoded = raw_tail.decode("utf-8", errors="replace")
    redacted = redactor(decoded).strip()
    bounded = redacted.encode("utf-8")[-max_bytes:]
    return bounded.decode("utf-8", errors="replace").strip()


def _write_tail(log_stream, safe_tail: str, max_bytes: int) -> int:
    encoded = safe_tail.encode("utf-8")[-max_bytes:]
    log_stream.seek(0)
    log_stream.truncate(0)
    log_stream.write(encoded)
    log_stream.flush()
    return len(encoded)


def run_bounded_command(
    command: Sequence[str],
    *,
    timeout_seconds: float,
    max_output_bytes: int,
    cwd: Path | None = None,
    log_path: Path | None = None,
    env: Mapping[str, str] | None = None,
    redactor: Callable[[str], str] = lambda value: value,
    stop_grace_seconds: float = DEFAULT_STOP_GRACE_SECONDS,
) -> BoundedCommandResult:
    """Run one command with bounded output and cleanup of its complete process group."""

    if not command:
        raise ValueError("command must not be empty")
    if not isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be a positive finite number")
    if max_output_bytes <= 0:
        raise ValueError("max_output_bytes must be positive")
    if not isfinite(stop_grace_seconds) or stop_grace_seconds <= 0:
        raise ValueError("stop_grace_seconds must be a positive finite number")

    log_stream = None
    if log_path is not None:
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_stream = log_path.open("w+b")
        except OSError as error:
            raise RuntimeError(f"failed to open command log: {redactor(str(error))}") from error

    started_at = time.monotonic()
    tail_buffer = _BoundedTail(max_output_bytes)
    reader_error: BaseException | None = None
    process: subprocess.Popen[bytes] | None = None
    reader: threading.Thread | None = None
    timed_out = False
    timeout_error: subprocess.TimeoutExpired | None = None
    cleanup_error: str | None = None
    return_code: int | None = None

    try:
        try:
            process = subprocess.Popen(
                list(command),
                cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                env=dict(env) if env is not None else None,
            )
        except OSError as error:
            if log_stream is not None:
                log_stream.close()
            raise RuntimeError(f"failed to start command: {redactor(str(error))}") from error
        if process.stdout is None:  # pragma: no cover - guaranteed by stdout=PIPE
            raise RuntimeError("command output pipe was not created")

        def drain_output() -> None:
            nonlocal reader_error
            try:
                while chunk := process.stdout.read(DEFAULT_READ_BYTES):
                    tail_buffer.append(chunk)
            except (OSError, ValueError) as error:
                reader_error = error

        reader = threading.Thread(
            target=drain_output,
            name="swegen-command-output",
            daemon=True,
        )
        reader.start()
        try:
            return_code = process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired as error:
            timed_out = True
            timeout_error = error
    finally:
        if process is not None:
            cleanup_error = _terminate_process_group(process, stop_grace_seconds)
            if reader is not None:
                reader.join(timeout=stop_grace_seconds)
                if reader.is_alive() and process.stdout is not None:
                    try:
                        os.close(process.stdout.fileno())
                    except OSError as error:
                        cleanup_error = cleanup_error or f"stdout close failed: {error}"
                    reader.join(timeout=stop_grace_seconds)
                if reader.is_alive():
                    cleanup_error = cleanup_error or "command output reader did not stop"
            if process.stdout is not None:
                try:
                    process.stdout.close()
                except OSError as error:
                    cleanup_error = cleanup_error or f"stdout close failed: {error}"

    raw_tail, total_output_bytes = tail_buffer.snapshot()
    safe_tail = _safe_tail(raw_tail, max_bytes=max_output_bytes, redactor=redactor)
    written_bytes = 0
    try:
        if log_stream is not None:
            written_bytes = _write_tail(log_stream, safe_tail, max_output_bytes)
    except OSError as error:
        raise RuntimeError(
            f"failed to write command log: {redactor(str(error))}; tail: {safe_tail or '<empty>'}"
        ) from error
    finally:
        if log_stream is not None:
            log_stream.close()

    if timed_out:
        raise TimeoutError(
            f"command timed out after {timeout_seconds:g} seconds; tail: {safe_tail or '<empty>'}"
        ) from timeout_error
    if reader_error is not None:
        raise RuntimeError(
            f"failed while streaming command output; tail: {safe_tail or '<empty>'}"
        ) from reader_error
    if cleanup_error is not None:
        raise RuntimeError(
            f"failed to clean up command process: {redactor(cleanup_error)}; "
            f"tail: {safe_tail or '<empty>'}"
        )
    if return_code is None:
        raise RuntimeError(f"command produced no exit status; tail: {safe_tail or '<empty>'}")

    return BoundedCommandResult(
        returncode=return_code,
        duration_seconds=round(time.monotonic() - started_at, 3),
        output_bytes=total_output_bytes,
        log_bytes=written_bytes,
        output_truncated=total_output_bytes > len(raw_tail),
        tail=safe_tail,
    )


__all__ = ["BoundedCommandResult", "run_bounded_command"]
