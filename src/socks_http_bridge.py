#!/usr/bin/env python3
"""Expose an HTTP CONNECT proxy backed by an upstream SOCKS5 proxy.

Claude Code's transport accepts HTTP(S) proxy URLs but not SOCKS URLs.  This
small bridge lets each worker use a local HTTP proxy while the bridge performs
the actual connection through its assigned SOCKS5 endpoint with remote DNS.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import signal
from datetime import UTC, datetime
from pathlib import Path


def emit(event: str, **fields: object) -> None:
    print(
        json.dumps(
            {
                "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
                "event": event,
                **fields,
            },
            sort_keys=True,
        ),
        flush=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--socks-host", required=True)
    parser.add_argument("--socks-port", type=int, required=True)
    parser.add_argument("--connect-timeout", type=float, default=30.0)
    parser.add_argument("--pid-file", type=Path)
    return parser.parse_args()


def split_target(target: str) -> tuple[str, int]:
    if target.startswith("["):
        host, separator, suffix = target[1:].partition("]")
        if not separator or not suffix.startswith(":"):
            raise ValueError(f"invalid CONNECT target: {target}")
        return host, int(suffix[1:])
    host, separator, port = target.rpartition(":")
    if not separator or not host:
        raise ValueError(f"invalid CONNECT target: {target}")
    return host, int(port)


async def read_socks_reply(reader: asyncio.StreamReader) -> None:
    version, result, _reserved, address_type = await reader.readexactly(4)
    if version != 5 or result != 0:
        raise ConnectionError(f"SOCKS5 connect failed (version={version}, result={result})")
    if address_type == 1:
        await reader.readexactly(4)
    elif address_type == 3:
        length = (await reader.readexactly(1))[0]
        await reader.readexactly(length)
    elif address_type == 4:
        await reader.readexactly(16)
    else:
        raise ConnectionError(f"invalid SOCKS5 address type: {address_type}")
    await reader.readexactly(2)


async def connect_via_socks(
    socks_host: str,
    socks_port: int,
    target_host: str,
    target_port: int,
    timeout: float,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(socks_host, socks_port), timeout=timeout
    )
    try:
        writer.write(b"\x05\x01\x00")
        await writer.drain()
        if await asyncio.wait_for(reader.readexactly(2), timeout=timeout) != b"\x05\x00":
            raise ConnectionError("SOCKS5 proxy rejected unauthenticated access")

        encoded_host = target_host.encode("idna")
        if len(encoded_host) > 255:
            raise ValueError("target hostname is too long for SOCKS5")
        request = (
            b"\x05\x01\x00\x03"
            + bytes([len(encoded_host)])
            + encoded_host
            + target_port.to_bytes(2, "big")
        )
        writer.write(request)
        await writer.drain()
        await asyncio.wait_for(read_socks_reply(reader), timeout=timeout)
        return reader, writer
    except BaseException:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        raise


async def pump(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while data := await reader.read(65536):
        writer.write(data)
        await writer.drain()


async def relay(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    remote_reader: asyncio.StreamReader,
    remote_writer: asyncio.StreamWriter,
) -> None:
    tasks = {
        asyncio.create_task(pump(client_reader, remote_writer)),
        asyncio.create_task(pump(remote_reader, client_writer)),
    }
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*done, *pending, return_exceptions=True)


async def handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    args: argparse.Namespace,
) -> None:
    remote_writer: asyncio.StreamWriter | None = None
    target = "unknown"
    try:
        raw_headers = await asyncio.wait_for(
            client_reader.readuntil(b"\r\n\r\n"), timeout=args.connect_timeout
        )
        request_line = raw_headers.split(b"\r\n", 1)[0].decode("ascii", errors="replace")
        method, target, _version = request_line.split(" ", 2)
        if method.upper() != "CONNECT":
            client_writer.write(
                b"HTTP/1.1 405 Method Not Allowed\r\nConnection: close\r\n\r\n"
            )
            await client_writer.drain()
            return

        target_host, target_port = split_target(target)
        remote_reader, remote_writer = await connect_via_socks(
            args.socks_host,
            args.socks_port,
            target_host,
            target_port,
            args.connect_timeout,
        )
        client_writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await client_writer.drain()
        await relay(client_reader, client_writer, remote_reader, remote_writer)
    except (asyncio.IncompleteReadError, ConnectionResetError):
        pass
    except Exception as error:
        emit("connection_error", target=target, error=f"{type(error).__name__}: {error}")
        with contextlib.suppress(Exception):
            client_writer.write(
                b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n"
            )
            await client_writer.drain()
    finally:
        if remote_writer is not None:
            remote_writer.close()
            with contextlib.suppress(Exception):
                await remote_writer.wait_closed()
        client_writer.close()
        with contextlib.suppress(Exception):
            await client_writer.wait_closed()


async def run(args: argparse.Namespace) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(signum, stop.set)

    server = await asyncio.start_server(
        lambda reader, writer: handle_client(reader, writer, args),
        args.listen_host,
        args.listen_port,
    )
    emit(
        "listening",
        listen=f"{args.listen_host}:{args.listen_port}",
        upstream=f"{args.socks_host}:{args.socks_port}",
    )
    async with server:
        await stop.wait()
    emit("stopped")


def main() -> int:
    args = parse_args()
    if not 1 <= args.listen_port <= 65535 or not 1 <= args.socks_port <= 65535:
        raise SystemExit("ports must be between 1 and 65535")
    if args.connect_timeout <= 0:
        raise SystemExit("--connect-timeout must be positive")

    if args.pid_file is not None:
        args.pid_file.parent.mkdir(parents=True, exist_ok=True)
        args.pid_file.write_text(f"{os.getpid()}\n")
    try:
        asyncio.run(run(args))
    finally:
        if args.pid_file is not None:
            try:
                if args.pid_file.read_text().strip() == str(os.getpid()):
                    args.pid_file.unlink()
            except OSError:
                pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
