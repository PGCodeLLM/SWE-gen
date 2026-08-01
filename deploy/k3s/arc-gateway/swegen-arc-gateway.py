#!/usr/bin/env python3
"""Pooled Arcyleung reverse gateway for SWE-gen workers.

The public Service points at LiteLLM, which translates Anthropic Messages calls
to OpenAI chat completions. LiteLLM then calls this private aiohttp gateway.
The gateway owns a bounded pool of reusable CONNECT+TLS sessions to the working
corporate parent proxy, avoiding one fresh CONNECT per worker request.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import Counter
from urllib.parse import unquote, urlsplit, urlunsplit

from aiohttp import (
    BasicAuth,
    ClientError,
    ClientSession,
    ClientTimeout,
    TCPConnector,
    web,
)


ORIGIN = os.environ.get(
    "ARC_ORIGIN", "https://arcyleung-ubuntu.tailb940e6.ts.net"
).rstrip("/")
POOL_SIZE = int(os.environ.get("ARC_POOL_SIZE", "64"))
MAX_ATTEMPTS = int(os.environ.get("ARC_MAX_ATTEMPTS", "4"))
LISTEN_HOST = os.environ.get("ARC_LISTEN_HOST", "127.0.0.1")
LISTEN_PORT = int(os.environ.get("ARC_LISTEN_PORT", "3130"))

HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


def proxy_settings(value: str) -> tuple[str, BasicAuth | None]:
    parsed = urlsplit(value)
    if not parsed.hostname:
        raise RuntimeError("proxy URL has no hostname")
    port = parsed.port or 8080
    proxy_url = f"{parsed.scheme or 'http'}://{parsed.hostname}:{port}"
    auth = None
    if parsed.username is not None:
        auth = BasicAuth(unquote(parsed.username), unquote(parsed.password or ""))
    return proxy_url, auth


def sibling_proxy(value: str, hostname: str) -> str:
    parsed = urlsplit(value)
    userinfo = parsed.netloc.rsplit("@", 1)[0] + "@" if "@" in parsed.netloc else ""
    return urlunsplit(
        (parsed.scheme, userinfo + f"{hostname}:{parsed.port or 8080}", "", "", "")
    )


SG_PROXY_RAW = os.environ["ARC_SG_PROXY"]
SG_PROXY = proxy_settings(SG_PROXY_RAW)
HK_PROXY = proxy_settings(
    os.environ.get(
        "ARC_HK_PROXY", sibling_proxy(SG_PROXY_RAW, "proxyhk-spl.huawei.com")
    )
)
STARTED = time.monotonic()
COUNTERS: Counter[str] = Counter()
ACTIVE = 0


def route_for(path: str) -> tuple[str, tuple[str, BasicAuth | None]]:
    # The public LiteLLM sidecar translates Anthropic Messages traffic into
    # chat completions, so normal production traffic takes the healthy HK path.
    # Keep SG as a compatibility path only for callers that reach this private
    # port directly with an Anthropic endpoint.
    if path == "/v1/models" or path.startswith("/v1/chat/completions"):
        return "hk", HK_PROXY
    return "sg", SG_PROXY


async def health(_: web.Request) -> web.Response:
    return web.json_response(
        {
            "status": "ok",
            "active": ACTIVE,
            "uptime_seconds": int(time.monotonic() - STARTED),
        }
    )


async def metrics(_: web.Request) -> web.Response:
    return web.json_response(
        {
            "active": ACTIVE,
            "pool_size": POOL_SIZE,
            "uptime_seconds": int(time.monotonic() - STARTED),
            "counters": dict(COUNTERS),
        }
    )


async def proxy_request(request: web.Request) -> web.StreamResponse:
    global ACTIVE
    ACTIVE += 1
    COUNTERS["requests"] += 1
    route_name, (proxy_url, proxy_auth) = route_for(request.path)
    COUNTERS[f"route_{route_name}"] += 1
    body = await request.read()
    excluded_headers = HOP_BY_HOP | {"host", "content-length"}
    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in excluded_headers
    }
    target = ORIGIN + request.rel_url.path_qs
    session: ClientSession = request.app["client"]

    try:
        last_error = "upstream request failed"
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                upstream = await session.request(
                    request.method,
                    target,
                    data=body,
                    headers=headers,
                    proxy=proxy_url,
                    proxy_auth=proxy_auth,
                    allow_redirects=False,
                )
                if upstream.status in {502, 503, 504} and attempt < MAX_ATTEMPTS:
                    last_error = f"upstream HTTP {upstream.status}"
                    COUNTERS[f"retry_http_{upstream.status}"] += 1
                    await upstream.read()
                    upstream.release()
                    await asyncio.sleep(min(0.25 * (2 ** (attempt - 1)), 2.0))
                    continue

                response_headers = {
                    key: value
                    for key, value in upstream.headers.items()
                    if key.lower() not in excluded_headers
                }
                response = web.StreamResponse(
                    status=upstream.status,
                    reason=upstream.reason,
                    headers=response_headers,
                )
                await response.prepare(request)
                async for chunk in upstream.content.iter_chunked(64 * 1024):
                    await response.write(chunk)
                await response.write_eof()
                upstream.release()
                COUNTERS[f"status_{upstream.status}"] += 1
                return response
            except (ClientError, asyncio.TimeoutError) as exc:
                last_error = type(exc).__name__
                COUNTERS[f"retry_{type(exc).__name__}"] += 1
                if attempt < MAX_ATTEMPTS:
                    await asyncio.sleep(min(0.25 * (2 ** (attempt - 1)), 2.0))

        COUNTERS["gateway_failures"] += 1
        return web.json_response(
            {"error": {"message": last_error, "type": "gateway_error"}},
            status=502,
        )
    finally:
        ACTIVE -= 1


async def start_client(app: web.Application) -> None:
    timeout = ClientTimeout(total=None, connect=60, sock_connect=60, sock_read=None)
    connector = TCPConnector(
        limit=POOL_SIZE * 2,
        limit_per_host=POOL_SIZE,
        keepalive_timeout=600,
        force_close=False,
        enable_cleanup_closed=True,
        ttl_dns_cache=3600,
    )
    app["client"] = ClientSession(
        timeout=timeout,
        connector=connector,
        auto_decompress=False,
    )


async def stop_client(app: web.Application) -> None:
    await app["client"].close()


def main() -> None:
    app = web.Application(client_max_size=16 * 1024 * 1024)
    app.on_startup.append(start_client)
    app.on_cleanup.append(stop_client)
    app.router.add_get("/healthz", health)
    app.router.add_get("/metrics", metrics)
    app.router.add_route("*", "/{path:.*}", proxy_request)
    web.run_app(
        app,
        host=LISTEN_HOST,
        port=LISTEN_PORT,
        access_log=None,
        shutdown_timeout=60,
    )


if __name__ == "__main__":
    main()
