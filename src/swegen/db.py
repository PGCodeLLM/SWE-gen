"""Postgres connection layer for the swegen ledger.

Replaces the append-only JSONL ledgers with tables in a dedicated
``swegen_distributed`` database. Configuration is environment-driven (via
``python-dotenv``, which is already a project dependency) so no credentials are
hard-coded in source.

Environment variables (all optional, with the dev defaults shown):

    SWEGEN_PG_HOST=7.237.95.141
    SWEGEN_PG_PORT=5432
    SWEGEN_PG_USER=root
    SWEGEN_PG_PASSWORD=...        # read from .env, never committed
    SWEGEN_PG_DB=swegen_distributed
    SWEGEN_PG_POOL_MIN=1
    SWEGEN_PG_POOL_MAX=4

The schema (``schema.sql`` beside this module) is applied idempotently on the
first connection so a fresh database is bootstrapped automatically.
"""

from __future__ import annotations

import os
from pathlib import Path
from threading import Lock
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover - dotenv is a declared dep, but stay safe
    pass

_SCHEMA_PATH = Path(__file__).with_name("schema.sql")

# Dev defaults — host/db match the provisioned swegen_distributed instance. The
# password is NEVER defaulted here; it must come from the environment / .env.
_DEFAULTS = {
    "host": "7.237.95.141",
    "port": "5432",
    "user": "root",
    "dbname": "swegen_distributed",
}

_pool: ConnectionPool | None = None
_pool_lock = Lock()


def _dsn() -> str:
    """Build a libpq conninfo string from SWEGEN_PG_* env (with dev defaults)."""
    host = os.environ.get("SWEGEN_PG_HOST", _DEFAULTS["host"])
    port = os.environ.get("SWEGEN_PG_PORT", _DEFAULTS["port"])
    user = os.environ.get("SWEGEN_PG_USER", _DEFAULTS["user"])
    dbname = os.environ.get("SWEGEN_PG_DB", _DEFAULTS["dbname"])
    password = os.environ.get("SWEGEN_PG_PASSWORD", "")
    if not password:
        raise RuntimeError(
            "SWEGEN_PG_PASSWORD is not set. Put it in a gitignored .env "
            "(see swegen.db) or export it in the environment."
        )
    return (
        f"host={host} port={port} dbname={dbname} user={user} password={password}"
    )


def apply_schema(conn: psycopg.Connection) -> None:
    """Apply schema.sql idempotently (CREATE TABLE IF NOT EXISTS ...)."""
    sql = _SCHEMA_PATH.read_text(encoding="utf-8")
    with conn.transaction():
        conn.execute(sql)


def get_pool() -> ConnectionPool:
    """Return the process-local connection pool, creating it on first use."""
    global _pool
    if _pool is not None:
        return _pool
    with _pool_lock:
        if _pool is not None:
            return _pool
        min_size = int(os.environ.get("SWEGEN_PG_POOL_MIN", "1"))
        max_size = int(os.environ.get("SWEGEN_PG_POOL_MAX", "4"))
        # open=False so we can bootstrap the schema before serving checks.
        pool = ConnectionPool(
            conninfo=_dsn(),
            min_size=min_size,
            max_size=max_size,
            open=False,
            kwargs={"row_factory": dict_row},
        )
        pool.open(wait=True)
        # Apply schema once on a dedicated connection.
        with pool.connection() as conn:
            apply_schema(conn)
        _pool = pool
        return _pool


def close_pool() -> None:
    """Close the pool (mainly for tests / clean shutdown)."""
    global _pool
    with _pool_lock:
        if _pool is not None:
            _pool.close()
            _pool = None


def connection() -> psycopg.Connection:
    """Context-managed connection from the pool (use as `with connection() as c:`)."""
    return get_pool().connection()


def execute(sql: str, params: tuple[Any, ...] | None = None) -> None:
    """Run a write statement inside a committed transaction."""
    with connection() as conn:
        with conn.transaction():
            conn.execute(sql, params)


def query_all(sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    """Run a read query, returning a list of dict rows."""
    with connection() as conn:
        cur = conn.execute(sql, params)
        return list(cur.fetchall())


def query_one(sql: str, params: tuple[Any, ...] | None = None) -> dict[str, Any] | None:
    """Run a read query, returning the first dict row or None."""
    with connection() as conn:
        cur = conn.execute(sql, params)
        return cur.fetchone()
