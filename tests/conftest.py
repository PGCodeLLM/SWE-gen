from collections.abc import Iterator

import pytest

_NO_POOL_MESSAGE = (
    "unit tests must not open a PostgreSQL pool; swegen.db.get_pool builds its "
    "DSN from the repo-root .env and connects to the live swegen_distributed "
    "database. Stub the caller instead of reaching the network."
)


@pytest.fixture(autouse=True)
def isolate_ledger_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep unit tests independent from the live PostgreSQL deployment."""

    monkeypatch.setenv("SWEGEN_LEDGER_BACKEND", "jsonl")


@pytest.fixture(autouse=True)
def forbid_postgres_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    """Turn an accidental production-Postgres connection into a loud failure.

    ``SWEGEN_LEDGER_BACKEND=jsonl`` only redirects ``LedgerRepo``; callers that
    import ``swegen.db`` directly still reach the live database. Patching
    ``get_pool`` covers every path into it (``connection``, ``execute``,
    ``query_all`` and ``query_one`` all funnel through the pool) and costs
    nothing, unlike breaking the DSN, which pays psycopg's connection retries.
    """

    try:
        from swegen import db
    except ImportError:  # pragma: no cover - db deps are declared, but stay safe
        return

    def _forbidden() -> None:
        raise RuntimeError(_NO_POOL_MESSAGE)

    monkeypatch.setattr(db, "get_pool", _forbidden)


@pytest.fixture(scope="session", autouse=True)
def close_postgres_pool_at_exit() -> Iterator[None]:
    """Close any pool opened during the session so pytest can exit promptly.

    A leaked ``psycopg_pool.ConnectionPool`` keeps its worker and scheduler
    threads alive, and pytest waits 5s on each one at interpreter shutdown.
    """

    yield
    try:
        from swegen import db

        db.close_pool()
    except Exception:  # pragma: no cover - teardown must never fail the run
        pass
