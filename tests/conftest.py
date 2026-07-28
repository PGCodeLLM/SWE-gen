import pytest


@pytest.fixture(autouse=True)
def isolate_ledger_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep unit tests independent from the live PostgreSQL deployment."""

    monkeypatch.setenv("SWEGEN_LEDGER_BACKEND", "jsonl")
