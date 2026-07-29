from contextlib import nullcontext

from swegen import db


class RecordingConnection:
    def __init__(self) -> None:
        self.executions: list[tuple[str, object | None]] = []

    def transaction(self):
        return nullcontext()

    def execute(self, sql: str, params: object | None = None) -> None:
        self.executions.append((sql, params))


def test_apply_schema_serializes_bootstrap_with_transaction_advisory_lock(
    monkeypatch,
    tmp_path,
) -> None:
    schema_path = tmp_path / "schema.sql"
    schema_path.write_text("SELECT 'schema';", encoding="utf-8")
    monkeypatch.setattr(db, "_SCHEMA_PATH", schema_path)
    connection = RecordingConnection()

    db.apply_schema(connection)  # type: ignore[arg-type]

    assert connection.executions == [
        (db._SCHEMA_ADVISORY_LOCK_SQL, db._SCHEMA_ADVISORY_LOCK_KEYS),
        ("SELECT 'schema';", None),
    ]
