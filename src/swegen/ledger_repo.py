"""Repository layer over the swegen Postgres ledger.

This replaces the copy-pasted JSONL helpers — ``append_private_jsonl``,
``load_latest_postchecks``, ``load_success_ledger`` — with a single module that
talks to the ``swegen`` database (see :mod:`swegen.db`).

Design notes
------------

* **Append, don't upsert.** ``append()`` is a plain ``INSERT``; "latest wins"
  is computed on read by :meth:`load_latest`, mirroring the JSONL
  ``load_latest_postchecks`` which kept the newest record per ``instance`` by
  ``(attempt, timestamp, line_index)``. A ``BIGSERIAL id`` stands in for
  ``line_index`` and ``written_at`` for ``timestamp``. Because all shards now
  write into one table, the old per-shard ``path_index`` tiebreak collapses to
  ``id`` — behavior-preserving since the shared ledger was always meant to win.

* **Backend switch.** ``backend="postgres"`` (default) talks to the DB;
  ``backend="jsonl"`` delegates to the legacy file helpers so workers can fall
  back during rollout and the backfill/verify scripts can compare the two.
  Selected via the ``SWEGEN_LEDGER_BACKEND`` env var or per-instance.

* **Path → table registry.** Workers carry a ledger *path* (e.g.
  ``postcheck-status.jsonl``). The repo maps a path's stem to its table so the
  call-site diff stays minimal: ``LedgerRepo(path)`` works regardless of
  backend.

The JSONL helpers are imported lazily so this module can be used in
environments where the worker scripts' helper definitions are unavailable
(e.g. unit tests of the repo in isolation).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable

from . import db

# Map a ledger file stem (or full filename) to its DB table and the JSON
# ``event`` tag the worker records carry. Add new ledgers here.
_TABLE_REGISTRY: dict[str, tuple[str, str]] = {
    "postcheck-status": ("postcheck_status", "postcheck_status"),
    "postcheck-status-shared": ("postcheck_status", "postcheck_status"),
    "reward-backfill-status": ("reward_backfill_status", "reward_backfill_status"),
    "blacklist": ("blacklist", "blacklist"),
    "create": ("create_success", "create_success"),
    "stage3-reward-guard": ("stage3_reward_guard", "stage3_reward_guard"),
    "orchestrator-progress": ("orchestrator_progress", "orchestrator_progress"),
    "orchestrator-instance-status": (
        "orchestrator_instance_status",
        "orchestrator_instance_status",
    ),
    "all_images": ("pushed_images", "pushed_images"),
    "pushed_images": ("pushed_images", "pushed_images"),
}

# Tables whose "latest wins" is keyed by `instance` (postcheck-style dedup).
_INSTANCE_KEYED = {
    "postcheck_status",
    "reward_backfill_status",
}
# Tables whose "latest wins" is keyed by `task_id` (success-ledger dedup).
_TASK_KEYED = {"create_success"}

# Columns available on each table (the generic status-ledger columns). Used to
# avoid INSERTing columns a table doesn't have. Kept in sync with schema.sql.
_GENERIC_STATUS_COLS = {
    "event",
    "instance",
    "attempt",
    "status",
    "stage",
    "worker_id",
    "checker_node",
    "source_node",
    "schema_version",
    "timestamp",
}
_TABLE_COLUMNS: dict[str, set[str]] = {
    "postcheck_status": _GENERIC_STATUS_COLS | {"merged_from_backfill"},
    "reward_backfill_status": _GENERIC_STATUS_COLS,
    "blacklist": {"instance", "stage", "blacklisted_at"},
    "create_success": set(),
    "stage3_reward_guard": {"event", "instance"},
    "orchestrator_progress": {"event", "pr", "instance", "status"},
    "orchestrator_instance_status": {"event", "instance", "status"},
    "pushed_images": {"event", "instance"},
}


def _resolve(path: str | Path) -> tuple[str, str]:
    """Return (table, event) for a ledger path, raising on unknown ledgers."""
    p = Path(path)
    stem = p.stem  # e.g. "postcheck-status" from "postcheck-status.jsonl"
    if stem in _TABLE_REGISTRY:
        return _TABLE_REGISTRY[stem]
    # Try the full name in case of unusual stems.
    name = p.name
    if name in _TABLE_REGISTRY:
        return _TABLE_REGISTRY[name]
    raise KeyError(f"Unknown ledger path {path!r}; not in _TABLE_REGISTRY")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _unjson(value: Any) -> Any:
    """Undo JSONB serialization. psycopg deserializes JSONB columns to Python
    objects automatically, but defend against the str case for safety."""
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        return json.loads(value)
    return value


class LedgerRepo:
    """Read/write the swegen ledger, abstracting postgres vs. jsonl backends.

    Parameters
    ----------
    path:
        The ledger file path the worker uses (e.g. ``.../postcheck-status.jsonl``).
        Used to resolve the target table and, in jsonl mode, the actual file.
    backend:
        ``"postgres"`` (default) or ``"jsonl"``. If omitted, reads the
        ``SWEGEN_LEDGER_BACKEND`` env var, defaulting to ``"postgres"``.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        backend: str | None = None,
    ) -> None:
        self.path = Path(path)
        self.table, self.event = _resolve(self.path)
        self.backend = (backend or os.environ.get("SWEGEN_LEDGER_BACKEND", "postgres")).lower()
        if self.backend not in ("postgres", "jsonl"):
            raise ValueError(f"unknown ledger backend: {self.backend!r}")

    # ------------------------------------------------------------------ writes

    def append(self, record: dict[str, Any]) -> None:
        """Append a record. In postgres mode this is an INSERT; jsonl mode
        delegates to the worker's atomic ``append_private_jsonl`` helper."""
        if self.backend == "jsonl":
            self._append_jsonl(record)
            return
        self._append_pg(record)

    def _append_pg(self, record: dict[str, Any]) -> None:
        cols: dict[str, Any] = {"payload": _json(record)}

        # Generic status-ledger columns — only the tables that actually have
        # them get these set (validated against _TABLE_COLUMNS below).
        generic = {
            "event": record.get("event", self.event),
            "instance": record.get("instance"),
            "attempt": record.get("attempt") if isinstance(record.get("attempt"), int) else None,
            "status": record.get("status"),
            "stage": record.get("stage"),
            "worker_id": record.get("worker_id"),
            "checker_node": record.get("checker_node"),
            "source_node": record.get("source_node"),
            "schema_version": record.get("schema_version"),
            "timestamp": record.get("timestamp"),
        }
        for k, v in generic.items():
            if k in _TABLE_COLUMNS[self.table]:
                cols[k] = v
        # merged_from_backfill is postcheck-only
        if "merged_from_backfill" in _TABLE_COLUMNS[self.table]:
            cols["merged_from_backfill"] = bool(record.get("merged_from_backfill", False))

        # create_success-specific columns
        if self.table == "create_success":
            cols["task_id"] = record.get("task_id") or record.get("instance")
            cols["key"] = record.get("key")
            cols["repo"] = record.get("repo")
            cols["pr"] = record.get("pr")
            cols["harbor"] = record.get("harbor")
            cols["ts"] = record.get("ts")
        # blacklist-specific columns
        if self.table == "blacklist":
            cols["attempts"] = record.get("attempts")
            cols["blacklisted_at"] = record.get("blacklisted_at")
            cols["error"] = record.get("error")
        # pushed_images-specific columns
        if self.table == "pushed_images":
            cols["registry"] = record.get("registry")
            cols["suffix"] = record.get("suffix", "")
            cols["swr_url"] = record.get("swr_url")
            cols["pushed"] = bool(record.get("pushed", False))
        # orchestrator_progress-specific
        if self.table == "orchestrator_progress":
            cols["pr"] = record.get("pr")

        self._insert(cols)

    def _insert(self, cols: dict[str, Any]) -> None:
        # Drop None-valued optional columns so DB defaults apply; keep payload
        # (non-nullable) and any explicitly-set columns.
        clean = {k: v for k, v in cols.items() if v is not None or k == "payload"}
        colnames = list(clean.keys())
        placeholders = ", ".join(f"%({c})s" for c in colnames)
        collist = ", ".join(colnames)
        sql = f"INSERT INTO {self.table} ({collist}) VALUES ({placeholders})"
        # Named params via the connection directly (db.execute takes positional).
        with db.connection() as conn:
            with conn.transaction():
                conn.execute(sql, clean)

    def _append_jsonl(self, record: dict[str, Any]) -> None:
        """Delegate to the legacy atomic-append helper. Imported lazily so the
        repo can be used without the worker modules on the path."""
        from slurm_validation_worker import append_private_jsonl  # type: ignore

        append_private_jsonl(self.path, record)

    # ------------------------------------------------------------------- reads

    def load_latest(self) -> dict[str, dict[str, Any]]:
        """Newest record per instance (postcheck/backfill tables) or per
        task_id (success ledger). Direct translation of
        ``load_latest_postchecks`` / ``load_success_ledger``."""
        if self.backend == "jsonl":
            return self._load_latest_jsonl()
        return self._load_latest_pg()

    def _load_latest_pg(self) -> dict[str, dict[str, Any]]:
        if self.table in _INSTANCE_KEYED:
            key_col = "instance"
        elif self.table in _TASK_KEYED:
            key_col = "task_id"
        else:
            # Tables without a natural dedup key: return every row keyed by a
            # synthetic id (callers that want ordering should use load_all).
            rows = db.query_all(
                f"SELECT id, payload FROM {self.table} ORDER BY id ASC"
            )
            return {str(r["id"]): _unjson(r["payload"]) for r in rows}

        # ORDER BY mirrors the original JSONL dedup tuples:
        #   * postcheck/backfill: (attempt, timestamp, line_index) — id ~ line_index
        #   * create_success:     (timestamp/ts, line_index) — load_success_ledger
        if self.table in _TASK_KEYED:
            order = "COALESCE(ts, '') DESC, id DESC"
        else:
            order = "COALESCE(attempt, 1) DESC, COALESCE(timestamp, '') DESC, id DESC"
        sql = (
            f"SELECT DISTINCT ON ({key_col}) payload FROM {self.table} "
            f"ORDER BY {key_col}, {order}"
        )
        rows = db.query_all(sql)
        out: dict[str, dict[str, Any]] = {}
        for r in rows:
            rec = _unjson(r["payload"])
            k = rec.get(key_col)
            if not isinstance(k, str) or not k:
                # success ledger falls back to harbor basename for the key
                harbor = rec.get("harbor")
                k = Path(harbor).name if isinstance(harbor, str) else ""
            if k:
                out[k] = rec
        return out

    def _load_latest_jsonl(self) -> dict[str, dict[str, Any]]:
        from run_dashboard import load_latest_postchecks, load_success_ledger  # type: ignore

        if self.table in _TASK_KEYED:
            return load_success_ledger(self.path)
        return load_latest_postchecks(self.path)

    def load_all(self) -> list[dict[str, Any]]:
        """Every record in insertion order (for incremental-tail readers and
        readers that want the full history, not just latest-per-key)."""
        if self.backend == "jsonl":
            return self._load_all_jsonl()
        rows = db.query_all(
            f"SELECT payload FROM {self.table} ORDER BY id ASC"
        )
        return [_unjson(r["payload"]) for r in rows]

    def _load_all_jsonl(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        if not self.path.is_file():
            return out
        with self.path.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(rec, dict):
                    out.append(rec)
        return out

    def load_accepted(self) -> list[str]:
        """Instance IDs with status='accepted' (push_all_verified /
        retroactive_push)."""
        if self.backend == "jsonl":
            return [
                rec.get("instance")
                for rec in self.load_all()
                if rec.get("status") == "accepted" and rec.get("instance")
            ]
        rows = db.query_all(
            "SELECT DISTINCT instance FROM postcheck_status "
            "WHERE status = 'accepted' AND instance IS NOT NULL "
            "ORDER BY instance"
        )
        return [r["instance"] for r in rows]


# ---------------------------------------------------------------------------
# Backfill helper (used by scripts/backfill_jsonl_to_pg.py)
# ---------------------------------------------------------------------------

def backfill_row(
    table: str,
    record: dict[str, Any],
    *,
    source_file: str,
    source_line: int,
) -> bool:
    """Insert a backfilled JSONL row tagged with its origin so the import is
    idempotent. Returns True if inserted, False if it was already present
    (ON CONFLICT skip via the partial unique index)."""
    repo = LedgerRepo.__new__(LedgerRepo)
    repo.path = Path(source_file)
    repo.table = table
    repo.event = record.get("event", table)
    repo.backend = "postgres"
    with db.connection() as conn:
        with conn.transaction():
            cur = conn.execute(
                f"SELECT 1 FROM {table} WHERE source_file = %s AND source_line = %s",
                (source_file, source_line),
            )
            if cur.fetchone() is not None:
                return False
            cols = {
                "payload": _json(record),
                "source_file": source_file,
                "source_line": source_line,
            }
            generic = {
                "instance": record.get("instance"),
                "attempt": record.get("attempt") if isinstance(record.get("attempt"), int) else None,
                "status": record.get("status"),
                "stage": record.get("stage"),
                "event": record.get("event", table),
                "timestamp": record.get("timestamp"),
            }
            allowed = _TABLE_COLUMNS.get(table, set())
            for k, v in generic.items():
                if k in allowed:
                    cols[k] = v
            if table == "create_success":
                cols["task_id"] = record.get("task_id") or record.get("instance")
                cols["key"] = record.get("key")
                cols["repo"] = record.get("repo")
                cols["pr"] = record.get("pr")
                cols["harbor"] = record.get("harbor")
                cols["ts"] = record.get("ts")
            if table == "blacklist":
                cols["attempts"] = record.get("attempts")
                cols["blacklisted_at"] = record.get("blacklisted_at")
                cols["error"] = record.get("error")
            clean = {k: v for k, v in cols.items() if v is not None or k == "payload"}
            colnames = list(clean.keys())
            placeholders = ", ".join(f"%({c})s" for c in colnames)
            collist = ", ".join(colnames)
            conn.execute(
                f"INSERT INTO {table} ({collist}) VALUES ({placeholders})", clean
            )
            return True
