#!/usr/bin/env python3
"""Export platform-pushed, reward-non-hacking images into github_pr_records.

Reads the ``pushed_images`` table from the swegen ledger DB
(``swegen_distributed`` on the source host, via ``swegen.db`` — env-driven,
no hard-coded credentials) and upserts the **platform-registry, pushed=true**
subset into ``public.github_pr_records`` on the target PR-state DB.

Each platform-pushed image corresponds to an instance that passed the reward-hack
check, so every exported row is written with ``status='success'`` and
``filter_category='feature_implementation'``.

Source record shape::

    instance_id = "<owner>__<repo>-<pr_number>"   e.g. avajs__eslint-plugin-ava-258
    swr_url      = "<registry>/<repo-path>:<instance_id>"   -> image_ref

Mapping (target column <- source):
    owner           <- instance_id before the first "__"
    repo            <- instance_id after "__", minus the trailing "-<pr_number>"
    pr_number       <- the trailing integer after the last "-" in the repo-PR part
    image_ref       <- swr_url
    status          <- 'success'
    filter_category <- 'feature_implementation'

Upsert key: the target's unique constraint ``(owner, repo, pr_number)``. On
conflict the row is overwritten to ``status='success'`` with the new
``image_ref`` and ``filter_category``; ``attempt_count`` is preserved (not
clobbered) and ``updated_at`` is bumped. New rows get ``attempt_count=0``
(column default).

Credentials: the source DB uses the existing ``swegen.db`` layer
(``SWEGEN_PG_*`` env / .env; password required, never defaulted). The target DB
is configured via ``SWEGEN_TARGET_PG_*`` env vars (host/db/user have defaults
from the provisioning; password is required and never defaulted in source):

    SWEGEN_TARGET_PG_HOST=7.237.88.77
    SWEGEN_TARGET_PG_PORT=5432
    SWEGEN_TARGET_PG_DB=swegen_pr_state_db
    SWEGEN_TARGET_PG_USER=root
    SWEGEN_TARGET_PG_PASSWORD=...        # required

Usage:
    # Export all platform-pushed rows (default)
    SWEGEN_PG_PASSWORD=... SWEGEN_TARGET_PG_PASSWORD=... \
        .venv/bin/python3 export_platform_pushed_to_pr_state.py

    # Dry run: report what would be inserted/updated, write nothing
    ... export_platform_pushed_to_pr_state.py --dry-run

    # Limit (for smoke tests)
    ... export_platform_pushed_to_pr_state.py --limit 50

    # Use a different source suffix (default _platform) or pushed filter
    ... export_platform_pushed_to_pr_state.py --suffix "" --pushed
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

# Make the in-repo `swegen` package importable when run from the repo root.
_SRC = Path(__file__).resolve().parent / "src"
if _SRC.is_dir():
    sys.path.insert(0, str(_SRC))

from swegen import db as src_db  # noqa: E402

# ── Target DB config (env-driven, password never defaulted) ────────────────

_TARGET_DEFAULTS = {
    "host": "7.237.88.77",
    "port": "5432",
    "dbname": "swegen_pr_state_db",
    "user": "root",
}


def _target_dsn() -> str:
    """Build a libpq conninfo for the target PR-state DB from env."""
    host = os.environ.get("SWEGEN_TARGET_PG_HOST", _TARGET_DEFAULTS["host"])
    port = os.environ.get("SWEGEN_TARGET_PG_PORT", _TARGET_DEFAULTS["port"])
    dbname = os.environ.get("SWEGEN_TARGET_PG_DB", _TARGET_DEFAULTS["dbname"])
    user = os.environ.get("SWEGEN_TARGET_PG_USER", _TARGET_DEFAULTS["user"])
    password = os.environ.get("SWEGEN_TARGET_PG_PASSWORD", "")
    if not password:
        raise RuntimeError(
            "SWEGEN_TARGET_PG_PASSWORD is not set. Put it in a gitignored .env "
            "or export it in the environment."
        )
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


# ── instance_id parsing ────────────────────────────────────────────────────


def parse_instance_id(instance_id: str) -> tuple[str, str, int] | None:
    """Split ``<owner>__<repo>-<pr>`` into (owner, repo, pr_number).

    Owner is everything before the first ``__``. The remainder is ``<repo>-<pr>``
    where repo may itself contain dashes, so the PR number is the trailing
    integer after the *last* dash. Returns None if the shape doesn't match.
    """
    if "__" not in instance_id:
        return None
    owner, rest = instance_id.split("__", 1)
    if "-" not in rest:
        return None
    repo, pr_str = rest.rsplit("-", 1)
    if not pr_str.isdigit() or not owner or not repo:
        return None
    return owner, repo, int(pr_str)


# ── core export ────────────────────────────────────────────────────────────

TARGET_TABLE = "github_pr_records"

# ON CONFLICT (owner, repo, pr_number) DO UPDATE: overwrite status/image_ref/
# filter_category to success, preserve attempt_count, bump updated_at.
# (No RETURNING: psycopg3 executemany doesn't surface per-row RETURNING results.
# Insert/update counts are derived instead from a pre-upsert existence probe.)
_UPSERT_SQL = f"""
INSERT INTO {TARGET_TABLE}
    (owner, repo, pr_number, image_ref, status, filter_category, attempt_count)
VALUES (%s, %s, %s, %s, 'success', 'feature_implementation', 0)
ON CONFLICT (owner, repo, pr_number) DO UPDATE SET
    image_ref      = EXCLUDED.image_ref,
    status         = EXCLUDED.status,
    filter_category = EXCLUDED.filter_category,
    updated_at     = now()
"""


def read_source_rows(*, suffix: str, pushed: bool, limit: int | None) -> list[dict]:
    """Read platform-pushed rows from the source ledger DB.

    Returns one row per (instance, swr_url). If the same instance has multiple
    pushed records for the suffix, the newest (highest id) wins so we export a
    single current image_ref per PR.
    """
    where = ["suffix = %s"]
    params: list = [suffix]
    if pushed:
        where.append("pushed = TRUE")
    # Distinct on instance, newest first -> one row per instance.
    sql = (
        "SELECT DISTINCT ON (instance) instance, swr_url, id "
        "FROM pushed_images "
        "WHERE " + " AND ".join(where) + " "
        "ORDER BY instance, id DESC"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return src_db.query_all(sql, tuple(params))


def export(*, suffix: str, pushed: bool, limit: int | None, dry_run: bool,
           batch_size: int = 500) -> dict:
    """Read source rows, upsert into the target, return a summary dict."""
    rows = read_source_rows(suffix=suffix, pushed=pushed, limit=limit)

    parsed: list[tuple[str, str, int, str]] = []
    skipped: list[str] = []
    for r in rows:
        inst = r["instance"]
        swr_url = r["swr_url"]
        parts = parse_instance_id(inst)
        if parts is None or not swr_url:
            skipped.append(inst)
            continue
        owner, repo, pr = parts
        parsed.append((owner, repo, pr, swr_url))

    summary = {
        "source_rows": len(rows),
        "parsed": len(parsed),
        "skipped_unparseable": len(skipped),
        "skipped_samples": skipped[:10],
        "inserted": 0,
        "updated": 0,
        "dry_run": dry_run,
    }
    if skipped:
        print(f"  WARN: skipped {len(skipped)} unparseable instance_id(s); "
              f"samples: {skipped[:5]}", file=sys.stderr)

    if dry_run:
        # Probe the target for insert/update breakdown without writing.
        with psycopg.connect(_target_dsn(), row_factory=dict_row) as tgt:
            with tgt.cursor() as cur:
                cur.execute(
                    "CREATE TEMP TABLE _src_keys "
                    "(owner text, repo text, pr_number int, image_ref text) ON COMMIT DROP"
                )
                cur.executemany(
                    "INSERT INTO _src_keys VALUES (%s,%s,%s,%s)", parsed
                )
                cur.execute(
                    f"SELECT count(*) FILTER (WHERE t.id IS NULL) AS new, "
                    f"count(*) FILTER (WHERE t.id IS NOT NULL) AS existing "
                    f"FROM _src_keys s LEFT JOIN {TARGET_TABLE} t "
                    f"USING (owner, repo, pr_number)"
                )
                counts = cur.fetchone()
        summary["would_insert"] = counts["new"]
        summary["would_update"] = counts["existing"]
        return summary

    with psycopg.connect(_target_dsn(), row_factory=dict_row) as tgt:
        with tgt.transaction():
            with tgt.cursor() as cur:
                # Pre-upsert existence probe: how many of our keys already exist?
                # (All writes go through the same ON CONFLICT, so existing rows
                # become updates and the rest become inserts.)
                cur.execute(
                    "CREATE TEMP TABLE _src_keys "
                    "(owner text, repo text, pr_number int, image_ref text) ON COMMIT DROP"
                )
                cur.executemany(
                    "INSERT INTO _src_keys VALUES (%s,%s,%s,%s)", parsed
                )
                cur.execute(
                    f"SELECT count(*) AS existing FROM _src_keys s "
                    f"JOIN {TARGET_TABLE} t USING (owner, repo, pr_number)"
                )
                existing = cur.fetchone()["existing"]
                for start in range(0, len(parsed), batch_size):
                    chunk = parsed[start : start + batch_size]
                    cur.executemany(_UPSERT_SQL, chunk)
    summary["updated"] = existing
    summary["inserted"] = len(parsed) - existing
    return summary


# ── CLI ────────────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Export platform-pushed images to github_pr_records."
    )
    ap.add_argument(
        "--suffix", default="_platform",
        help="pushed_images suffix to select (default: '_platform'). "
             "Use '' for the trajectory registry.",
    )
    ap.add_argument(
        "--pushed", action=argparse.BooleanOptionalAction, default=True,
        help="Select pushed=true rows only (default: --pushed; use "
             "--no-pushed to include unpushed candidates).",
    )
    ap.add_argument("--limit", type=int, default=None,
                    help="Limit source rows (for smoke tests).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Report what would be inserted/updated, write nothing.")
    args = ap.parse_args(argv)

    print(f"Reading from source ledger DB (swegen_distributed) "
          f"[suffix={args.suffix!r}, pushed={args.pushed}, limit={args.limit}]",
          flush=True)
    s = export(suffix=args.suffix, pushed=args.pushed, limit=args.limit,
               dry_run=args.dry_run)

    if s["dry_run"]:
        print(f"DRY RUN: source_rows={s['source_rows']} parsed={s['parsed']} "
              f"skipped={s['skipped_unparseable']} "
              f"-> would_insert={s['would_insert']} would_update={s['would_update']}")
    else:
        print(f"source_rows={s['source_rows']} parsed={s['parsed']} "
              f"skipped={s['skipped_unparseable']}")
        print(f"inserted={s['inserted']} updated={s['updated']} "
              f"total_written={s['inserted'] + s['updated']}")
    print("DONE: all rows inserted." if not s["dry_run"] else "DONE: dry run.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
