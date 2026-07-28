# JSONL → Postgres Ledger Migration — Summary

Status: **migration complete; compatibility retirement deferred**. Writers,
readers, schema, backfill, and parity checks have landed and historical ledgers
were imported. The remaining work in section 7 is intentionally delayed until
the PostgreSQL/PGMQ k3s pipeline passes its live end-to-end validation.

> The source repository is `/data/work/alex/SWE-gen`; the integrated branch is
> `slurm-swegen` in its linked worktree. `/data/work/slurm-swegen` remains the
> deployed Slurm tree until the k3s worker rollout replaces it.

---

## 1. What changed and why

The swegen pipeline recorded state in append-only `.jsonl` ledgers scattered
across run directories and the NFS share. That does not scale across the sharded
multi-region Slurm workers (concurrent appends, no cross-shard visibility, no
querying). The ledger is now a set of Postgres tables.

**Design rules (see `src/swegen/schema.sql` header):**

- **Append, don't upsert.** Every write is a plain `INSERT`. "Latest wins" is
  computed on read with `DISTINCT ON (key) ... ORDER BY attempt DESC,
  written_at DESC, id DESC`, mirroring the old JSONL dedup
  (`load_latest_postchecks` kept the newest record per `instance` by
  `(attempt, timestamp, line_index)`). A `BIGSERIAL id` stands in for
  `line_index`; `written_at` for timestamp. This preserves the original
  write-never-blocks semantics and avoids upsert contention across shards.
- **Hot fields are typed columns; the full record is kept in `payload JSONB`**
  so no data is lost vs. the JSONL files and reads can reconstruct the exact
  dict the workers emitted.
- **`source_file` + `source_line`** tag rows imported by the JSONL backfill so
  the import is idempotent (partial unique index per table). Worker-written
  rows leave them `NULL`.

## 2. Database topology (important — do not collide)

- The pipeline now uses a **dedicated database `swegen_distributed`**. All 8
  tables live in its default `public` schema.
- **A pre-existing `mindforge` database on the same host contains its own
  `swegen` schema — that must NOT be touched.** The `swegen_distributed` DB was
  created specifically to avoid that collision. Never point the pipeline at
  `mindforge`.
- Connection config is environment-driven via `python-dotenv`; **no credentials
  are hard-coded in source.** `db.py` deliberately has *no* default for the
  password — it raises `RuntimeError` if `SWEGEN_PG_PASSWORD` is missing.

  | env var | default | notes |
  |---|---|---|
  | `SWEGEN_PG_HOST` | `7.237.95.141` | |
  | `SWEGEN_PG_PORT` | `5432` | |
  | `SWEGEN_PG_USER` | `root` | |
  | `SWEGEN_PG_PASSWORD` | *(none — required)* | from env / gitignored `.env` |
  | `SWEGEN_PG_DB` | `swegen_distributed` | |
  | `SWEGEN_PG_POOL_MIN` / `_MAX` | `1` / `4` | |
  | `SWEGEN_LEDGER_BACKEND` | `postgres` | set `jsonl` to fall back to the old path |

  > Neither repo's committed `.env` ships the password. Run backfill/ops with
  > it exported inline, e.g.
  > `SWEGEN_PG_PASSWORD='…' .venv/bin/python3 backfill_jsonl_to_pg.py …`
  > Credentials are recorded in `memory/swegen-postgres-target.md`.

## 3. Files

### Core (new)
- `src/swegen/db.py` — connection layer. `get_pool()` returns a process-local
  `psycopg.ConnectionPool` (dict_row factory, so JSONB → Python dict
  automatically). `apply_schema()` runs `schema.sql` idempotently on first
  connect (`CREATE TABLE IF NOT EXISTS`).
- `src/swegen/schema.sql` — the 8 tables (see §4).
- `src/swegen/ledger_repo.py` — `LedgerRepo`: the single read/write abstraction
  over the ledger. Resolves a ledger *path stem* → `(table, event)`, then
  `append()` (INSERT) / `load_latest()` (dedup read) / `load_all()` in postgres
  mode, delegating to the legacy atomic-append helper in jsonl mode. Also hosts
  the batched idempotent backfill helpers `backfill_row` / `backfill_rows`.

### Writers (refactored to `LedgerRepo`)
Workers that previously called `append_private_jsonl(path, record)` now go
through `LedgerRepo(path).append(record)`. The repo picks the backend from
`SWEGEN_LEDGER_BACKEND` (default `postgres`, falls back to `jsonl`). Touched:
`src/slurm_validation_worker.py`, `src/slurm_reward_backfill_worker.py`,
`src/push_all_verified.py` (`_append_image_record`), and the orchestrator
emitters for `orchestrator-progress` / `orchestrator-instance-status`.

### Readers (refactored to be backend-aware)
Each reader queries Postgres in pg mode and keeps its original JSONL file scan
in jsonl mode. Touched:
- `src/retroactive_push.py` — `load_accepted_instances` → `repo.load_accepted()`
  in pg mode.
- `src/slurm_two_node.py` — `completed_instances` → `repo.load_all()` over the
  `create` stem (resolves to `create_success`) in pg mode.
- `src/orchestrator.py` — `_create_log_has_success` →
  `SELECT 1 FROM create_success WHERE task_id = %s OR (harbor IS NOT NULL AND
  split_part(harbor,'/',-1) = %s) LIMIT 1` in pg mode.
- `src/slurm_collect.py` — archive pattern set split into ledger vs. non-ledger;
  ledger patterns are dropped from the archive set when backend is postgres
  (they live in the DB now, not on the shared FS).
- `src/stage3_reward_guard.py` — `scan()` uses a `ledger_id` cursor
  (`SELECT id, payload FROM reward_backfill_status WHERE id > %s ORDER BY id`)
  in pg mode; `resuming` checks both `ledger_inode` (jsonl) and `ledger_id` (pg).
- `src/run_dashboard.py` — status views read from Postgres.

### Ops scripts (new, repo-root)
- `backfill_jsonl_to_pg.py` — idempotently imports historical `.jsonl` ledgers
  into their tables. Tags each row with `source_file`+`source_line` so re-runs
  are a no-op. CLI: `--run-dir` / `--root` (repeatable) / `--file` (repeatable)
  / `--dry-run`. Skips timestamped snapshots (stems matching `\.(before|after)-`)
  and per-shard orchestrator copies are mapped to the base tables.
- `pg_ledger_verify.py` — parity check: compares the Postgres "latest per key"
  view against a JSONL reference view per file. CLI: `--run-dir` / `--root` /
  `--file` / `--show-diffs N`. **Caveat:** its reference readers for tables
  without a clean per-file key (`blacklist`, `pushed_images`,
  `stage3_reward_guard`) key by synthetic id and are noisy; trust
  `postcheck_status` / `create_success` (`only_jsonl=0` is the real signal).

## 4. The 8 tables

| table | source ledger(s) | dedup key | notes |
|---|---|---|---|
| `postcheck_status` | `postcheck-status.jsonl` (+shared merged copy on NFS) | `instance` | has `attempt`, `merged_from_backfill` |
| `reward_backfill_status` | `reward-backfill-status.jsonl` | `instance` | |
| `blacklist` | `blacklist.jsonl` | — (append) | `attempts`, `blacklisted_at`, `error` |
| `create_success` | `create.jsonl` (success ledger) | `task_id` | `repo`, `pr`, `harbor`, `ts`; key falls back to harbor basename |
| `stage3_reward_guard` | `stage3-reward-guard.jsonl` | — (synthetic id) | |
| `orchestrator_progress` | `orchestrator-progress*.jsonl` (+per-shard copies) | — | `pr` |
| `orchestrator_instance_status` | `orchestrator-instance-status*.jsonl` (+per-shard copies) | `instance` | |
| `pushed_images` | `all_images<suffix>.jsonl` / `pushed_images<suffix>.jsonl` | — | `registry`, `suffix`, `swr_url`, `pushed` |

### Path-stem → table resolution (`ledger_repo._resolve`)
- Registry stems map directly (e.g. `postcheck-status` → `postcheck_status`).
- `all_images*` / `pushed_images*` prefixes → `pushed_images` (the trailing
  part of the stem is the per-registry `suffix`, e.g. `all_images_platform` →
  `suffix='_platform'`).
- `orchestrator-progress` / `orchestrator-instance-status` with a shard suffix
  (`-{region}-n{N}-{a|b|c}-r{N}`, e.g. `-de-n1-a-r6`) → the base orchestrator
  table. The shard-suffix regex is `_SHARD_SUFFIX_RE`.

## 5. Backfill result (2026-07-28)

Full import against `/data/work/slurm-swegen/runs/20260716-sol-max-full-16w`
plus `/data/nfs_shared/swegen/postcheck-status.jsonl`. Final per-table row
counts in `swegen_distributed`:

| table | rows |
|---|---|
| `blacklist` | 498 |
| `create_success` | 10,055 |
| `orchestrator_instance_status` | 94,094 |
| `orchestrator_progress` | 92,774 |
| `postcheck_status` | 262,720 |
| `pushed_images` | 26,135 |
| `reward_backfill_status` | 149,305 |
| `stage3_reward_guard` | 256 |

Totals: **608,147 imported, 1,555 skipped (already present), 86 malformed.**

- **The 86 malformed lines are genuine filesystem corruption** in
  `/data/nfs_shared/swegen/postcheck-status.jsonl` — NUL-byte padding from
  torn writes during atomic append (one even has partial valid JSON after the
  NUL pad). Correctly skipped; not a code bug.
- **Backfill scope:** authoritative unsuffixed ledgers **plus** per-shard copies
  (`-de-n1-a-r6` etc.); timestamped snapshots (`.before-*` / `.after-*`)
  skipped by design.
- **`pushed_images` backfill fix:** raw image-ledger lines key the instance on
  `instance_id` (not `instance`) and carry no `registry`/`suffix`. The backfill
  column-builder derives `instance` from `instance_id`, `suffix` from the
  source filename stem, and `pushed` from the stem prefix
  (`pushed_images*` → true, `all_images*` → false). `registry` (the short
  label) is not recoverable from a raw line and is left NULL (nullable).

## 6. Known gotchas

- **`executemany` is a cursor method in psycopg3**, not a connection method.
  `backfill_rows` uses `cur = conn.cursor(); cur.executemany(sql, params)`.
- **psycopg3 + `dict_row`:** JSONB columns auto-deserialize to Python
  dicts/lists; read rows with `r["col"]`, not `r[0]`.
- **Latest-wins across merged sources is intentional.** When the same
  `instance` appears in the NFS postcheck ledger at `attempt=1` and in a
  run-dir/shard copy at `attempt=5`, Postgres surfaces `attempt=5`. The parity
  tool reports this as `differing` against a single-file JSONL reference — that
  is the merge working correctly, not data loss.
- **Schema is applied on first connect**, so a fresh `swegen_distributed` DB is
  bootstrapped automatically. Do not run `schema.sql` manually against
  `mindforge`.

## 7. Deferred JSONL compatibility retirement

The `jsonl` backend is preserved as a fallback (`SWEGEN_LEDGER_BACKEND=jsonl`)
so the migration remains reversible during live validation. These are genuine
outstanding cleanup items, not missing migration commits. Remove them only
after the PostgreSQL/PGMQ k3s pipeline completes an end-to-end task and the
existing Slurm services no longer require rollback compatibility:

- `append_private_jsonl` / `read_appended_jsonl` and the inode/offset resume
  logic in `src/slurm_validation_worker.py` — still used by the jsonl backend
  path of `LedgerRepo` and by `stage3_reward_guard.scan()` in jsonl mode.
- `load_latest_postchecks` / `load_success_ledger` (JSONL file-scan dedup) in
  `src/slurm_validation_worker.py` — superseded by `LedgerRepo.load_latest()`
  but still referenced by `pg_ledger_verify.py`'s reference readers and the
  jsonl fallback readers.
- `src/merge_backfill_into_postcheck.py` — a legacy one-shot merger retained
  for rollout compatibility. Retire it once queue handoffs write stage results
  directly and transactionally in PostgreSQL.
- The per-run `.jsonl` files themselves are no longer authoritative once
  backfilled, but workers in `jsonl` mode still write them. After flipping fully
  to postgres, the `jsonl` branches in `LedgerRepo` and the backend-aware
  readers can be deleted along with the file-scan helpers.
- `slurm_collect.py`'s `_ARCHIVE_LEDGER_PATTERNS` / `_active_archive_patterns()`:
  the ledger-archive behavior is gated off in pg mode; once jsonl mode is gone,
  collapse back to a single pattern set.

## 8. Commit state

- `f2b6aa8` — Postgres ledger foundation: `db.py`, `schema.sql`, `ledger_repo.py`.
- `3a37d06` — refactor ledger writers to use `LedgerRepo`.
- `a20abfe` — reader refactor, backfill/verification utilities, final database
  defaults, and this migration summary.
- `131098e` on `slurm-swegen` — merge of the complete Postgres migration with
  the PGMQ/Slurm branch, including test isolation and compatibility fixes.

There is no uncommitted JSONL-to-Postgres migration work. Section 7 tracks the
separate, intentionally deferred removal of the rollback backend.
