# Exporting pushed Harbor tasks

`deploy/k3s/export-pushed-harbor-tasks.py` packages every Harbor task whose
Push stage succeeded into a single zip, together with the SWR image reference
recorded for each one.

Task files are not on disk. The distributed pipeline stores them in PostgreSQL
(`pipeline_task_files`), so the script reads them out of the database rather
than from a workspace directory. Rows stream through a server-side cursor and
are written straight into the archive: the full export is over a gigabyte
uncompressed and is never held in memory.

## Usage

```bash
SWEGEN_PG_HOST='REPLACE_WITH_POSTGRES_HOST' \
SWEGEN_PG_PORT='5432' \
SWEGEN_PG_USER='REPLACE_WITH_DATABASE_USER' \
SWEGEN_PG_DB='swegen_distributed' \
SWEGEN_PG_PASSWORD="${swegen_pg_password}" \
  uv run python deploy/k3s/export-pushed-harbor-tasks.py \
    --output /data/swegen-exports/pushed-harbor-tasks-YYYYMMDD.zip
```

Prefer injecting the password from a secret manager rather than entering it in
shell history. Inside the cluster it can be read from the Secret:

```bash
export SWEGEN_PG_PASSWORD="$(kubectl -n swegen-pipeline get secret swegen-database \
  -o jsonpath='{.data.SWEGEN_PG_PASSWORD}' | base64 -d)"
```

`--output` is the only required argument. Parent directories are created if
missing, and the path is resolved before use so the summary prints an absolute
location.

For an incremental export, exclude the exact task identities from a previous
archive's `manifest.json`. A timestamp cutoff pins the new archive even while
the pipeline keeps completing tasks, and the expected-count guard aborts before
the zip is opened if the selected set is not the audited set:

```bash
uv run python deploy/k3s/export-pushed-harbor-tasks.py \
  --exclude-manifest /data/swegen-exports/previous-export.zip \
  --pushed-through '2026-08-07T17:07:04+08:00' \
  --expected-task-count 2544 \
  --output /data/swegen-exports/pushed-harbor-tasks-incremental.zip
```

`--exclude-manifest` accepts either a manifest JSON or a zip containing a
top-level `manifest.json`. A submission state file is not sufficient because it
contains a count but not the exported `(task_id, task_version)` identities.
Database selection and file streaming use one repeatable-read snapshot.

## What gets exported

A task is included when it has a `succeeded` row in `pipeline_stage_results`
for `stage = 'push'`. That is the authoritative record of a task reaching SWR.
Failed and skipped pushes are excluded: their images are not in the registry,
so including them would misrepresent the archive.

Where a task has several push results, the most recent success wins. Tasks are
keyed by `(task_id, task_version)`, so two versions of the same task export as
two directories.

## Archive layout

Task directories sit at the archive root, so unpacking yields the Harbor tasks
directly with no wrapper directory to strip. `manifest.json` is their only
sibling.

```text
manifest.json
<task_id>__v<task_version>/
    environment/Dockerfile          <- thin, pulls the pushed SWR image
    environment/Dockerfile.source   <- original build-from-source recipe
    environment/bug.patch
    instruction.md
    solution/fix.patch
    solution/solve.sh
    tests/...
    swr-image.json
```

Stored file modes are preserved, so `solve.sh` and other scripts remain
executable after extraction.

The image is already built and in SWR, so `environment/Dockerfile` only pulls
it rather than rebuilding the environment from source:

```dockerfile
FROM REGISTRY_HOST/NAMESPACE/REPOSITORY:<task_id>
WORKDIR /app/src
CMD ["sleep", "infinity"]
```

The original recipe is kept beside it as `environment/Dockerfile.source`. It is
the only record of how the environment was produced, so it stays available for
rebuilding if an image is ever removed from the registry or the registry is
unreachable.

`swr-image.json` carries the per-task registry metadata:

```json
{
  "task_id": "01mf02__jaq-100",
  "task_version": 1,
  "repo": "01mf02/jaq",
  "pr": 100,
  "directory": "01mf02__jaq-100__v1",
  "pushed_at": "2026-08-02T11:04:53.918273+00:00",
  "swr_image": "REGISTRY_HOST/NAMESPACE/REPOSITORY:sha256-...",
  "registry": "platform",
  "suffix": "_platform",
  "remote_buildkit": true
}
```

`manifest.json` repeats every entry under a `tasks` array and adds
`generated_at`, `task_count`, `file_count`, and `uncompressed_bytes`, so the
set can be indexed without unpacking the archive.

## Verifying an export

The script prints a summary ending in the absolute archive path. Check that
`tasks missing image` is zero — a non-zero count means a task recorded a
successful push without a `remote_tag`, which points at a Push stage that
completed without publishing.

```text
tasks              : 2968
files              : 34278
uncompressed bytes : 1,313,303,449
tasks missing image: 0
archive bytes      : 738,390,050
archive            : /data/swegen-exports/pushed-harbor-tasks-20260803.zip
```

Confirm the archive independently before shipping it:

```bash
python3 - <<'PY'
import json, zipfile

archive = zipfile.ZipFile("/data/swegen-exports/pushed-harbor-tasks-20260803.zip")
print("corrupt entry:", archive.testzip() or "none")
manifest = json.loads(archive.read("manifest.json"))
# Task directories are top level, so anything with a "/" belongs to one.
directories = {name.split("/")[0] for name in archive.namelist() if "/" in name}
print("manifest task_count:", manifest["task_count"])
print("task directories   :", len(directories))
PY
```

`manifest.json`'s `task_count` and the number of task directories must agree.

## Reconciling the count against SWR

An export is a point-in-time snapshot. While the pipeline is running, Push
keeps completing tasks, so re-running the query below minutes later will
legitimately return a higher number than the archive contains. Compare against
`generated_at` in `manifest.json` rather than against the live total.

Beyond that, the export counts what the pipeline database believes was pushed,
which can drift from the registry: SWR may still hold tags from runs the current
database no longer tracks, and a count taken from push *attempts* rather than
successes will read higher. When an expected total disagrees with the export,
check which of the three is being counted before assuming the archive is short.

```sql
SELECT status, count(DISTINCT task_id || '/' || task_version)
FROM pipeline_stage_results
WHERE stage = 'push'
GROUP BY status;
```

## Skill-variant archives

`deploy/k3s/exporter/build_skill_variants.py` turns one baseline export into a
set of otherwise-identical archives that differ only in which Agent Skills each
task carries. This exists to A/B whether a given skill combination changes agent
success rates, so everything except the skill set must stay byte-identical.

```bash
.venv/bin/python deploy/k3s/exporter/build_skill_variants.py \
  --baseline /data/swegen-exports/harbor-tasks-skills-20260803-baseline.zip \
  --output-dir /data/swegen-exports \
  --prefix harbor-tasks-skills-20260803
```

Variants are built by streaming the baseline zip, not by re-querying PostgreSQL
once per variant. The task bodies are identical across variants and total well
over a gigabyte, so re-reading them from the database N times would be far
slower and would load the pipeline's live database for no benefit.

Which subsets get built comes from `deploy/k3s/exporter/skill_map.yaml`:
`skills` is the pool, `always-on` is forced into every subset, and
`--min-enabled` (default 4) filters the powerset. With four skills, one
always-on, and a minimum of four, that yields exactly five variants.

Each variant writes `skills/<name>/...` into every task directory and appends
one line to `instruction.md` naming the enabled skills. Per `agent-skills.md`,
the platform injects the Dockerfile `COPY` steps itself at image build time and
defaults `skills_dir` to `/skills` whenever a non-empty `skills/` is present, so
this script deliberately edits neither the Dockerfile nor `task.toml`.

Two constraints from that spec are enforced here rather than left to fail at
build time: the entry point must be spelled exactly `SKILL.md`, and symlinks
under `skills/` are dropped, since the platform rejects them.

### Verifying a variant

```bash
unzip -Z1 variant.zip | grep -c '/skills/.*/SKILL\.md$'   # == tasks x skills
unzip -p variant.zip '<task>/instruction.md' | tail -2    # skills line present
unzip -t variant.zip                                      # integrity
```

The `SKILL.md` count divided by the task-directory count must equal the number
of skills in that variant. `name` in each `SKILL.md` frontmatter must match its
directory name, or OpenCode silently fails to discover the skill.
