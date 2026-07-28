# Pushing verified SWE-gen images to SWR — findings & pipeline cleanup notes

Status doc for the task: build + push the **pre-fix** Docker images (base commit +
deps installed + `bug.patch` applied, `fix.patch` NOT applied) of every
reward-hack-verified SWE-gen instance to the SWR registries, so they don't need
rebuilding later. Also records why the current Slurm/export pipeline is messy and
what to refactor.

Run under analysis: `runs/20260716-sol-max-full-16w`.

---

## 1. What "verified" means & where the authoritative set lives

An instance is verified iff: **oracle reward = 1, nop reward = 0, reward-hack
`is_hacking = False`**. The dashboard calls this `reward_clean` (= `baseline_valid`
AND `reward_hack` state `pass`).

The authoritative ledgers (this run):
- `.validation-worker/reward-backfill-status.jsonl` — 9,516 instances checked;
  **6,321 `status=pass`** (reward-hack passed). This is the dashboard's ~6,349.
- `.validation-worker/postcheck-status.jsonl` — per-instance nop/oracle rewards.
  Intersecting: **6,319** have oracle=1 & nop=0 & is_hacking=False → the real target.

**Gotcha:** `.validation-worker/postcheck-status.jsonl` shows only **177 `accepted`**
on this node — that's just this node's slice, NOT the full set. Do not use the
`accepted` status as the target; use the reward-backfill pass set ∩ oracle/nop.

Helper to regenerate the target list: filter reward-backfill `status==pass`,
require postcheck `oracle.reward==1 and nop.reward==0`. Saved as
`.validation-worker/target_verified_6319.txt`.

## 2. Two Dockerfile variants (root cause of the mess)

Every task has two forms of `environment/Dockerfile`, producing the **same pre-fix
image** (both end with `git reset --hard` then apply `bug.patch`; build context is
`environment/` only, so `solution/fix.patch` never enters the image):

| Variant | FROM | Repo source | Buildable on this node? |
|---|---|---|---|
| **self-contained** | `ubuntu:24.04` | `git clone` + fetch base commit | ✅ yes (public GitHub via proxy) |
| **preloaded** (postprocessed) | `swr-aifm-code-data-platform-6sudmx.../swesandbox/ubuntu:24.04` | repo pre-baked in base image at `/app/{owner}/{repo}` | ❌ no — 6sudmx unreachable here |

`src/coder-data-platform/postprocess.py` transforms self-contained → preloaded:
`replace_from()` rewrites FROM to a CWM-resolved image_ref, inserts the "move
preloaded platform repo to /app/src" block, and swaps the git-clone for a checkout.

**The bug:** the `reward-hack-accepted-*.tar.gz` task-pack and the
`tasks_voyager_postprocessed/` dirs contain the **preloaded** form, but their FROM
was left as the *placeholder* `swesandbox/ubuntu:24.04` (never rewritten to a real
resolved image_ref). So they can't build anywhere the 6sudmx base isn't published.

## 3. Where the task dirs actually live (answer: they still exist locally)

Original per-node workspaces were synced back into this run. Both variants exist:

- **self-contained** (`FROM ubuntu:24.04`) — USE THESE:
  - `runs/.../export/sources/<node>/tasks/<instance>/environment/Dockerfile`
  - `runs/.../slurm-nodes/<node>/tasks/<instance>/environment/Dockerfile`
  - `runs/.../.validation-worker/tasks/<instance>/...` (this node's 177 + some)
  - `runs/.../tasks_non_hacking_20260720/<instance>/...`
- **preloaded / broken FROM** — AVOID:
  - `runs/.../export/task-packs/reward-hack-accepted-*.tar.gz`
  - `runs/.../{,slurm-nodes/<node>/}tasks_voyager_postprocessed/<instance>/...`

Coverage: all **6,319** verified instances have a self-contained Dockerfile across
these roots (2,453 in the "primary" roots + 3,866 recovered from
`export/sources/<node>/tasks`). **No SWR pull, no CWM resolve, no git-clone
reconstruction needed** — the self-contained Dockerfiles already git-clone.

## 4. The push tooling (current, working)

- `src/push_all_verified.py` — ThreadPool (24–32 workers), builds each image once,
  pushes to BOTH registries, deletes locally. Flags: `--instance-list`,
  `--extra-task-dir` (repeatable), `--registries trajectory,platform`,
  `--skip-registry-check`, `--workers`.
  - `find_task_dir()` now **skips Dockerfiles whose FROM hits an unreachable base**
    (`6sudmx`) and prefers a self-contained variant from another root.
  - `_safe_rmi()` — rmi cleanup is best-effort (times out harmlessly under load;
    never affects push correctness — JSONL is written before cleanup).
- `src/export_pushed_images.py` — verifies which images exist in each registry
  (via `docker pull -q`, concurrent) and rewrites `pushed_images*.jsonl`.

Registries (build once → push both):
- **trajectory**: `swr-coder-data-trajectory-o84wch.swr-pro.myhuaweicloud.com/aifm.coder.exp/swegen/generated:<id>` → `pushed_images.jsonl`
- **platform**: `swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/swesandbox/public/swe-gen/feature-implementation/generated:<id>` → `pushed_images_platform.jsonl`
  (creds: `/data/work/arthur/voyager_swr_upload/sz_swr_creds.txt`)

Bookkeeping JSONLs in `.validation-worker/`: `all_images{,_platform}.jsonl`
(every target → SWR URL) and `pushed_images{,_platform}.jsonl` (verified pushed).

### Launch command (all 6,319, both registries)

```bash
cd /data/work/slurm-swegen && source .env
RUN=runs/20260716-sol-max-full-16w
PYTHONPATH=src .venv/bin/python3 src/push_all_verified.py \
  --run-dir "$RUN" \
  --instance-list "$RUN/.validation-worker/target_verified_6319.txt" \
  --extra-task-dir "$RUN/export/sources/ecs-...-0002/tasks" \
  --extra-task-dir "$RUN/export/sources/ecs-...-0005/tasks" \
  --extra-task-dir "$RUN/export/sources/ecs-...-0006/tasks" \
  --extra-task-dir "$RUN/export/sources/ecs-...-0003/tasks" \
  --extra-task-dir "$RUN/tasks_non_hacking_20260720" \
  --proxy-env .env --workers 24 --skip-registry-check \
  --registries trajectory,platform
```
Re-runnable: already-pushed instances are skipped via the pushed_images JSONLs.

Typical failures (all retriable, none structural): `docker build` timeouts under
high concurrency (30-min cap), transient npm/git-submodule/TLS network hiccups,
`containerd-mount ... device or resource busy` (lower `--workers` to fix). One
instance-id sanitization edge case produced an invalid tag
(`hb__amol-__dukpy--swegenimage` → double dash) — worth hardening `local_image_tag`.

## 5. CWM SDK path (only needed if you WANT the preloaded base, which we don't)

Documented for completeness; NOT used in the final solution.
- SDK: `cwm-sdk 0.1.10`; resolver in `postprocess.py::make_cwm_resolver()` uses
  `CwmClient(platform_api_base_url="http://159.138.1.45:6066", username/password="Alex Yang")`
  then `client.images.resolve(kind="repo", repo_full_name="owner/repo")["image_ref"]`.
- Blocker from this node: `159.138.1.45:6066` is unreachable (direct filtered, proxy
  → netentsec block). Reachable hosts `voyager.rnd.huawei.com:8088` /
  `7.156.130.178:8088` authenticate but their DB returns "repo image mapping not
  found" for our repos. All SWR-side SDK calls (resolve training / exists / pull)
  time out here. → Dead end from this node; the self-contained dirs make it moot.

---

## 6. Pipeline cleanup TODO (the mess to fix)

The Slurm export/postprocess pipeline is confusing and error-prone. Concrete items:

1. **Never ship a task-pack with an unrewritten placeholder FROM.** The
   `reward-hack-accepted-*.tar.gz` preloaded Dockerfiles have FROM
   `swesandbox/ubuntu:24.04` that `postprocess.py::replace_from` was supposed to
   replace. Either run postprocess before packing, or pack the self-contained form.
   Add a validation step (`validate_dockerfile`) that FAILS the export if FROM still
   equals the placeholder.
2. **One canonical task-dir location.** Right now the same instance exists in ≥5
   places (`.validation-worker/tasks`, `export/sources/<node>/tasks`,
   `slurm-nodes/<node>/tasks`, `tasks_voyager_postprocessed`, `tasks_non_hacking_*`,
   the tar-packs) with two different FROM variants. Pick one authoritative source
   of truth per variant and symlink/generate the rest.
3. **Make "passed reward-hack" trivially queryable.** The count is spread across
   `reward-backfill-status.jsonl` (reward verdict) + `postcheck-status.jsonl`
   (nop/oracle). Emit a single `verified_instances.jsonl` (instance → rewards →
   is_hacking → task_dir path) at export time.
4. **Fold image push into the pipeline.** The validation worker already knows when
   an instance is accepted; push to SWR there (build once, push both registries)
   instead of a separate retroactive sweep. Store the SWR URL back in the ledger.
5. **CWM resolver reachability.** postprocess hard-codes `159.138.1.45:6066` which
   is only reachable from the original build network. Make the host configurable and
   fail loudly (not silently emit a placeholder FROM) when resolve fails.
6. **Instance-id → image-tag sanitization** produced an invalid double-dash tag;
   centralize and test `local_image_tag`/`_docker_image_name`.
7. **Registry image cap.** SWR limits images per repo, hence the common-repo +
   unique-tag scheme (`.../generated:<instance_id>`). Keep this invariant documented
   so nobody reintroduces per-instance repositories.
