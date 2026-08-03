# Build a dependency intermediate image

Use this when a task's Docker build times out at **600 seconds** on the remote
BuildKit farm while fetching or compiling dependencies.

The idea: build the slow dependency work **once** as a base image in SWR, then
make the task Dockerfile a thin `FROM` of that base. A 600-second timeout
becomes a ~60-second build.

Only do this when cold dependency work actually exceeds 600s. If it is faster,
stop — an intermediate is not worth it.

## Step 1 — check for an existing base

```bash
swegen-pipeline buildkit-intermediate list --repo OWNER/REPO
```

At the task's checkout, hash the lockfile:

```bash
sha256sum Cargo.lock        # or package-lock.json, yarn.lock, go.sum, pnpm-lock.yaml
```

If a `ready` entry has the same lockfile SHA-256, skip to Step 5 and reuse it.

## Step 2 — claim before building

`dependency_key` = lockfile SHA-256. `build_key` = SHA-256 of your base
Dockerfile.

```bash
swegen-pipeline buildkit-intermediate claim --repo OWNER/REPO \
  --dependency-key LOCK_SHA256 --build-key BASE_DOCKERFILE_SHA256 \
  --lockfile-path Cargo.lock --lockfile-sha256 LOCK_SHA256 \
  --dockerfile-sha256 BASE_DOCKERFILE_SHA256 \
  --commit-sha COMMIT --source-task-id TASK_ID \
  --cold-build-seconds MEASURED_SECONDS
```

Proceed only if `claimed` is true. If false, another worker owns it — wait and
reuse instead. Keep the returned `claim_token` and `suggested_image_ref`.

## Step 3 — write the base Dockerfile

Copy the task Dockerfile, then:

- keep everything up to and including the dependency fetch;
- **delete everything from `COPY bug.patch` onward** — the base must not
  contain the patch or any task-specific step;
- point the checkout at the commit you claimed;
- bound compiler concurrency (see gotchas).

Put it in its own directory, outside the task directory.

## Step 4 — build, push, register

```bash
timeout 3600 docker build --network=host --progress=plain \
  -t SUGGESTED_IMAGE_REF BASE_CONTEXT_DIR
docker push SUGGESTED_IMAGE_REF

swegen-pipeline buildkit-intermediate complete \
  --claim-token CLAIM_TOKEN --image-ref SUGGESTED_IMAGE_REF \
  --build-seconds ACTUAL_SECONDS --cold-build-seconds MEASURED_SECONDS
```

On any failure:

```bash
swegen-pipeline buildkit-intermediate fail \
  --claim-token CLAIM_TOKEN --error 'short reason'
```

## Step 5 — make the task Dockerfile thin

The task Dockerfile becomes: base image, reset to the task's commit, apply
`bug.patch`, rebuild only what changed, drop `.git`.

```dockerfile
FROM REGISTRY/REPOSITORY:dep-...@sha256:DIGEST
WORKDIR /app/src
RUN git fetch --depth 1 origin TASK_COMMIT && git checkout --detach FETCH_HEAD
COPY bug.patch /tmp/bug.patch
RUN git apply --ignore-whitespace /tmp/bug.patch && rm /tmp/bug.patch
RUN cargo build --offline     # or the task's own build step
RUN rm -rf /app/src/.git
```

Always pin `@sha256:DIGEST` as well as the tag.

Then run NOP and Oracle as usual. **NOP=0 and Oracle=1 are still required** —
a faster build proves nothing about task correctness.

## Gotchas and recovery

These are real failures already seen in production.

### Cargo

| symptom | fix |
|---|---|
| `cargo fetch` spends 20–40 min on `Updating crates.io index` | Use the sparse index, not the git index: `CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse`. This is the single most common cause. |
| index fetch stalls behind the proxy | `ENV CARGO_NET_GIT_FETCH_WITH_CLI=true` |
| git dependency (not crates.io) times out | Add a git retry loop; keep `--network=host` on the build |
| build killed by memory or open-file limits | `ENV CARGO_BUILD_JOBS=1` (or 8 on a large repo), raise `ulimit -n` |
| `cargo fetch --all-features` rejected | Old toolchain — use `cargo fetch --locked` |
| `supported edition values are 2015 or 2018, but 2021 is unknown` | The pinned toolchain is older than a dependency. **Not fixable here** — fail the claim and leave the task alone. |
| `--locked` says the lockfile needs updating | The repo's committed lockfile is stale. Do not hand-edit it; fail the claim. |

Mirrors are injected automatically — do not add your own registry config.

### Go

| symptom | fix |
|---|---|
| `go mod download` times out on `proxy.golang.org` | `ENV GOPROXY=http://mirrors.tools.huawei.com/goproxy GOSUMDB=off` |
| checksum mismatch against the public sumdb | `GOSUMDB=off` and `GONOSUMDB=*` |
| module still fetched from GitHub directly | **Do not set `GOPRIVATE`** — it makes Go bypass the proxy and clone over git, which is the unreachable path. |

### rustup / toolchain

| symptom | fix |
|---|---|
| `static.rust-lang.org` download stalls | Add a retry loop; build with `--network=host` and the proxy CA env vars |
| `rustup --component rustfmt clippy` misparses | Repeat the flag: `--component rustfmt --component clippy` |
| `rustup: not found` | The image never installed it — install rustup before any cargo step |

### npm / yarn

| symptom | fix |
|---|---|
| `npm ci` postinstall hits GitHub and times out | `npm ci --ignore-scripts`, pre-install the binary separately |
| bare `yarn config` exits non-zero on Berry | Use `yarn config get <key>` |
| native module needs system headers | Add the dev package (e.g. `libprotobuf-dev`) to the apt step |

### Claim bookkeeping

- Changing the base Dockerfile **changes its SHA-256, so the `build_key`
  changes**. Fail the old claim and claim again with the new key.
- Never register the final task-specific image as an intermediate.
- Never put credentials in a Dockerfile, metadata, or an error message.

## When to give up

Fail the claim and move on if:

- cold dependency work is already under 600s;
- the toolchain is too old for its dependencies (edition/MSRV conflict);
- the base build exceeds the 3600s ceiling twice with different fixes.

A failed claim is a normal outcome. It frees the key for another worker and
costs nothing.
