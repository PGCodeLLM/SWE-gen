---
title: SWE-gen SDK 接入链路参考
---

# SWE-gen SDK 接入链路参考

## 1. 文档目的

本文档面向需要接入 Harbor/CWM 平台的其他产线，说明可以如何参考当前 `sdk-swegen-task` 的实现，把平台登录、平台数据查询、repo archive 下载、镜像解析/构建、task 提交等能力接到 CWM SDK 上。

`sdk-swegen-task` 在这里主要作为一份参考实现：它已经完成了从账号密码登录平台，到查询候选数据、下载 repo、解析/构建 repo image、生成 Harbor task，并通过 SDK 提交平台执行的功能链路。当前链路已验证到 oracle 通过，真实平台产轨迹中也已出现 `reward=1` 的结果。其他产线可以复用同样的接入模式，但不需要照搬 SWE-gen 的 candidate 筛选和 task 内容生成逻辑。

## 2. 推荐接入形态

其他产线接 SDK 时，建议把平台访问收敛成一个统一 client helper，而不是在每个脚本里重复处理平台 URL、账号和密码。

当前仓的参考实现是 `sdk_swegen/cwm_client.py`：

```python
from sdk_swegen.cwm_client import create_cwm_client

with create_cwm_client(
    platform_api_base_url="http://<platform-host>:8088",
    platform_username="<username>",
    platform_password="<password>",
) as client:
    ...
```

推荐产线侧也保留类似封装，职责包括：

- 从命令行参数或环境变量读取平台 URL、账号、密码。
- 创建 `cwm.CwmClient`。
- 让上层业务脚本只依赖一个 client，不重复处理登录细节。

常用环境变量：

| 环境变量 | 含义 |
| --- | --- |
| `CWM_URL` | 平台 API 地址 |
| `CWM_PLATFORM_USERNAME` | 平台用户名 |
| `CWM_PLATFORM_PASSWORD` | 平台密码 |

## 3. SDK 能力与产线映射

### 3.1 平台登录

产线脚本可以通过账号、密码和平台 URL 创建 SDK client：

```python
with create_cwm_client(
    platform_api_base_url=base_url,
    platform_username=username,
    platform_password=password,
) as client:
    ...
```

适用场景：

- 本地 smoke test。
- 产线任务批量提交。
- 只希望通过平台账号访问数据和任务能力。

### 3.2 平台数据查询

SDK 提供平台数据查询能力。不同产线可以基于自己的业务逻辑查询 repo、PR、issue、relation 或其他平台数据。

SWE-gen 的参考做法：

- 默认使用 SDK 查询 repo/PR/issue/relation，匹配 PR-Issue candidate。
- `run_swegen_v1_single_repo_from_sdk.py` 和 `run_swegen_v1_batch_by_owner.py` 默认走 SDK 查询。

其他产线迁移建议：

- 把候选数据查询封装成一层 provider。
- 使用 SDK provider 输出产线业务对象。
- 上层 task 生成逻辑只依赖业务对象，不直接处理平台查询细节。

一体化 pipeline 参考参数：

```bash
--candidate-source cwm-sdk
```

### 3.3 repo archive 下载

SDK 已提供 repo archive 下载能力：

```python
client.db.obs.download_repo_archive(repo_full_name, download_root)
client.db.obs.download_repos_archive(...)
```

SWE-gen 的参考封装：

- `download_repo_archive_via_sdk()`
- `download_repos_archive_via_sdk()`

其他产线可以复用同样模式：

```text
业务选择 repo
        ↓
SDK 下载 repo archive
        ↓
产线自己解压、定位 workspace、抽取业务所需文件
        ↓
进入 task 生成逻辑
```

注意：SDK 负责把 archive 下载到本地，产线仍然需要负责 repo 解压后的业务处理，例如 checkout、patch 生成、测试文件选择、题目生成等。

### 3.4 repo image 解析与构建

SDK 可以解析或构建平台管理的 public repo image：

```python
client.images.resolve(
    kind="repo",
    repo_full_name=repo_full_name,
    label=label,
)
```

```python
client.repo_images.build_public_repo_images(
    repo_full_names=[repo_full_name],
    force_rebuild=True,
    include_git_dir=True,
    label=label,
    wait=True,
)
```

`sdk-swegen-task` 默认使用 `build_public_repo_images(...)`，由平台托管 repo image 构建和发布。

推荐产线优先采用 repo image 链路，而不是让每个 task 在 Dockerfile 构建阶段重复下载 repo。原因是：

- task 包更小。
- 同一个 repo 的多个 task 可以复用 repo image。
- Dockerfile 不依赖构建期 OBS 访问。
- repo 路径、git checkout、patch 应用方式更稳定。

repo image 链路要求 repo image 内 repo 路径约定为 `/app/<owner>/<repo>`。

### 3.5 task 提交

SDK 提供 task 自动提交能力：

```python
client.tasks.submit_task_dirs_auto(...)
```

SWE-gen 的参考入口：

- `scripts/batch_upload_task_dirs.py`

CWM SDK wheel 的 `client.tasks.submit_task_dirs_auto()` 支持 task zip 提交、产线绑定、Agent/Model、`n_sampling`、Terminus 2 专用 `max_steps`、confirm、续跑状态和分批等参数。`scripts/batch_upload_task_dirs.py` 是 SWE-gen 产线 wrapper，不是 wheel 自带命令；它在调用 SDK 之外额外提供上传后质量检查参数，例如 `--post-submit-check`、`--min-success-count` 和 `--min-success-rate`。

参考入口支持能力包括：

- 账号密码登录平台。
- 提交 task dir 或 task zip。
- 指定 line name。
- 绑定 production line name/id。
- 设置 agent type / model。
- 设置 execution backend。
- 设置 verification mode。
- 设置 sampling 和 max steps。
- 使用 state file 续传。

其他产线可以把自己的 task 产物保持为 Harbor 标准 task dir/zip，然后直接复用 SDK submit 能力。

## 4. 产线端到端参考链路

其他产线可以参考下面的通用链路改造：

```text
读取平台 URL、账号、密码
        ↓
create_cwm_client()
        ↓
SDK 查询业务候选数据
        ↓
SDK 下载 repo archive，必要时解析/构建 repo image
        ↓
产线生成 Harbor task dir
        ↓
打包 task zip
        ↓
SDK 提交平台
        ↓
轮询或查看平台 run 状态
```

SWE-gen 当前实现对应为：

```text
账号密码登录平台
        ↓
SDK 查询 PR-Issue candidate
        ↓
SDK 下载 repo archive
        ↓
抽取 PR patch、bug patch、测试文件和 instruction
        ↓
SDK 通过平台托管入口解析或构建 public repo image
        ↓
生成 Harbor task dir
        ↓
打包 task zip
        ↓
SDK submit_task_dirs_auto() 提交平台
```

## 5. task 环境生成链路

SWE-gen 当前使用 repo image 生成 task 环境，其他产线可以参考同一形态。

### 5.1 `repo-image`

这是当前默认链路，也建议其他产线优先参考。

流程：

```text
SDK resolve repo image
        ↓
已有 published image -> 直接使用
        ↓
没有镜像、mapping 缺失或 force rebuild -> SDK 通过平台托管入口触发 repo image 构建
        ↓
SDK 等待镜像发布
        ↓
task Dockerfile FROM 该 repo image
        ↓
按需 checkout 到目标 commit
        ↓
按需应用产线自己的 bug/setup patch 或其他源码准备逻辑
        ↓
生成 task.zip，按需提交平台
```

Dockerfile 参考形态。下面是 SWE-gen 的 repo patch 任务示例，不是所有产线必须照写的模板：

```dockerfile
FROM <swr repo image>

WORKDIR /app/<owner>/<repo>
RUN git checkout --detach <target_sha>
RUN git submodule update --init || true
RUN git reset --hard

COPY bug.patch /tmp/bug.patch
RUN git apply /tmp/bug.patch && rm /tmp/bug.patch

RUN rm -rf .git

WORKDIR /app/<owner>/<repo>
```

关键约定：

- task 内不再生成 `environment/obs_download.py`。
- Dockerfile 不在 task 构建阶段访问 OBS。
- workspace 使用 `/app/<owner>/<repo>`。
- checkout、submodule、reset、patch 都按产线任务需要选择；没有 patch 的产线不需要生成 `bug.patch`。
- 如果使用 git patch，推荐用 `git apply`，避免依赖镜像内必须安装 `patch` 命令。
- `task.toml` 应写入 workspace，例如 `environment.workspace_dir = "/app/<owner>/<repo>"`。
- verifier 和 solution 都应使用同一个 workspace。

SWE-gen 相关参数：

```bash
--image-strategy repo-image-or-build
--repo-image-label <label>
--repo-image-force-rebuild true
--repo-image-wait true
```

当前 CLI 默认 `--image-strategy repo-image-or-build`，即优先 resolve 已有 repo image，缺失时触发平台托管构建；如果希望缺失时直接失败，可以显式使用 `--image-strategy repo-image`。

注意：`repo image mapping not found` 在当前实现里会被视为“镜像缺失”，继续调用平台托管的 `build_public_repo_images(...)`，不会直接导致 task 生成失败。

## 6. SWE-gen 参考入口

单仓生成：

```bash
CWM_URL="http://<platform-host>:8088" \
CWM_PLATFORM_USERNAME="<username>" \
CWM_PLATFORM_PASSWORD="<password>" \
python scripts/run_swegen_v1_single_repo_from_sdk.py \
  --platform-url http://<platform-host>:8088 \
  --username "<username>" \
  --password "<password>" \
  --line-name <line-name> \
  --repo-full-name owner/repo \
  --work-root /path/to/work \
  --image-strategy repo-image-or-build \
  --repo-image-force-rebuild true \
  --repo-image-wait true \
  --validation-mode none \
  --limit 1 \
  --agent-type oracle \
  --runtime-agent-injection-enabled false \
  --confirm-run true
```

owner 批量生成：

```bash
CWM_URL="http://<platform-host>:8088" \
CWM_PLATFORM_USERNAME="<username>" \
CWM_PLATFORM_PASSWORD="<password>" \
python scripts/run_swegen_v1_batch_by_owner.py \
  --platform-url http://<platform-host>:8088 \
  --username "<username>" \
  --password "<password>" \
  --line-name <line-name> \
  --final-root /path/to/final-output \
  --work-root /path/to/work-dir \
  --image-strategy repo-image-or-build \
  --repo-image-wait true \
  --validation-mode none \
  --agent-type oracle \
  --runtime-agent-injection-enabled false \
  --confirm-run true
```

`run_swegen_v1_batch_by_owner.py` 当前不接收 `--owner`，会通过 SDK 从平台获取 owner 列表/count，并固化到 `progress/owners_todo.txt`。如果需要只按某个 owner 做过滤生成，可使用 `run_swegen_v1_single_repo_from_sdk.py` 的 batch filter 模式并传 `--owner <owner>`。

提交已有 task zip：

```bash
CWM_PLATFORM_USERNAME="<username>" \
CWM_PLATFORM_PASSWORD="<password>" \
python scripts/batch_upload_task_dirs.py \
  --input-path /path/to/task.zip \
  --line-name <line-name> \
  --production-line-name <production-line-name> \
  --base-url http://<platform-host>:8088 \
  --username "<username>" \
  --password "<password>" \
  --agent-type oracle \
  --execution-backend cpu \
  --verification-mode enabled \
  --runtime-agent-injection-enabled false \
  --confirm true \
  --n-sampling 1 \
  --post-submit-check true \
  --min-success-count 1 \
  --min-success-rate 0.2 \
  --output-json /path/to/upload_report.json
```

本示例不在提交命令里配置 Agent 运行超时；按 task 维度写在 archive 内的 `task.toml` `[agent].timeout_sec`。`max_steps` 只适用于 Terminus 2 步数上限，不用于 Oracle 提交。

生成并提交一体化：

```bash
CWM_URL="http://<platform-host>:8088" \
CWM_PLATFORM_USERNAME="<username>" \
CWM_PLATFORM_PASSWORD="<password>" \
python scripts/run_swegen_v1_task_pipeline.py \
  --platform-url http://<platform-host>:8088 \
  --username "<username>" \
  --password "<password>" \
  --candidate-source cwm-sdk \
  --image-strategy repo-image-or-build \
  --validation-mode light \
  --final-root /path/to/final-output \
  --work-root /path/to/work-dir \
  --batch-size 100 \
  --upload-base-url http://<platform-host>:8088 \
  --upload-username "<username>" \
  --upload-password "<password>" \
  --upload-line-name <line-name> \
  --upload-production-line-name <production-line-name> \
  --upload-agent-type oracle \
  --upload-execution-backend cpu \
  --upload-runtime-agent-injection-enabled false \
  --upload-confirm true
```

一体化 pipeline 默认会在上传后开启平台后验检查：等待平台 run 完成，汇总 oracle reward，并要求 `success_count >= 1` 且 `success_rate >= 0.2`。这个标准不是要求本地逐 task 跑 oracle，也不是要求整批 100% 成功，而是防止“整批全 0”继续扩大提交。单独调用 `batch_upload_task_dirs.py` 时，为兼容旧流程，仍需显式传 `--post-submit-check true` 才会等待平台结果。

## 7. task 产物参考

本节给的是 Harbor task dir 参考形态，不是要求所有产线照抄 SWE-gen 的文件组织。不同产线可以有不同的 patch 形式、验证脚本和 solution 组织方式；提交平台前需要保证平台可识别的基础文件存在，并且 task 内部 workspace 约定一致。

当前平台上传校验至少要求：

```text
task_dir/
├── instruction.md
├── task.toml
├── environment/
│   └── Dockerfile
```

其中 `tests/test.sh` 通常在开启验证时需要；`solution/solve.sh` 用于 oracle 或参考解。`tests/`、`solution/`、`bug.patch`、`fix.patch`、测试文件目录等属于产线自己的 task 约定，不是平台统一强制格式。SWE-gen 当前会生成类似下面的扩展结构：

```text
task_dir/
├── instruction.md
├── task.toml
├── environment/
│   ├── Dockerfile
│   └── bug.patch
├── solution/
│   ├── fix.patch
│   └── solve.sh
└── tests/
    ├── test.sh
    └── <test files>
```

`repo-image` 链路下，`task.toml` 可以记录 repo image、workspace、资源和超时信息，便于运行和排障。Agent 运行超时使用 `[agent].timeout_sec`。下面字段与当前平台 task 示例和 pipeline 读取逻辑一致；具体数值应按产线任务复杂度调整，不要求固定为 1200 秒：

```toml
schema_version = "1.1"
artifacts = []

[metadata]
repo_full_name = "owner/repo"
source_commit = "0000000000000000000000000000000000000000"
programming_language = "node"

[agent]
timeout_sec = 1200.0

[verifier]
timeout_sec = 1200.0

[environment]
build_timeout_sec = 1200.0
cpus = 2
memory_mb = 4096
storage_mb = 10240
runtime_family = "node"
runtime_profile = "swegen-node"
mirror_profile = "china"
image_strategy = "repo-image"
resolved_image_ref = "registry.example.com/swesandbox/public/repo/owner-repo:v1"
workspace_dir = "/app/owner/repo"
```

这样 verifier、solution 和后续排障都可以明确知道 task 使用了哪一个基础 repo image，以及应该在哪个 workspace 运行。

`environment/Dockerfile` 的最小参考形态：

```dockerfile
FROM registry.example.com/swesandbox/public/repo/owner-repo:v1

WORKDIR /app/owner/repo
RUN git checkout --detach 0000000000000000000000000000000000000000

WORKDIR /app/owner/repo
```

SWE-gen Node task 会按需要增加 runtime、依赖和 patch 处理，例如：

```dockerfile
FROM registry.example.com/swesandbox/public/repo/owner-repo:v1

# Optional: only needed when the base repo image does not already include
# the runtime/tools required by this task.
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    ca-certificates \
    build-essential \
    python3 \
    python3-pip \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://repo.huaweicloud.com/nodejs/v20.11.1/node-v20.11.1-linux-x64.tar.xz \
    | tar -xJ -C /usr/local --strip-components=1

WORKDIR /app/owner/repo
RUN git checkout --detach 0000000000000000000000000000000000000000
RUN git submodule update --init || true
RUN git reset --hard

COPY bug.patch /tmp/bug.patch
RUN git apply /tmp/bug.patch && rm /tmp/bug.patch

RUN rm -rf .git

WORKDIR /app/owner/repo
```

这里需要注意：

- `apt-get install` 和 Node runtime 安装只是 SWE-gen Node 任务的示例；如果 repo image 已包含运行时，或产线使用 Java/Python/ArkTS/其他运行时，应替换成自己的最小增量。
- `git submodule update --init || true` 只适合确实需要 submodule 的 repo；不需要 submodule 的产线可以不写。
- `git reset --hard` 用于确保 patch 应用前 workspace 干净；如果产线有自己的源码准备方式，可以替换。
- `bug.patch` / `fix.patch` 是 SWE-gen 的 bug 注入和参考修复形态；其他产线可以使用不同文件名、不同 patch 机制，或完全没有 patch。

Node task 的 `tests/test.sh` 参考：

```bash
#!/bin/bash

set -euo pipefail

reward_value=0
write_reward() {
  set +e
  mkdir -p /verifier
  mkdir -p /logs/verifier
  printf '%s\n' "$reward_value" > /verifier/reward.txt
  printf '%s\n' "$reward_value" > /logs/verifier/reward.txt
}
trap write_reward EXIT

echo "phase=bootstrap" >&2
mkdir -p /verifier
mkdir -p /logs/verifier
printf '%s\n' "$reward_value" > /verifier/reward.txt
printf '%s\n' "$reward_value" > /logs/verifier/reward.txt
if [ ! -d /app/owner/repo ]; then
  printf '%s\n' 'missing workspace: /app/owner/repo' >&2
  exit 2
fi
cd /app/owner/repo

echo "runtime_family=node" >&2
echo "runner_kind=mocha" >&2
echo "selector_mode=file" >&2
echo "confidence=high" >&2

echo "phase=copy_tests" >&2
mkdir -p tests/lib
cp /tests/tests/lib/example-feature.test.js tests/lib/example-feature.test.js
cp /tests/tests/lib/example-regression.test.js tests/lib/example-regression.test.js

echo "phase=setup" >&2
npm install --legacy-peer-deps >/dev/null 2>&1 || npm install >/dev/null 2>&1 || true
npm run build --if-present >/dev/null 2>&1 || true
npm run rollup --if-present >/dev/null 2>&1 || true

echo "phase=run_tests" >&2
set +e
npx mocha tests/lib/example-feature.test.js tests/lib/example-regression.test.js
test_status=$?
set -e

if [ $test_status -eq 0 ]; then
  reward_value=1
fi
exit "$test_status"
```

SWE-gen 使用 `fix.patch` 时的 `solution/solve.sh` 参考：

```bash
#!/bin/bash

set -euo pipefail
cd /app/owner/repo

git apply /solution/fix.patch

npm install --legacy-peer-deps >/dev/null 2>&1 || npm install >/dev/null 2>&1 || true
npm run build --if-present >/dev/null 2>&1 || true
npm run rollup --if-present >/dev/null 2>&1 || true
```

对其他产线来说，关键不是照抄示例里的测试文件、patch 文件或 Dockerfile 命令，而是保持这几个约定：

- Dockerfile 基于 repo image，只保留当前 task 需要的最小环境增量和源码准备步骤。
- `task.toml.environment.workspace_dir`、Dockerfile `WORKDIR`、`test.sh`、`solve.sh` 使用同一个 workspace。
- verifier 启动时先写 `reward=0`，只有目标测试通过后才写 `reward=1`。
- solution 应只做该产线定义的参考修复动作；SWE-gen 是应用 `/solution/fix.patch`，其他产线可以不是 patch。
- Node 场景如果需要安装 runtime，优先使用明确版本；其他语言或已预装 runtime 的 repo image 不需要照搬 Node 示例。

## 8. 已验证的参考结果

当前仓用真实平台账号密码和 URL 验证过新链路。近期验证已经覆盖从 task 生成、repo image Dockerfile、oracle verifier、打包上传到平台产轨迹 reward 的闭环。

验证路径：

```text
账号密码登录平台
        ↓
SDK 查询 candidate
        ↓
SDK 下载 repo archive
        ↓
SDK 通过平台托管入口 resolve/build public repo image
        ↓
生成 FROM 新 repo image 的 task
        ↓
生成 task zip
        ↓
SDK 提交 task bundle
        ↓
确认平台 run
```

验证结论：

- SDK 登录、candidate 查询、repo archive 下载、repo image resolve/build、task 生成、task zip 打包、SDK 提交平台的链路已跑通。
- 生成的 task 已通过 oracle 验证。
- 真实平台产轨迹中已确认存在 `reward=1` 的任务，不再是整批全 0。
- 提交参数使用 `agent_type=oracle`、`execution_backend=cpu`、`verification_mode=enabled`。
- task Dockerfile 使用 `FROM <repo image>`，不包含 `obs_download.py`，构建阶段不再访问 OBS。
- pipeline 上传后会执行平台后验检查，按批次统计 oracle reward，并用 `success_count` / `success_rate` 拦住整批全 0。

这说明参考链路至少覆盖了：

- SDK 登录。
- SDK candidate 查询。
- SDK repo archive 下载。
- SDK repo image resolve/build。
- 平台托管 public repo image 构建入口。
- Harbor task 生成。
- task zip 生成。
- SDK task 提交。
- production line 解析与绑定。
- Oracle run 创建和确认。
- oracle 后验结果回收。
- 批次级成功率门槛。

## 9. 其他产线迁移检查项

其他产线接入 SDK 时，建议逐项确认：

- 是否有统一的 SDK client helper。
- 是否支持通过账号、密码、URL 登录平台。
- 候选数据查询是否已经抽象成 provider，便于替换为 SDK provider。
- repo 获取是否改为 SDK repo archive 下载或 repo image。
- task Dockerfile 是否避免在构建阶段重复做外部下载。
- task workspace 是否在 `task.toml`、`test.sh`、`solve.sh` 中保持一致。
- task zip 是否能直接交给 `client.tasks.submit_task_dirs_auto()`。
- 上传脚本是否支持 production line name/id、backend、verification mode、sampling、max steps。
- task 生成路径是否已经切到 `repo-image` 或 `repo-image-or-build`。
- 是否有真实平台 smoke test，至少验证到 task dir/zip 生成；如果要验证提交链路，再单独确认 run 进入 `building` 或 `running`。

## 10. 边界说明

需要注意：

- SDK 解决的是平台访问和公共能力复用，不替代产线自己的 task 业务逻辑。
- `repo-image` 链路要求 repo image 内路径为 `/app/<owner>/<repo>`。
- `build_public_repo_images(...)` 是默认平台托管入口。
- 平台 run 进入 `running` 说明提交和构建链路打通，不等价于 agent/verifier 最终完成。
- `--validation-mode light` 只保证结构校验和 Docker build，不等价于 oracle reward 会成功。
- `--validation-mode none` 只表示本地不跑严格验证，不代表 task 质量已满足正式产线要求。
- 如果产线选择“不本地逐 task 跑 oracle”，建议必须开启上传后的平台后验质量门槛，至少拦住 `success_count == 0` 的批次。
