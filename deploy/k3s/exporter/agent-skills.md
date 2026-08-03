---
title: Agent Skills 注入说明
---

# Agent Skills 注入说明

## 这篇文档适合谁看

- **适合对象**：准备在 Harbor task-dir 中给 ClaudeCode、Codex、OpenCode、Terminus 2 等 Agent 增加项目规则、审查清单或工作流提示的 task 作者和产线维护者。
- **解决问题**：说明 `skills/` 目录怎么组织、平台如何在镜像构建时注入技能文件、各 Agent 如何发现技能，以及如何验证技能是否生效。
- **建议阅读场景**：task 需要携带一组可复用的项目约定、调试流程、代码审查标准，或需要排查 Agent 运行时没有看到技能文件时，看这篇。

更新时间：2026-07-21 GMT+08:00

Skills 是随 task-dir 一起提交的 Markdown 指令文件。平台在镜像构建阶段把这些文件放进 Agent 运行环境，让 Agent 在执行任务时可以按需读取额外的知识、规则或工作流。

需要先区分两个概念：

- `instruction.md` 是每个 task 的主任务说明，Agent 一定会看到。
- `skills/` 是可复用的补充材料。Agent 能看到技能名称和描述，但完整内容通常要在 Agent 主动调用技能时才加载。

## 1. 快速开始

推荐在 task-dir 根目录使用 Agent 原生的技能子目录格式：

```text
my_task/
├── instruction.md
├── task.toml
├── environment/
│   ├── Dockerfile
│   └── ...
└── skills/
    └── coding_standards/
        ├── SKILL.md
        └── reference.md
```

`skills/coding_standards/SKILL.md`：

```markdown
---
name: coding_standards
description: 提供项目编码规范和代码审查检查项
---

# 代码审查标准

## 必须检查项

- 所有公开函数必须有类型注解
- 错误处理不能使用裸 except
- SQL 查询必须使用参数化

## 代码风格

- 单行不超过 100 字符
- 使用 f-string 而非 .format()
```

开头两个 `---` 之间的内容叫 YAML frontmatter，是 Agent 发现技能时读取的元数据，不是正文标题。OpenCode 要求至少包含 `name` 和 `description`，并且 `name` 必须和技能目录名 `coding_standards` 一致。

**文件名区分大小写，必须精确写成 `SKILL.md`。`skill.md` 和 `Skill.md` 都是无效入口文件，OpenCode 不会发现它们。**

提交后平台会在镜像构建时自动完成注入。Agent 运行时可通过技能名调用，例如 `/coding_standards`。

建议技能内容写成清晰、可执行的规则或流程，不要把单个 task 的目标写进技能里；单个 task 的目标仍然放在 `instruction.md`。

## 2. 支持的目录格式

平台支持标准子目录格式和扁平便利格式。为了兼容不同 Agent，推荐直接使用标准子目录格式。

### 标准子目录格式（推荐）

```text
skills/
├── coding_standards/
│   └── SKILL.md
└── review_checklist/
    └── SKILL.md
```

已有子目录会原样复制。平台不会递归把子目录中的 `skill.md` 改成 `SKILL.md`，也不会自动补充 YAML frontmatter。

### 扁平便利格式

```text
skills/
├── coding_standards.md
└── review_checklist.md
```

构建时，平台只归一化 `skills/` 根目录下直接存在的 Markdown 文件：

```text
skills/coding_standards.md
    -> skills/coding_standards/SKILL.md
```

这项归一化只改变目录和文件名，不会生成或修改文件内容。供 OpenCode 使用时，扁平源文件本身也必须包含有效的 `name` 和 `description` frontmatter。

## 3. 构建注入流程

当 task archive 中包含非空 `skills/` 目录时，平台构建服务会执行以下流程：

```text
task_dir/skills/coding_rules.md
    ↓
1. 归一化为 skills/coding_rules/SKILL.md
    ↓
2. 读取 task.toml 的 [environment].skills_dir，默认使用 /skills
    ↓
3. 改写 Dockerfile，追加技能 COPY
    ↓
4. 如未显式配置 skills_dir，向 task.toml 注入 skills_dir = "/skills"
```

Dockerfile 会被注入两类 COPY：

| COPY 目标 | 用途 | 主要使用者 |
| --- | --- | --- |
| `<skills_dir>/`，默认 `/skills/` | Harbor 原生路径，Agent 启动时从这里复制到自身配置目录 | ClaudeCode、Codex、OpenCode、Terminus 2 |
| `<WORKDIR>/.claude/skills/` | Claude Code 项目级技能发现路径 | ClaudeCode |

ClaudeCode 需要这两个路径同时存在：Harbor 路径用于运行前注册，`.claude/skills/` 用于 Claude Code 自身的项目级技能发现。

`<WORKDIR>` 按 Docker 语义解析：每个 `FROM` 会把工作目录重置为 `/`，后续 `WORKDIR` 指令按顺序累计解析。如果最终阶段没有 `WORKDIR`，平台默认按 Harbor 惯例使用 `/workspace/repo`。

## 4. task.toml 配置

多数场景不需要手动配置。只要 task-dir 根目录包含 `skills/`，平台会默认注入：

```toml
[environment]
skills_dir = "/skills"
```

如果需要把技能文件放到镜像里的其他路径，可以显式指定：

```toml
[environment]
skills_dir = "/custom/skills/path"
```

显式指定后，平台会使用该路径作为 Harbor 原生 COPY 目标：

```text
COPY skills/ /custom/skills/path/
```

`skills_dir = ""` 表示显式禁用技能注入。即使 task archive 里有 `skills/`，平台也不会向 Dockerfile 添加技能 COPY，Agent 运行时也不会看到这些技能文件。

## 5. Agent 兼容性

| Agent | Skills 支持 | 发现机制 | 备注 |
| --- | --- | --- | --- |
| ClaudeCode | 完整支持 | `.claude/skills/<name>/SKILL.md` 项目发现 + Harbor 运行前复制注册 | 需要双路径 COPY |
| Codex | 支持 | Harbor 运行前复制到 `$HOME/.agents/skills/` | |
| OpenCode | 支持 | Harbor 运行前复制到 `~/.config/opencode/skills/<name>/SKILL.md` | 必须包含 `name` 和 `description` frontmatter，且 `name` 与目录名一致 |
| Terminus 2 | 支持 | 直接从 `skills_dir` 查找 `SKILL.md` | 归一化后格式兼容 |
| OpenHands SDK | 部分支持 | 使用 OpenHands 自身的技能发现路径 | 可能忽略 Harbor `skills_dir` |
| SweAgent | 不支持 | 无技能加载代码 | `skills_dir` 字段被接受但无实际效果 |
| MiniSweAgent | 不支持 | 无技能加载代码 | `skills_dir` 字段被接受但无实际效果 |

当前 `cpu` / `npu` 在线入口底层会进入平台构建和 Harbor / Voyager 执行链路。只要该 task 需要镜像构建，技能文件就可以通过构建注入进入镜像。使用预构建镜像且不触发构建的场景，无法再把新的 `skills/` 文件注入到镜像里。

## 6. 技能激活行为

技能不是强制自动执行的脚本，也不是一定会被完整读取的上下文。

技能文件被注入容器后，Agent 通常能看到可用技能的名称和描述；完整内容只有在 Agent 决定调用技能时才会加载。因此：

- 简单任务中，Agent 可能直接完成工作，不会主动查看技能。
- 技能里写的隐含要求不会天然生效，例如“完成后创建 proof.txt”。
- 如果某条规则必须执行，应在 `instruction.md` 中明确提醒 Agent 参考对应项目规范。

推荐写法是在 `instruction.md` 中自然引导，而不是把技能当成隐藏强约束：

```markdown
修复 calc.py 中的 bug。请遵循项目的代码维护规范。
```

如果必须确保调用某个技能，可以直接写明技能名：

```markdown
修复 calc.py 中的 bug。开始前请调用 /coding_standards 获取代码规范。
```

## 7. 验证技能是否生效

排查时按以下顺序看：

1. 确认 task archive 根目录下存在非空 `skills/`。
2. 确认 `task.toml` 没有写 `skills_dir = ""`。
3. 查看 Agent 运行日志，例如 OBS 中的 `logs.txt.zst`，搜索 `cp -r /skills/*` 或对应 Agent 的技能复制命令。
4. 注意复制命令成功只证明文件进入了配置目录，不证明 Agent 已发现技能。
5. 如果使用 OpenCode，检查目标路径是否为 `<name>/SKILL.md`，文件名大小写是否准确，frontmatter 是否包含 `name` 和 `description`，以及 `name` 是否与目录名一致。还可以从请求日志的 `<available_skills>` 中确认目标技能是否实际注册。
6. 如果使用 ClaudeCode，确认 Dockerfile 最终阶段有合理的 `WORKDIR`，或接受平台默认的 `/workspace/repo`。
7. 可以在测试用技能中加入一个明确的标记动作，例如要求创建 `proof.txt`，再通过 artifact 或日志确认 Agent 是否读取过技能。

常见现象和处理方式：

| 现象 | 可能原因 | 处理方式 |
| --- | --- | --- |
| 日志里没有技能复制命令 | `skills/` 为空、被禁用，或使用了不触发构建的预构建镜像 | 检查 archive、`skills_dir` 和镜像构建链路 |
| ClaudeCode 看不到技能 | `.claude/skills/` 没注入到最终 `WORKDIR` | 检查 Dockerfile 最终阶段 `WORKDIR` |
| OpenCode 日志显示已执行 `cp -r /skills/*`，但提示技能不存在 | 文件已复制，但因入口文件名、frontmatter 或目录名不符合要求而未被发现 | 检查精确文件名 `SKILL.md`、必填字段 `name` 和 `description`，以及 `name` 与目录名是否一致；再确认 `<available_skills>` 中是否出现目标技能 |
| Codex/OpenCode 已发现技能但没有主动执行 | Agent 判断当前任务无需调用，或主任务没有明确引导 | 在 `instruction.md` 中增加项目规范提示或明确调用技能 |
| OpenHands 没识别 `skills_dir` | OpenHands SDK 使用自身发现路径 | 需要单独验证 OpenHands 当前版本的技能发现逻辑 |

## 8. 安全和边界

- `skills/` 中的符号链接会被拒绝或删除，防止通过软链读取宿主机敏感文件。
- 空目录不会触发 COPY 注入，也不会导致任务失败。
- `task.toml` 注释中的 `skills_dir` 字符串不会被当作配置；平台按 TOML 解析 `[environment].skills_dir`。
- `skills/` 下可以保留子目录结构，但具体能发现到多深取决于 Agent 实现。ClaudeCode 常用的是一级目录下的 `SKILL.md`。
- 平台只归一化 `skills/` 根目录下的扁平 Markdown 文件，不会递归修正已有子目录中的文件名，也不会生成 frontmatter。
- 技能适合放可复用规则、流程和知识，不适合放密钥、账号、生产凭据或只属于单个 task 的目标描述。

## 9. 完整示例

```text
fix-calculator/
├── task.toml
├── instruction.md
├── environment/
│   ├── Dockerfile
│   └── repo/
│       └── calculator.py
├── skills/
│   └── debugging_workflow/
│       └── SKILL.md
└── tests/
    └── test.sh
```

`skills/debugging_workflow/SKILL.md`：

```markdown
---
name: debugging_workflow
description: 提供修复代码缺陷时使用的系统化调试流程
---

# 调试工作流

当修复 bug 时，按以下步骤执行：

1. 先运行现有测试，确认失败用例
2. 阅读相关源码，定位根因
3. 编写修复代码
4. 重新运行测试验证通过
5. 检查是否引入新问题

## 代码修改原则

- 最小改动原则，不重构无关代码
- 保持原有代码风格
```

`task.toml`：

```toml
[metadata]
name = "fix-calculator"
task_type = "bugfix"

[agent]
timeout_sec = 600

[environment]
dockerfile = "Dockerfile"

[verification]
type = "script"
script = "tests/test.sh"
```

无需手动添加 `skills_dir`。平台检测到 `skills/` 后会自动注入默认路径。

`instruction.md` 可以这样引导：

```markdown
修复 calculator.py 中的计算错误。请遵循项目调试工作流和代码维护规范。
```

提交运行后，Agent 会在运行环境中看到 `debugging_workflow` 技能，并可在需要时加载完整内容。
