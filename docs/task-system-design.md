# BingOps 任务系统设计（Job 执行引擎）

> 状态：设计定稿，待 P1 实施（2026-08-25）
> 范围：**P1 = Ansible 刚需先行**；Terraform / state backend 挂起至 P2，不废弃
> 上游输入：`docs/ticket-system-gap-analysis.md`（Phase C 执行引擎）、`docs/cloud-sync-design.md`（架构同构参照）

## 0. 已锁定决策

| # | 决策 | 结论 |
|---|------|------|
| 1 | 执行面形态 | 独立项目 `bingops-runner`，Python + ansible-runner，与 bingops 控制面分离 |
| 2 | 代码存放 | GitLab 唯一事实源（tag 不可移动）；P1 runner `git clone --depth 1 --branch <tag>`；OSS 制品层 P2 |
| 3 | 回滚作者层 | role/playbook 内 `bingops_action` do/undo 条件契约（同仓同文件防漂移）+ `block/rescue` 步内自愈 |
| 4 | 回滚编排层 | 引擎逆序重跑 undo（`attempt_type=rollback`）；默认手动触发，runbook 级 opt-in 自动 |
| 5 | Secret 管理 | HashiCorp Vault 唯一存放；下发消息只带钥匙名；runner AppRole 现场取钥 |
| 6 | Terraform state | **不存 Vault**（KB 级 secret 库 vs MB 级高频 blob，性质不同）；P2 用 bingops 实现 http backend + OSS blob |
| 7 | 日志脱敏 | runner 出机前 redact（Vault 取值加入掩码列表） |
| 8 | 灰度 | step 级 `serial`（1 / 30%）+ `batch_pause_sec` 批间暂停 |

---

## 1. 总体架构：控制面 / 执行面分离

```
UI ──→ bingops（控制面，无状态 FastAPI）
        ├── runbook 管理 / 任务创建 / 审批挂接 / 目标锁
        ├── 生产 ──→ Kafka [job-dispatch] ──→ bingops-runner（执行面）
        │                                     ├── Vault AppRole 取钥
        │                                     ├── git clone pinned tag 取代码
        │                                     └── ansible-runner 逐步执行
        └── 消费 ←── Kafka [job-events]  ←──┘ （step 事件 + 逐行日志）
              └── 写 job_steps / job_step_logs（单一写者）
```

与现有同步链路是**同一模式的镜像**：同步链路是「外部生产者 → Kafka → bingops 落库」，任务链路是「bingops 生产下发 → runner 执行 → 事件回流落库」。runner = 反向的 cloud-syncer，团队零认知成本。

**纪律**：bingops 不跑 ansible/terraform；runner 不写业务表；Kafka at-least-once 靠 message_id 去重 + 幂等消化。

---

## 2. 存储五分工

| 资产 | 存放 | 写者 | 读者 |
|------|------|------|------|
| Runbook 元数据（params_schema/steps） | bingops PG | bingops API | bingops |
| 代码（playbook/role/tf module） | GitLab（tag） | 人（MR） | CI / runner |
| 制品 tarball + sha256 | OSS | 仅 CI（P2） | runner（只读） |
| Secret（SSH key/云 AK） | Vault | 运维 | runner（AppRole 只读） |
| Terraform state | OSS blob + bingops http backend（P2） | runner 经 bingops | runner 经 bingops |

---

## 3. Runbook 模型

### 3.1 定义示例

```yaml
name: 批量重启服务
category: restart
target_models: [aliyun_ecs, gcp_compute]  # 目标范围硬校验；P1 默认即此两类
risk_level: medium            # low/medium/high/critical，叠加环境维度提级（P3）
auto_rollback: false          # opt-in 自动回滚，默认手动
params_schema:
  svc: {type: string, required: true}
steps:
  - key: restart_app
    name: 重启 {{ svc }}
    type: ansible             # P1 仅 ansible；terraform 枚举占位 P2 点亮
    playbook: ansible/playbooks/app_restart.yml
    timeout_sec: 600
    serial: "30%"             # 灰度批次
    batch_pause_sec: 60       # 批间暂停
    rollbackable: true        # 引擎回滚 = 同 playbook 重发 + bingops_action=undo
  - key: run_migration
    type: ansible
    playbook: ansible/playbooks/db_migrate.yml
    rollbackable: false       # 不可逆步骤：UI 启动前高亮，回滚链不穿过它
```

### 3.2 do/undo 条件契约（作者层）

```yaml
# roles/app_restart/tasks/main.yml
- include_tasks: "{{ 'undo.yml' if bingops_action | default('do') == 'undo' else 'do.yml' }}"
```

- 操作与逆操作同仓同文件，永不漂移；引擎回滚无需独立 rollback playbook 路径
- 步内瞬时失败用 ansible 原生 `block/rescue/always` 自愈（如起服失败先尝试拉起），与步级回滚互补不替代
- **CI 门禁（P2）**：标 `rollbackable: true` 的 role 必须引用 `bingops_action`，防只写 do 忘写 undo

### 3.3 版本语义

- `runbooks.version` 整数，每次编辑 +1
- 任务创建时 **runbook_version + steps + code_ref（git tag）三快照** 进 execution 行——在跑任务永远用创建时的定义与代码

---

## 4. 执行流程与状态机

### 4.1 端到端流程

1. UI 圈选目标（CMDB 选择器）→ 创建 `job_executions`（params/targets/version 快照）
2. 并发校验：target_resource_ids 与在跑 execution 交集命中即拒绝（同资源单执行锁）
3. 审批（P3）：risk_level + 环境维度 → 挂 ticket，通过才下发
4. bingops 发 `job-dispatch`（**只带 ssh_key_ref 钥匙名，不带 secret**）
5. runner 消费 → Vault 取钥（临时文件 0600，用完即删）→ 拼 ad-hoc inventory → 逐步执行
6. runner 流式发 `job-events`：`step_started → log(seq 递增) → step_finished`
7. bingops 消费落库 → 前端 SSE live tail
8. 失败 → 按策略手动/自动回滚：已完成且 rollbackable 的步骤**逆序**重跑 undo；不可逆步骤阻断回滚链并告警
9. 终态 → CMDB `change_log`（source='job'）

### 4.2 状态机

```
execution: pending → awaiting_approval(P3) → running → success / cancelled
                                       ↘ failed → rolling_back → rolled_back / partial_rollback
step:      pending → running → success / failed / skipped / rolled_back / rollback_failed
```

回滚执行在 `job_steps` 记为同 step_key、`attempt_type='rollback'` 的新行，日志/审计天然齐全。

---

## 5. Inventory 与网络

- **Inventory 源 = CMDB**：下发时 bingops 从目标快照生成 `[{resource_id, name, ip, ssh_user, ssh_key_ref}]`；runner 拼 inventory JSON，资源选择器能力在此变现
- **目标范围硬校验**：runbook.`target_models` 声明 scope（默认 `[aliyun_ecs, gcp_compute]`），`create_execution` 对快照 model_code 越界即 400；前端选择器按 target_models 传 `model_id` 过滤（UX 层，不替代后端校验）；K8s 对象 P2 以 local 模式扩入
- **网络可达**：runner 部署于同 VPC 直连 22 端口；跨 VPC/IDC 留 runbook 级 `proxy_hop` 字段（P1 不实现，渲染为 ProxyCommand）
- **connection 契约**（runbook 级）：`ssh_user` 登录用户 / `ssh_key_ref` Vault 键名 / `become` 默认 false / `become_method` 默认 sudo / `become_user` 默认 root；**sudo 密码不进配置**：宿主机 NOPASSWD sudoers 由 bootstrap runbook 统刷，退路 `become_password_ref` 走 Vault+no_log；runner 渲染为 inventory 变量 `ansible_become*`

---

## 6. 步骤日志

- ansible-runner 结构化事件回调逐行上报（含 host + task 粒度），terraform（P2）stdout 逐行
- **脱敏在出 runner 机器前**：Vault 取值进 redact 列表，命中替换 `***`
- 保留 90 天 PG 表定期 purge，量大迁 OSS
- 审计落库：execution 记录 code_ref（tag）——可精确复现"当时跑的是哪份代码"

---

## 7. 与现有体系挂接

| 体系 | 挂接方式 | 阶段 |
|------|---------|------|
| CMDB | 目标选择器圈选；执行后 change_log(source='job')；terraform 新建资源由 cloud-syncer 自动发现闭环 | P1/P2 |
| 工单 | 高危 execution 挂 ticket_id，审批通过才下发 | P3 |
| 变更封禁 | change_freezes 窗口校验（执行前） | P3 |
| RBAC | 新增权限码 `runbook:*`、`job:list/get/create/cancel/rollback`，按权限码规范同步 schema.sql 种子 | P1 |
| 环境维度 | **待决策**：CMDB 加 `environment` 通用列；P1 暂仅按 runbook.risk_level 门控 | 待定 |

---

## 8. 分期

| 期 | 内容 | 验收 |
|----|------|------|
| P1 | runner 骨架 + Vault + ansible 步骤 + 日志 live tail + 灰度 + 手动回滚 + git clone | 「批量重启」runbook 端到端：圈选→执行→灰度→日志→失败手动回滚→change_log |
| P2 | terraform executor + http backend state（版本化=原生快照回滚）+ 自动回滚链 + OSS 制品层 + lint 门禁 | 「创建 RDS」失败自动逆序回滚；state 版本可追溯 |
| P3 | 工单审批 + 封禁窗口 + 环境维度提级 + 漂移检测（state vs cloud-syncer 对账） | 高危无审批不可执行 |

---

## 9. P1 详设

### 9.1 表 DDL（实施时落 `sql/migrations/vN_jobs.sql`）

```sql
-- ============================================================================
-- Runbook（任务模板）
-- ============================================================================
CREATE TABLE runbooks (
    id            BIGSERIAL PRIMARY KEY,
    name          VARCHAR(128) NOT NULL UNIQUE,
    category      VARCHAR(64),                      -- restart / deploy / data_ops ...
    description   TEXT,
    params_schema JSONB        NOT NULL DEFAULT '{}',   -- 用户入参动态表单
    steps         JSONB        NOT NULL DEFAULT '[]',   -- 有序步骤，契约见 §3
    connection    JSONB        NOT NULL DEFAULT '{}',   -- {ssh_user, ssh_key_ref, become, become_method, become_user}
    target_models JSONB        NOT NULL DEFAULT '["aliyun_ecs", "gcp_compute"]',
    version       INT          NOT NULL DEFAULT 1,      -- 编辑 +1，execution 快照
    risk_level    VARCHAR(16)  NOT NULL DEFAULT 'low',
    auto_rollback BOOLEAN      NOT NULL DEFAULT FALSE,
    is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
    created_by    BIGINT       REFERENCES users(id) ON DELETE SET NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- 任务执行实例
-- ============================================================================
CREATE TABLE job_executions (
    id               BIGSERIAL PRIMARY KEY,
    runbook_id       BIGINT       NOT NULL REFERENCES runbooks(id),
    runbook_version  INT          NOT NULL,             -- 创建时快照
    code_ref         VARCHAR(128) NOT NULL,             -- git tag 快照
    params           JSONB        NOT NULL DEFAULT '{}',
    target_resources JSONB        NOT NULL DEFAULT '[]',-- [{resource_id,name,ip,ssh_user,ssh_key_ref}]
    steps_snapshot   JSONB        NOT NULL DEFAULT '[]',-- 创建时步骤快照
    connection       JSONB        NOT NULL DEFAULT '{}',-- 连接配置快照（回滚下发同需）
    status           VARCHAR(32)  NOT NULL DEFAULT 'pending',
    rollback_policy  VARCHAR(16)  NOT NULL DEFAULT 'manual',
    ticket_id        BIGINT,                            -- P3 审批挂接
    triggered_by     BIGINT       NOT NULL REFERENCES users(id),
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_job_exec_status  ON job_executions (status);
CREATE INDEX idx_job_exec_runbook ON job_executions (runbook_id);

-- ============================================================================
-- 步骤执行记录（回滚=同 step_key 的 rollback 行）
-- ============================================================================
CREATE TABLE job_steps (
    id            BIGSERIAL PRIMARY KEY,
    execution_id  BIGINT      NOT NULL REFERENCES job_executions(id) ON DELETE CASCADE,
    step_key      VARCHAR(64) NOT NULL,
    step_name     VARCHAR(128),
    type          VARCHAR(16) NOT NULL DEFAULT 'ansible',  -- ansible | terraform(P2)
    attempt_type  VARCHAR(16) NOT NULL DEFAULT 'do',       -- do | rollback
    status        VARCHAR(32) NOT NULL DEFAULT 'pending',
    serial        VARCHAR(16),
    exit_code     INT,
    error_message TEXT,
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (execution_id, step_key, attempt_type)
);
CREATE INDEX idx_job_step_exec ON job_steps (execution_id);

-- ============================================================================
-- 步骤日志（90 天保留）
-- ============================================================================
CREATE TABLE job_step_logs (
    id        BIGSERIAL PRIMARY KEY,
    step_id   BIGINT      NOT NULL REFERENCES job_steps(id) ON DELETE CASCADE,
    seq       INT         NOT NULL,
    level     VARCHAR(16) NOT NULL DEFAULT 'info',
    host      VARCHAR(128),                          -- ansible 事件归属目标机
    line      TEXT        NOT NULL,
    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (step_id, seq)
);
CREATE INDEX idx_job_log_step ON job_step_logs (step_id, seq);
```

### 9.2 Kafka 契约

**job-dispatch**（bingops → runner；`command` 区分执行/回滚）：

```json
{
  "message_id": "uuid4",
  "command": "execute | rollback",
  "execution_id": 123,
  "code_ref": "v1.2.0",
  "params": {"svc": "order-soa"},
  "connection": {"ssh_user": "ops", "ssh_key_ref": "prod-node-key",
                 "become": false, "become_user": "root", "become_method": "sudo"},
  "targets": [{"resource_id": 1, "name": "web-1", "ip": "10.0.0.1",
               "region": "cn-guangzhou", "model_code": "aliyun_ecs"}],
  "steps": [{"key": "restart_app", "type": "ansible",
             "playbook": "ansible/playbooks/app_restart.yml",
             "timeout_sec": 600, "serial": "30%", "batch_pause_sec": 60,
             "rollbackable": true}],
  "rollback_of": null
}
```

凭据两级结构：消息级 `connection`（runbook 声明）打底，target 级同名字段非空可覆盖，runner 解析时合并；确需 sudo 密码时在 connection/target 带 `become_password_ref`（Vault 钥匙名）。契约校验失败（如缺 ssh_key_ref）runner 回流 `prepare` 失败事件而非静默丢弃。

回滚下发 = `command: "rollback"`，runner 对已完成步骤逆序重跑（extra_vars 注入 `bingops_action=undo`）。

**job-events**（runner → bingops）：

```json
{"message_id": "uuid4", "execution_id": 123, "step_key": "restart_app",
 "attempt_type": "do",
 "event_type": "step_started | log | step_finished | execution_finished",
 "seq": 17, "level": "info", "host": "web-1",
 "line": "TASK [restart] starting...",
 "status": "success | failed", "exit_code": 0, "error": null,
 "timestamp": "2026-08-25T10:00:00Z"}
```

### 9.3 runner 项目骨架（独立仓库 bingops-runner）

```
bingops-runner/
├── runner/
│   ├── core/            # config / logging / exceptions（照 bingops skills 规范）
│   ├── kafka/           # consumer(job-dispatch) + producer(job-events)
│   ├── vault_client.py  # AppRole 取钥，内存 TTL 缓存，不落盘（临时 keyfile 除外）
│   ├── inventory.py     # targets → inventory JSON + 临时 keyfile(0600, 用完即删)
│   ├── redact.py        # 出机前脱敏
│   ├── executors/
│   │   ├── ansible_executor.py   # ansible-runner 事件回调 → job-events
│   │   └── terraform_executor.py # P2 占位
│   └── main.py          # 并发信号量限流 / 优雅退出 / message_id 去重
├── deploy/              # Dockerfile（python + ansible + terraform binary），同 VPC 部署
└── .qoder/skills/       # 拷贝 bingops 4 个 skill + 新建 bingops-runbook-authoring（runbook 编写规范：do/undo 契约、redact、灰度声明）
```

运行时要点：单 runner 并发 execution 数用信号量限流；step timeout 强制 kill；at-least-once 重放靠 message_id 去重；优雅退出等待当前 step 结束再退。

### 9.4 API 端点（bingops，P1）

- `runbook` CRUD + 版本管理：`/api/v1/jobs/runbooks`
- 执行：`POST /api/v1/jobs/executions`（创建即快照）、`GET` 列表/详情、`POST .../cancel`、`POST .../rollback`
- 日志：`GET /api/v1/jobs/steps/{id}/logs?after_seq=`（SSE live tail）

---

## 10. 待决策项

| 项 | 说明 | 阻塞阶段 |
|----|------|---------|
| CMDB `environment` 通用列 | 审批提级/风险评级依赖；存量回填成本随时间增长 | P3（P1 不阻塞） |
| GitLab 自建与否 | 决定 P2 terraform state 是否可先用 GitLab 原生 backend 过渡 | P2 |
