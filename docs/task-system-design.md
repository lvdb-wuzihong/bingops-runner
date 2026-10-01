# BingOps 任务系统设计（Job 执行引擎）

> 状态：设计定稿， P1 实施中（2026-08-25）；**v26 作者面简化已落地**（2026-09-29，见 §3.1）
> 范围：**P1 = Ansible 刚需先行**；Terraform / state backend 挂起至 P2，不废弃
> 简化取向：作者面只保留 5 个必读概念，其余字段一律“有安全缺省值 + UI 不出现”（冻结项见 §0#9）
> 上游输入：`docs/ticket-system-gap-analysis.md`（Phase C 执行引擎）、`docs/cloud-sync-design.md`（架构同构参照）

## 0. 已锁定决策

| # | 决策 | 结论 |
|---|------|------|
| 1 | 执行面形态 | 独立项目 `bingops-runner`，Python + ansible-runner，与 bingops 控制面分离 |
| 2 | 代码存放 | GitLab 唯一事实源（tag 不可移动）；P1 runner `git clone --depth 1 --branch <tag>`；OSS 制品层 P2 |
| 3 | 入口自愈 | role/playbook 内 `block/rescue/always` 做步内瞬时失败处理（**v37：do/undo 契约不再是平台能力**，保留 undo 分支仅为将来可选复用） |
| 4 | 回滚编排层 | **已于 v37 整体下线**（见 #20）。曾为“引擎逆序重跑 undo（`attempt_type=rollback`）+ 一律手动”，但 runner 从未实现，属纸面契约 |
| 5 | Secret 管理 | HashiCorp Vault 唯一存放；下发消息只带钥匙名；runner AppRole 现场取钥 |
| 6 | Terraform state | **不存 Vault**（KB 级 secret 库 vs MB 级高频 blob，性质不同）；P2 用 bingops 实现 http backend + OSS blob |
| 7 | 日志脱敏 | runner 出机前 redact（Vault 取值加入掩码列表） |
| 8 | 灰度 | ~~step 级 `serial` + `batch_pause_sec`~~ **v30 已删除**：多目标并发度是**执行机的部署级配置**（runner `max_parallel_hosts`），不是每个任务的属性。单目标场景下这两个字段永远用不上，却占着表单两格 |
| 9 | **作者面简化**（v26） | 糖在 API 入口、归一在 service、**存储与 dispatch 契约零改动**；只减必填性与 UI 暴露面，**不删 DB 列与消息字段**（真删会伤runner 已实现的分步逻辑与历史行）；硬校验一条不放松 |
| 10 | **多执行器引擎**（v27） | 任务模块不是“ansible 引擎”而是**步骤编排引擎**：executor 注册表按 `exec_type` 分发，本轮落地 shell / python / ansible，terraform 只注册类型占位。**注：执行器抽象未变，但 v29 已把消息里的 steps 数组改为 step 对象（见 #13），runner 需同步重构** |
| 11 | **执行位置与目标解耦**（v27） | `run_on: target \| local`（v29 起是 runbook 的步骤列，缺省按 `exec_type` 推断）；**targets 与 `connection.ssh_key_ref` 的必填性均由它派生**——这是无主机 API 任务的根因 |
| 12 | **凭据三层分离**（v27） | `params_schema`（明文）/ `secrets_schema`（需走 Vault 的入参，只存钥匙名）/ `connection.ssh_key_ref`（目标机私钥）各自一个字段；不再靠 `*_ref` 后缀做隐式魔法（后缀约定降级为兼容路径，新建不用） |
| 13 | **扁平单步引擎**（v28→v29） | **一个 runbook = 一个步骤**，且 `steps` 概念彻底消失：API 入参、`runbooks.steps` 列、`job_executions.steps_snapshot`、dispatch 的 steps 数组全部删除，改为步骤列 + 单个 `step_snapshot` 对象 + 消息里的 `step` 对象。**属破坏性变更，runner 需按本文档同步重构**。你们仓库已证明该形态正确：整条流程写在一个 playbook 里，role 才是复杂度容身处 |
| 14 | **字段按“该问谁”分类**（v30） | 任务属性（跑什么/在哪跑）才进表单；基础设施属性（登录凭据/超时/版本/并发度）配一次即可；记账属性（key/name/run_on）全由后端生成。据此删掉 `undo_command`（回滚统一走 `BINGOPS_ACTION=undo` 约定）与 `serial`/`batch_pause_sec`（并发度下沉到 runner 配置）——**三个都是“每次都不填但每次都看得到”的噪声字段** |
| 15 | **凭据属于机器，不属于任务**（v31） | 不变式：**凭据的解析时机必须与“机器被确定的时机”一致**。机器在执行期选定，所以定义期无法决定用哪把钥匙——`connection.ssh_key_ref` 从必填降为兜底，新增 `credentials` 凭据目录，执行期逐台解析写入 `targets[]`。同时这也修掉了“一条 runbook 打 30 台只能共用一把钥匙”的表达空洞 |
| 16 | **登录身份归执行，不归任务也不归钥匙**（v33→v34） | 用户环境事实：**同一把钥匙常被授权给不同主机的不同用户（跨用户是常态）**。两次修正：v33 先删掉凭据上的 `login_user`（授权不属于钥匙材料）；v34 再撤掉主机标签 `ssh_user`/`ssh_credential` 的链路地位（预配标签不是用户要的交互），登录用户/钥匙/提权全部在**执行时填写**：`ExecutionCreate.ssh_user + ssh_credential + become`。凭据目录退回纯钥匙材料（同 `vault_path` 多条目 = 多身份），解析链：执行时填写 > `runbook.connection` 存量兜底 > 400 |
| 17 | **runbook 定义面零连接字段**（v34） | 截图里“兜底登录用户/兜底登录密钥”两个框的根因在后端契约（`RunbookCreate` 还在收 `connection`，`ExecutionCreate` 没有连接入口）。v34 撤掉创建面全部连接字段，`runbooks.connection` 列保留仅作存量兜底与提权存储；**runbook 只回答“连上之后干什么”，“怎么连、用谁连”属于执行** |
| 18 | **网关关联维度只留 VPC**（v35） | v32 的 `scope` 四维（vpc/账号/区域/资源 ID）+ `priority` 是“没拍板就把选择权外包给表单”。VPC 与跳板天然一对一，所以 `scope` → `vpc_ids` 单列、`priority` 删除，并加“一个 VPC 只能一条启用网关”写入校验（重复 409）。附带更正一个错报：曾结论“aliyun_ecs/gcp_compute 无 vpc_id”——那是拿 `cmdb-model-presets.md` 推断的，**真实库两个机型都有**（文档不是事实源） |
| 19 | **目标机与版本不得缓存在模板上**（v36） | 删 `runbooks.default_target_resource_ids` / `default_code_ref`。目标机是全文唯一“选错就是事故”的字段（目标锁/封禁/审批/审计的对象），预填把它从“必须确认”降级成“不假思索”；且会腐烂（CMDB 自增 ID、旧 tag）。省点击的正当需求改由前端「复用上次的」读 `job_executions` 快照实现。同时新增 `exec_type=script`：仓库内脚本必须走 ansible `script` 模块推送执行，旧文档“shell 写 `bash scripts/x.sh`”是错的 |
| 20 | **回滚能力整体下线**（v37） | 删 `runbooks.rollbackable`、`job_executions.rollback_policy`、`job_steps.attempt_type`、dispatch 的 `command` 字段、`POST /executions/{id}/rollback`、权限 `job:rollback`，以及 `rolling_back`/`rolled_back`/`partial_rollback`/`rollback_failed` 四个状态。**理由：runner 从未实现 undo，这是一整条纸面契约**；留着会让人以为“失败可以一键撤销”，而内联命令还会产生**假成功回滚**（重跑 `df -h` 正常退出→回流 rollback success，什么都没撤销）。失败就落 `failed`，由人看日志修 |

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

### 3.1 最小写法（照抄即可）

作者只需理解 **3 个概念**：叫什么（name）、怎么跑（exec_type + entry）、每次变什么（params_schema / secrets_schema）。其余字段全有安全缺省值，UI 不必出现。**“在哪跑”与“用谁连”都属于执行期**（v34 连接三件套、v36 目标机与版本），不在模板里。

**`exec_type` 决定 runner 用哪个 executor，`entry` 的语义随之变**（v29 扁平单步：一个 runbook = 一个步骤，`steps` 数组已从 API、表结构与消息三层删除）：

| exec_type | entry 写什么 | `run_on` 缺省 | 需要目标机与 ssh_key_ref？ |
|-----------|-------------|--------------|--------------------------|
| `ansible` | playbook 路径 | `target` | 是 |
| `shell` | **命令字符串**（跑仓库脚本就写 `bash scripts/x.sh`） | `target` | 是 |
| `python` | 仓库内脚本入口 | `local` | **否**（runner 本机执行） |
| `terraform` | 工作目录 | `local` | 否（门控未开，本轮不可创建） |

```yaml
name: magento2生产数据同步到测试环境
exec_type: ansible
entry: ansible/playbooks/mysql_data_sync_prod_to_test.yml
ssh_user: root
ssh_key_ref: povison_key_pair         # 第三层凭据：目标机 SSH 私钥（Vault 键名）
params_schema:
  tables: {type: string, description: "仅同步这些表，逗号分隔；空=全库"}
secrets_schema:                       # 第二层凭据：需走 Vault 的入参（v27 与 params 分开）
  DB_PASSWORD: {required: true, description: 生产库密码, default_ref: "magento2/prod/readonly#password"}
timeout_sec: 3600                     # 长耗时任务必须显式调大（缺省 600 会误杀）
```

纯 API 任务（无目标机、无 SSH）只换两行：

```yaml
name: 开通阿里云RAM子账号
exec_type: python                     # → run_on 自动为 local
entry: scripts/aliyun_create_ram_user.py
risk_level: high
params_schema:
  user_name:    {type: string, required: true, description: RAM 登录名}
  display_name: {type: string, required: true}
secrets_schema:
  ALICLOUD_ACCESS_KEY_ID:     {required: true, description: 用哪个云账号执行}
  ALICLOUD_ACCESS_KEY_SECRET: {required: true}
```

**执行时只填参数**（目标与版本走继承）：

```json
{"runbook_id": 1,
 "params": {"tables": "catalog_product_entity"},
 "secrets": {"DB_PASSWORD": "magento2/prod/readonly#password"}}
```

**落库与下发**（`job_service._build_step` / `step_of`）：

| 你写的 | 落到哪里 |
|--------|---------|
| `exec_type` / `entry` / `run_on` / `timeout_sec` | **runbooks 的 4 个步骤列**（v29 扁平化；v30 删 `undo_command`/`serial`/`batch_pause_sec`；**v37 删 `rollbackable`**）——**无数组、无 JSON 可拼** |
| `ssh_user` / `ssh_key_ref` / `become*` | v34 已从创建面撤除（`runbooks.connection` 列仅留作存量兜底与提权存储） |
| 缺省 `run_on` | 按 `exec_type` 推断后**显式落列**（下游不再各自推断） |
| 缺省 `timeout_sec` | 600 |
| `default_target_resource_ids` / `default_code_ref` | **v36 已删除**（目标机与版本属于每次执行） |

创建 execution 时，4 个步骤列被组装成**单个 `step_snapshot` 对象**快照（`step_of()`，含固定 `key="main"` 与 `name=任务名`），dispatch 按它走。

**执行期入参与回落链**（`create_execution`）：

- `target_resource_ids`：**必须显式传**（v36）——目标机是整个系统里唯一“选错就是事故”的字段（目标锁/封禁/审批/审计全以它为对象），预选项会把必须确认的一步变成不假思索；无 target 型步骤的任务可空
- `code_ref`：显式传 \< `BINGOPS_JOB_DEFAULT_CODE_REF`（默认空）\< 400 引导填 tag。**刻意不给“默认 main”**，也不在 runbook 上缓存默认版本：分支 HEAD 会移动、旧 tag 会静默生效
- “省一次点选”的正当需求由前端做「复用上次的目标机/版本」，数据源是 `job_executions` 快照——历史不会腐烂，且是真实发生过的事
- 目标机照走全部硬校验（`status=running`、`target_models` 白名单、并发目标锁）——简化的是填写量，不是安全边界
- 报错即文档：不满足步骤契约时，400 message 内嵌最小可用示例（`MINIMAL_RUNBOOK_HINT`）

**平台不提供回滚**（v37）：没有 `rollbackable` 字段、没有 `BINGOPS_ACTION=undo` 注入、没有 rollback 端点。失败就是 `failed` 终态，由人看日志修。曾以为“至少得知道哪些步骤可逆”而保留 `rollbackable`，但**一个只能靠人工维护、又无行为后果的布尔值就是噪声**；而内联命令还会产生假成功回滚（见 §0 #20）。

**多步编排已彻底移除**（v29）：你们自己的仓库已证明正确形态——整条流程写在一个 playbook 里（厚 role、薄 runbook），steps 数组没有真实使用场景。删除范围 = API 入参 + `runbooks.steps` 列 + `job_executions.steps_snapshot` + dispatch 消息的 `steps` 数组；**`job_steps` 表保留**（一步一行，v37 后连 `attempt_type` 也删了，日志与审计结构不变）。将来真要恢复多步，是“加一张步骤表”级别的演进，不是回到 JSON 数组。

#### 历史形态：v28 及更早的多步 YAML（已废弃，仅供读旧数据参考）

```yaml
# ══ Runbook 全字段 demo：magento2 生产数据同步到测试环境 ══
# UI 按 §3.3 用 YAML 编辑（js-yaml 转 JSON 提交）；字段即契约，注释可保留
name: magento2生产数据同步到测试环境
category: 数据同步
target_models:                            # 目标范围硬校验：执行清单只允许这些模型
  - aliyun_ecs
  - gcp_compute
risk_level: low                           # low/medium/high/critical，叠加环境提级（P3）
auto_rollback: false                      # 失败自动回滚（opt-in），默认手动
connection:                               # 消息级打底，target 级同名字段可覆盖；只存钥匙名
  ssh_user: root                          # 登录用户
  ssh_key_ref: povison_key_pair           # Vault 键名 → runner 临时注入
  become: false                           # 提权开关（默认 false）
  become_method: sudo                     # 可选，默认 sudo
  become_user: root                       # 可选，默认 root
  proxy_hop:                              # 跨 VPC 跳板（可选；同 VPC 直连就删掉这段）
    host: 10.0.0.5
    user: ops
    ssh_key_ref: bastion_key_pair
params_schema:                            # 执行时填的入参；后端校验 required/type/enum 并回填 default
  src_host:  {type: string, required: true, description: 生产库地址}
  src_db:    {type: string, required: true, description: 生产库名}
  dest_host: {type: string, required: true, description: 测试库地址}
  dest_db:   {type: string, required: true, description: 测试库名}
  tables:    {type: string, description: 仅同步这些表，逗号分隔；空=全库}
  wipe_dest: {type: boolean, default: false, description: 导入前清空测试库}
  src_db_password_ref: {type: string, required: true, description: 生产库密码的 Vault 键名}
steps:
  - key: backup_dest                      # ① 先备份目标库 → 回滚的材料
    name: 备份目标库
    type: ansible                         # P1 仅 ansible；terraform 占位 P2 点亮
    playbook: ansible/playbooks/mysql_backup_dest.yml
    timeout_sec: 1800
    rollbackable: true                    # undo = 重导备份文件（do/undo 契约见 §3.2）
  - key: sync_data                        # ② 同步数据；数据覆盖不可逆
    name: dump 生产并导入测试
    type: ansible
    playbook: ansible/playbooks/mysql_data_sync_prod_to_test.yml
    timeout_sec: 3600
    rollbackable: false                   # 不可逆步：整链回滚 = ① 的 undo 恢复备份
```

**执行时填的 params**（新增执行弹窗；`*_ref` 只传钥匙名，绝不传明文）：

```json
{"src_host": "10.0.1.10", "src_db": "magento2_prod",
 "dest_host": "10.0.2.10", "dest_db": "magento2_test",
 "tables": "", "wipe_dest": true,
 "src_db_password_ref": "magento2-prod-db"}
```

**playbook 怎么接参数**（params 键原样成为 extra_vars；`_ref` 已被 runner 换成环境变量）：

```yaml
# ansible/playbooks/mysql_data_sync_prod_to_test.yml
- hosts: all                              # hosts = 执行时圈选的目标机（操作机语义）
  tasks:
    - name: dump 生产库
      community.mysql.mysql_db:
        name: "{{ src_db }}"
        state: dump
        target: /tmp/m2_dump.sql
        login_host: "{{ src_host }}"
        login_password: "{{ lookup('env', 'SRC_DB_PASSWORD') }}"   # src_db_password_ref → env
      no_log: true                        # 引用敏感值的 task 必须加
    - name: 导入测试库
      community.mysql.mysql_db:
        name: "{{ dest_db }}"
        state: import
        target: /tmp/m2_dump.sql
```

**字段速查**：
- ~~`serial` / `batch_pause_sec`~~：v30 已删除（多目标并发度改为 runner 部署级配置）
- `_ref` 后缀参数 → runner 注入 env：`src_db_password_ref` 剥后缀转大写 = `SRC_DB_PASSWORD`
- `proxy_hop` 渲染细节与坑（ProxyCommand 显式 `-i`）见 §5
- 敏感值双保险：task `no_log: true` + runner redact 兜底

### 3.2 入口脚本的自愈与 undo（作者层，v37 后仅为可选能力）

**平台不提供回滚**（v37），因此不会注入 `BINGOPS_ACTION=undo`、也不会重跑入口去“撤销”。但入口内部的自愈仍然鼓励：

- **步内瞬时失败靠脚本自己**：ansible 用原生 `block/rescue/always`（如起服失败先尝试拉起）；shell/python 脚本用退出码 + 内部重试。这类自愈在**一次执行内**完成，不依赖平台记账，因此与回滚能力无关
- 入口里保留 undo 分支（`undo.yml` / `if __name__ == "__main__" and os.getenv("BINGOPS_ACTION") == "undo"`）**无害**，将来若重建撤销能力可直接复用；但现在**没有任何调用方**，不要把它当作安全网写运维流程
- 失败的正确应对：看执行详情页的日志 → 人工修复 → 需要重跑就再发起一次执行（目标锁会卡住并发重复）

### 3.3 编辑面约定：YAML 对人、JSON 对机器

- **存储与 API 契约仅 JSON**（params_schema/secrets_schema/connection/步骤列为 JSONB/标量，API 收 dict）；后端不解析 YAML，单一文法、单一校验入口（`_build_step`）
- **前端编辑面用 YAML**（Monaco + js-yaml@4，YAML 1.2 core schema，避 1.1 `yes/on` 布尔坑）：提交时 `yaml.load` → 校验 → JSON 调现有 API；编辑回显 `yaml.dump`（保插入序，往返无 diff 噪音）
- 标量配置（name/category/risk_level/target_models）用**结构化表单**；v34 后 runbook 不再持有连接信息，UI 上已无 connection 编辑区（`runbooks.connection` 列仅存量兜底）
- **v29：单步 runbook 只填 `exec_type` + `entry`**（见 §3.1）——两个必填项，无互斥字段、无数组；**后端不存两套语法**，编辑回显直读步骤列
- YAML 校验仅为 UX 即时反馈；权威仍是后端 400
- 与未来 GitLab runbook-as-code 演进同构：仓库与 UI 共用 YAML 语法，CI 转 JSON 同步进平台
- ~~**CI 门禁（P2）**：标 `rollbackable: true` 的 role 必须引用 `bingops_action`~~——**v37 随回滚能力下线**（字段已删，门禁无对象可校）

### 3.4 版本语义

- `runbooks.version` 整数，每次编辑 +1
- 任务创建时 **runbook_version + steps + code_ref（git tag）三快照** 进 execution 行——在跑任务永远用创建时的定义与代码
- code_ref 回落链（v36）：**显式传 > 平台配置 `BINGOPS_JOB_DEFAULT_CODE_REF` > 400**。`runbook.default_code_ref` 已删——在模板上缓存版本会把“选错旧 tag”变成静默默认；前端用「复用上次的版本」从执行历史带入。回落只影响**创建时填什么**，不影响快照：解析后的实际值照旧写入 execution 行
- **但只有依赖仓库代码的类型才必填**（v38）：`ansible` / `script` / `python` / `terraform` 的入口是仓库里的文件，必须固定版本；`shell` 的 entry 是内联命令，runner 不 clone 任何代码，code_ref 留空即合法（消息里为空串，runner 跳过 clone）。判据在 `job_service.requires_code_ref(exec_type)`，执行创建与工单下发共用——v36 曾对所有类型一刀切必填，“跑一句 df -h”会被 400 卡住

### 3.5 步骤类型契约（v27 多执行器）

表驱动校验（`job_service._build_step` + `EXEC_TYPE_RUN_ON`）：`exec_type` 决定默认执行位置，`entry` 的语义随类型变。**v29 扁平单步**：步骤直接存 `runbooks` 的 4 个列，不再有 JSONB 步骤数组。

| exec_type | entry 语义 | `run_on` 缺省 | 需要 targets | 参数注入 |
|-----------|-----------|--------------|-------------|---------|
| `ansible` | playbook 路径 | `target` | 是 | extra_vars(params) + env(secrets) |
| `shell` | **内联命令**（`df -h`、`systemctl restart nginx`） | `target`（可写 `local`） | `run_on=target` 时是 | env（params+secrets） |
| **`script`（v36 新增）** | **仓库内脚本文件路径**（`scripts/dump_prod.sh`） | `target`（可写 `local`） | `run_on=target` 时是 | env + argv |
| `python` | 仓库内脚本入口 | `local` | 否 | env + argv |
| `terraform` | 工作目录 | `local` | 否 | `-var` / tfvars；**本轮拒绝创建/执行**（门控未开，state 方案未定） |

其余步骤列：`timeout_sec`（600）。`_build_step` 把推断后的 `run_on` **显式落列**，下游不再各自推断。**v37：原「回滚入口」列已整列删除**——平台不触发 undo。

> **v36 为何新增 `script` 类型**：旧文档里写着“跑仓库脚本就写 `bash scripts/x.sh`”——**那是错的**：`shell` 的 entry 在**目标机的 shell** 里执行，那个路径相对目标机文件系统，而脚本只存在于 runbook git 仓库（只 clone 到 runner），结果必为 `No such file or directory`。Ansible 对这个场景有专门原语：**`script` 模块**（把控制机本地脚本推到目标机临时目录执行并回传 stdout，目标机不需预置文件）。拆成两个显式类型而不是“看 entry 像不像路径”自己猜，与你们否掉 `*_ref` 后缀魔法是同一条纪律。

> **shell 与 script 为何不合并**：分界不是“命令还是文件”，而是**“这段代码归谁管”**。`shell` 的内容在平台（内联 entry）——无版本、无评审、无 undo，它的正当场景就是 **ad-hoc 立即执行**（选机器 + 写命令 + 跑，不进仓库）；`script` 的内容在 git——随 code_ref 固定、可评审、可自带 undo。
> 只留 `script`：跑一句 `df -h` 也要先建文件、提 MR、打 tag；只留 `shell`：关键逻辑散落在平台快照里，三个月后没人知道它改过几版。选择规则：**要留痕/复用/回滚 → `script`；只是查一下、清一下 → `shell`；逻辑超过一个文件 → `ansible`**。

**v30 删掉的三个字段**（均为基础设施/噪声属性，不再由任务持有）：`undo_command`（回滚统一走 undo 约定）、`serial` 与 `batch_pause_sec`（多目标并发度 = runner 部署级配置 `max_parallel_hosts`）。

**派生与门控**：

- `needs_targets = (runbook.run_on == "target")` → 决定 `target_resource_ids` 是否必填（v36：目标机**必须每次显式传**，不再从 runbook 继承默认绑定）与是否要解析登录用户/凭据；`step_key` 恒为 `main`（`job_steps` 一行 + 回滚一行）
- 无目标任务**不绕过变更封禁**：`_freeze_hits_models` 中 `scope` 为空即全局命中，空 `model_codes` 照样拦（已核）
- 类型白名单 `BINGOPS_JOB_STEP_TYPES`（默认 `ansible,shell,script,python`）：runner 尚未支持某 executor 时收紧配置，平台侧即拒绝创建，而不是下发后失败

### 3.6 凭据三层分离（v27）

三层各走一个字段，**不再有“看后缀才知道是密钥”的隐式规则**：

| 层 | runbook 声明 | 执行时填 | 落库 | 值的性质 |
|----|-------------|---------|------|---------|
| 明文入参 | `params_schema` | `params` | `job_executions.params` | 普通值（库名、表名、开关） |
| Vault 入参 | `secrets_schema`：`{VAR: {required, description, default_ref}}` | `secrets: {VAR: "<Vault 路径#字段>"}` | `job_executions.secrets` | **只存钥匙名**，明文永不入库 |
| 目标机私钥 | —（v34 起不属于 runbook） | `ssh_credential`（凭据目录条目名） | `job_executions.connection` 快照 | 只存钥匙名，真值由 runner 取 Vault |

> **v31→v36 变迁**：目标机私钥先是从「任务属性」 搬为「机器属性」（v31 凭据目录 + 主机标签），再由 v34 抬到**执行面**（`ExecutionCreate.ssh_credential`）；`connection.ssh_key_ref` 降为存量兜底。v36 另把凭据目录的入口合并为单个 `vault_ref` 串（见 §5.1）。

规则：

- `secrets` 的变量名**必须落在 runbook 声明集内**，未声明即 400——防执行者注入任意变量名去拉未预期密钥；“能读哪些密钥路径”的最终判断交给 runner AppRole 的 Vault policy，平台不重复实现路径白名单（单一权限事实源）
- `default_ref` 命中时执行可不填（与 params 的 `default` 回填同构）；值必须是非空字符串，否则 400
- runner 在 executor **之前**统一解析 secrets → 注入**同名环境变量**（`DB_PASSWORD` → env `DB_PASSWORD`）→ 明文加入 redact 列表；四种 executor 共用同一解析器，脚本/playbook 侧与 Vault 零耦合
- 存量兼容：`params` 里 `*_ref` 后缀的旧约定 runner 继续按旧规则解析（剥后缀转大写 env），**不迁移、不破坏**；新建 runbook 一律用 `secrets_schema`
- **v32：`secrets` 的值可以直填 `credentials.name`**——条目可选声明 `kind`（如 `db_password`），此时强制走目录解析并校验类型匹配（拼错名字、拿错类型都在创建执行时 400）；未声明 `kind` 时目录命中就用，否则当裸 Vault 路径透传（存量行为不变）。平台只把名字展开成路径，仍不读 Vault
- ⚠ **工单自动下发路径不带 secrets**：`ticket_service` 构造 `ExecutionCreate` 只传 `params`（工单无密钥填写环节），因此**经由工单执行的 runbook，其 `secrets_schema` 每一条必须预置 `default_ref`**，否则执行报 `missing required secret`。要支持工单选人时填密钥，需给 `tickets.job_params` 加 `secrets` 键（本轮不做）

---

## 4. 执行流程与状态机

### 4.1 端到端流程

1. UI 圈选目标（CMDB 选择器，可按 VPC 筛）→ 创建 `job_executions`（params/targets/version 快照）；**v36：目标机必须每次显式选择**（不再从 runbook 继承默认绑定）
2. 并发校验：target_resource_ids 与在跑 execution 交集命中即拒绝（同资源单执行锁）
3. 审批（P3）：risk_level + 环境维度 → 挂 ticket，通过才下发
4. bingops 发 `job-dispatch`（**只带 ssh_key_ref 钥匙名，不带 secret**）
5. runner 消费 → Vault 取钥（临时文件 0600，用完即删）→ 拼 ad-hoc inventory → 逐步执行
6. runner 流式发 `job-events`：`step_started → log(seq 递增) → step_finished`
7. bingops 消费落库 → 前端 SSE live tail
8. 失败 → execution 落 `failed` 终态（**v37：平台不做回滚**，由人看日志修复后重新发起执行）
9. 终态 → CMDB `change_log`（source='job'）

### 4.2 状态机

```
execution: pending → awaiting_approval(P3) → running → success / failed / cancelled
step:      pending → running → success / failed / skipped
```

**v37 状态机收敛**：`rolling_back` / `rolled_back` / `partial_rollback` / `rollback_failed` 四个回滚态随能力一并下线；`job_steps` 一步一行（不再有 `attempt_type='rollback'` 的第二行）。

终态纪律（控面实现）：
- **终态以 runner 的 `execution_finished` 事件为准**：每次 dispatch 处理结束 runner 必发该事件，控面收到即落终态，不得自行推断或无限等待
- 步骤 `step_finished(failed)` 也**直接落 execution failed**（不等 runner 的 finished 事件），避免单步任务因 runner 异常退出而永久卡在 running
- `cancelled` 只能由用户通过 cancel API 产生（仅 pending/running 可取消，**取消不下发给 runner**——它只是控制面状态，runner 侧的真停依赖 runner 自身超时/信号机制）

---

## 5. Inventory 与网络

- **Inventory 源 = CMDB**：下发时 bingops 从目标快照生成 `[{resource_id, name, ip, gateway}]`——**v34 起目标快照是纯坐标 + 中转路径**，登录用户与凭据引用由执行面解析后统一回填进 `targets[]`（同一执行内同值，runner 优先读 target 上的值）；**v27：全 local 任务不建 inventory**（targets 为空，不取 SSH 私钥）
- **目标范围硬校验**：runbook.`target_models` 声明 scope（默认 `[aliyun_ecs, gcp_compute]`），`create_execution` 对快照 model_code 越界即 400；前端选择器按 target_models 传 `model_id` 过滤（UX 层，不替代后端校验）；K8s 对象 P2 以 local 模式扩入
- **执行态硬校验**：目标 `status` 必须 = `running`（stopped SSH 必失败、maintenance 变更中；unknown/NULL 按 fail-safe 从严 400，报错带资源名+实际状态）；前端选择器同步传 `status=running`
- **网络可达 / 跳板**：**v32 起由 `job_gateways` 表 + 自动选路接管，不再让 runbook 写 `proxy_hop`**（漏声明的后果是 SSH 超时，现象像 playbook 写错），渲染细节与 `-i` 坑见 §5.2 与 §9.3 第 7 条；`connection.proxy_hop` 保留为历史字段，新配置不再读它
- **connection 契约**（v34 起为执行期快照）：dispatch 的 `connection` = 存量 `runbook.connection` 兜底 + 本次执行解析出的 `ssh_user`/`ssh_key_ref`/`become`，runner 只读这一份不再二次猜测；`become_method` 默认 sudo / `become_user` 默认 root；**sudo 密码不进配置**：宿主机 NOPASSWD sudoers 由 bootstrap runbook 统刷，退路 `become_password_ref` 走 Vault+no_log；runner 渲染为 inventory 变量 `ansible_become*`

### 5.1 凭据解析（v31 凭据目录 → v34 执行面提供）

**凭据目录 `credentials`**：把“哪把钥匙、在哪取”收敛成可下拉选择的实体。只存 Vault 引用与元数据，**明文禁入**（入口有 `-----BEGIN` / `PRIVATE KEY` 等特征串拦截）；`name` 全局唯一；`kind ∈ ssh_key|cloud_ak|db_password|api_token|kubeconfig`。**目录的单位是钥匙材料，不含用户**——同一把钥匙授权给不同主机的不同用户是常态（跨用户），那属于每次执行的决定。

**入口只有一个 Vault 引用框**（v36）：`vault_ref` 接受运维在 Vault 侧熟悉的形状 `path` 或 `path#field`，服务层 `split_vault_ref()` 拆成两列存储、回显时再合成单串——前端不需要知道拆过。早期做成 `vault_path` + `vault_field` 两个输入框，反馈是“不知道哪个才是 Vault”。

**v36 删掉的三个字段**：`cloud_account` / `region`（适用范围）与 `is_default`（默认凭据）——它们只服务于“按机器自动解析凭据”那条链，而该链已在 v34 被“执行时人选”取代。**没有消费方的字段就是噪声**：表单上那两栏从此没有任何行为后果，却要求用户理解一个不存在的机制。

**解析优先级**（`credential_service.resolve_execution_credentials`，创建执行时一次性解析，无主机标签参与）：

```text
登录身份：ExecutionCreate.ssh_user（执行时填写）
          > runbook.connection.ssh_user（存量兜底）
          > 都没有：400，直接说清缺什么

钥匙材料：ExecutionCreate.ssh_credential（凭据目录条目名，后端展开成 Vault 引用）
          > runbook.connection.ssh_key_ref（存量兜底）
          > 都没有：400
```

两条硬规则：

- **目录条目名是强校验的**：执行时选的 `ssh_credential` 必须在目录里且 `kind=ssh_key`，打错当场 400——这正是目录存在的意义：不存在的选项在表单上就选不出来
- **不设平台级默认用户**：默认 `root` 这种约定会把漏配置变成高危行为；缺了就在执行弹窗里填，报错说人话

**为什么不搞 Vault 路径白名单**：能读哪些路径由 runner AppRole 的 Vault policy 决定（单一权限事实源），bingops 不重复实现一套权限。`verify_state` / `last_verified_at` 由 **runner 回填**，bingops 全程不连 Vault（方案 C：保住“Vault 唯一出口在 runner”这条纪律）。

**引用反查**（`GET /api/v1/credentials/{id}/usage`）：返回被多少 runbook 引用（v34 起主机标签不再是执行链路一环，仅作历史参考）。这是密钥轮换前的必看信息；有引用时**不允许删除**，只能停用（`is_active=false`）。

### 5.2 中转网关与选路（v32 四维 → v35 收敛为只按 VPC）

机器要怎么才被连到（直连还是经哪个 bastion）是**网络拓扑事实**，不是任务属性。让每个 runbook 写 `proxy_hop` 的后果是：漏写 → SSH 超时 → 现象像 playbook 写错。现在由机器所属 VPC 在执行期算出。

**`job_gateways`**：`name` / `host` / `port` / `login_user` / `ssh_credential` / **`vpc_ids`** / `remark`。关键三点：

- **关联维度只有 VPC 一个**：VPC 之间默认不通、同一 VPC 内的机器走同一个跳板，这是云网络的天然形状。库里 `aliyun_ecs.fields` 带 `vpc_id`+`vswitch_id`、`gcp_compute.fields` 带 `vpc_id`（已探测确认），机器选择器可直接用 `GET /cmdb/resources?model_id=…&field_key=vpc_id&field_value=vpc-2ze…` 筛
- **一个 VPC 只允许一条启用网关接管**（写入校验，重复即 409 指名是哪条）：多网关声明同一 VPC 时“谁生效”取决于遍历顺序，而 runner 并未实现跳板故障转移——这种多义没有正当用途，所以 `priority` 一并删除
- **`ssh_credential` 引用 `credentials.name`**（不是裸 Vault 路径）——轮换时能反查“哪些网关还在用这把钥匙”，且写入时校验存在与 kind 匹配

> **v32 为何做错了**：当时“网关按什么维度关联机器”没拍板，就把 vpc/账号/区域/资源 ID 四个维度全塞进 `scope` 让运维自己填——那是**把未决策外包给表单**：四个框里多数永远不填，填了还要记“任一命中 / 空不接管 / priority 升序”三条规则。v35 收敛后表单只剩一个 VPC 多选。
>
> 刻意不做“空 vpc_ids = 全局兜底网关”：一个误配条目就会接管全部流量，那种故障比连不上更难查。

**选路结果写入 dispatch 的 `targets[].gateway`**（`None` = 直连），**执行面可用 `gateway_name` 强制指定**（全员走该网关，未填自动选路）：

```json
"gateway": {"name": "gw-nocid", "host": "10.0.0.1", "port": 22,
            "ssh_user": "ops", "ssh_key_ref": "ssh/keys/bastion"}
```

> **连通性实测不做预检页**：v32 曾提供 reachability 视图（凭据齐不齐/缺什么），其前提是“凭据预配在主机标签上”；v34 凭据改为执行时提供后该前提不成立，视图已删除。机器能不能连通，由执行本身回答——失败事件会回流到执行记录，不需要单独的看板。批量 ping 实测待 ad-hoc 执行入口。
>
> **不做第二套 jumpserver**：本模块只回答“怎么连到”与“用哪把钥匙”；“谁能登录哪台机器”的授权继续留在 RBAC 与工单，否则必然与现有权限体系打架。

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
| 工单 | 高危 execution 挂 ticket_id，审批通过才下发；v27 注：工单下发不带 secrets，相关 runbook 需预置 `default_ref`（见 §3.6） | P3 |
| 变更封禁 | change_freezes 窗口校验（执行前） | P3 |
| RBAC | 新增权限码 `runbook:*`、`job:list/get/create/cancel/rollback`，按权限码规范同步 schema.sql 种子 | P1 |
| 环境维度 | **已决策**：不加 `environment` 列，复用标签体系（云资源 `env` 标签 manual→cloud 优先级，K8s 读 `k8s:env`），收敛为共享 `resolve_resource_env` helper，门控与展示同源；解析不到按 fail-safe 默认值（待定向：从严视为 production） | P3 |

---

## 8. 分期

| 期 | 内容 | 验收 |
|----|------|------|
| P1 | runner 骨架 + Vault + ansible 步骤 + 日志 live tail + git clone（~~灰度~~ v30 下沉为 runner 配置；~~手动回滚~~ v37 下线） | 「批量重启」runbook 端到端：圈选→执行→日志→失败可见（failed + 日志）→change_log |
| **P1.5（v27）** | 多执行器引擎：executor 注册表 + shell/python + 凭据三层分离 + targets/run_on 可选（terraform 仅占位） | 「开通 RAM 子账号」python 无目标任务端到端：只填 params+secrets → 执行 → 看日志 → 手动回滚 |
| P2 | terraform executor + http backend state（版本化=原生快照回滚）+ OSS 制品层 + lint 门禁 | 「创建 RDS」失败时可用 state 版本回退（terraform 天然具备，不需平台回滚引擎）；state 版本可追溯。**平台级「撤销」能力若真要建，需先回答“撤销什么、谁审批、失败如何可见”再重新设计（v37 已拆掉旧回滚链）** |
| P3 | 工单审批 + 封禁窗口 + 环境维度提级 + 漂移检测（state vs cloud-syncer 对账） | 高危无审批不可执行 |

---

## 9. P1 详设

### 9.1 表 DDL（已落 `sql/schema.sql`；增量为 v26~v31 迁移）

```sql
-- ============================================================================
-- 凭据目录（v31：只存 Vault 引用与元数据，明文禁入）
-- ============================================================================
CREATE TABLE credentials (
    id               BIGSERIAL PRIMARY KEY,
    name             VARCHAR(128) NOT NULL UNIQUE,  -- 引用键（主机标签填这个）
    kind             VARCHAR(32)  NOT NULL,         -- ssh_key|cloud_ak|db_password|api_token|kubeconfig
    -- v33 已删除 login_user：跨用户是常态，登录身份归主机标签 ssh_user
    vault_path       VARCHAR(512) NOT NULL,         -- 只存路径，绝不存值
    vault_field      VARCHAR(128),                  -- path#field 拆分后的字段名（入口为单串 vault_ref）
    -- v36 已删除 cloud_account / region / is_default：无消费方的字段（自动解析链已废）
    verify_state     VARCHAR(16)  NOT NULL DEFAULT 'unknown',  -- runner 回填
    last_verified_at TIMESTAMPTZ,
    remark           TEXT,
    is_active        BOOLEAN      NOT NULL DEFAULT TRUE,
    created_by       BIGINT       REFERENCES users(id) ON DELETE SET NULL,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE job_gateways (
    id               BIGSERIAL PRIMARY KEY,
    name             VARCHAR(128) NOT NULL UNIQUE,
    host             VARCHAR(128) NOT NULL,
    port             INT          NOT NULL DEFAULT 22,
    login_user       VARCHAR(64)  NOT NULL DEFAULT 'root',
    ssh_credential   VARCHAR(128),                     -- 引用 credentials.name（非裸路径）
    vpc_ids          JSONB        NOT NULL DEFAULT '[]',  -- 接管的 VPC（v35 唯一关联维度）
    remark           TEXT,
    is_active        BOOLEAN      NOT NULL DEFAULT TRUE,
    created_by       BIGINT       REFERENCES users(id) ON DELETE SET NULL,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_job_gateway_active ON job_gateways (is_active);

-- ============================================================================
-- Runbook（任务模板）
-- ============================================================================
CREATE TABLE runbooks (
    id            BIGSERIAL PRIMARY KEY,
    name          VARCHAR(128) NOT NULL UNIQUE,
    category      VARCHAR(64),                      -- restart / deploy / data_ops ...
    description   TEXT,
    params_schema JSONB        NOT NULL DEFAULT '{}',   -- 用户入参动态表单（条目 spec：type/required/default/enum/description，校验时回填 default）
    secrets_schema JSONB       NOT NULL DEFAULT '{}',   -- 需走 Vault 的入参声明 {VAR: {required, description, default_ref}}（v27 凭据三层）
    -- 唯一步骤（v29 扁平化：一个 runbook = 一个步骤，steps 数组列已删）
    exec_type        VARCHAR(16)  NOT NULL DEFAULT 'ansible',  -- ansible|shell|python|terraform
    entry            TEXT         NOT NULL DEFAULT '',         -- playbook/脚本路径、tf 目录；shell 为命令字符串
    run_on           VARCHAR(16)  NOT NULL DEFAULT 'target',   -- target=SSH 目标机 | local=runner 本机
    timeout_sec      INT          NOT NULL DEFAULT 600,
    -- v37 已删除 rollbackable：平台不提供回滚
    -- v30 已删除 undo_command / serial / batch_pause_sec（回滚统一约定 + 并发度下沉 runner）
    connection    JSONB        NOT NULL DEFAULT '{}',   -- {ssh_user, ssh_key_ref, become, become_method, become_user}；v31 起 ssh_key_ref 仅兜底
    target_models JSONB        NOT NULL DEFAULT '["aliyun_ecs", "gcp_compute"]',
    -- v36 已删除 default_target_resource_ids / default_code_ref：目标机与版本属于每次执行
    version       INT          NOT NULL DEFAULT 1,      -- 编辑 +1，execution 快照
    risk_level    VARCHAR(16)  NOT NULL DEFAULT 'low',
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
    secrets          JSONB        NOT NULL DEFAULT '{}',   -- {变量名: Vault 钥匙名}，明文永不入库（v27）
    target_resources JSONB        NOT NULL DEFAULT '[]',-- [{resource_id,name,ip,ssh_user,ssh_key_ref}]
    step_snapshot    JSONB        NOT NULL DEFAULT '{}',    -- 创建时快照的唯一步骤对象（v29）
    connection       JSONB        NOT NULL DEFAULT '{}',-- 连接配置快照（回滚下发同需）
    status           VARCHAR(32)  NOT NULL DEFAULT 'pending',
    -- v37 已删除 rollback_policy（manual|auto）：无回滚则无策略之分
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
    -- v37 已删除 attempt_type（do|rollback）：一步一行
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

**job-dispatch**（bingops → runner；`command` 区分执行/回滚，v29 为单 `step` 对象）：

```json
{
  "message_id": "uuid4",
  "execution_id": 123,
  "code_ref": "v1.2.0",
  "params": {"svc": "order-soa"},
  "connection": {"ssh_user": "ops", "ssh_key_ref": "ssh/keys/ops-vpc-a#private_key",
                 "become": false, "become_user": "root", "become_method": "sudo"},
  "targets": [{"resource_id": 1, "name": "web-1", "ip": "10.0.0.1",
               "region": "cn-guangzhou", "model_code": "aliyun_ecs",
               "ssh_user": "ops", "ssh_key_ref": "ssh/keys/ops-vpc-a#private_key",
               "gateway": null}],
  "step": {"key": "main", "name": "批量重启服务", "type": "ansible", "run_on": "target",
           "entry": "ansible/playbooks/app_restart.yml", "timeout_sec": 600}
}
```

凭据两级结构（v34）：`connection` 是执行期快照（存量兜底 + 本次执行解析出的 `ssh_user`/`ssh_key_ref`/`become`），`targets[]` 逐项回填同样的 `ssh_user`/`ssh_key_ref` + 各自的 `gateway`，**target 优先级高于 connection**。确需 sudo 密码时在 connection 带 `become_password_ref`（Vault 钥匙名）。契约校验失败 runner 回流 `prepare` 失败事件而非静默丢弃；bingops 侧已在创建执行时提前 400（缺登录用户/凭据目录无此条目），不会下发无法解析的目标。

**无主机任务示例（v27 python 型）**：`targets` 为空、`connection` 为空，密钥走 `secrets`：

```json
{
  "message_id": "uuid4", "command": "execute", "execution_id": 124, "code_ref": "v0.1.0",
  "params": {"user_name": "zhangsan", "display_name": "张三"},
  "secrets": {"ALICLOUD_ACCESS_KEY_ID": "aliyun#prod-ops-ak",
               "ALICLOUD_ACCESS_KEY_SECRET": "aliyun#prod-ops-sk"},
  "connection": {},
  "targets": [],
  "step": {"key": "main", "name": "开通阿里云RAM子账号", "type": "python", "run_on": "local",
           "entry": "scripts/aliyun_create_ram_user.py",
           "timeout_sec": 120, "rollbackable": true}
}
```

**消息形态变更（v29 破坏性 + v37 再收）**：`steps` 数组 → `step` 单对象；入口字段统一叫 `entry`（语义由 `type` 决定），新增 `run_on`；step 内不再有 `args`/`playbook`/`serial`/`undo_command` 等分叉字段。**v37：`command` 字段与 `step.rollbackable` 已删除**——取消（cancel）不下发，消息只剩“执行”一种语义。`execution_id` 仍是双方唯一关联键，事件流（job-events）除 `attempt_type` 已删外结构不变。

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
│   ├── inventory.py     # targets → inventory JSON + 临时 keyfile(0600, 用完即删)；无 targets 则不建
│   ├── secrets_resolver.py  # v27：统一解析 secrets（所有 executor 前置）
│   ├── redact.py        # 出机前脱敏
│   ├── executors/       # v27：注册表型，type → handler，统一接口 run(step, ctx)
│   │   ├── registry.py             # EXECUTORS = {"ansible":..., "shell":..., "script":..., "python":...}
│   │   ├── ansible_executor.py    # ansible-runner 事件回调 → job-events
│   │   ├── shell_executor.py      # run_on=target 复用 ansible ad-hoc；local 走 subprocess
│   │   ├── python_executor.py     # subprocess + requirements（镜像内置）
│   │   └── terraform_executor.py # 本轮占位（state 方案未定）
│   └── main.py          # 并发信号量限流 / 优雅退出 / message_id 去重
├── deploy/              # Dockerfile（python + ansible + terraform binary），同 VPC 部署
└── .qoder/skills/       # 拷贝 bingops 4 个 skill + 新建 bingops-runbook-authoring（runbook 编写规范：do/undo 契约、redact、灰度声明）
```

运行时要点：单 runner 并发 execution 数用信号量限流；step timeout 强制 kill；at-least-once 重放靠 message_id 去重；优雅退出等待当前 step 结束再退。

#### runner 侧实现清单（v27 多执行器 + v29 扁平单步）

1. **消息形态（v29 破坏性变更）**：`steps` 数组 → `step` 单对象；入口字段统一叫 `entry`，语义按 `type` 分叉（shell 恒为命令字符串）
   - **v38：`code_ref` 为空串时跳过 `git clone`**（内联 shell 命令无代码依赖），只有 `ansible/script/python/terraform` 才需要拉仓库；拉不到代码时回流 `prepare` 失败事件，不能静默当“没代码”跑
2. **executor 注册表**：`EXECUTORS: dict[type, Executor]`，统一接口 `run(step, ctx) -> (status, exit_code)`；**未知 type 回流 `prepare` 失败事件**，绝不静默丢弃（否则任务永久卡 running）
3. **StepContext 统一注入**：`params`（extra_vars / env）、`secrets`（已解析）、`targets`、`connection`、`workdir`（仓库 clone 根）、`event emitter`、`redact 列表`
   - **v34 凭据优先级**：`target.ssh_key_ref` > `connection.ssh_key_ref`；`target.ssh_user` > `connection.ssh_user`。**登录用户与钥匙由执行面解析后写入**（`ExecutionCreate.ssh_user` + `ssh_credential` → 目录展开成 Vault 引用），同一执行内同值；两者皆空且 `run_on=target` 属于上游漏配（bingops 已在创建执行时 400），runner 仍须回流 `prepare` 失败而不是抛异常
   - **v31 可选增强**：回填 `credentials.verify_state` / `last_verified_at`（取 Vault 成功=ok、失败=failed），让凭据目录页能显示“这把钥匙上次验证是通的”
4. **secrets 解析前置**到 executor 之前，一个解析器四种 type 共用；Vault 读失败 → step 失败并回流，不允许空值继续跑
5. **shell 远端执行复用 ansible ad-hoc**（`ansible -i inv -m shell -a "<entry>"`），**不用 paramiko 自研**：直接复用已实现的 inventory / Vault keyfile / become / proxy_hop 与日志格式，零新增 SSH 代码；`run_on: local` 走 `subprocess`
   - **v36 新增 `script` executor**：entry 是**仓库内脚本文件路径**，用 `ansible -i inv -m script <path> -a "<参数>"` 把脚本从 runner 推到目标机临时目录执行（目标机不需预置该文件）。**不能拿 shell 模块去 `bash scripts/x.sh`**：那个路径相对目标机文件系统，必失败（旧文档写错过）；两者共用同一套 inventory/凭据/跳板/日志栏
   - **v30：多目标并发度改由 runner 自己的配置 `max_parallel_hosts` 决定**（部署级），消息里不再下发 `serial` / `batch_pause_sec`；需要“逐台执行”就是把该配置调成 1
6. **python**：`subprocess`，cwd=仓库根，env 含 params+secrets，stdout/stderr 逐行 → log 事件，退出码 → step 状态；依赖策略 = 镜像内置 `requirements.txt`（加 SDK 即重建镜像），每任务临时 venv 作退路
7. **inventory 构建条件化**：`targets` 为空或 `run_on=local` 时不建 inventory、不取 SSH 私钥
   - **v32 跳板渲染**：`target.gateway` 非空则为该主机渲染 `ansible_ssh_common_args = -o ProxyCommand='ssh -i <跳板临时钥> -o StrictHostKeyChecking=accept-new -W %h:%p <gateway.ssh_user>@<gateway.host>:<gateway.port>'`——**必须显式 `-i`**：`ansible_ssh_private_key_file` 只作用于最终目标，`ProxyJump=user@host` 简写不认它；`gateway.ssh_key_ref` 为 null 时复用目标主机同一把钥匙；跳板钥同 Vault 纪律（临时 0600、用完即删、进 redact）
8. **v37：回滚已下线**——消息里没有 `command`，也没有 `BINGOPS_ACTION=undo` 注入。runner 只需处理“执行”一种语义；失败就回流 `step_finished(failed)` + `execution_finished(failed)`

### 9.4 API 端点（bingops，P1）

- `runbook` CRUD + 版本管理：`/api/v1/jobs/runbooks`
- 凭据目录（v31，平台级）：`/api/v1/credentials` CRUD + `GET /{id}/usage` 引用反查；权限码 `credential:list/get/create/update/delete`
- 中转网关（v32；v34 删 reachability 视图）：`/api/v1/job-gateways` CRUD；权限码 `gateway:list/get/create/update/delete`
- 执行：`POST /api/v1/jobs/executions`（创建即快照）、`GET` 列表/详情、`POST .../cancel`、`POST .../rollback`
- 日志：`GET /api/v1/jobs/steps/{id}/logs?after_seq=`（SSE live tail）

### 9.5 前端对接要点（v29 扁平单步 + v31 凭据目录）

| 页面 | 要改 | 不改的后果 |
|------|------|-----------|
| 新增/编辑 Runbook | **`exec_type` 下拉（ansible / shell / script / python）+ `entry` 单输入框** = 两个必填项；**v34 删“兜底登录用户/密钥/提权”三个框，v36 再删“默认目标机 / 默认代码版本”两个框**（表单再减 2）；`entry` 提示语按类型变：选 script 时写“仓库内脚本路径，如 scripts/dump.sh”，选 shell 时写“在目标机执行的命令” | 用户在定义期被迫回答“用谁连、打哪台、跑哪个版本”——这三件都是执行期决定 |
| 参数区 | `params_schema` + `secrets_schema` 合成**一张表**：每行「名字 / 类型 / 必填 / 默认 / 是否密钥」，勾选即拆进 `secrets_schema`。**两个 JSON 文本框归零，存储仍是三层分离** | 手写 JSON 正是“两小时写不出一个 runbook”的直接原因 |
| 步骤字段 | 只剩 `timeout_sec` 一个可选字段（**v30 已删 `undo_command`/`serial`/`batch_pause_sec`，v37 已删 `rollbackable`**，继续提交会被忽略）；`steps` 同样已不存在 | 表单里留着永远不填的字段 = 每次都要重新理解一遍它是什么意思 |
| 编辑回显 | 直读 runbook 响应的**步骤列**（`exec_type`/`entry`/`run_on`/`timeout_sec`，v29 已无 steps 数组；`run_on` 已显式回写） | 自己再推一遍缺省值，与后端推断不一致 |
| 新增执行 | **目标机必须每次选**（v36）；**代码版本框按 exec_type 显隐**（v38：shell 内联命令不依赖仓库代码，不渲染该框；ansible/script/python 才显示且必填）；可选做「复用上次的目标机 / 版本」按钮，数据源取当前用户对该 runbook 的最近一次 `job_executions`；无 target 型任务不渲染机器选择器 | 把“打到哪台”变成不假思索的预选项；或给内联命令强要一个 git tag（v36 的错，已修） |
| **执行弹窗连接区**（v34 新增） | 三个输入件：**登录用户**（文本框）+ **SSH 钥匙**（下拉，数据源 `GET /api/v1/credentials?kind=ssh_key`，提交 `ssh_credential=条目名`）+ **提权开关**（默认关）；另有可选 **中转网关** 下拉（`GET /api/v1/job-gateways`，留空自动选路）。目标型任务缺用户/钥匙后端 400，报错文案已可直接展示 | 不给入口用户就只能把身份写在 runbook 里，回到“定义期猜钥匙”的老路 |
| 执行详情 | **回滚相关的一切已下线**（v37）：`rollbackable` / `rollback_policy` / `attempt_type` 不再出现在响应体，回滚按钮与 `rolling_back`/`rolled_back`/`rollback_failed` 状态渲染全部移除；失败态就一个 `failed` | 留着回滚按钮 = 用户以为出事可以一键撤销（实际不能） |
| **凭据目录页** | `GET /api/v1/credentials?kind=ssh_key` 供执行弹窗下拉；详情页挂 `GET /{id}/usage` 展示引用反查（轮换前必看）；**表单只剩 4 个框**（v36）：名称 / 类型 / **Vault 引用（单串 `vault_ref`，形如 `ssh/keys/ops#private_key`）** / 备注——**已删：登录用户（v33）、适用范围云账号与区域、设为默认（v36）**；不得出现任何明文凭据输入框 | 回到手打路径的老问题；误删在用的钥匙；给用户两个“填了也不会发生”的框 |
| **中转网关页** | 表单只留 6 个框：名称 / 主机 IP / 端口 / 登录用户 / 跳板凭据（下拉，可空=复用目标机钥匙）/ **接管 VPC（多选，数据源 `GET /cmdb/resources?model_code=aliyun_vpc|gcp_vpc`，不让人手打 ID）**。**区域/云账号/资源 ID 三个框与 priority 已删**（v35）；同一 VPC 被别的网关占用时后端返 409，直接展示即可 | 四个维度框三个不填，用户仍要理解“任一命中/空不接管/priority 升序”三条规则 |
| **机器选择器按 VPC 筛**（v35 新增能力） | `GET /cmdb/resources?model_id=<aliyun_ecs>&status=running&field_key=vpc_id&field_value=vpc-2ze…`（`field_key` 不传则退回全字段匹配） | 跨 VPC 时只能逐台认，或把“哪些机器同网”记在人脑子里 |
| ~~主机标签配置/可达性看板~~（v34 已删） | 不需要为执行预配主机标签，`GET /job-gateways/reachability` 已随主机标签链路一并删除——连通性由执行本身回答，失败事件回流执行记录 | 维护一个前提已不存在的预检页 |

验证基线（后端已断言）：`POST /runbooks` 只传 `{name, exec_type, entry, params_schema, secrets_schema}` → 201；`POST /executions` 目标型任务缺 `ssh_user`/`ssh_credential` → 400 报错指名缺哪样；旧前端多传 `steps`/`auto_rollback`/`connection` 不报错但被忽略（需前端跟进移除渲染）。

---

## 10. 待决策项

| 项 | 说明 | 阻塞阶段 |
|----|------|---------|
| ~~CMDB `environment` 通用列~~（已结案） | 决策：不建列。env 事实源即运维约定（云标签 / K8s label），加列不解决覆盖问题反而引入双写漂移；门控与展示统一走 `resolve_resource_env` helper（K8s 读 `k8s:env`；云资源 manual 优先、cloud 兜底；无值按 fail-safe） | - |
| 无 env 时的 fail-safe 方向 | 从严（视为 production，多审批）还是从宽（低危放行）；建议从严 | P3 |
| **`target_models` 目标范围模型优化**（你定下轮再琢磨） | 现状是“模型 code 白名单”一维硬校；方向参照已有案例——**CMDB 输出 Prometheus HTTP SD 时 `http_config` 的做法**：目标集 + 如何访问（凭据引用/参数）一起结块描述，而不是拆成 `target_models` + `connection` + `secrets_schema` 三处。候选方案：目标选择器（selector：模型/标签/env/状态）+ 访问配置块绑定；本期不动契约 | P2 |
| GitLab 自建与否 | 决定 P2 terraform state 是否可先用 GitLab 原生 backend 过渡 | P2 |
| **批量 ping 实测连通** | 静态视图只能报“凭据齐不齐、匹配到哪条通道”，“要不要中转”必须实测。阻塞在 **ad-hoc 执行入口**（否则为测连通还得先建一个 runbook）；建议与 ad-hoc 一并做 | P2 |
| **连接三件套撤到执行面**（v34 已落地） | `RunbookCreate/Update` 撤掉 `connection`/`ssh_user`/`ssh_key_ref`/`become`/`become_method`/`become_user`；`ExecutionCreate` 新增 `ssh_user`/`ssh_credential`/`become`/`gateway_name`。解析链：执行时填写 > `runbook.connection` 存量兜底 > 400；主机标签 `ssh_credential`/`ssh_user` 不再是执行链路一环（`usage` 反查仍读历史标签）。**无 DB 迁移**（targets/connection 均为既有 JSONB，快照即审计） | 已落地 |
| **定义面再瘦身 + script 类型**（v36 已落地） | 删 `runbooks.default_target_resource_ids`/`default_code_ref`（目标机与版本归执行期，前端改做「复用上次的」）；删 `credentials.cloud_account`/`region`/`is_default` 与两个索引（无消费方）；凭据入口合并为单串 `vault_ref`（存储仍拆两列）；新增 `exec_type=script`（仓库脚本推送执行）。迁移：`sql/migrations/v36_definition_slim.sql` | 已落地 |
| **回滚能力整体下线**（v37 已落地） | 删 `runbooks.rollbackable` / `job_executions.rollback_policy` / `job_steps.attempt_type` / dispatch `command` 字段 / `POST /executions/{id}/rollback` / 权限 `job:rollback` / 四个回滚状态；`job_steps` 唯一约束改为 (execution_id, step_key)。理由见 §0 #20（runner 从未实现，属纸面契约；内联命令还会假成功回滚）。迁移：`sql/migrations/v37_drop_rollback.sql` | 已落地 |
| v26~v37 已收敛项（备忘） | `proxy_hop` → `job_gateways` 表（v32）+ VPC 单维度选路（v35）；`serial`/`batch_pause_sec` 已删（v30）；`auto_rollback` 已删（v28）；主机标签凭据与 `login_user` 退出执行链路（v33/v34）；默认目标机/默认版本/凭据适用范围已删（v36）；**回滚链整体已删（v37）** | - |
| 仍排除在本轮之外（防边重构边膨胀） | GitLab 仓库同步器、playbook-tree/tag 预检 API、自动回滚解冻、terraform apply 与 state、**多步编排**（v29 已从 API/表结构/消息三层全删；恢复 = 新增一张步骤表的演进） | P2 |
| ~~`type: python` 与 `exec_mode: local`~~（v27 已落地） | python executor 已实现（步骤级 `run_on=local`）；不再需要独立的 `exec_mode` 字段——执行位置属于步骤属性而非 runbook 属性 | 已结案 |
| ⚠ **runner 必须按 v29/v30/v37 新消息形态重构** | dispatch 的 `steps` 数组已改为 `step` 单对象、入口字段统一为 `entry`、新增 `run_on`/`secrets`；**v30 去掉了 step 里的 `serial`/`batch_pause_sec`/`undo_command`**，多目标并发度改读 runner 自己的 `max_parallel_hosts`；**v37 去掉了消息级的 `command` 与 `step.rollbackable`，事件里的 `attempt_type` 也不再需要**（runner 只处理“执行”一种语义）。已部署 runner 不升级则**所有新任务不可执行**；旧 execution 自带 step_snapshot，多余键被 pydantic 忽略。部 bingops 前先把 runner 跟齐，期间可用 `BINGOPS_JOB_STEP_TYPES=ansible` 只允许已验证类型 | v29/v30/v37 上线 |
| terraform executor 的 state 后端 | 本轮只注册 type 占位；启动时再定 local / http backend+OSS（先前分析已倾向 bingops 自实现 http backend，锁为协议原生） | P2 |
| **中转网关独立表**（v32 落地，v35 收敛） | `job_gateways` 表 + CRUD + 按 **vpc_id 单维度**选路写入 `targets[].gateway`，执行面可用 `gateway_name` 强制指定。`scope` 四维与 `priority` 已删（v35），一个 VPC 只允许一条启用网关接管（写入 409）。`GET /reachability` 预检视图已删（v34） | 已落地 |
| **`secrets_schema` 接凭据目录**（v32 已落地） | `secrets` 的值可直填 `credentials.name`，条目可选 `kind` 限定类型（声明了就强制走目录并校验，拼错名字/拿错类型在创建执行时 400）；未声明 `kind` 时保留裸 Vault 路径透传，存量 runbook 不受影响 | 已落地 |
| `verify_state` 回填由谁做 | 方案 C 已定（bingops 不连 Vault，避开破"Vault 唯一出口"纪律）；需 runner 实现：取 Vault 成功/失败时回写 `credentials.verify_state` + `last_verified_at` | v32（runner） |
