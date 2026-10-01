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
| 3 | 回滚作者层 | role/playbook 内 `bingops_action` do/undo 条件契约（同仓同文件防漂移）+ `block/rescue` 步内自愈 |
| 4 | 回滚编排层 | 引擎逆序重跑 undo（`attempt_type=rollback`）；**一律手动**——`auto_rollback` 已于 v28 从 API 契约与表结构删除（能填不生效比缺字段更坏）；`job_executions.rollback_policy` 保留，是 P2 解冻自动回滚时的落点 |
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

作者只需理解 **4 个概念**：叫什么（name）、怎么跑（exec_type + entry）、在哪跑（default_target_resource_ids + ssh_user/ssh_key_ref，仅 target 型）、每次变什么（params_schema / secrets_schema）。其余字段全有安全缺省值，UI 不必出现。

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

**落库与下发**（`job_service._build_step` / `_merge_connection` / `step_of`）：

| 你写的 | 落到哪里 |
|--------|---------|
| `exec_type` / `entry` / `run_on` / `timeout_sec` / `rollbackable` | **runbooks 的 5 个步骤列**（v29 扁平化；v30 又删掉 `undo_command`/`serial`/`batch_pause_sec`）——**无数组、无 JSON 可拼** |
| `ssh_user` / `ssh_key_ref` / `become*` | 并入 `connection` JSONB（与显式 `connection` 共存时，平铺字段覆盖同名键） |
| 缺省 `run_on` | 按 `exec_type` 推断后**显式落列**（下游不再各自推断） |
| 缺省 `timeout_sec` / `rollbackable` | 600 / **true**（不可逆是例外，需显式 false） |
| `default_target_resource_ids` / `default_code_ref` | 独立列（v26），执行未传时继承 |

创建 execution 时，8 个步骤列被组装成**单个 `step_snapshot` 对象**快照（`step_of()`，含固定 `key="main"` 与 `name=任务名`），dispatch 与回滚都按它走。

**继承与回落链**（`create_execution`）：

- `target_resource_ids`：显式传 \< `runbook.default_target_resource_ids` \< 400。**显式传空数组 `[]` 视为“无目标”，不参与继承**（用 `model_fields_set` 区分“未传”与“传空”）
- `code_ref`：显式传 \< `runbook.default_code_ref` \< `BINGOPS_JOB_DEFAULT_CODE_REF`（默认空）\< 400 引导填 tag。**刻意不给“默认 main”**：分支 HEAD 会移动，同一执行对应不同代码会破坏版本快照语义
- 继承来的目标**照走全部硬校验**（`status=running`、`target_models` 白名单、并发目标锁）——简化的是填写量，不是安全边界
- 报错即文档：不满足步骤契约时，400 message 内嵌最小可用示例（`MINIMAL_RUNBOOK_HINT`）

**回滚只有一个约定**：入口（脚本 / playbook）自己实现 undo 分支，平台回滚时就是**重跑同一 `entry` 并注入 `BINGOPS_ACTION=undo`**。因此 `rollbackable` 的含义很单纯：“这个入口有没有 undo 分支”。缺省 **true**（旧契约默认 false 会让所有人漏填，导致回滚链静默跳步）；不可逆任务显式关成 false。

内联命令（`df -h` 这种）天然没有 undo，那种任务请把“允许回滚”关掉——v30 不再提供 `undo_command` 字段去写第二套逆操作表达：同一件事有两种写法，就必然出现两种写法不一致的风险。

**多步编排已彻底移除**（v29）：你们自己的仓库已证明正确形态——整条流程写在一个 playbook 里（厚 role、薄 runbook），steps 数组没有真实使用场景。删除范围 = API 入参 + `runbooks.steps` 列 + `job_executions.steps_snapshot` + dispatch 消息的 `steps` 数组；**`job_steps` 表保留**（一步一行，回滚是同 key 的 `attempt_type='rollback'` 行，日志与审计结构不变）。将来真要恢复多步，是“加一张步骤表”级别的演进，不是回到 JSON 数组。`proxy_hop` / `become_user` 等高级字段仍可通过 `connection` 字典表达。

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

### 3.2 do/undo 条件契约（作者层）

```yaml
# roles/app_restart/tasks/main.yml
- include_tasks:
    file: "{{ 'undo.yml' if bingops_action | default('do') == 'undo' else 'do.yml' }}"
```

（必须用 `file:` 映射形式；单行简写里的 `==` 会被 mod_args 误拆为 k=v 选项报 Invalid options）

- 操作与逆操作同仓同文件，永不漂移；引擎回滚无需独立 rollback playbook 路径
- 步内瞬时失败用 ansible 原生 `block/rescue/always` 自愈（如起服失败先尝试拉起），与步级回滚互补不替代

### 3.3 编辑面约定：YAML 对人、JSON 对机器

- **存储与 API 契约仅 JSON**（params_schema/secrets_schema/connection/步骤列为 JSONB/标量，API 收 dict）；后端不解析 YAML，单一文法、单一校验入口（`_build_step`）
- **前端编辑面用 YAML**（Monaco + js-yaml@4，YAML 1.2 core schema，避 1.1 `yes/on` 布尔坑）：提交时 `yaml.load` → 校验 → JSON 调现有 API；编辑回显 `yaml.dump`（保插入序，往返无 diff 噪音）
- 标量配置（name/category/risk_level/auto_rollback/target_models）用**结构化表单**；connection 是 runbook 定义的一部分（见 §3.1 示例），UI 以 5 个已知键的结构化表单编辑、序列化进 connection 字段，不进 YAML 自由区
- **v29：单步 runbook 只填 `exec_type` + `entry`**（见 §3.1）——两个必填项，无互斥字段、无数组；`connection` 嵌套全键等高级字段在 service 入口归一，**后端不存两套语法**，编辑回显直接读步骤列与 connection
- YAML 校验仅为 UX 即时反馈；权威仍是后端 400
- 与未来 GitLab runbook-as-code 演进同构：仓库与 UI 共用 YAML 语法，CI 转 JSON 同步进平台
- **CI 门禁（P2）**：标 `rollbackable: true` 的 role 必须引用 `bingops_action`，防只写 do 忘写 undo

### 3.4 版本语义

- `runbooks.version` 整数，每次编辑 +1
- 任务创建时 **runbook_version + steps + code_ref（git tag）三快照** 进 execution 行——在跑任务永远用创建时的定义与代码
- code_ref 缺省回落链（`runbook.default_code_ref` → 平台配置）只影响**创建时填什么**，不影响快照：解析后的实际值照旧写入 execution 行

### 3.5 步骤类型契约（v27 多执行器）

表驱动校验（`job_service._build_step` + `EXEC_TYPE_RUN_ON`）：`exec_type` 决定默认执行位置，`entry` 的语义随类型变。**v29 扁平单步**：步骤直接存 `runbooks` 的 8 个列，不再有 JSONB 步骤数组。

| exec_type | entry 语义 | `run_on` 缺省 | 需要 targets | 参数注入 | 回滚入口 |
|-----------|-----------|--------------|-------------|---------|---------|
| `ansible` | playbook 路径 | `target` | 是 | extra_vars(params) + env(secrets) | 同 playbook + `BINGOPS_ACTION=undo` |
| `shell` | **命令字符串**（仓库脚本写 `bash scripts/x.sh`） | `target`（可写 `local`） | `run_on=target` 时是 | env（params+secrets） | `BINGOPS_ACTION=undo`（内联命令无 undo，应关 `rollbackable`） |
| `python` | 仓库内脚本入口 | `local` | 否 | env + argv | 同脚本 undo 分支 |
| `terraform` | 工作目录 | `local` | 否 | `-var` / tfvars | **本轮拒绝创建/执行**（门控未开，state 方案未定） |

其余步骤列：`timeout_sec`（600）、`rollbackable`（true）。`_build_step` 把推断后的 `run_on` **显式落列**，下游不再各自推断。shell 语义已收敛为“entry 恒为命令”，避开“这是路径还是命令”的隐式判断（与你们否掉 `*_ref` 后缀魔法是同一条纪律）。

**v30 删掉的三个字段**（均为基础设施/噪声属性，不再由任务持有）：`undo_command`（回滚统一走 undo 约定）、`serial` 与 `batch_pause_sec`（多目标并发度 = runner 部署级配置 `max_parallel_hosts`）。

**派生与门控**：

- `needs_targets = (runbook.run_on == "target")` → 同时决定 `target_resource_ids` 与 `connection.ssh_key_ref` 是否必填；`step_key` 恒为 `main`（`job_steps` 一行 + 回滚一行）
- 无目标任务**不绕过变更封禁**：`_freeze_hits_models` 中 `scope` 为空即全局命中，空 `model_codes` 照样拦（已核）
- 类型白名单 `BINGOPS_JOB_STEP_TYPES`（默认 `ansible,shell,python`）：runner 尚未支持某 executor 时收紧配置，平台侧即拒绝创建，而不是下发后失败

### 3.6 凭据三层分离（v27）

三层各走一个字段，**不再有“看后缀才知道是密钥”的隐式规则**：

| 层 | runbook 声明 | 执行时填 | 落库 | 值的性质 |
|----|-------------|---------|------|---------|
| 明文入参 | `params_schema` | `params` | `job_executions.params` | 普通值（库名、表名、开关） |
| Vault 入参 | `secrets_schema`：`{VAR: {required, description, default_ref}}` | `secrets: {VAR: "<Vault 路径#字段>"}` | `job_executions.secrets` | **只存钥匙名**，明文永不入库 |
| 目标机私钥 | `connection.ssh_key_ref` | —（runbook 级） | `runbooks.connection` | 只存钥匙名 |

> **v31 变更**：上表第三层已从「任务属性」搬迁为「机器属性」——`ssh_key_ref` 不再是 runbook 必填项，而是由 `credentials` 凭据目录 + 主机标签在执行期逐台解析（见 §5.1）；`connection.ssh_key_ref` 降为存量兜底。

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

1. UI 圈选目标（CMDB 选择器）→ 创建 `job_executions`（params/targets/version 快照）；**v26：runbook 已绑默认目标时此步跳过选择**（继承规则见 §3.1）
2. 并发校验：target_resource_ids 与在跑 execution 交集命中即拒绝（同资源单执行锁）
3. 审批（P3）：risk_level + 环境维度 → 挂 ticket，通过才下发
4. bingops 发 `job-dispatch`（**只带 ssh_key_ref 钥匙名，不带 secret**）
5. runner 消费 → Vault 取钥（临时文件 0600，用完即删）→ 拼 ad-hoc inventory → 逐步执行
6. runner 流式发 `job-events`：`step_started → log(seq 递增) → step_finished`
7. bingops 消费落库 → 前端 SSE live tail
8. 失败 → **手动**触发回滚（v26 冻结自动回滚）：已完成且 rollbackable 的步骤**逆序**重跑 undo；不可逆步骤阻断回滚链并告警
9. 终态 → CMDB `change_log`（source='job'）

### 4.2 状态机

```
execution: pending → awaiting_approval(P3) → running → success / cancelled
                                       ↘ failed → rolling_back → rolled_back / partial_rollback / rollback_failed
step:      pending → running → success / failed / skipped / rolled_back / rollback_failed
```

回滚执行在 `job_steps` 记为同 step_key、`attempt_type='rollback'` 的新行，日志/审计天然齐全。

回滚链纪律（控制面实现）：
- 回滚下发只包含**已完成且 rollbackable** 的步骤（控制面按 `job_steps` 状态过滤）；runner 无状态，不知道哪些步骤跑过，给什么跑什么
- 零已完成步骤的失败（如 prepare 阶段失败）直接落终态，不进 `rolling_back`
- 回滚下发后收到失败事件（含 runner 契约校验失败回流的 rollback attempt 事件）落 `rollback_failed`/`partial_rollback`，不得无限等待
- **终态以 runner 的 `execution_finished` 事件为准**：每次 dispatch 处理结束 runner 必发该事件（do → success/failed；rollback → rolled_back/partial_rollback/rollback_failed），控制面收到即落 execution 终态，不得自行推断或无限等待

---

## 5. Inventory 与网络

- **Inventory 源 = CMDB**：下发时 bingops 从目标快照生成 `[{resource_id, name, ip, ssh_user, ssh_key_ref, gateway}]`——**v31 起凭据逐台携带**（不同密钥/不同跳板的机器可混在同一任务里），runner 优先用 target 上的值，缺失才回落 `connection`；资源选择器能力在此变现。**v27：全 local 任务不建 inventory**（targets 为空，不取 SSH 私钥）
- **目标范围硬校验**：runbook.`target_models` 声明 scope（默认 `[aliyun_ecs, gcp_compute]`），`create_execution` 对快照 model_code 越界即 400；前端选择器按 target_models 传 `model_id` 过滤（UX 层，不替代后端校验）；K8s 对象 P2 以 local 模式扩入
- **执行态硬校验**：目标 `status` 必须 = `running`（stopped SSH 必失败、maintenance 变更中；unknown/NULL 按 fail-safe 从严 400，报错带资源名+实际状态）；前端选择器同步传 `status=running`
- **网络可达 / 跳板**：**v32 起由 `job_gateways` 表 + 自动选路接管，不再让 runbook 写 `proxy_hop`**（漏声明的后果是 SSH 超时，现象像 playbook 写错），渲染细节与 `-i` 坑见 §5.2 与 §9.3 第 7 条；`connection.proxy_hop` 保留为历史字段，新配置不再读它
- **connection 契约**（v31 降级为任务级兜底）：`ssh_user` 可作“身份要求”保留（高危任务强制低权账号）；`ssh_key_ref` **不再必填**，仅在目标机无标签且目录无默认时兜底；`become` 默认 false / `become_method` 默认 sudo / `become_user` 默认 root；**sudo 密码不进配置**：宿主机 NOPASSWD sudoers 由 bootstrap runbook 统刷，退路 `become_password_ref` 走 Vault+no_log；runner 渲染为 inventory 变量 `ansible_become*`

### 5.1 凭据解析（v31 凭据目录）

**凭据目录 `credentials`**：把“哪把钥匙、属于谁、能干什么”收敛成可下拉选择的实体。只存 Vault 引用与元数据，**明文禁入**（入口有 `-----BEGIN` / `PRIVATE KEY` 等特征串拦截）；`name` 全局唯一（因为引用点是裸字符串）；`kind ∈ ssh_key|cloud_ak|db_password|api_token|kubeconfig`。

**主机侧引用**：主机资源标签 `ssh_credential = credentials.name`（复用 `cmdb_resource_tags`，零新表）。`login_user` 绑在凭据上，选钥匙顺带定身份。

**解析优先级**（`job_service._resolve_target_credentials`，执行创建时逐台计算）：

```
主机标签 ssh_credential
  → 适用范围（cloud_account + region）唯一命中
  → 同 kind 的 is_default 条目
  → runbook.connection.ssh_key_ref（存量兜底，行为不变）
  → 都没有：400，报错指名是哪台机器缺什么
```

两条硬规则：

- **多命中不猜**：候选 > 1 且无默认项时直接 400 列出候选名。猜错的后果是用错账号连上生产机，比报错严重得多
- **登录用户优先级**：`connection.ssh_user`（任务声明的身份要求）> 凭据自带 `login_user`。刻意**不设平台级默认用户**——默认 `root` 这种约定会把漏配置变成高危行为

**为什么不搞 Vault 路径白名单**：能读哪些路径由 runner AppRole 的 Vault policy 决定（单一权限事实源），bingops 不重复实现一套权限。`verify_state` / `last_verified_at` 由 **runner 回填**，bingops 全程不连 Vault（方案 C：保住“Vault 唯一出口在 runner”这条纪律）。

**引用反查**（`GET /api/v1/credentials/{id}/usage`）：返回被多少主机标签 / 多少 runbook 引用。这是密钥轮换前的必看信息；有引用时**不允许删除**，只能停用（`is_active=false`）。

### 5.2 中转网关与选路（v32）

机器要怎么才被连到（直连还是经哪个 bastion）是**网络拓扑事实**，不是任务属性。让每个 runbook 写 `proxy_hop` 的后果是：漏写 → SSH 超时 → 现象像 playbook 写错。现在由机器归属在执行期算出。

**`job_gateways`**：`name` / `host` / `port` / `login_user` / `ssh_credential` / `scope` / `priority`。关键三点：

- **`ssh_credential` 引用 `credentials.name`**（不是裸 Vault 路径）——轮换时能反查“哪些网关还在用这把钥匙”，且写入时校验存在与 kind 匹配
- **`scope` 多维匹配**：`{vpc_ids, cloud_accounts, regions, resource_ids}`，命中任一即服务。**空 scope 不匹配任何机器**——不提供“全局兜底网关”，因为一个误配条目就会接管全部流量，那种故障比连不上更难查
- **多命中按 `priority` 升序取首**，排序在 `pick_gateway` 内部做（不依赖调用方传已排序列表）

**选路结果写入 dispatch 的 `targets[].gateway`**（`None` = 直连）：

```json
"gateway": {"name": "gw-nocid", "host": "10.0.0.1", "port": 22,
            "ssh_user": "ops", "ssh_key_ref": "ssh/keys/bastion"}
```

**可达性总览**（`GET /api/v1/job-gateways/reachability`）：逐台报 `凭据是否解析得到 / 走哪条路 / 缺什么`，`missing` 直接给原因文本（无 IP、无凭据）。这是“凭据归机器”后的必需品——机器数据完整性成了执行成功的前提，没有这个页，失败只会表现为“SSH 超时”。

> **视图边界**：无网关 = 直连，**不算缺口**。一台机器到底需不需要中转，只有实测能知道；本视图只保证“凭据与 IP 齐不齐、配了网关的机器能不能匹配上”。批量 ping 实测待 ad-hoc 执行入口（否则为了测连通还得先建一个 runbook）。
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
| P1 | runner 骨架 + Vault + ansible 步骤 + 日志 live tail + 手动回滚 + git clone（~~灰度~~ v30 下沉为 runner 配置） | 「批量重启」runbook 端到端：圈选→执行→日志→失败手动回滚→change_log |
| **P1.5（v27）** | 多执行器引擎：executor 注册表 + shell/python + 凭据三层分离 + targets/run_on 可选（terraform 仅占位） | 「开通 RAM 子账号」python 无目标任务端到端：只填 params+secrets → 执行 → 看日志 → 手动回滚 |
| P2 | terraform executor + http backend state（版本化=原生快照回滚）+ 自动回滚链 + OSS 制品层 + lint 门禁 | 「创建 RDS」失败自动逆序回滚；state 版本可追溯 |
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
    login_user       VARCHAR(64),                   -- 该钥匙对应的系统用户
    vault_path       VARCHAR(512) NOT NULL,         -- 只存路径，绝不存值
    vault_field      VARCHAR(128),                  -- path#field 拆分后的字段名
    cloud_account    VARCHAR(128),                  -- 适用范围，NULL = 不限
    region           VARCHAR(64),
    is_default       BOOLEAN      NOT NULL DEFAULT FALSE,   -- 同 kind 唯一（部分索引）
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
    scope            JSONB        NOT NULL DEFAULT '{}',  -- vpc_ids/cloud_accounts/regions/resource_ids
    priority         INT          NOT NULL DEFAULT 100,   -- 多命中时升序取首
    remark           TEXT,
    is_active        BOOLEAN      NOT NULL DEFAULT TRUE,
    created_by       BIGINT       REFERENCES users(id) ON DELETE SET NULL,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_job_gateway_active ON job_gateways (is_active, priority);

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
    rollbackable     BOOLEAN      NOT NULL DEFAULT TRUE,       -- 不可逆任务显式 false
    -- v30 已删除 undo_command / serial / batch_pause_sec（回滚统一约定 + 并发度下沉 runner）
    connection    JSONB        NOT NULL DEFAULT '{}',   -- {ssh_user, ssh_key_ref, become, become_method, become_user}；v31 起 ssh_key_ref 仅兜底
    target_models JSONB        NOT NULL DEFAULT '["aliyun_ecs", "gcp_compute"]',
    default_target_resource_ids JSONB NOT NULL DEFAULT '[]',  -- 执行未传 target 时继承（v26）
    default_code_ref VARCHAR(128),                  -- 执行未传 code_ref 时继承（v26）
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

**job-dispatch**（bingops → runner；`command` 区分执行/回滚，v29 为单 `step` 对象）：

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
               "region": "cn-guangzhou", "model_code": "aliyun_ecs",
               "ssh_user": "ops", "ssh_key_ref": "ssh/keys/ops-vpc-a", "gateway": null}],
  "step": {"key": "main", "name": "批量重启服务", "type": "ansible", "run_on": "target",
           "entry": "ansible/playbooks/app_restart.yml",
           "timeout_sec": 600, "rollbackable": true}
}
```

凭据两级结构：消息级 `connection`（runbook 声明）打底，**v31 起 `targets[]` 逐项携带 `ssh_user` / `ssh_key_ref` / `gateway`，且优先级高于 connection**——同一任务里不同密钥、不同跳板的机器可混着打。确需 sudo 密码时在 connection/target 带 `become_password_ref`（Vault 钥匙名）。契约校验失败（如既无 target 凭据又无 connection 兜底）runner 回流 `prepare` 失败事件而非静默丢弃；bingops 侧已在创建执行时提前 400，不会下发无法解析的目标。

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

**消息形态变更（v29，破坏性）**：`steps` 数组 → `step` 单对象；字段名 `playbook`/`command`/`script`/`working_dir` 统一为 `entry`（语义由 `type` 决定），新增 `run_on`；step 内不再有 `args`/`playbook`/`serial`/`undo_command` 等分叉字段；`command=rollback` 时控制面把 `step_snapshot` 原样重发并约定注入 `BINGOPS_ACTION=undo`。`execution_id` 仍是双方唯一关联键，事件流（job-events）结构完全不变。

回滚下发 = `command: "rollback"`，控制面把快照步骤原样重发，runner 注入 `BINGOPS_ACTION=undo` 重跑同一入口（v29 单步模型下不存在“逆序多步”）。

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
│   │   ├── registry.py             # EXECUTORS = {"ansible":..., "shell":..., "python":...}
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
2. **executor 注册表**：`EXECUTORS: dict[type, Executor]`，统一接口 `run(step, ctx) -> (status, exit_code)`；**未知 type 回流 `prepare` 失败事件**，绝不静默丢弃（否则任务永久卡 running）
3. **StepContext 统一注入**：`params`（extra_vars / env）、`secrets`（已解析）、`targets`、`connection`、`workdir`（仓库 clone 根）、`event emitter`、`redact 列表`
   - **v31 凭据优先级**：`target.ssh_key_ref` > `connection.ssh_key_ref`；`target.ssh_user` > `connection.ssh_user`。同一任务内不同密钥的机器要能分开连；两者都空且 `run_on=target` 属于上游漏配（bingops 已在创建执行时 400），runner 仍须回流 `prepare` 失败而不是抛异常
   - **v31 可选增强**：回填 `credentials.verify_state` / `last_verified_at`（取 Vault 成功=ok、失败=failed），让凭据目录页能显示“这把钥匙上次验证是通的”
4. **secrets 解析前置**到 executor 之前，一个解析器四种 type 共用；Vault 读失败 → step 失败并回流，不允许空值继续跑
5. **shell 远端执行复用 ansible ad-hoc**（`ansible -i inv -m shell -a "<entry>"`），**不用 paramiko 自研**：直接复用已实现的 inventory / Vault keyfile / become / proxy_hop 与日志格式，零新增 SSH 代码；`run_on: local` 走 `subprocess`
   - **v30：多目标并发度改由 runner 自己的配置 `max_parallel_hosts` 决定**（部署级），消息里不再下发 `serial` / `batch_pause_sec`；需要“逐台执行”就是把该配置调成 1
6. **python**：`subprocess`，cwd=仓库根，env 含 params+secrets+`BINGOPS_ACTION`，stdout/stderr 逐行 → log 事件，退出码 → step 状态；依赖策略 = 镜像内置 `requirements.txt`（加 SDK 即重建镜像），每任务临时 venv 作退路
7. **inventory 构建条件化**：`targets` 为空或 `run_on=local` 时不建 inventory、不取 SSH 私钥
   - **v32 跳板渲染**：`target.gateway` 非空则为该主机渲染 `ansible_ssh_common_args = -o ProxyCommand='ssh -i <跳板临时钥> -o StrictHostKeyChecking=accept-new -W %h:%p <gateway.ssh_user>@<gateway.host>:<gateway.port>'`——**必须显式 `-i`**：`ansible_ssh_private_key_file` 只作用于最终目标，`ProxyJump=user@host` 简写不认它；`gateway.ssh_key_ref` 为 null 时复用目标主机同一把钥匙；跳板钥同 Vault 纪律（临时 0600、用完即删、进 redact）
8. **回滚（v30 收敛为单一约定）**：`command=rollback` 时**统一注入 `BINGOPS_ACTION=undo`** 重跑同一 `entry`（与现有 ansible `bingops_action` extra_var 同一命名体系）；**`undo_command` 字段已删除**，入口没实现 undo 分支就执行失败并回流 `rollback_failed`（可见，不会静默）。step_key 恒为 `main`，回滚行靠 `attempt_type=rollback` 区分

### 9.4 API 端点（bingops，P1）

- `runbook` CRUD + 版本管理：`/api/v1/jobs/runbooks`
- 凭据目录（v31，平台级）：`/api/v1/credentials` CRUD + `GET /{id}/usage` 引用反查；权限码 `credential:list/get/create/update/delete`
- 中转网关（v32）：`/api/v1/job-gateways` CRUD + `GET /reachability` 主机可达性；权限码 `gateway:list/get/create/update/delete`
- 执行：`POST /api/v1/jobs/executions`（创建即快照）、`GET` 列表/详情、`POST .../cancel`、`POST .../rollback`
- 日志：`GET /api/v1/jobs/steps/{id}/logs?after_seq=`（SSE live tail）

### 9.5 前端对接要点（v29 扁平单步 + v31 凭据目录）

| 页面 | 要改 | 不改的后果 |
|------|------|-----------|
| 新增/编辑 Runbook | **`exec_type` 下拉（ansible / shell / python）+ `entry` 单输入框** = 两个必填项（v29 已无四个互斥入口字段）；**v31 起 `ssh_user` / `ssh_key_ref` 不再是必填项**（凭据属于机器）；只留 3~5 个框：名称、执行方式、入口、默认目标机（+ 参数区） | 让作者填他本不该知道的钥匙名，是“提前猜”而不是“配置” |
| 参数区 | `params_schema` + `secrets_schema` 合成**一张表**：每行「名字 / 类型 / 必填 / 默认 / 是否密钥」，勾选即拆进 `secrets_schema`。**两个 JSON 文本框归零，存储仍是三层分离** | 手写 JSON 正是“两小时写不出一个 runbook”的直接原因 |
| 步骤字段 | 只剩 `timeout_sec` 与 `rollbackable` 两个可选字段（**v30 已删 `undo_command` / `serial` / `batch_pause_sec`**，继续提交会被忽略）；`steps` 同样已不存在 | 表单里留着永远不填的字段 = 每次都要重新理解一遍它是什么意思 |
| 编辑回显 | 直读 runbook 响应的**步骤列**（`exec_type`/`entry`/`run_on`/`timeout_sec`/`rollbackable`…，v29 已无 steps 数组；`run_on` 已显式回写） | 自己再推一遍缺省值，与后端推断不一致 |
| 新增执行 | `target_resource_ids` 与 `code_ref` **去掉必填限制**：runbook 响应已带 `default_target_resource_ids`/`default_code_ref`，非空则预填可留空；无目标任务（entry 型）不渲染机器选择器 | 卡住提交，或强迫用户每次背 CMDB 数 ID 与 git tag |
| 执行详情 | `rollback_policy` 恒 manual，自动回滚开关从 UI 移除；`auto_rollback` **已从响应体删除**，前端任何引用都是 undefined | 用户勾了“失败自动回滚”以为已生效（实际始终手动） |
| **凭据目录页**（新增） | `GET /api/v1/credentials?kind=ssh_key` 作为下拉数据源；详情页挂 `GET /{id}/usage` 展示引用反查（轮换前必看）；**表单上不要出现任何明文凭据输入框**，也不要把 `vault_path` 当可编辑文本让运维背 | 回到手打路径的老问题；误删在用的钥匙 |
| 执行弹窗回显 | 选完目标机后，从响应 `target_resources[].ssh_user/ssh_key_ref` **只读展示**“将以 ops@10.0.0.5 访问，密钥 ssh/keys/ops-vpc-a”，不做下拉选择（多命中已在后端 400，不会到这一步） | 用户无法确认“到底会用哪把钥匙”，出错时只能猜 |

验证基线（后端已断言）：`POST /runbooks` 只传 `{name, exec_type, entry, params_schema, secrets_schema}` → 201 且步骤列已按类型推断（`run_on=local` 时不要求 `ssh_key_ref`）；旧前端多传 `steps`/`auto_rollback` 不报错但被忽略（需前端跟进移除渲染）。

---

## 10. 待决策项

| 项 | 说明 | 阻塞阶段 |
|----|------|---------|
| ~~CMDB `environment` 通用列~~（已结案） | 决策：不建列。env 事实源即运维约定（云标签 / K8s label），加列不解决覆盖问题反而引入双写漂移；门控与展示统一走 `resolve_resource_env` helper（K8s 读 `k8s:env`；云资源 manual 优先、cloud 兜底；无值按 fail-safe） | - |
| 无 env 时的 fail-safe 方向 | 从严（视为 production，多审批）还是从宽（低危放行）；建议从严 | P3 |
| **`target_models` 目标范围模型优化**（你定下轮再琢磨） | 现状是“模型 code 白名单”一维硬校；方向参照已有案例——**CMDB 输出 Prometheus HTTP SD 时 `http_config` 的做法**：目标集 + 如何访问（凭据引用/参数）一起结块描述，而不是拆成 `target_models` + `connection` + `secrets_schema` 三处。候选方案：目标选择器（selector：模型/标签/env/状态）+ 访问配置块绑定；本期不动契约 | P2 |
| GitLab 自建与否 | 决定 P2 terraform state 是否可先用 GitLab 原生 backend 过渡 | P2 |
| **批量 ping 实测连通** | 静态视图只能报“凭据齐不齐、匹配到哪条通道”，“要不要中转”必须实测。阻塞在 **ad-hoc 执行入口**（否则为测连通还得先建一个 runbook）；建议与 ad-hoc 一并做 | P2 |
| v26~v32 已收敛项（备忘） | `proxy_hop` 已从 `connection` 字典**升级为 `job_gateways` 表 + 自动选路**（v32）；`serial`/`batch_pause_sec` 已删（v30，并发度归 runner `max_parallel_hosts`）；`auto_rollback` 已删（v28，自动回滚解冻时重建在 execution 层） | - |
| 仍排除在本轮之外（防边重构边膨胀） | GitLab 仓库同步器、playbook-tree/tag 预检 API、自动回滚解冻、terraform apply 与 state、**多步编排**（v29 已从 API/表结构/消息三层全删；恢复 = 新增一张步骤表的演进） | P2 |
| ~~`type: python` 与 `exec_mode: local`~~（v27 已落地） | python executor 已实现（步骤级 `run_on=local`）；不再需要独立的 `exec_mode` 字段——执行位置属于步骤属性而非 runbook 属性 | 已结案 |
| ⚠ **runner 必须按 v29/v30 新消息形态重构** | dispatch 的 `steps` 数组已改为 `step` 单对象、入口字段统一为 `entry`、新增 `run_on`/`secrets`；**v30 又去掉了 step 里的 `serial`/`batch_pause_sec`/`undo_command`**，多目标并发度改读 runner 自己的 `max_parallel_hosts`。已部署 runner 不升级则**所有新任务不可执行**（无法解析 `step`）；旧 ansible 任务回滚不受影响（历史 execution 自带 step_snapshot，多余键被忽略）。部 bingops 前先把 runner 跟齐，期间可用 `BINGOPS_JOB_STEP_TYPES=ansible` 只允许已验证类型 | v29/v30 上线 |
| terraform executor 的 state 后端 | 本轮只注册 type 占位；启动时再定 local / http backend+OSS（先前分析已倾向 bingops 自实现 http backend，锁为协议原生） | P2 |
| **中转网关独立表 + 可达视图**（v32 已落地） | `job_gateways` 表 + CRUD + 按 scope 选路写入 `targets[].gateway` + `GET /reachability` 静态可达视图。**网关关联维度未拍板，故 `scope` 同时支持 `vpc_ids / cloud_accounts / regions / resource_ids` 命中任一**（CIDR 未支持：需路由知识且 CMDB 无逐主机 CIDR）；真实跨 VPC 场景验证后可收敛为单一维度 | 已落地 |
| **`secrets_schema` 接凭据目录**（v32 已落地） | `secrets` 的值可直填 `credentials.name`，条目可选 `kind` 限定类型（声明了就强制走目录并校验，拼错名字/拿错类型在创建执行时 400）；未声明 `kind` 时保留裸 Vault 路径透传，存量 runbook 不受影响 | 已落地 |
| `verify_state` 回填由谁做 | 方案 C 已定（bingops 不连 Vault，避开破"Vault 唯一出口"纪律）；需 runner 实现：取 Vault 成功/失败时回写 `credentials.verify_state` + `last_verified_at` | v32（runner） |
