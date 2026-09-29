# bingops-runner

BingOps 任务系统执行面（Job 执行引擎），按 [docs/task-system-design.md](docs/task-system-design.md) 实施，当前阶段 **P1**。

## 定位

与 bingops 控制面分离的独立执行进程：

```
bingops（控制面）── Kafka[job-dispatch] ──→ bingops-runner ──→ 目标机（ansible）
        ↑                                                          │
        └── 落库 job_steps / job_step_logs ←── Kafka[job-events] ←─┘
```

**纪律**：bingops 不跑 ansible；runner 不写业务表；Kafka at-least-once 靠 message_id 去重。

## 结构

```
runner/
├── core/            # config / logging / exceptions / models（Kafka 契约 dataclass）
├── kafka/           # consumer(job-dispatch) + producer(job-events)
├── vault_client.py  # AppRole 取钥，KV v2 任意 path#field，内存 TTL 缓存
├── git_fetcher.py   # git clone --depth 1 --branch <tag>（pinned，不可移动）
├── inventory.py     # targets → inventory JSON + 临时 keyfile(0600, 用完即删)；无 targets 不建
├── secrets_resolver.py  # v27：secrets/存量 _ref 统一解析为 env，executor 前置
├── redact.py        # 出机前脱敏（Vault 取值进掩码列表）
├── executors/       # v27 注册表型：type → handler，统一 run(step, ctx)
│   ├── registry.py           # ansible / shell / python / terraform(门控占位)
│   ├── ansible_executor.py   # 事件回调 → job-events；灰度分批；超时强杀
│   ├── shell_executor.py     # target 复用 ansible ad-hoc；local 走 subprocess
│   ├── python_executor.py    # subprocess + 镜像内置依赖
│   └── terraform_executor.py # 门控未开，P2 点亮
└── main.py          # 信号量限流 / message_id 去重 / 优雅退出 / 单步编排
```

## 执行流程（单条 dispatch，v29 扁平单步）

1. message_id 去重 → 信号量获取并发位
2. exec_type 门禁：未知类型在 step_started 前回流 prepare 失败
3. `git clone --depth 1 --branch <code_ref>` 取代码快照
4. secrets 解析前置：`{VAR: "path#field"}` + 存量 `*_ref` params → 同名 env，进脱敏列表
5. `run_on=target` 时 Vault 取钥 → 临时 keyfile(0600) → 拼 inventory；local/无 targets 不建
6. 注册表分发 executor 执行单步（ansible 支持 serial 灰度 + batch_pause_sec）
7. 事件流回流：`step_started → log(seq 递增) → step_finished → execution_finished`
8. `command=rollback` 时：shell 优先 undo_command，其余注入 `BINGOPS_ACTION=undo`
   （ansible 同时保留 extra_vars `bingops_action` 兼容存量 playbook）；不可逆步骤回滚空转
9. 结束清理：keyfile 删除、工作目录清除

## 本地开发

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env   # 填入实际配置，启动时自动加载（环境变量优先）
python -m runner
```

## 构建镜像

```bash
docker build -f deploy/Dockerfile -t bingops-runner:latest .
```

## P1 验收口径

「批量重启」runbook 端到端：圈选 → 执行 → 灰度 → 日志 live tail → 失败手动回滚 → change_log。
