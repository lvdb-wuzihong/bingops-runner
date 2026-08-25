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
├── vault_client.py  # AppRole 取钥，内存 TTL 缓存，不落盘
├── git_fetcher.py   # git clone --depth 1 --branch <tag>（pinned，不可移动）
├── inventory.py     # targets → inventory JSON + 临时 keyfile(0600, 用完即删)
├── redact.py        # 出机前脱敏（Vault 取值进掩码列表）
├── executors/
│   ├── ansible_executor.py   # 事件回调 → job-events；灰度分批；超时强杀
│   └── terraform_executor.py # P2 占位
└── main.py          # 信号量限流 / message_id 去重 / 优雅退出
```

## 执行流程（单条 dispatch）

1. message_id 去重 → 信号量获取并发位
2. `git clone --depth 1 --branch <code_ref>` 取代码快照
3. Vault AppRole 按 `ssh_key_ref` 现场取钥 → 写临时 keyfile(0600) → 拼 inventory
4. 逐 step 执行 ansible playbook（`serial` 灰度分批 + `batch_pause_sec` 批间暂停）
5. 事件流回流：`step_started → log(seq 递增) → step_finished`
6. `command=rollback` 时：rollbackable 步骤**逆序**重跑，extra_vars 注入 `bingops_action=undo`
7. 结束清理：keyfile 删除、工作目录清除

## 本地开发

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env   # 填入实际 Kafka/Vault/GitLab 配置
python -m runner
```

## 构建镜像

```bash
docker build -f deploy/Dockerfile -t bingops-runner:latest .
```

## P1 验收口径

「批量重启」runbook 端到端：圈选 → 执行 → 灰度 → 日志 live tail → 失败手动回滚 → change_log。
