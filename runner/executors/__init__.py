"""executors：步骤执行器（v27 注册表型）。统一接口 run(step, ctx) -> StepResult。

type → handler 分发见 registry.py；未知 type 必须报错回流，绝不静默丢弃。
"""

import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from runner.core.models import Target
from runner.redact import Redactor

# executor 内部产生的日志行回调：(level, host, line)
EventCallback = Callable[[str, str | None, str], None]


@dataclass
class StepResult:
    rc: int
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.rc == 0


@dataclass
class StepContext:
    """统一注入上下文（设计文档 §9.3 实现清单 #3）。"""

    repo_dir: str                       # 仓库 clone 根
    workdir: str                        # 本 execution 临时目录
    params: dict[str, Any]              # 清洗后的明文入参
    secrets_env: dict[str, str]         # 已解析的 secrets 环境变量
    action: str                         # do | undo
    targets: list[Target]
    connection: dict[str, Any]
    inventory: dict[str, Any] | None    # {"inventory_path","envvars"}；run_on=local 为 None
    event_cb: EventCallback
    redactor: Redactor
    timeout_sec: int
    max_parallel_hosts: int = 0   # v30：多目标并发度（部署级配置）；0=一次全部，1=逐台

    def subprocess_env(self, include_params: bool) -> dict[str, str]:
        """本地子进程环境：secrets 恒注入，params 按类型选，BINGOPS_ACTION 恒注入。"""
        env = dict(os.environ)
        if include_params:
            env.update({k: str(v) for k, v in self.params.items()})
        env.update(self.secrets_env)
        env["BINGOPS_ACTION"] = self.action
        return env
