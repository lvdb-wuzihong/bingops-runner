"""executors：步骤执行器（v27 注册表型）。统一接口 run(step, ctx) -> StepResult。

type → handler 分发见 registry.py；未知 type 必须报错回流，绝不静默丢弃。
"""

import os
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from runner.core.exceptions import StepTimeout
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
    targets: list[Target]
    connection: dict[str, Any]
    inventory: dict[str, Any] | None    # {"inventory_path","envvars"}；run_on=local 为 None
    event_cb: EventCallback
    redactor: Redactor
    timeout_sec: int
    max_parallel_hosts: int = 0   # v30：多目标并发度（部署级配置）；0=一次全部，1=逐台

    def subprocess_env(self, include_params: bool) -> dict[str, str]:
        """本地子进程环境：系统 env + params（按类型选）+ secrets。"""
        env = dict(os.environ)
        if include_params:
            env.update({k: str(v) for k, v in self.params.items()})
        env.update(self.secrets_env)
        return env


def run_local_stream(cmd: str | list[str], ctx: StepContext,
                     shell: bool = False) -> StepResult:
    """本地子进程流式执行：stdout/stderr 逐行→log 事件，超时强杀。

    shell/script/python 的 local 分支共用；env 含 params+secrets。
    """
    env = ctx.subprocess_env(include_params=True)
    deadline = time.monotonic() + ctx.timeout_sec
    proc = subprocess.Popen(
        cmd, shell=shell, cwd=ctx.repo_dir, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace",
    )
    try:
        for line in proc.stdout:
            ctx.event_cb("info", None, line.rstrip("\n"))
            if time.monotonic() >= deadline:
                proc.kill()
                raise StepTimeout(f"step 超时（{ctx.timeout_sec}s）被强制终止")
        proc.wait(timeout=max(1, int(deadline - time.monotonic())))
    except StepTimeout:
        raise
    except subprocess.TimeoutExpired:
        proc.kill()
        raise StepTimeout(f"step 超时（{ctx.timeout_sec}s）被强制终止")
    rc = proc.returncode
    if rc != 0:
        return StepResult(rc=rc, error=f"退出码 {rc}")
    return StepResult(rc=0)
