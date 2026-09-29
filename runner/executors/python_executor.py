"""python 执行器：subprocess 跑仓库内脚本（run_on 恒为 local）。

- cwd=仓库根；env = 系统环境 + params + secrets + BINGOPS_ACTION
- stdout/stderr 逐行 → log 事件；退出码 → step 状态
- 依赖策略：镜像内置 requirements.txt（加 SDK 即重建镜像），每任务临时 venv 作退路（P2）
"""

import logging
import os
import subprocess
import sys
import time

from runner.core.exceptions import ExecutorError, StepTimeout
from runner.core.models import StepSpec
from runner.executors import StepContext, StepResult

logger = logging.getLogger(__name__)


class PythonExecutor:

    def run(self, step: StepSpec, ctx: StepContext) -> StepResult:
        script = os.path.join(ctx.repo_dir, step.entry)
        if not step.entry or not os.path.isfile(script):
            raise ExecutorError(f"python entry 不存在: {step.entry}")

        env = ctx.subprocess_env(include_params=True)
        deadline = time.monotonic() + ctx.timeout_sec
        logger.info("python 执行: %s", script)
        # 用 runner 自身解释器：镜像 requirements 已装好业务 SDK
        proc = subprocess.Popen(
            [sys.executable, script], cwd=ctx.repo_dir, env=env,
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
        logger.info("python 结束: rc=%s", rc)
        if rc != 0:
            return StepResult(rc=rc, error=f"python 退出码 {rc}")
        return StepResult(rc=0)
