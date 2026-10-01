"""python 执行器：subprocess 跑仓库内脚本（run_on 恒为 local）。

- cwd=仓库根；env = 系统环境 + params + secrets
- stdout/stderr 逐行 → log 事件；退出码 → step 状态
- 依赖策略：镜像内置 requirements.txt（加 SDK 即重建镜像），每任务临时 venv 作退路（P2）
"""

import logging
import os
import sys

from runner.core.exceptions import ExecutorError
from runner.core.models import StepSpec
from runner.executors import StepContext, StepResult, run_local_stream

logger = logging.getLogger(__name__)


class PythonExecutor:

    def run(self, step: StepSpec, ctx: StepContext) -> StepResult:
        script = os.path.join(ctx.repo_dir, step.entry)
        if not step.entry or not os.path.isfile(script):
            raise ExecutorError(f"python entry 不存在: {step.entry}")
        logger.info("python 执行: %s", script)
        # 用 runner 自身解释器：镜像 requirements 已装好业务 SDK
        return run_local_stream([sys.executable, script], ctx)
