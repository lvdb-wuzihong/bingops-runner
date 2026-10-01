"""script 执行器（v36 新增）：entry 是**仓库内脚本文件路径**。

- run_on=target：ansible `script` 模块把脚本从 runner 推到目标机临时目录执行
  （目标机不需预置该文件），params/secrets 经 task 级 environment 送达
- run_on=local：subprocess `bash <script>`，cwd=仓库根

与 shell 的分界是"这段代码归谁管"：script 的内容在 git（随 code_ref 固定、可评审），
shell 的内容在平台（内联 entry）。禁止拿 shell 模块去 `bash scripts/x.sh`：
那个路径相对目标机文件系统，必失败（旧文档写错过）。
"""

import logging
import os

from runner.core.exceptions import ExecutorError
from runner.core.models import StepSpec
from runner.executors import StepContext, StepResult, run_local_stream
from runner.executors.ansible_executor import run_playbook_batches, write_env_playbook

logger = logging.getLogger(__name__)


class ScriptExecutor:

    def run(self, step: StepSpec, ctx: StepContext) -> StepResult:
        script = os.path.join(ctx.repo_dir, step.entry)
        if not step.entry or not os.path.isfile(script):
            raise ExecutorError(f"script entry 不存在: {step.entry}")
        if step.run_on == "local":
            return run_local_stream(["bash", script], ctx)

        playbook = write_env_playbook(
            ctx.workdir, "ansible.builtin.script", {"path": script},
            self._target_env(ctx), "script")
        return run_playbook_batches(playbook, ctx)

    @staticmethod
    def _target_env(ctx: StepContext) -> dict[str, str]:
        env = {k: str(v) for k, v in ctx.params.items()}
        env.update(ctx.secrets_env)
        return env
