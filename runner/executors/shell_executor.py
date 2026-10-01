"""shell 执行器：entry 恒为**内联命令**（v36 语义收敛：在目标机的 shell 里执行）。

- run_on=target：动态单任务 playbook + shell 模块 + task 级 environment
  （修正存量 bug：ad-hoc 的 envvars 只在控制节点生效，目标命令看不到 params/secrets）
- run_on=local：subprocess 流式执行

仓库脚本请用 exec_type=script（ScriptExecutor）——那个路径相对目标机文件系统不存在。
"""

import logging

from runner.core.exceptions import ExecutorError
from runner.core.models import StepSpec
from runner.executors import StepContext, StepResult, run_local_stream
from runner.executors.ansible_executor import run_playbook_batches, write_env_playbook

logger = logging.getLogger(__name__)


class ShellExecutor:

    def run(self, step: StepSpec, ctx: StepContext) -> StepResult:
        if not step.entry:
            raise ExecutorError("shell 步骤缺少 entry（内联命令）")
        if step.run_on == "local":
            return run_local_stream(step.entry, ctx, shell=True)

        playbook = write_env_playbook(
            ctx.workdir, "ansible.builtin.shell", step.entry,
            self._target_env(ctx), "shell")
        return run_playbook_batches(playbook, ctx)

    @staticmethod
    def _target_env(ctx: StepContext) -> dict[str, str]:
        env = {k: str(v) for k, v in ctx.params.items()}
        env.update(ctx.secrets_env)
        return env
