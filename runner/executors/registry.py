"""executor 注册表：按 exec_type 分发（v27 多执行器引擎）。

未知 type 抛 ExecutorError，由 main 回流 prepare 失败事件——绝不静默丢弃，
否则任务永久卡 running。terraform 只注册类型占位，run 时门控拒绝。
"""

from runner.core.exceptions import ExecutorError
from runner.core.models import StepSpec
from runner.executors.ansible_executor import AnsibleExecutor
from runner.executors.python_executor import PythonExecutor
from runner.executors.script_executor import ScriptExecutor
from runner.executors.shell_executor import ShellExecutor
from runner.executors.terraform_executor import TerraformExecutor

EXECUTORS = {
    "ansible": AnsibleExecutor,
    "shell": ShellExecutor,
    "script": ScriptExecutor,    # v36：仓库脚本推送执行（ansible script 模块）
    "python": PythonExecutor,
    "terraform": TerraformExecutor,  # 门控未开：state 方案未定，本轮拒绝执行
}


def get_executor(step: StepSpec):
    cls = EXECUTORS.get(step.type)
    if cls is None:
        raise ExecutorError(
            f"未知 exec_type: {step.type}（注册表仅支持 {sorted(EXECUTORS)}）")
    return cls()
