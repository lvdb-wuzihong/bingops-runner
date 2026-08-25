"""executors：步骤执行器。P1 仅 ansible；terraform 为 P2 占位。"""

from collections.abc import Callable
from dataclasses import dataclass

from runner.core.exceptions import ExecutorError
from runner.core.models import StepSpec


@dataclass
class StepResult:
    rc: int
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.rc == 0


# executor 内部产生的日志行回调：(level, host, line)
EventCallback = Callable[[str, str | None, str], None]


def get_executor(step: StepSpec):
    """按步骤类型分发 executor。P2 点亮 terraform 分支。"""
    if step.type == "ansible":
        from runner.executors.ansible_executor import AnsibleExecutor
        return AnsibleExecutor()
    if step.type == "terraform":
        raise ExecutorError("terraform 步骤 P2 才点亮，当前不可执行")
    raise ExecutorError(f"未知步骤类型: {step.type}")
