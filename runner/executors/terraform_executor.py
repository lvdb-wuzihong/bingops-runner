"""terraform 执行器 —— 类型占位，门控未开（state 方案未定，P2 点亮）。

注册表里保留 type 是为了让"未知类型"与"门控未开"两种失败可区分；
控制面侧 BINGOPS_JOB_STEP_TYPES 默认不含 terraform，正常到不了这里。

P2 要点预留：
- stdout 逐行回流 job-events
- state 走 bingops http backend + OSS blob（state 不存 Vault）
- 失败自动逆序回滚链（自动回滚解冻时重建在 execution 层）
"""

from runner.core.exceptions import ExecutorError
from runner.core.models import StepSpec
from runner.executors import StepContext, StepResult


class TerraformExecutor:

    def run(self, step: StepSpec, ctx: StepContext) -> StepResult:
        raise ExecutorError(
            "terraform 执行器门控未开（state 后端方案未定，P2 点亮）；"
            "平台侧应通过 BINGOPS_JOB_STEP_TYPES 拒绝创建此类任务")
