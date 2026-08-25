"""terraform 执行器 —— P2 占位，P1 不实现（设计文档 §8 分期）。

P2 要点预留：
- stdout 逐行回流 job-events
- state 走 bingops http backend + OSS blob（state 不存 Vault）
- 失败自动逆序回滚链（auto_rollback runbook 级 opt-in）
"""


class TerraformExecutor:
    """P2 点亮。保留类名供 get_executor 引用。"""
