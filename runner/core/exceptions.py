"""runner 异常体系。所有自定义异常继承 RunnerError，main 统一捕获并转为 failed 事件。"""


class RunnerError(Exception):
    """runner 可预期错误的基类，error_message 会随 step_finished 事件回流。"""


class ConfigError(RunnerError):
    """配置缺失或非法。"""


class VaultError(RunnerError):
    """Vault 认证或取钥失败。"""


class GitFetchError(RunnerError):
    """clone pinned tag 失败（tag 不存在 / 网络不可达等）。"""


class InventoryError(RunnerError):
    """目标列表为空或 inventory 构建失败。"""


class ExecutorError(RunnerError):
    """executor 准备或运行阶段错误（非 playbook 本身失败）。"""


class StepTimeout(RunnerError):
    """step 超时被强制终止。"""
