"""shell 执行器：entry 恒为命令字符串（v27 语义收敛）。

- run_on=target：复用 ansible ad-hoc（`ansible -i inv -m shell -a "<entry>"`），
  不自研 paramiko——直接复用 inventory / Vault keyfile / become 与日志格式，零新增 SSH 代码
- run_on=local：subprocess，cwd=仓库根

回滚（v30 统一约定）：重跑同一 entry 并注入 BINGOPS_ACTION=undo env；
内联命令（df -h 这种）天然没有 undo，应把 rollbackable 关成 false。
"""

import logging
import os
import subprocess
import time

import ansible_runner

from runner.core.exceptions import ExecutorError, StepTimeout
from runner.core.models import StepSpec
from runner.executors import StepContext, StepResult
from runner.executors.ansible_executor import (finish_result, make_event_handler)

logger = logging.getLogger(__name__)


class ShellExecutor:

    def run(self, step: StepSpec, ctx: StepContext) -> StepResult:
        if not step.entry:
            raise ExecutorError("shell 步骤缺少 entry（命令字符串）")
        if step.run_on == "local":
            return self._run_local(step, ctx)
        return self._run_remote(step, ctx)

    # ------------------------------------------------------------------
    # local：subprocess 流式输出
    # ------------------------------------------------------------------
    def _run_local(self, step: StepSpec, ctx: StepContext) -> StepResult:
        env = ctx.subprocess_env(include_params=True)
        deadline = time.monotonic() + ctx.timeout_sec
        logger.info("shell local 执行: %s", step.entry)
        proc = subprocess.Popen(
            step.entry, shell=True, cwd=ctx.repo_dir, env=env,
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
        logger.info("shell local 结束: rc=%s", rc)
        if rc != 0:
            return StepResult(rc=rc, error=f"shell 退出码 {rc}")
        return StepResult(rc=0)

    # ------------------------------------------------------------------
    # target：ansible ad-hoc module=shell
    # ------------------------------------------------------------------
    def _run_remote(self, step: StepSpec, ctx: StepContext) -> StepResult:
        if ctx.inventory is None:
            raise ExecutorError("run_on=target 但 inventory 缺失（targets 为空？）")
        batch_dir = os.path.join(ctx.workdir, f"batch-{int(time.time() * 1000)}")
        os.makedirs(batch_dir, exist_ok=True)

        envvars = dict(ctx.inventory["envvars"])
        envvars.update(ctx.secrets_env)
        envvars["BINGOPS_ACTION"] = ctx.action

        deadline = time.monotonic() + ctx.timeout_sec

        def cancel_callback() -> bool:
            return time.monotonic() >= deadline

        event_handler, host_event_count = make_event_handler(ctx.event_cb)

        logger.info("shell remote 执行: %s", step.entry)
        res = ansible_runner.run(
            private_data_dir=batch_dir,
            inventory=ctx.inventory["inventory_path"],
            module="shell",
            module_args=step.entry,
            host_pattern="all",
            envvars=envvars,
            event_handler=event_handler,
            cancel_callback=cancel_callback,
            rotate_artifacts=1,
            quiet=True,
        )
        return finish_result(res, batch_dir, ctx.timeout_sec, host_event_count)
