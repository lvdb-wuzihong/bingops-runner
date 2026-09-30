"""ansible-runner 执行器：结构化事件回调 → 日志行，支持多目标分批与超时强杀。

多目标并发度（v30）：不再由任务声明 serial/batch_pause_sec，而是读 runner
部署级配置 max_parallel_hosts（0=一次全部，1=逐台）；任一批失败即终止后续批次。

超时：ansible-runner cancel_callback 周期轮询，到点返回 True 强杀。
describe_event / log_artifact_tail 为模块级函数，shell 远端执行复用。
"""

import json
import glob
import logging
import os
import time
from typing import Any

import ansible_runner

from runner.core.exceptions import ExecutorError, StepTimeout
from runner.core.models import StepSpec
from runner.executors import StepContext, StepResult

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# 共享 helper（shell_executor 远端模式复用）
# ----------------------------------------------------------------------
def describe_event(data: dict) -> tuple[str, str | None, str | None]:
    """ansible-runner 事件 → (level, host, line)。"""
    event = data.get("event", "")
    ed = data.get("event_data", {}) or {}
    host = ed.get("remote_addr") or ed.get("host")
    task = ed.get("task") or ed.get("play")

    if event == "runner_on_failed":
        msg = (ed.get("res") or {}).get("msg", "")
        return "error", host, f"[FAILED] {task}: {msg}".rstrip(": ")
    if event == "runner_on_unreachable":
        return "error", host, f"[UNREACHABLE] {host}"
    if event == "runner_on_ok":
        changed = (ed.get("res") or {}).get("changed", False)
        tag = "changed" if changed else "ok"
        return "info", host, f"[{tag}] {task}"
    if event == "runner_on_skipped":
        return "info", host, f"[skipped] {task}"
    if event == "playbook_on_task_start":
        return "info", None, f"TASK [{task}]"
    if event == "playbook_on_play_start":
        return "info", None, f"PLAY [{ed.get('name') or ed.get('play')}]"
    return "debug", None, None


def log_artifact_tail(batch_dir: str, lines: int = 60) -> str:
    """记录 stdout 尾部全文，并返回一句可进 error 消息的摘要。"""
    snippet = ""
    for stdout_file in sorted(glob.glob(
            os.path.join(batch_dir, "artifacts", "*", "stdout"))):
        try:
            with open(stdout_file, encoding="utf-8", errors="replace") as f:
                tail_lines = f.readlines()[-lines:]
        except OSError as e:
            logger.warning("读取 artifact 失败: %s", e)
            continue
        tail = "".join(tail_lines)
        logger.error("ansible stdout 原文 (%s):\n%s", stdout_file, tail)
        if not snippet:
            # 优先取 [ERROR]/ERROR!/FAILED 行，否则取最后非空行
            key = [l.strip() for l in tail_lines
                   if l.strip().startswith(("[ERROR]", "ERROR!", "fatal:"))]
            pick = key[0] if key else (tail_lines[-1].strip() if tail_lines else "")
            snippet = pick[:200]
    return snippet


def split_batches(inventory_path: str, max_parallel_hosts: int) -> list[list[str]]:
    """按部署级并发度切批（v30）；<=0 表示一次全部，1 即逐台执行。"""
    with open(inventory_path, encoding="utf-8") as f:
        hosts = list(json.load(f)["all"]["hosts"].keys())
    if max_parallel_hosts <= 0 or max_parallel_hosts >= len(hosts):
        return [hosts]
    return [hosts[i:i + max_parallel_hosts]
            for i in range(0, len(hosts), max_parallel_hosts)]


def subset_inventory(inventory_path: str, hosts: list[str], dest: str) -> str:
    """从全量 inventory 抽出子集，写为临时 inventory 供本批使用。"""
    with open(inventory_path, encoding="utf-8") as f:
        full = json.load(f)
    subset = {"all": {"hosts": {h: full["all"]["hosts"][h] for h in hosts}}}
    with open(dest, "w", encoding="utf-8") as f:
        json.dump(subset, f, indent=2)
    return dest


def make_event_handler(event_cb) -> tuple[Any, dict]:
    """返回 (event_handler, 主机事件计数器)；error 行同时落 runner 进程日志。"""
    host_event_count = {"n": 0}

    def event_handler(data: dict) -> bool:
        if data.get("event", "").startswith("runner_on_"):
            host_event_count["n"] += 1
        level, host, line = describe_event(data)
        if line:
            if level == "error":
                logger.error("ansible: host=%s %s", host, line)
            event_cb(level, host, line)
        return True

    return event_handler, host_event_count


def finish_result(res, batch_dir: str, timeout_sec: int,
                  host_event_count: dict) -> StepResult:
    """ansible-runner 返回对象 → StepResult（超时/空跑/失败统一判定）。"""
    if res.status == "canceled" or res.rc == "timeout":
        log_artifact_tail(batch_dir)
        raise StepTimeout(f"step 超时（{timeout_sec}s）被强制终止")
    logger.info("ansible 批次结束: status=%s rc=%s", res.status, res.rc)
    rc = int(res.rc) if isinstance(res.rc, int) else 1
    if rc == 0 and host_event_count["n"] == 0:
        # 典型原因：play 的 hosts: 写了固定组名，与 runner ad-hoc inventory 不匹配
        log_artifact_tail(batch_dir)
        return StepResult(rc=1,
                          error="play 未匹配到任何主机（检查 play hosts: 是否为 all）")
    if rc != 0:
        # 解析/启动阶段的错误不产生 host 事件，只能从 stdout 原文看
        snippet = log_artifact_tail(batch_dir)
        error = f"playbook 退出码 {rc}"
        if res.status:
            error += f"（status={res.status}）"
        if snippet:
            error += f"：{snippet}"
        return StepResult(rc=rc, error=error)
    return StepResult(rc=0)


class AnsibleExecutor:

    def run(self, step: StepSpec, ctx: StepContext) -> StepResult:
        if ctx.inventory is None:
            raise ExecutorError("ansible 步骤需要 inventory（run_on=target 且 targets 非空）")
        playbook = os.path.join(ctx.repo_dir, step.entry)
        if not step.entry or not os.path.isfile(playbook):
            raise ExecutorError(f"playbook 不存在: {step.entry}")

        # role 搜索路径：覆盖仓库两种常见布局，绝对路径不受 ansible cwd 影响
        env = dict(ctx.inventory["envvars"])
        env.setdefault("ANSIBLE_ROLES_PATH", os.pathsep.join([
            os.path.join(ctx.repo_dir, "ansible", "roles"),
            os.path.join(ctx.repo_dir, "roles"),
        ]))
        env.update(ctx.secrets_env)
        env["BINGOPS_ACTION"] = ctx.action

        # do/undo 契约：存量 playbook 读 extra_vars.bingops_action
        extra_vars = {**ctx.params, "bingops_action": ctx.action}

        batches = split_batches(ctx.inventory["inventory_path"], ctx.max_parallel_hosts)
        if len(batches) > 1:
            ctx.event_cb("info", None,
                         f"多目标并发度 max_parallel_hosts={ctx.max_parallel_hosts}，"
                         f"共 {len(batches)} 批")

        for batch in batches:
            result = self._run_batch(
                step=step, playbook=playbook, hosts=batch,
                inventory_path=ctx.inventory["inventory_path"],
                envvars=env, extra_vars=extra_vars, workdir=ctx.workdir,
                timeout_sec=ctx.timeout_sec, event_cb=ctx.event_cb,
            )
            if not result.ok:
                return result
        return StepResult(rc=0)

    # ------------------------------------------------------------------
    # 单批执行
    # ------------------------------------------------------------------
    def _run_batch(self, step: StepSpec, playbook: str, hosts: list[str],
                   inventory_path: str, envvars: dict[str, str],
                   extra_vars: dict[str, Any], workdir: str,
                   timeout_sec: int, event_cb) -> StepResult:
        batch_dir = os.path.join(workdir, f"batch-{int(time.time() * 1000)}")
        os.makedirs(batch_dir, exist_ok=True)
        inv = subset_inventory(inventory_path, hosts,
                               os.path.join(batch_dir, "inventory.json"))

        deadline = time.monotonic() + timeout_sec

        def cancel_callback() -> bool:
            return time.monotonic() >= deadline

        event_handler, host_event_count = make_event_handler(event_cb)

        logger.info("ansible 批次执行: playbook=%s hosts=%s", playbook, hosts)
        res = ansible_runner.run(
            private_data_dir=batch_dir,
            playbook=playbook,
            inventory=inv,
            extravars=dict(extra_vars),
            envvars=dict(envvars),
            event_handler=event_handler,
            cancel_callback=cancel_callback,
            rotate_artifacts=1,
            quiet=True,
        )
        return finish_result(res, batch_dir, timeout_sec, host_event_count)
