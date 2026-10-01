"""ansible-runner 执行器：结构化事件回调 → 日志行，支持多目标分批与超时强杀。

多目标并发度（v30）：读 runner 部署级配置 max_parallel_hosts（0=一次全部，1=逐台）；
任一批失败即终止后续批次。超时：cancel_callback 周期轮询，到点强杀。

本模块同时提供跨 executor 共享的基础设施：
- run_playbook_batches：分批跑 playbook（ansible/shell target/script target 共用）
- write_env_playbook：生成单任务动态 playbook，用 task 级 environment 把 params/secrets
  送进目标机（ad-hoc 的 envvars 只在控制节点生效，v36 修正存量 bug）
- describe_event / log_artifact_tail / finish_result 等事件与结果管道
"""

import json
import glob
import logging
import os
import time
from typing import Any

import ansible_runner
import yaml

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
    """返回 (event_handler, 主机事件计数器)；error 行同时落 runner 进程日志。

    除任务状态摘要行外，还把命令结果正文（stdout/stderr/debug msg）逐行上报，
    否则 shell/script 任务在控制面只能看到 [changed] 看不到执行输出。
    """
    host_event_count = {"n": 0}

    def event_handler(data: dict) -> bool:
        event = data.get("event", "")
        if event.startswith("runner_on_"):
            host_event_count["n"] += 1
        level, host, line = describe_event(data)
        if line:
            if level == "error":
                logger.error("ansible: host=%s %s", host, line)
            event_cb(level, host, line)
        if event in ("runner_on_ok", "runner_on_failed"):
            res = (data.get("event_data") or {}).get("res") or {}
            # debug 模块的输出在 msg；命令/脚本在 stdout/stderr
            if event == "runner_on_ok" and res.get("msg"):
                event_cb("info", host, str(res["msg"]))
            stdout = res.get("stdout_lines") or (res.get("stdout") or "").splitlines()
            for l in stdout:
                event_cb("info", host, str(l))
            stderr = res.get("stderr_lines") or (res.get("stderr") or "").splitlines()
            for l in stderr:
                event_cb("warn", host, str(l))
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


def write_env_playbook(workdir: str, module: str, args: Any,
                       env: dict[str, str], label: str) -> str:
    """生成单任务动态 playbook（hosts: all + task 级 environment）。

    shell/script 的 target 执行用：目标机命令看不到控制节点的 envvars，
    必须把 params/secrets 写进 environment 才能送达脚本/命令。
    文件含明文 secret，0600，随 exec_dir 统一清理；日志栏另有 redact 兑底。
    """
    os.makedirs(workdir, exist_ok=True)
    play = [{"name": f"bingops {label} step", "hosts": "all", "gather_facts": False,
             "tasks": [{"name": f"{label}: {str(args)[:120]}",
                        module: args,
                        "environment": {k: str(v) for k, v in env.items()}}]}]
    path = os.path.join(workdir, f"{label}-playbook.yml")
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(play, f, allow_unicode=True)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return path


def run_playbook_batches(playbook: str, ctx: StepContext,
                         extra_vars: dict[str, Any] | None = None) -> StepResult:
    """按 max_parallel_hosts 分批执行 playbook（ansible/shell/script target 共用）。"""
    if ctx.inventory is None:
        raise ExecutorError("target 执行需要 inventory（run_on=target 且 targets 非空）")
    envvars = dict(ctx.inventory["envvars"])
    # role 搜索路径：覆盖仓库两种常见布局，绝对路径不受 ansible cwd 影响
    envvars.setdefault("ANSIBLE_ROLES_PATH", os.pathsep.join([
        os.path.join(ctx.repo_dir, "ansible", "roles"),
        os.path.join(ctx.repo_dir, "roles"),
    ]))
    # 控制节点 env：ansible playbook 的 lookup('env', KEY) 从这里读 secrets
    envvars.update(ctx.secrets_env)

    batches = split_batches(ctx.inventory["inventory_path"], ctx.max_parallel_hosts)
    if len(batches) > 1:
        ctx.event_cb("info", None,
                     f"多目标并发度 max_parallel_hosts={ctx.max_parallel_hosts}，"
                     f"共 {len(batches)} 批")
    for batch in batches:
        result = _run_batch(playbook, batch, ctx, envvars, extra_vars or {})
        if not result.ok:
            return result
    return StepResult(rc=0)


def _run_batch(playbook: str, hosts: list[str], ctx: StepContext,
               envvars: dict[str, str], extra_vars: dict[str, Any]) -> StepResult:
    batch_dir = os.path.join(ctx.workdir, f"batch-{int(time.time() * 1000)}")
    os.makedirs(batch_dir, exist_ok=True)
    inv = subset_inventory(ctx.inventory["inventory_path"], hosts,
                           os.path.join(batch_dir, "inventory.json"))

    deadline = time.monotonic() + ctx.timeout_sec

    def cancel_callback() -> bool:
        return time.monotonic() >= deadline

    event_handler, host_event_count = make_event_handler(ctx.event_cb)

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
    return finish_result(res, batch_dir, ctx.timeout_sec, host_event_count)


class AnsibleExecutor:

    def run(self, step: StepSpec, ctx: StepContext) -> StepResult:
        playbook = os.path.join(ctx.repo_dir, step.entry)
        if not step.entry or not os.path.isfile(playbook):
            raise ExecutorError(f"playbook 不存在: {step.entry}")
        # v37：平台不再注入 bingops_action/BINGOPS_ACTION；
        # 自带 undo 分支的存量 playbook 用 default('do') 即可正常运行
        return run_playbook_batches(playbook, ctx, extra_vars=dict(ctx.params))
