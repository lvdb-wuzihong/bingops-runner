"""ansible-runner 执行器：结构化事件回调 → 日志行，支持灰度批次与超时强杀。

灰度（设计文档决策 8）：
- serial: "N"（绝对批大小）或 "30%"（百分比），目标列表按批切分依次执行
- batch_pause_sec: 批间暂停，给观测留窗口
- 任一批失败即终止后续批次

超时：ansible-runner cancel_callback 周期轮询，到点返回 True 强杀。
"""

import json
import glob
import logging
import math
import os
import time
from collections.abc import Callable
from typing import Any

import ansible_runner

from runner.core.exceptions import ExecutorError, StepTimeout
from runner.core.models import StepSpec
from runner.executors import StepResult

logger = logging.getLogger(__name__)

EventCallback = Callable[[str, str | None, str], None]


class AnsibleExecutor:

    def run(self, step: StepSpec, repo_dir: str, inventory_path: str,
            envvars: dict[str, str], extra_vars: dict[str, Any],
            workdir: str, timeout_sec: int, event_cb: EventCallback) -> StepResult:
        playbook = os.path.join(repo_dir, step.playbook or "")
        if not step.playbook or not os.path.isfile(playbook):
            raise ExecutorError(f"playbook 不存在: {step.playbook}")

        batches = self._split_batches(inventory_path, step.serial)
        if step.serial:
            event_cb("info", None,
                     f"灰度模式: serial={step.serial}，共 {len(batches)} 批")

        for idx, batch in enumerate(batches):
            if idx > 0 and step.batch_pause_sec:
                event_cb("info", None,
                         f"批间暂停 {step.batch_pause_sec}s...")
                time.sleep(step.batch_pause_sec)

            result = self._run_batch(
                step=step, playbook=playbook, hosts=batch,
                inventory_path=inventory_path, envvars=envvars,
                extra_vars=extra_vars, workdir=workdir,
                timeout_sec=timeout_sec, event_cb=event_cb,
            )
            if not result.ok:
                return result
        return StepResult(rc=0)

    # ------------------------------------------------------------------
    # 灰度分批
    # ------------------------------------------------------------------
    def _split_batches(self, inventory_path: str,
                       serial: str | None) -> list[list[str]]:
        with open(inventory_path, encoding="utf-8") as f:
            hosts = list(json.load(f)["all"]["hosts"].keys())
        if not serial:
            return [hosts]

        serial = serial.strip()
        if serial.endswith("%"):
            pct = float(serial[:-1]) / 100.0
            size = max(1, math.ceil(len(hosts) * pct))
        else:
            size = int(serial)
        return [hosts[i:i + size] for i in range(0, len(hosts), size)]

    @staticmethod
    def _subset_inventory(inventory_path: str, hosts: list[str],
                          dest: str) -> str:
        """从全量 inventory 抽出子集，写为临时 inventory 供本批使用。"""
        with open(inventory_path, encoding="utf-8") as f:
            full = json.load(f)
        subset = {"all": {"hosts": {h: full["all"]["hosts"][h] for h in hosts}}}
        with open(dest, "w", encoding="utf-8") as f:
            json.dump(subset, f, indent=2)
        return dest

    # ------------------------------------------------------------------
    # 单批执行
    # ------------------------------------------------------------------
    def _run_batch(self, step: StepSpec, playbook: str, hosts: list[str],
                   inventory_path: str, envvars: dict[str, str],
                   extra_vars: dict[str, Any], workdir: str,
                   timeout_sec: int, event_cb: EventCallback) -> StepResult:
        batch_dir = os.path.join(workdir, f"batch-{int(time.time() * 1000)}")
        os.makedirs(batch_dir, exist_ok=True)
        inv = self._subset_inventory(inventory_path, hosts,
                                     os.path.join(batch_dir, "inventory.json"))

        deadline = time.monotonic() + timeout_sec

        def cancel_callback() -> bool:
            return time.monotonic() >= deadline

        def event_handler(data: dict) -> bool:
            level, host, line = self._describe_event(data)
            if line:
                # error 行同时落 runner 进程日志：bingops 日志链路出问题时 kubectl logs 仍可排障
                if level == "error":
                    logger.error("ansible: host=%s %s", host, line)
                event_cb(level, host, line)
            return True

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

        if res.status == "canceled" or res.rc == "timeout":
            self._log_artifact_tail(batch_dir)
            raise StepTimeout(f"step 超时（{timeout_sec}s）被强制终止")
        logger.info("ansible 批次结束: status=%s rc=%s", res.status, res.rc)
        rc = int(res.rc) if isinstance(res.rc, int) else 1
        if rc != 0:
            # 解析/启动阶段的错误不产生 host 事件，只能从 stdout 原文看
            snippet = self._log_artifact_tail(batch_dir)
            error = f"playbook 退出码 {rc}"
            if res.status:
                error += f"（status={res.status}）"
            if snippet:
                error += f"：{snippet}"
            event_cb("error", None, error)
            return StepResult(rc=rc, error=error)
        return StepResult(rc=0)

    # ------------------------------------------------------------------
    # 失败时从 artifacts 捞 ansible stdout 原文
    # ------------------------------------------------------------------
    @staticmethod
    def _log_artifact_tail(batch_dir: str, lines: int = 60) -> str:
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

    # ------------------------------------------------------------------
    # ansible-runner 事件 → 日志行
    # ------------------------------------------------------------------
    @staticmethod
    def _describe_event(data: dict) -> tuple[str, str | None, str | None]:
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
