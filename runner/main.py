"""runner 主流程：消费 job-dispatch → 编排执行 → 回流 job-events。

职责（设计文档 §9.3）：
- message_id 去重（at-least-once 重放防护，进程内有界 LRU）
- 信号量限流：单 runner 并发 execution 数 = RUNNER_MAX_CONCURRENT
- 优雅退出：SIGTERM/SIGINT 停消费，等待在跑 execution 的当前 step 结束
- 回滚：command=rollback 时对 rollbackable 步骤逆序重跑（bingops_action=undo）

纪律：runner 不写业务表，只通过 job-events 回流；准备阶段（git/vault/inventory）
失败用 step_key="prepare" 的失败事件告知控制面。
"""

import logging
import os
import shutil
import signal
import threading
import uuid
from collections import OrderedDict
from itertools import count

from runner.core.config import Config
from runner.core.exceptions import RunnerError
from runner.core.logging import setup_logging
from runner.core.models import DispatchMessage, StepEvent, StepSpec
from runner.executors import get_executor
from runner.git_fetcher import GitFetcher
from runner.inventory import InventoryBuilder
from runner.kafka.consumer import DispatchConsumer
from runner.kafka.producer import EventsProducer
from runner.redact import Redactor
from runner.vault_client import VaultClient

logger = logging.getLogger(__name__)

_DEDUP_MAX = 10_000


class JobWorker:
    def __init__(self, config: Config, producer: EventsProducer) -> None:
        self._config = config
        self._producer = producer
        self._vault = VaultClient(config)
        self._git = GitFetcher(config)
        self._inventory = InventoryBuilder(self._vault)

        self._semaphore = threading.BoundedSemaphore(config.max_concurrent_executions)
        self._dedup: OrderedDict[str, bool] = OrderedDict()
        self._dedup_lock = threading.Lock()
        self._threads: list[threading.Thread] = []
        self._threads_lock = threading.Lock()

    # ------------------------------------------------------------------
    # 消费入口
    # ------------------------------------------------------------------
    def handle(self, payload: dict) -> None:
        message_id = payload.get("message_id", "")
        if self._seen(message_id):
            logger.info("重复 message_id=%s，跳过", message_id)
            return
        try:
            msg = DispatchMessage.from_dict(payload)
        except (KeyError, TypeError, ValueError) as e:
            # 契约校验失败：尽力回流 prepare 失败事件，不让 execution 在控制面假死
            logger.error("dispatch 契约校验失败: %s payload=%s", e, payload)
            execution_id = payload.get("execution_id")
            if execution_id is not None:
                # attempt_type 跟随 command：rollback 下发失败时控制面才能收到回滚链的收尾信号
                attempt = "rollback" if payload.get("command") == "rollback" else "do"
                self._emit_contract_failure(int(execution_id), str(e), attempt)
            return
        logger.info("收到 dispatch: command=%s execution_id=%s code_ref=%s",
                    msg.command, msg.execution_id, msg.code_ref)
        t = threading.Thread(
            target=self._guarded_run, args=(msg,),
            name=f"exec-{msg.execution_id}", daemon=False,
        )
        with self._threads_lock:
            self._threads = [x for x in self._threads if x.is_alive()]
            self._threads.append(t)
        t.start()

    def _seen(self, message_id: str) -> bool:
        if not message_id:
            return False
        with self._dedup_lock:
            if message_id in self._dedup:
                return True
            self._dedup[message_id] = True
            while len(self._dedup) > _DEDUP_MAX:
                self._dedup.popitem(last=False)
            return False

    def _emit_contract_failure(self, execution_id: int, error: str,
                               attempt_type: str = "do") -> None:
        """消息无法解析时，以 prepare 伪步骤告知控制面 execution 失败。"""
        try:
            self._producer.send_event(StepEvent(
                message_id=str(uuid.uuid4()), execution_id=execution_id,
                step_key="prepare", attempt_type=attempt_type,
                event_type="step_started", seq=1,
            ))
            self._producer.send_event(StepEvent(
                message_id=str(uuid.uuid4()), execution_id=execution_id,
                step_key="prepare", attempt_type=attempt_type,
                event_type="step_finished", seq=2,
                status="failed", error=f"dispatch 契约校验失败: {error}",
            ))
            self._producer.flush()
        except Exception:
            logger.exception("契约失败事件回流失败 execution_id=%s", execution_id)

    def _guarded_run(self, msg: DispatchMessage) -> None:
        # 限流：超出并发上限时在此等待空位
        with self._semaphore:
            try:
                self._run_execution(msg)
            except Exception:
                logger.exception("execution %s 意外异常", msg.execution_id)
            finally:
                self._producer.flush()

    # ------------------------------------------------------------------
    # 单 execution 编排
    # ------------------------------------------------------------------
    def _run_execution(self, msg: DispatchMessage) -> None:
        seq = count(1)
        redactor = Redactor()
        exec_dir = os.path.join(
            self._config.workdir, "executions",
            f"{msg.execution_id}-{msg.message_id[:8]}")
        current_step: StepSpec | None = None

        def emit(step_key: str, attempt_type: str, event_type: str, **kw) -> None:
            event = StepEvent(
                message_id=str(uuid.uuid4()),
                execution_id=msg.execution_id,
                step_key=step_key, attempt_type=attempt_type,
                event_type=event_type, seq=next(seq), **kw,
            )
            self._producer.send_event(event)

        def on_log(step_key: str, attempt_type: str):
            def cb(level: str, host: str | None, line: str) -> None:
                emit(step_key, attempt_type, "log", level=level,
                     host=host, line=redactor.apply(line))
            return cb

        is_rollback = msg.command == "rollback"
        attempt = "rollback" if is_rollback else "do"
        try:
            # ---- 准备阶段：git clone pinned tag ----
            repo_dir = self._git.fetch(msg.code_ref, os.path.join(exec_dir, "repo"))

            # ---- 步骤序列：回滚 = rollbackable 步骤逆序 + undo ----
            if is_rollback:
                steps = [s for s in reversed(msg.steps) if s.rollbackable]
                extra_vars = {**msg.params, "bingops_action": "undo"}
                if not steps:
                    logger.warning("execution %s 无可回滚步骤", msg.execution_id)
                    return
            else:
                steps = msg.steps
                extra_vars = dict(msg.params)

            # ---- inventory：Vault 取钥 + keyfile（上下文退出即清理）----
            inv_dir = os.path.join(exec_dir, "inventory")
            with self._inventory.build(msg.targets, inv_dir, redactor) as inv:
                for step in steps:
                    current_step = step
                    emit(step.key, attempt, "step_started")
                    timeout = step.timeout_sec or self._config.default_step_timeout_sec
                    try:
                        executor = get_executor(step)
                        result = executor.run(
                            step=step, repo_dir=repo_dir,
                            inventory_path=inv["inventory_path"],
                            envvars=inv["envvars"], extra_vars=extra_vars,
                            workdir=os.path.join(exec_dir, f"step-{step.key}"),
                            timeout_sec=timeout,
                            event_cb=on_log(step.key, attempt),
                        )
                    except RunnerError as e:
                        emit(step.key, attempt, "step_finished", status="failed",
                             exit_code=None, error=str(e))
                        logger.error("step %s 失败: %s", step.key, e)
                        return  # 失败即停，后续步骤不再执行（控制面置 failed）
                    emit(step.key, attempt, "step_finished",
                         status="success" if result.ok else "failed",
                         exit_code=result.rc, error=result.error)
                    if not result.ok:
                        return

        except RunnerError as e:
            # 准备阶段失败（git/vault/inventory）：以 prepare 伪步骤告知控制面
            logger.error("execution %s 准备阶段失败: %s", msg.execution_id, e)
            emit("prepare", attempt, "step_started")
            emit("prepare", attempt, "step_finished",
                 status="failed", error=str(e))
        finally:
            shutil.rmtree(exec_dir, ignore_errors=True)

    # ------------------------------------------------------------------
    # 优雅退出
    # ------------------------------------------------------------------
    def wait_idle(self, timeout_sec: float = 3600.0) -> None:
        """等待在跑 execution 结束（当前 step 自然收尾，受 step timeout 约束）。"""
        with self._threads_lock:
            threads = list(self._threads)
        for t in threads:
            t.join(timeout_sec)


def main() -> None:
    config = Config.from_env()
    setup_logging(config.log_level)
    logger.info("bingops-runner 启动: max_concurrent=%s",
                config.max_concurrent_executions)

    producer = EventsProducer(config)
    worker = JobWorker(config, producer)
    consumer = DispatchConsumer(config, worker.handle)

    def _shutdown(signum, _frame) -> None:
        logger.info("收到信号 %s，停止消费并等待在跑任务...", signum)
        consumer.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        consumer.run()
    finally:
        worker.wait_idle()
        consumer.close()
        producer.close()
        logger.info("bingops-runner 已退出")


if __name__ == "__main__":
    main()
