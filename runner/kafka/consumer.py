"""job-dispatch 消费者：bingops → runner 下发执行/回滚命令。

手动提交 offset：消息处理完（或至少进入去重判断后）才 commit，
配合 bingops 侧 message_id 去重消化 at-least-once 重放。
"""

import json
import logging
from collections.abc import Callable

from confluent_kafka import Consumer, KafkaError

from runner.core.config import Config

logger = logging.getLogger(__name__)

Handler = Callable[[dict], None]


class DispatchConsumer:
    def __init__(self, config: Config, handler: Handler) -> None:
        self._topic = config.kafka_dispatch_topic
        self._handler = handler
        self._running = False
        self._consumer = Consumer({
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "group.id": config.kafka_group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 30000,
        })

    def run(self) -> None:
        """阻塞轮询，直到 stop() 被调用。"""
        self._consumer.subscribe([self._topic])
        self._running = True
        logger.info("开始消费 topic=%s", self._topic)
        while self._running:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("kafka 错误: %s", msg.error())
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                self._handler(payload)
            except Exception:
                # handler 内部应自行兜底发 failed 事件；这里防止消费线程被打死
                logger.exception("dispatch 消息处理异常 offset=%s", msg.offset())
            finally:
                self._consumer.commit(message=msg)

    def stop(self) -> None:
        self._running = False

    def close(self) -> None:
        self._running = False
        self._consumer.close()
