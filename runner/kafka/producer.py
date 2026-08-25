"""job-events 生产者：runner → bingops 事件回流。

at-least-once：acks=all 保证 broker 侧不丢；bingops 侧靠 message_id 去重消化重复。
"""

import json
import logging
from typing import Any

from confluent_kafka import Producer

from runner.core.config import Config
from runner.core.models import StepEvent

logger = logging.getLogger(__name__)


class EventsProducer:
    def __init__(self, config: Config) -> None:
        self._topic = config.kafka_events_topic
        self._producer = Producer({
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,  # 日志行高频，轻微攒批降低压力
        })

    def send_event(self, event: StepEvent) -> None:
        self._send(event.to_dict())

    def _send(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        # execution_id 作为分区 key：同一 execution 的事件保序
        self._producer.produce(
            self._topic,
            value=body,
            key=str(payload["execution_id"]).encode("utf-8"),
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err is not None:
            logger.error("job-events 投递失败: %s", err)

    def flush(self, timeout_sec: float = 10.0) -> None:
        remaining = self._producer.flush(timeout_sec)
        if remaining > 0:
            logger.warning("flush 超时，仍有 %s 条事件未投递", remaining)

    def close(self) -> None:
        self.flush()
