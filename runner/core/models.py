"""消息契约 dataclass：与 bingops 侧 Kafka 契约（设计文档 §9.2）一一对应。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class Target:
    """目标主机（来自 CMDB 目标快照）。"""

    resource_id: int
    name: str
    ip: str
    ssh_user: str
    ssh_key_ref: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Target":
        return cls(
            resource_id=int(d["resource_id"]),
            name=d["name"],
            ip=d["ip"],
            ssh_user=d.get("ssh_user", "ops"),
            ssh_key_ref=d["ssh_key_ref"],
        )


@dataclass
class StepSpec:
    """步骤定义快照（随 dispatch 下发，执行期间不受 runbook 编辑影响）。"""

    key: str
    name: str
    type: str
    playbook: str | None = None
    timeout_sec: int | None = None
    serial: str | None = None
    batch_pause_sec: int | None = None
    rollbackable: bool = False

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "StepSpec":
        return cls(
            key=d["key"],
            name=d.get("name") or d["key"],
            type=d.get("type", "ansible"),
            playbook=d.get("playbook"),
            timeout_sec=d.get("timeout_sec"),
            serial=d.get("serial"),
            batch_pause_sec=d.get("batch_pause_sec"),
            rollbackable=bool(d.get("rollbackable", False)),
        )


@dataclass
class DispatchMessage:
    """job-dispatch 消息；command=execute | rollback。"""

    message_id: str
    command: str
    execution_id: int
    code_ref: str
    params: dict[str, Any]
    targets: list[Target]
    steps: list[StepSpec]
    rollback_of: int | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DispatchMessage":
        return cls(
            message_id=d["message_id"],
            command=d["command"],
            execution_id=int(d["execution_id"]),
            code_ref=d["code_ref"],
            params=d.get("params") or {},
            targets=[Target.from_dict(t) for t in d.get("targets") or []],
            steps=[StepSpec.from_dict(s) for s in d.get("steps") or []],
            rollback_of=d.get("rollback_of"),
        )


@dataclass
class StepEvent:
    """job-events 消息体（runner → bingops）。"""

    message_id: str
    execution_id: int
    step_key: str
    attempt_type: str  # do | rollback
    event_type: str  # step_started | log | step_finished
    seq: int | None = None
    level: str = "info"
    host: str | None = None
    line: str | None = None
    status: str | None = None  # success | failed（step_finished 时）
    exit_code: int | None = None
    error: str | None = None
    timestamp: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "execution_id": self.execution_id,
            "step_key": self.step_key,
            "attempt_type": self.attempt_type,
            "event_type": self.event_type,
            "seq": self.seq,
            "level": self.level,
            "host": self.host,
            "line": self.line,
            "status": self.status,
            "exit_code": self.exit_code,
            "error": self.error,
            "timestamp": self.timestamp,
        }
