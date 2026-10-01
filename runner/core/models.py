"""消息契约 dataclass：与 bingops 侧 Kafka 契约（设计文档 §9.2）一一对应。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class Target:
    """目标主机（来自 CMDB 目标快照）。

    凭据字段可缺省，由消息级 connection 打底后在 DispatchMessage 解析时合并；
    become_password 只带钥匙名，runner 现场从 Vault 取。
    """

    resource_id: int
    name: str
    ip: str
    ssh_user: str = "ops"
    ssh_key_ref: str = ""
    become: bool = False
    become_user: str | None = None
    become_method: str | None = None
    become_password_ref: str | None = None
    gateway: dict[str, Any] | None = None  # v32 中转网关 {name,host,port,ssh_user,ssh_key_ref}；None=直连

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Target":
        # v31：凭据归机器；ssh_key_ref 两级都缺时由 main 在 step_started 前回流 prepare 失败
        return cls(
            resource_id=int(d["resource_id"]),
            name=d["name"],
            ip=d["ip"],
            ssh_user=d.get("ssh_user", "ops"),
            ssh_key_ref=d.get("ssh_key_ref", ""),
            become=bool(d.get("become", False)),
            become_user=d.get("become_user"),
            become_method=d.get("become_method"),
            become_password_ref=d.get("become_password_ref"),
            gateway=d.get("gateway") or None,
        )


@dataclass
class StepSpec:
    """唯一步骤定义快照（v29 扁平单步，v30 收敛为 5 字段）。

    entry 语义随 type 分叉：ansible=playbook 路径 / shell=命令字符串 /
    python=仓库内脚本入口 / terraform=工作目录。run_on 缺省按 type 推断。
    v30 已删：serial/batch_pause_sec（并发度下沉 runner 配置 max_parallel_hosts）、
    undo_command（回滚统一 BINGOPS_ACTION=undo 约定）。旧消息带这些键会被忽略。
    """

    key: str
    name: str
    type: str
    run_on: str  # target | local
    entry: str
    timeout_sec: int | None = None
    rollbackable: bool = True

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "StepSpec":
        step_type = d.get("type", "ansible")
        # v29 统一为 entry；兼容在途旧消息的分叉字段名
        entry = (d.get("entry") or d.get("playbook") or d.get("command")
                 or d.get("script") or d.get("working_dir") or "")
        return cls(
            key=d.get("key") or "main",
            name=d.get("name") or d.get("key") or "main",
            type=step_type,
            run_on=d.get("run_on") or _DEFAULT_RUN_ON.get(step_type, "target"),
            entry=entry,
            timeout_sec=d.get("timeout_sec"),
            rollbackable=bool(d.get("rollbackable", True)),
        )


# exec_type → run_on 缺省（与控制面 EXEC_TYPE_RUN_ON 同表）
_DEFAULT_RUN_ON = {"ansible": "target", "shell": "target",
                   "python": "local", "terraform": "local"}


@dataclass
class DispatchMessage:
    """job-dispatch 消息；command=execute | rollback。

    v29：steps 数组已废，改为单个 step 对象；secrets 为 {变量名: Vault路径#字段}，
    只带钥匙名，明文由 runner 现场取。凭据两级结构（v34）：消息级 connection
    是执行期快照（存量兑底 + 本次执行填写的 ssh_user/ssh_key_ref/become）打底，
    target 级同名字段非空可覆盖。
    """

    message_id: str
    command: str
    execution_id: int
    code_ref: str
    params: dict[str, Any]
    targets: list[Target]
    step: StepSpec
    secrets: dict[str, str] = field(default_factory=dict)
    connection: dict[str, Any] = field(default_factory=dict)
    rollback_of: int | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DispatchMessage":
        conn = d.get("connection") or {}
        targets: list[Target] = []
        for t in d.get("targets") or []:
            # connection 打底，target 级非空字段覆盖
            merged = {k: v for k, v in conn.items() if v is not None}
            merged.update({k: v for k, v in t.items() if v is not None})
            targets.append(Target.from_dict(merged))
        # v29 单 step；兼容在途旧消息的 steps 数组（取首步）
        step_dict = d.get("step") or (d.get("steps") or [None])[0]
        if not step_dict:
            raise KeyError("step")
        return cls(
            message_id=d["message_id"],
            command=d["command"],
            execution_id=int(d["execution_id"]),
            code_ref=d["code_ref"],
            params=d.get("params") or {},
            targets=targets,
            step=StepSpec.from_dict(step_dict),
            secrets=d.get("secrets") or {},
            connection=conn,
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
