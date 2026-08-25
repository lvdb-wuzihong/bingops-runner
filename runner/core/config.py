"""运行配置：全部来自环境变量，容器化部署时注入。

必填项缺失在启动时即抛 ConfigError（fail fast），避免任务半途失败。
"""

import os
from dataclasses import dataclass

from runner.core.exceptions import ConfigError


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise ConfigError(f"缺少必需环境变量: {name}")
    return value


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise ConfigError(f"环境变量 {name} 必须为整数，当前值: {raw}")


@dataclass(frozen=True)
class Config:
    # ---- Kafka ----
    kafka_bootstrap_servers: str
    kafka_dispatch_topic: str
    kafka_events_topic: str
    kafka_group_id: str

    # ---- Vault ----
    vault_addr: str
    vault_role_id: str
    vault_secret_id: str
    vault_kv_mount: str

    # ---- GitLab（代码事实源，tag 不可移动）----
    git_repo_url: str
    git_username: str | None
    git_token: str | None

    # ---- 运行时 ----
    max_concurrent_executions: int
    workdir: str
    default_step_timeout_sec: int
    log_level: str

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            kafka_bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS"),
            kafka_dispatch_topic=os.environ.get("KAFKA_DISPATCH_TOPIC", "job-dispatch"),
            kafka_events_topic=os.environ.get("KAFKA_EVENTS_TOPIC", "job-events"),
            kafka_group_id=os.environ.get("KAFKA_GROUP_ID", "bingops-runner"),
            vault_addr=_env("VAULT_ADDR"),
            vault_role_id=_env("VAULT_ROLE_ID"),
            vault_secret_id=_env("VAULT_SECRET_ID"),
            vault_kv_mount=os.environ.get("VAULT_KV_MOUNT", "secret"),
            git_repo_url=_env("GIT_REPO_URL"),
            git_username=os.environ.get("GIT_USERNAME"),
            git_token=os.environ.get("GIT_TOKEN"),
            max_concurrent_executions=_env_int("RUNNER_MAX_CONCURRENT", 4),
            workdir=os.environ.get("RUNNER_WORKDIR", "/var/lib/bingops-runner"),
            default_step_timeout_sec=_env_int("RUNNER_STEP_TIMEOUT_SEC", 600),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
        )
