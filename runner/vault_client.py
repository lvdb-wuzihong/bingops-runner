"""Vault AppRole 客户端：现场取钥，内存 TTL 缓存，不落盘。

纪律（设计文档决策 5/7）：
- 下发消息只带钥匙名（ssh_key_ref），runner 用 AppRole 登录后按名取钥
- 取出的每个值立即注册进 Redactor 掩码列表
- 缓存仅供同一 execution 内复用，进程退出即清空
"""

import logging
import time

import hvac

from runner.core.config import Config
from runner.core.exceptions import VaultError

logger = logging.getLogger(__name__)

_DEFAULT_TTL_SEC = 300


def _vault_error_detail(e: Exception) -> str:
    """hvac 异常的 str() 常丢 errors 列表，这里把类型/状态码/响应体原文都挖出来。"""
    parts = [type(e).__name__]
    resp = getattr(e, "response", None)
    if resp is not None:
        parts.append(f"status={getattr(resp, 'status_code', None)}")
        try:
            parts.append(f"body={resp.text[:300]}")
        except Exception:
            pass
    errors = getattr(e, "errors", None)
    if errors:
        parts.append(f"errors={errors}")
    if str(e):
        parts.append(f"msg={str(e)}")
    return " | ".join(parts)


class VaultClient:
    def __init__(self, config: Config) -> None:
        self._config = config
        self._client: hvac.Client | None = None
        self._token_expire_at: float = 0.0
        # key_ref -> (value, expire_at)
        self._cache: dict[str, tuple[str, float]] = {}

    def _ensure_login(self) -> hvac.Client:
        if self._client and time.time() < self._token_expire_at:
            return self._client
        try:
            client = hvac.Client(url=self._config.vault_addr)
            resp = client.auth.approle.login(
                role_id=self._config.vault_role_id,
                secret_id=self._config.vault_secret_id,
            )
        except Exception as e:
            raise VaultError(f"Vault AppRole 登录失败: {_vault_error_detail(e)}") from e
        lease = resp.get("auth", {}).get("lease_duration", _DEFAULT_TTL_SEC)
        self._client = client
        # 提前 60s 过期，避免边界竞态
        self._token_expire_at = time.time() + max(lease - 60, 30)
        logger.info("Vault AppRole 登录成功，token 有效期 %ss", lease)
        return client

    def get_secret(self, key_ref: str, ttl_sec: int = _DEFAULT_TTL_SEC) -> str:
        """按钥匙名取钥（KV v2：secret/data/bingops/keys/<key_ref>，字段 value）。"""
        cached = self._cache.get(key_ref)
        if cached and time.time() < cached[1]:
            return cached[0]

        client = self._ensure_login()
        path = f"bingops/keys/{key_ref}"
        try:
            resp = client.secrets.kv.v2.read_secret_version(
                path=path, mount_point=self._config.vault_kv_mount
            )
            value = resp["data"]["data"].get("value")
        except Exception as e:
            raise VaultError(f"读取 secret 失败 [{key_ref}]: {_vault_error_detail(e)}") from e
        if not value:
            raise VaultError(f"secret [{key_ref}] 缺少 value 字段")

        self._cache[key_ref] = (value, time.time() + ttl_sec)
        logger.info("Vault 取钥成功: %s", key_ref)
        return value

    def clear_cache(self) -> None:
        self._cache.clear()
