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

    def read_kv(self, path: str, field: str = "value",
                ttl_sec: int = _DEFAULT_TTL_SEC) -> str:
        """读 KV v2 任意路径的指定字段（v27 secrets 契约：path#field）。

        缓存键含 path+field；取出的值由调用方负责注册进 redact。
        """
        cache_key = f"{path}#{field}"
        cached = self._cache.get(cache_key)
        if cached and time.time() < cached[1]:
            return cached[0]

        client = self._ensure_login()
        try:
            resp = client.secrets.kv.v2.read_secret_version(
                path=path, mount_point=self._config.vault_kv_mount
            )
            value = resp["data"]["data"].get(field)
        except Exception as e:
            raise VaultError(f"读取 secret 失败 [{cache_key}]: {_vault_error_detail(e)}") from e
        if not value:
            raise VaultError(f"secret [{cache_key}] 缺少字段 {field}")

        self._cache[cache_key] = (value, time.time() + ttl_sec)
        logger.info("Vault 取钥成功: %s", cache_key)
        return value

    def read_ref(self, ref: str, ttl_sec: int = _DEFAULT_TTL_SEC) -> str:
        """凭据引用解析（v31/v32）：三种形态统一入口。

        - "path#field"：显式路径+字段
        - 含 "/" 的路径（如 ssh/keys/ops-vpc-a）：直接当 KV 路径，字段缺省 value
        - 裸钥匙名（存量约定）：补 bingops/keys/ 前缀
        """
        if "#" in ref:
            path, field = ref.rsplit("#", 1)
        elif "/" in ref:
            path, field = ref, "value"
        else:
            path, field = f"bingops/keys/{ref}", "value"
        return self.read_kv(path, field, ttl_sec)

    def get_secret(self, key_ref: str, ttl_sec: int = _DEFAULT_TTL_SEC) -> str:
        """按钥匙名取目标机私钥等（存量约定：secret/<mount>/bingops/keys/<ref> 的 value 字段）。"""
        return self.read_kv(f"bingops/keys/{key_ref}", "value", ttl_sec)

    def clear_cache(self) -> None:
        self._cache.clear()
