"""secrets 统一解析器（v27 凭据三层分离）：所有 executor 前置共用。

两类入参都解析为环境变量，脚本/playbook 侧与 Vault 零耦合：
1. `secrets: {VAR: "<Vault路径>#<字段>"}`（v27 新契约）→ env 同名 VAR
2. params 里 `*_ref` 后缀键（存量兼容，不迁移不破坏）→ 剥后缀转大写 env，
   值按存量约定从 bingops/keys/<钥匙名> 取，且从 params 中剔除

纪律：Vault 读失败即抛 VaultError（step 失败并回流），不允许空值继续跑；
每个明文取出后立即注册进 redact。
"""

import logging

from runner.redact import Redactor
from runner.vault_client import VaultClient

logger = logging.getLogger(__name__)


def parse_ref(ref: str) -> tuple[str, str]:
    """'magento2/prod/readonly#password' → ('magento2/prod/readonly', 'password')；
    无 # 时字段缺省 value。"""
    if "#" in ref:
        path, field = ref.rsplit("#", 1)
        return path, field
    return ref, "value"


def legacy_env_name(key: str) -> str:
    """src_db_password_ref → SRC_DB_PASSWORD。"""
    return key[: -len("_ref")].upper()


class SecretsResolver:
    def __init__(self, vault: VaultClient) -> None:
        self._vault = vault

    def resolve(self, secrets: dict[str, str], params: dict,
                redactor: Redactor) -> tuple[dict, dict[str, str]]:
        """返回 (清洗后的 params, secrets 环境变量 dict)。"""
        env: dict[str, str] = {}

        for var, ref in (secrets or {}).items():
            path, field = parse_ref(ref)
            value = self._vault.read_kv(path, field)
            redactor.register(value)
            env[var] = value

        cleaned_params: dict = {}
        for key, value in params.items():
            if key.endswith("_ref") and isinstance(value, str) and value:
                secret = self._vault.get_secret(value)
                redactor.register(secret)
                env[legacy_env_name(key)] = secret
                logger.info("存量 _ref 参数解析: %s → env %s",
                            key, legacy_env_name(key))
            else:
                cleaned_params[key] = value

        return cleaned_params, env
