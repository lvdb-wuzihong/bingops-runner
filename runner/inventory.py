"""targets → ad-hoc inventory JSON + 临时 SSH keyfile。

纪律（设计文档 §5）：inventory 源是 CMDB 目标快照，runner 现场拼装；
keyfile 权限 0600，用完即删（cleanup 负责）。
"""

import json
import logging
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from runner.core.exceptions import InventoryError
from runner.core.models import Target
from runner.vault_client import VaultClient

logger = logging.getLogger(__name__)

# 免交互连接必备：跳过首次 known_hosts 确认
_SSH_ARGS = ("-o StrictHostKeyChecking=no "
             "-o UserKnownHostsFile=/dev/null "
             "-o ConnectTimeout=10")


class InventoryBuilder:
    def __init__(self, vault: VaultClient) -> None:
        self._vault = vault

    @contextmanager
    def build(self, targets: list[Target], workdir: str,
              redactor=None) -> Iterator[dict[str, Any]]:
        """构建 inventory 与 keyfile，退出上下文时自动清理临时文件。

        yield: {"inventory_path": str, "envvars": dict}
        """
        if not targets:
            raise InventoryError("targets 为空，无法构建 inventory")
        os.makedirs(workdir, exist_ok=True)

        envvars: dict[str, str] = {"ANSIBLE_HOST_KEY_CHECKING": "False"}
        keyfile_by_ref: dict[str, str] = {}
        hosts: dict[str, Any] = {}
        try:
            for ref in sorted({t.ssh_key_ref for t in targets}):
                key_content = self._vault.get_secret(ref)
                if redactor is not None:
                    redactor.register(key_content)
                path = os.path.join(workdir, f"keyfile-{ref}")
                with open(path, "w", encoding="utf-8") as f:
                    f.write(key_content.rstrip("\n") + "\n")
                self._chmod_0600(path)
                keyfile_by_ref[ref] = path
                envvars[f"ANSIBLE_PRIVATE_KEY_FILE_{self._env_suffix(ref)}"] = path

            for t in targets:
                hosts[t.name] = {
                    "ansible_host": t.ip,
                    "ansible_user": t.ssh_user,
                    "ansible_ssh_private_key_file": keyfile_by_ref[t.ssh_key_ref],
                    "ansible_ssh_common_args": _SSH_ARGS,
                }

            inventory = {"all": {"hosts": hosts}}
            inventory_path = os.path.join(workdir, "inventory.json")
            with open(inventory_path, "w", encoding="utf-8") as f:
                json.dump(inventory, f, indent=2)

            yield {"inventory_path": inventory_path, "envvars": envvars}
        finally:
            for path in keyfile_by_ref.values():
                try:
                    os.remove(path)
                except OSError:
                    logger.warning("keyfile 删除失败: %s", path)

    @staticmethod
    def _env_suffix(key_ref: str) -> str:
        return "".join(c if c.isalnum() else "_" for c in key_ref).upper()

    @staticmethod
    def _chmod_0600(path: str) -> None:
        try:
            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
        except OSError:
            # Windows 开发机无 POSIX 权限位，忽略
            pass
