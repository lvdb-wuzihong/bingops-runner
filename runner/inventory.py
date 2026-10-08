"""targets → ad-hoc inventory JSON + 临时 SSH keyfile（v31/v32）。

纪律（设计文档 §5/§5.1/§5.2/§9.3#7）：
- v31 凭据归机器：targets[] 逐台携带 ssh_user/ssh_key_ref（优先级高于 connection，
  合并在 models.DispatchMessage 完成）；keyfile 0600、用完即删、取值即注册 redact
- v32 跳板：target.gateway 非空则渲染 ProxyCommand，跳板钥必须显式 -i 写进命令
  （ansible_ssh_private_key_file 只作用于最终目标）；gateway.ssh_key_ref 为空复用目标机钥匙
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


def _safe_name(ref: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in ref)


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
        missing = [t.name for t in targets if not t.ssh_key_ref]
        if missing:
            raise InventoryError(
                "以下目标无可用凭据（主机标签与 connection 兜底均未命中）："
                + ", ".join(missing))
        os.makedirs(workdir, exist_ok=True)

        envvars: dict[str, str] = {"ANSIBLE_HOST_KEY_CHECKING": "False"}
        temp_files: list[str] = []
        hosts: dict[str, Any] = {}
        try:
            # ---- 目标机私钥：逐台解析，按引用去重 ----
            keyfile_by_ref: dict[str, str] = {}
            for ref in sorted({t.ssh_key_ref for t in targets}):
                keyfile_by_ref[ref] = self._write_keyfile(
                    workdir, f"keyfile-{_safe_name(ref)}", ref,
                    redactor, temp_files)
                envvars[f"ANSIBLE_PRIVATE_KEY_FILE_{_safe_name(ref).upper()}"] = \
                    keyfile_by_ref[ref]

            # ---- become 密码：只存内存不进盘，取出即注册脱敏 ----
            become_password_by_ref: dict[str, str] = {}
            for ref in sorted({t.become_password_ref for t in targets
                               if t.become_password_ref}):
                password = self._vault.read_ref(ref)
                if redactor is not None:
                    redactor.register(password)
                become_password_by_ref[ref] = password

            # ---- 跳板钥：按 (网关名, 钥匙引用) 去重；引用为空复用目标机钥匙 ----
            gw_keyfile: dict[tuple, str] = {}

            def gateway_keyfile(t: Target) -> str:
                gw = t.gateway
                ref = gw.get("ssh_key_ref") or t.ssh_key_ref
                cache_key = (gw.get("name") or gw.get("host"), ref)
                if cache_key not in gw_keyfile:
                    gw_keyfile[cache_key] = self._write_keyfile(
                        workdir, f"keyfile-gw-{_safe_name(str(cache_key[0]))}-{_safe_name(ref)}",
                        ref, redactor, temp_files)
                return gw_keyfile[cache_key]

            for t in targets:
                host_vars: dict[str, Any] = {
                    "ansible_host": t.ip,
                    "ansible_user": t.ssh_user,
                    "ansible_ssh_private_key_file": keyfile_by_ref[t.ssh_key_ref],
                    "ansible_ssh_common_args": _SSH_ARGS,
                }
                if t.gateway:
                    gw = t.gateway
                    gpath = gateway_keyfile(t)
                    user = gw.get("ssh_user") or "root"
                    host_addr = gw.get("host")
                    port = gw.get("port") or 22
                    # 跳板钥必须显式 -i：ansible_ssh_private_key_file 只作用于最终目标；
                    # 跳板端口必须 -p：ssh 的 destination 无 user@host:port 冒号语法（scp 写法），
                    # 否则 gw_host:port 被整体当主机名解析，proxy 立即退出 → UNREACHABLE
                    host_vars["ansible_ssh_common_args"] = (
                        f'{_SSH_ARGS} -o ProxyCommand="ssh -i {gpath} '
                        f'-o StrictHostKeyChecking=accept-new '
                        f'-p {port} -W %h:%p {user}@{host_addr}"'
                    )
                    logger.info("target %s 经网关 %s 中转", t.name, gw.get("name"))
                if t.become:
                    host_vars["ansible_become"] = True
                    host_vars["ansible_become_user"] = t.become_user or "root"
                    host_vars["ansible_become_method"] = t.become_method or "sudo"
                    if t.become_password_ref:
                        host_vars["ansible_become_password"] = \
                            become_password_by_ref[t.become_password_ref]
                hosts[t.name] = host_vars

            inventory = {"all": {"hosts": hosts}}
            inventory_path = os.path.join(workdir, "inventory.json")
            with open(inventory_path, "w", encoding="utf-8") as f:
                json.dump(inventory, f, indent=2)
            # host_vars 可能含 become_password 明文与跳板钥路径，与 keyfile 同等防护
            self._chmod_0600(inventory_path)

            yield {"inventory_path": inventory_path, "envvars": envvars}
        finally:
            for path in temp_files:
                try:
                    os.remove(path)
                except OSError:
                    logger.warning("keyfile 删除失败: %s", path)

    def _write_keyfile(self, workdir: str, filename: str, ref: str,
                       redactor, temp_files: list[str]) -> str:
        """按凭据引用取私钥 → 写 0600 临时文件 → 注册脱敏。"""
        content = self._vault.read_ref(ref)
        if redactor is not None:
            redactor.register(content)
        path = os.path.join(workdir, filename)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content.rstrip("\n") + "\n")
        self._chmod_0600(path)
        temp_files.append(path)
        return path

    @staticmethod
    def _chmod_0600(path: str) -> None:
        try:
            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
        except OSError:
            # Windows 开发机无 POSIX 权限位，忽略
            pass
