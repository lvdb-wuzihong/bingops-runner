#!/usr/bin/env python3
"""Vault 轮换期限巡检。

检查两类期限：
1. AppRole secret_id 的原生 expiration_time
2. KV secret/<mount>/bingops/keys/* 的轮换约定：custom_metadata.rotate_by（YYYY-MM-DD）
   KV 静态 secret 没有原生 TTL，rotate_by 是团队约定，靠本脚本巡检兜底

用法：
    python scripts/vault_rotation_check.py [--warn-days 14]
环境变量与 runner 相同：VAULT_ADDR / VAULT_ROLE_ID / VAULT_SECRET_ID / VAULT_KV_MOUNT

退出码：0=全部健康  1=存在到期/临期/未登记  2=连接或读取失败
建议挂 CI 定时任务或 K8s CronJob，超阈值即告警。
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timezone

import hvac


def parse_ts(value: str | None) -> datetime | None:
    """Vault 时间戳带纳秒（...123456789Z），fromisoformat 不认，截掉小数部分。"""
    if not value:
        return None
    return datetime.fromisoformat(value.split(".")[0].replace("Z", "+00:00"))


def check_secret_id(client: hvac.Client, role_name: str, secret_id: str,
                    warn_days: int) -> bool:
    """返回 False 表示存在告警项。无 lookup 权限时跳过不告警。"""
    try:
        info = client.auth.approle.read_secret_id(secret_id=secret_id)
    except hvac.exceptions.Forbidden:
        print("[SKIP] secret_id 过期时间：无 secret-id/lookup 权限"
              f"（需 policy 授 auth/approle/role/{role_name}/secret-id/lookup update）")
        return True
    exp = parse_ts(info.get("data", {}).get("expiration_time"))
    if exp is None:
        print("[WARN] secret_id 未设 secret_id_ttl，永不过期（建议设置）")
        return False
    days = (exp - datetime.now(timezone.utc)).days
    tag = "WARN" if days <= warn_days else "OK  "
    print(f"[{tag}] secret_id 过期时间 {exp.date()}（剩 {days} 天）")
    return days > warn_days


def check_kv_keys(client: hvac.Client, mount: str, keys_path: str,
                  warn_days: int) -> bool:
    today = date.today()
    healthy = True
    listing = client.secrets.kv.v2.list_secrets(path=keys_path, mount_point=mount)
    for key in listing.get("data", {}).get("keys", []):
        if key.endswith("/"):
            continue  # 子目录，P1 不递归
        meta = client.secrets.kv.v2.read_secret_metadata(
            path=f"{keys_path}/{key}", mount_point=mount)
        rotate_by = (meta.get("data", {}).get("custom_metadata") or {}).get("rotate_by")
        if not rotate_by:
            print(f"[WARN] {keys_path}/{key} 未登记 rotate_by（轮换期限失管）")
            healthy = False
            continue
        try:
            deadline = date.fromisoformat(rotate_by)
        except ValueError:
            print(f"[WARN] {keys_path}/{key} rotate_by 格式非法: {rotate_by}")
            healthy = False
            continue
        days = (deadline - today).days
        if days < 0:
            print(f"[EXPI] {keys_path}/{key} rotate_by={rotate_by} 已过期 {-days} 天")
            healthy = False
        elif days <= warn_days:
            print(f"[WARN] {keys_path}/{key} rotate_by={rotate_by}（剩 {days} 天）")
            healthy = False
        else:
            print(f"[OK  ] {keys_path}/{key} rotate_by={rotate_by}（剩 {days} 天）")
    return healthy


def main() -> int:
    parser = argparse.ArgumentParser(description="Vault 轮换期限巡检")
    parser.add_argument("--warn-days", type=int, default=14)
    parser.add_argument("--keys-path", default="bingops/keys")
    parser.add_argument("--role-name", default="bingops-runner")
    args = parser.parse_args()

    mount = os.environ.get("VAULT_KV_MOUNT", "secret")
    try:
        client = hvac.Client(url=os.environ["VAULT_ADDR"])
        client.auth.approle.login(role_id=os.environ["VAULT_ROLE_ID"],
                                  secret_id=os.environ["VAULT_SECRET_ID"])
    except Exception as e:
        print(f"[FAIL] Vault 登录失败: {e}", file=sys.stderr)
        return 2

    healthy = check_secret_id(client, args.role_name,
                              os.environ["VAULT_SECRET_ID"], args.warn_days)
    try:
        healthy = check_kv_keys(client, mount, args.keys_path, args.warn_days) and healthy
    except Exception as e:
        print(f"[FAIL] KV 元数据读取失败（检查 policy 是否授 "
              f"secret/metadata/{args.keys_path}/* read）: {e}", file=sys.stderr)
        return 2
    return 0 if healthy else 1


if __name__ == "__main__":
    sys.exit(main())
