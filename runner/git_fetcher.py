"""按 pinned tag 拉取代码（设计文档决策 2：GitLab 唯一事实源，tag 不可移动）。

`git clone --depth 1 --branch <tag>`：只取该 tag 快照，可精确复现"当时跑的是哪份代码"。
"""

import logging
import os
import shutil
import subprocess

from runner.core.config import Config
from runner.core.exceptions import GitFetchError

logger = logging.getLogger(__name__)


class GitFetcher:
    def __init__(self, config: Config) -> None:
        self._config = config

    def _authed_url(self) -> str:
        url = self._config.git_repo_url
        if self._config.git_token and url.startswith(("http://", "https://")):
            user = self._config.git_username or "gitlab-ci-token"
            scheme, rest = url.split("://", 1)
            return f"{scheme}://{user}:{self._config.git_token}@{rest}"
        return url

    def fetch(self, code_ref: str, dest: str) -> str:
        """clone 指定 tag 到 dest，返回工作目录路径。dest 已存在则先清除重建。"""
        if os.path.exists(dest):
            shutil.rmtree(dest)
        os.makedirs(os.path.dirname(dest), exist_ok=True)

        cmd = ["git", "clone", "--depth", "1", "--branch", code_ref,
               self._authed_url(), dest]
        # 日志中不打印带 token 的 URL
        logger.info("git clone --depth 1 --branch %s -> %s", code_ref, dest)
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=300,
            )
        except subprocess.TimeoutExpired as e:
            raise GitFetchError(f"git clone 超时 [tag={code_ref}]") from e
        if result.returncode != 0:
            # stderr 可能含 token，只保留末行错误概要
            tail = (result.stderr or "").strip().splitlines()[-1:] or ["unknown error"]
            raise GitFetchError(f"git clone 失败 [tag={code_ref}]: {tail[0]}")
        return dest
