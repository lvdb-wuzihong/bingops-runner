"""日志初始化：stdout JSON 化输出，容器场景直接交给采集器。

注意：这里只配置 runner 自身进程日志；任务日志走 job-events 回流，两者独立。
"""

import logging
import sys

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=LOG_FORMAT,
        stream=sys.stdout,
    )
    # 第三方库噪音压制
    for noisy in ("urllib3", "hvac", "kafka"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
