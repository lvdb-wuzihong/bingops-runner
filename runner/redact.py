"""出机前脱敏：所有流向 job-events 的日志文本必须先过 Redactor。

纪律（设计文档 §6）：Vault 取出的每个 secret 值在取出后立即注册进掩码列表，
命中即替换为 `***`，长值先替换，避免短值先命中残留片段。
"""

MASK = "***"


class Redactor:
    def __init__(self) -> None:
        self._secrets: list[str] = []

    def register(self, *values: str | None) -> None:
        """注册敏感值。空串/None 忽略；去重。"""
        for v in values:
            if v and v not in self._secrets:
                self._secrets.append(v)

    def apply(self, text: str | None) -> str:
        if not text:
            return ""
        # 长值优先替换，防止子串先命中
        for v in sorted(self._secrets, key=len, reverse=True):
            if v in text:
                text = text.replace(v, MASK)
        return text
