from dataclasses import dataclass, field
from typing import Any, Optional

@dataclass
class RpcTaskMeta:
    task_id: int
    submit_ts: float
    extra: dict[str, Any]

@dataclass
class RpcErrorResult:
    error: Exception
    meta: RpcTaskMeta