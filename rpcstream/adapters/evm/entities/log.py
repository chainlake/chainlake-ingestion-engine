from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Log:
    type: str = "log"
    log_index: int | None = None
    transaction_hash: str | None = None
    transaction_index: int | None = None
    block_hash: str | None = None
    block_number: int | None = None
    block_timestamp: int | None = None
    address: str | None = None
    data: str | None = None
    topics: list[str] = field(default_factory=list)
    removed: bool = False
