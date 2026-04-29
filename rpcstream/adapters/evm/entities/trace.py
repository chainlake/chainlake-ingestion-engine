from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Trace:
    type: str = "trace"
    block_number: int | None = None
    block_hash: str | None = None
    block_timestamp: int | None = None
    transaction_hash: str | None = None
    trace_id: str | None = None
    parent_trace_id: str | None = None
    trace_index: int | None = None
    from_address: str | None = None
    to_address: str | None = None
    value: int | None = None
    input: str | None = None
    output: str | None = None
    call_type: str | None = None
    trace_type: str | None = None
    status: str | None = None
    error: str | None = None
    gas: int | None = None
    gas_used: int | None = None
    depth: int | None = None
    subtraces: int | None = None
    reward_type: str | None = None
    trace_address: list[int] = field(default_factory=list)
    trace_source: str | None = None
    trace_method: str | None = None
