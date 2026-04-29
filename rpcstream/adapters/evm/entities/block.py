from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Block:
    type: str = "block"
    number: int | None = None
    hash: str | None = None
    parent_hash: str | None = None
    nonce: int | None = None
    sha3_uncles: str | None = None
    logs_bloom: str | None = None
    transactions_root: str | None = None
    state_root: str | None = None
    receipts_root: str | None = None
    miner: str | None = None
    difficulty: int | None = None
    total_difficulty: int | None = None
    size: int | None = None
    extra_data: str | None = None
    gas_limit: int | None = None
    gas_used: int | None = None
    timestamp: int | None = None
    transaction_count: int = 0
    base_fee_per_gas: int | None = None
    withdrawals_root: str | None = None
    withdrawals: list = field(default_factory=list)
    blob_gas_used: int | None = None
    excess_blob_gas: int | None = None
