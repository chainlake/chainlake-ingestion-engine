from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Transaction:
    type: str = "transaction"
    hash: str | None = None
    block_hash: str | None = None
    transaction_index: int | None = None
    from_address: str | None = None
    to_address: str | None = None
    nonce: int | None = None
    block_number: int | None = None
    block_timestamp: int | None = None
    value: int | None = None
    gas: int | None = None
    gas_price: int | None = None
    max_fee_per_gas: int | None = None
    max_priority_fee_per_gas: int | None = None
    max_fee_per_blob_gas: int | None = None
    transaction_type: int | None = None
    chain_id: int | None = None
    v: int | None = None
    r: str | None = None
    s: str | None = None
    input: str | None = None
    blob_versioned_hashes: list[str] = field(default_factory=list)
