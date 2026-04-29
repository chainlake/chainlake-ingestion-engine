from dataclasses import asdict

from rpcstream.adapters.evm.entities.transaction import Transaction
from rpcstream.utils.utils import hex_to_dec

# transaction flatten
def parse_transactions(block: dict):
    txs = block.get("transactions", [])

    results = []
    for i, tx in enumerate(txs):
        entity = Transaction(
            hash=tx.get("hash"),
            block_hash=block.get("hash"),
            transaction_index=i,
            from_address=tx.get("from"),
            to_address=tx.get("to"),
            nonce=hex_to_dec(tx.get("nonce")),
            block_number=hex_to_dec(block.get("number")),
            block_timestamp=hex_to_dec(block.get("timestamp")),
            value=hex_to_dec(tx.get("value")),
            gas=hex_to_dec(tx.get("gas")),
            gas_price=hex_to_dec(tx.get("gasPrice")),
            max_fee_per_gas=hex_to_dec(tx.get("maxFeePerGas")),
            max_priority_fee_per_gas=hex_to_dec(tx.get("maxPriorityFeePerGas")),
            max_fee_per_blob_gas=hex_to_dec(tx.get("maxFeePerBlobGas")),
            transaction_type=hex_to_dec(tx.get("type")),
            chain_id=hex_to_dec(tx.get("chainId")),
            v=hex_to_dec(tx.get("v")),
            r=tx.get("r"),
            s=tx.get("s"),
            input=tx.get("input"),
            blob_versioned_hashes=tx.get("blobVersionedHashes") or [],
        )
        results.append(asdict(entity))

    return results
