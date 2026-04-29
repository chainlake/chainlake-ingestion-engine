from dataclasses import asdict

from rpcstream.adapters.evm.entities.block import Block
from rpcstream.utils.utils import hex_to_dec

# block (eth_getBlockByNumber)
def parse_blocks(block: dict):
    entity = Block(
        number=hex_to_dec(block["number"]),
        hash=block["hash"],
        parent_hash=block["parentHash"],
        nonce=hex_to_dec(block.get("nonce")),
        sha3_uncles=block.get("sha3Uncles"),
        logs_bloom=block.get("logsBloom"),
        transactions_root=block.get("transactionsRoot"),
        state_root=block.get("stateRoot"),
        receipts_root=block.get("receiptsRoot"),
        miner=block.get("miner"),
        difficulty=hex_to_dec(block.get("difficulty", "0x0")),
        total_difficulty=hex_to_dec(block.get("totalDifficulty", "0x0")),
        size=hex_to_dec(block.get("size", "0x0")),
        extra_data=block.get("extraData"),
        gas_limit=hex_to_dec(block.get("gasLimit", "0x0")),
        gas_used=hex_to_dec(block.get("gasUsed", "0x0")),
        timestamp=hex_to_dec(block.get("timestamp", "0x0")),
        transaction_count=len(block.get("transactions", [])),
        base_fee_per_gas=hex_to_dec(block.get("baseFeePerGas")),
        withdrawals_root=block.get("withdrawalsRoot"),
        withdrawals=block.get("withdrawals") or [],
        blob_gas_used=hex_to_dec(block.get("blobGasUsed")),
        excess_blob_gas=hex_to_dec(block.get("excessBlobGas")),
    )
    return asdict(entity)
    
