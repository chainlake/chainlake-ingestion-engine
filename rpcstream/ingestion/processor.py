from rpcstream.adapters.evm.parser.parse_blocks import parse_blocks
from rpcstream.adapters.evm.parser.parse_transactions import parse_transactions
from rpcstream.adapters.evm.parser.parse_receipts_logs import parse_receipts
from rpcstream.adapters.evm.parser.parse_traces import parse_traces_auto


class EVMProcessor:

    def __init__(self, logger=None):
        self.logger = logger
        
    def process(self, pipeline_type, block_number, value):

        if self.logger:
            self.logger.debug(f"[Processor] Parsing block {block_number}")

        try:
            if pipeline_type == "block":
                return self.process_block_pipeline(block_number, value)

            elif pipeline_type == "receipt":
                return self.process_receipt_pipeline(block_number, value)

            elif pipeline_type == "trace":
                return self.process_trace_pipeline(block_number, value)

            else:
                raise ValueError(f"Unknown pipeline: {pipeline_type}")
        
        except Exception as e:
            if self.logger:
                self.logger.error(f"[Processor] ERROR block {block_number}: {e}")
            raise

    # -------------------------
    # BLOCK PIPELINE
    # -------------------------
    def process_block_pipeline(self, block_number, value):
        
        block = parse_blocks(value)
        txs = parse_transactions(value)

        return {
            "block": [block],
            "transaction": txs
        }

    # -------------------------
    # RECEIPT PIPELINE
    # -------------------------
    def process_receipt_pipeline(self, block_number, value):
        receipts, logs = parse_receipts(value)

        return {
            "receipt": receipts,
            "log": logs
        }

    # -------------------------
    # TRACE PIPELINE
    # -------------------------
    def process_trace_pipeline(self, block_number, value):
        traces = parse_traces_auto(value, block_number, "trace_block")

        return {
            "trace": traces
        }