from dataclasses import dataclass
from rpcstream.adapters.evm.rpc_requests import build_get_block_by_number
from rpcstream.adapters.evm.rpc_requests import build_get_block_receipts
from rpcstream.adapters.evm.rpc_requests import build_debug_trace_block

class RpcFetcher:
    def __init__(self, scheduler, pipeline_type, logger=None):
        self.scheduler = scheduler
        self.pipeline_type = pipeline_type
        self.logger = logger

    async def fetch(self, block_number):
        if self.pipeline_type == "block":
            req = build_get_block_by_number(block_number, True)

        elif self.pipeline_type == "receipt":
            req = build_get_block_receipts(block_number)

        elif self.pipeline_type == "trace":
            req = build_debug_trace_block(block_number)

        # log before response
        if self.logger and self.logger.isEnabledFor(10):
            self.logger.debug(
                "fetcher.request",
                component="fetcher",
                method="eth_getBlockByNumber",
                block=block_number
            )

        value, meta = await self.scheduler.submit_request(req)
   
        # log after response
        if self.logger and self.logger.isEnabledFor(10):
            self.logger.info(
                "fetcher.response",
                component="fetcher",
                method="eth_getBlockByNumber",
                latency_ms=meta.extra.get("latency_ms"),
                block=block_number
            )
   
        return value, meta