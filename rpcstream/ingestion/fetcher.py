from rpcstream.adapters.evm.rpc_requests import build_get_block_by_number
from rpcstream.adapters.evm.rpc_requests import build_get_block_receipts
from rpcstream.adapters.evm.rpc_requests import build_debug_trace_block

class EvmRpcFetcher:
    def __init__(self, scheduler, entities, logger=None, tracker=None):
        self.scheduler = scheduler
        self.entities = entities  # List of entities to fetch, e.g., ["block", "transaction"]
        self.logger = logger
        self.tracker = tracker

    async def fetch(self, block_number):
        # -------------------------
        # LOG BEFORE
        # -------------------------
        if self.logger:
            self.logger.debug(
                "fetcher.request",
                component="fetcher",
                entities=self.entities,
                block=block_number,
            )
        
        # Initialize a dictionary to store results for the relevant entities
        raw_data = {}
        req_method = {}
        # Fetch only the entities defined in the pipeline.yaml
        if "block" in self.entities and "transaction" not in self.entities:
            req = build_get_block_by_number(block_number, False)  # or True depending on the config
            result = await self.scheduler.submit_request(req)
            raw_data["block"] = result
            req_method["block"] = req.method

        if "transaction" in self.entities:
            req = build_get_block_by_number(block_number, True)
            result = await self.scheduler.submit_request(req)
            raw_data["transaction"] = result
            req_method["transaction"] = req.method
            if "block" in self.entities:
                raw_data["block"] = result
                req_method["block"] = req.method

        if "receipt" in self.entities or "log" in self.entities:
            req = build_get_block_receipts(block_number)
            result = await self.scheduler.submit_request(req)
            raw_data["receipt"] = result
            req_method["receipt"] = req.method
            if "log" in self.entities:
                raw_data["log"] = result  # Logs are parsed from receipts
                req_method["log"] = req.method
    
        if "trace" in self.entities:
            req = build_debug_trace_block(block_number)
            result = await self.scheduler.submit_request(req)
            req_method["trace"] = req.method
            raw_data["trace"] = result

        # Log after fetch
        if self.logger:
            for entity, respone in raw_data.items():
                self.logger.debug(
                    "fetcher.response",
                    component="fetcher",
                    method = req_method[entity],
                    block=block_number,
                    entity=entity
                )
        
        return raw_data
