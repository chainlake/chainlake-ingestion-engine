import json
import asyncio
import time
from rpcstream.client.models import RpcErrorResult

from rpcstream.metrics.engine import (
    BLOCK_COUNTER,
    ROW_COUNTER,
    BLOCK_LATENCY,
    QUEUE_WAIT,
    INFLIGHT,
    ERROR_COUNTER,
    DLQ_COUNTER,
    CHAIN_LAG,
    TOTAL_TIME,
)

class IngestionEngine:
    def __init__(
        self, 
        fetcher, 
        processor, 
        sink, 
        topics, 
        dlq_topics=None,
        metrics=None, 
        concurrency=10, 
        logger=None
    ):
        self.fetcher = fetcher
        self.processor = processor
        self.sink = sink
        self.topics = topics
        self.dlq_topics = dlq_topics or {}
        self.metrics = metrics
        self.semaphore = asyncio.Semaphore(concurrency)
        self.logger = logger


    async def run_stream(self, block_source):
        await self.sink.start()

        queue = asyncio.Queue(maxsize=1000)

        async def producer():
            while True:
                block = await block_source.next_block()
                if block is None:
                    break
                await queue.put(block)

            # signal shutdown
            for _ in range(self.semaphore._value):
                await queue.put(None)

        async def worker():
            while True:
                block = await queue.get()
                if block is None:
                    break
                await self._run_one(block)

        workers = [
            asyncio.create_task(worker())
            for _ in range(self.semaphore._value)
        ]

        await producer()
        await asyncio.gather(*workers)
        await self.sink.close()


    async def _run_one(self, block_number):
        try:
            INFLIGHT.add(1)
            start_total = time.time()
            
            # FETCH
            result = await self.fetcher.fetch(block_number)

            # HANDLE RPC FAILURE
            if isinstance(result, RpcErrorResult):
                error_msg = result.error
                ERROR_COUNTER.add(1, {"stage": "rpc"})
                
                if self.logger:
                    self.logger.warn(
                        "engine.rpc_failed",
                        component="engine",
                        pipeline=self.fetcher.pipeline_type,
                        block=block_number,
                        error=error_msg,
                    )
                for entity in self.dlq_topics.keys():
                    await self._send_dlq(
                        entity=entity,
                        block_number=block_number,
                        error=error_msg,
                        stage="rpc"
                    )
                
                return
            
            # SUCCESS PATH
            value, meta = result

            latency = meta.extra.get("latency_ms", 0)
            queue_wait = meta.extra.get("queue_wait_ms", 0)
            
            # LAG CALCULATION
            lag = None
            if self.fetcher.tracker:
                latest_block = self.fetcher.tracker.get_latest()
                if latest_block is not None:
                    lag = latest_block - block_number

            # PROCESS (PIPELINE ROUTING)
            pipeline_type = self.fetcher.pipeline_type
            
            parsed = self.processor.process(pipeline_type, block_number, value)

            # METRICS
            BLOCK_COUNTER.add(1, {"pipeline": self.fetcher.pipeline_type})
            BLOCK_LATENCY.record(latency, {"pipeline": self.fetcher.pipeline_type})
            QUEUE_WAIT.record(queue_wait, {"pipeline": self.fetcher.pipeline_type})

            # SINK (GENERIC)
            for entity, rows in parsed.items():
                ROW_COUNTER.add(len(rows), {"entity": entity})
                
                topic = self.topics.get(entity)
                if not topic:
                    continue
                
                await self.sink.send(topic, rows)

            # LOGGING (entity agnostic)
            self.logger.info(
                "engine.processed",
                component="engine",
                pipeline=self.fetcher.pipeline_type,
                block=block_number,
                latency_ms=latency,
                queue_wait_ms=queue_wait,
                chain_lag=lag,
            )
            
            CHAIN_LAG.record(lag, {"pipeline": pipeline_type})
            
            if self.logger and self.logger.isEnabledFor(10):
                preview = f"type={type(value)} size={len(value) if hasattr(value,'__len__') else 'NA'}"
                self.logger.debug(
                    "engine.block_processed",
                    component="engine",
                    pipeline=self.fetcher.pipeline_type,
                    block=block_number,
                    process_preview=preview
                )

        except Exception as e:
            error_msg = repr(e)
            ERROR_COUNTER.add(1, {"stage": "processor"})
            
            if self.logger:
                self.logger.error(
                    "engine.processor_error",
                    component="engine",
                    pipeline=self.fetcher.pipeline_type,
                    block=block_number,
                    error=error_msg
                )
            
            # send to all entities DLQ
            for entity in self.dlq_topics.keys():
                await self._send_dlq(
                    entity=entity,
                    block_number=block_number,
                    error=error_msg,
                    stage="processor",
                )
            
            if self.metrics:
                self.metrics.record_error()

        finally:
            INFLIGHT.add(-1)

            total_ms = (time.time() - start_total) * 1000
            # optional but VERY useful
            TOTAL_TIME.record(total_ms, {"pipeline": self.fetcher.pipeline_type})



    async def _send_dlq(self, entity, block_number, error, stage):
        topic  = self.dlq_topics.get(entity)
        DLQ_COUNTER.add(1, {"entity": entity, "stage": stage})
        
        if not topic:
            if self.logger:
                self.logger.warn(
                    "engine.dlq_missing_topic",
                    component="engine",
                    entity=entity,
                    block=block_number,
                )
            return

        payload = {
            "block": block_number,
            "entity": entity,
            "pipeline": self.fetcher.pipeline_type,
            "stage": stage,
            "error": error,
        }

        await self.sink.send(topic, [payload])

        if self.logger:
            self.logger.warn(
                "engine.dlq_sent",
                component="engine",
                entity=entity,
                block=block_number,
                stage=stage,
                error=error,
            )