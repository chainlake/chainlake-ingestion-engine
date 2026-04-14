import json
import asyncio


class IngestionEngine:
    def __init__(self, fetcher, processor, sink, topics, metrics=None, concurrency=10, logger=None):
        self.fetcher = fetcher
        self.processor = processor
        self.sink = sink
        self.topics = topics
        self.metrics = metrics
        self.semaphore = asyncio.Semaphore(concurrency)
        self.logger = logger

    async def run_batch(self, start, end):
        tasks = [
            asyncio.create_task(self._run_one(block))
            for block in range(start, end + 1)
        ]

        await asyncio.gather(*tasks)

        self.sink.flush()
        self._print_summary()

    async def _run_one(self, block_number):
        async with self.semaphore:
            try:
                # -------------------------
                # FETCH
                # -------------------------
                value, meta = await self.fetcher.fetch(block_number)

                latency = meta.extra.get("latency_ms", 0)
                queue_wait = meta.extra.get("queue_wait_ms", 0)
                
                # -------------------------
                # PROCESS (PIPELINE ROUTING)
                # -------------------------
                pipeline_type = self.fetcher.pipeline_type
                
                parsed = self.processor.process(pipeline_type, block_number, value)

                # -------------------------
                # SINK (GENERIC)
                # -------------------------
                for entity, rows in parsed.items():
                    topic = self.topics.get(entity)
                    if not topic:
                        continue
                    self.sink.send(topic, rows)

                # -------------------------
                # METRICS (RAW only)
                # -------------------------
                if self.metrics:
                    self.metrics.record(
                        latency,
                        queue_wait,
                    )

                # -------------------------
                # LOGGING (entity agnostic)
                # -------------------------
                self.logger.info(
                    f"[Block {block_number}] latency={latency:.2f}ms "
                    f"queue_wait={queue_wait:.2f}ms"
                )

                if self.logger.isEnabledFor(10):  # DEBUG
                    preview = str(value)[:150]
                    self.logger.debug(f"Raw RPC response: {preview}")

            except Exception as e:
                print(f"[Block {block_number}] ERROR: {e}")
                if self.metrics:
                    self.metrics.record_error()

    def _print_summary(self):
        if not self.metrics:
            return

        summary = self.metrics.summary()

        print("\n==============================")
        print(" GLOBAL METRICS")
        print("==============================")
        print(f"Elapsed (s)       : {summary['elapsed_sec']:.2f}")
        print(f"Total requests    : {summary['requests']}")
        print(f"Success           : {summary['success']}")
        print(f"Errors            : {summary['errors']}")
        print(f"RPS               : {summary['rps']:.2f}")
        print(f"Avg latency (ms)  : {summary['avg_latency']:.2f}")
        print(f"P95 latency (ms)  : {summary['p95_latency']:.2f}")
        print(f"Avg payload (KB)  : {summary['avg_payload_kb']:.2f}")
        print(f"Avg tx count      : {summary['avg_tx']:.2f}")