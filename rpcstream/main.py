import asyncio
from confluent_kafka import Producer

from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.resolver import resolve
from rpcstream.runtime.observability.provider import build_observability

from rpcstream.client.jsonrpc import JsonRpcClient
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler

from rpcstream.ingestion.engine import IngestionEngine
from rpcstream.ingestion.fetcher import EvmRpcFetcher
from rpcstream.sinks.kafka.producer import KafkaWriter

from rpcstream.adapters.evm.identity.event_id_calculator import EventIdCalculator
from rpcstream.adapters.evm.identity.event_time_calculator import EventTimeCalculator
from rpcstream.adapters.evm.schema import EVM_ENTITY_SCHEMAS
from rpcstream.adapters.evm.processor import PROCESSOR_REGISTRY
from rpcstream.sinks.kafka.protobuf import DLQ_SCHEMA

from rpcstream.planner.block_source import RealtimeBlockSource
from rpcstream.runtime.block_tracker import BlockHeadTracker

from rpcstream.utils.logger import JsonLogger
import os


async def main():
    # Load config(typed)   
    config_path = os.getenv("PIPELINE_CONFIG", "pipeline.yaml")
    config = load_pipeline_config(config_path)

    # Resolve config
    runtime = resolve(config)
    logger = JsonLogger(level=config.logLevel)
    topic_maps = runtime.topic_map
    main_topics = topic_maps.main
    dlq_topics = topic_maps.dlq
    observability = build_observability(runtime.observability.config, runtime.pipeline.name)
    await observability.start()

    client = None
    tracker = None
    
    try:
        client = JsonRpcClient(
            base_url=runtime.client.base_url,
            timeout_sec=runtime.client.timeout_sec,
            max_retries=runtime.client.max_retries,
            logger=logger,
            observability=observability,
        )
        
        # -------------------------
        # TRACKER
        # -------------------------
        tracker = BlockHeadTracker(
            client=client,
            poll_interval=runtime.tracker.poll_interval,
            logger=logger,
        )
        
        await tracker.start()
        
        # -------------------------
        # SCHEDULER
        # -------------------------

        scheduler = AdaptiveRpcScheduler(
            client,
            initial_inflight=runtime.scheduler.initial_inflight,
            max_inflight=runtime.scheduler.max_inflight,
            min_inflight=runtime.scheduler.min_inflight,
            latency_target_ms=runtime.scheduler.latency_target_ms,
            logger=logger,
            observability=observability,
        )

        # Pass tracker to fetcher
        fetcher = EvmRpcFetcher(scheduler, runtime.entities, logger, tracker)

        # -------------------------
        # PROCESSOR
        # -------------------------
        # Load processors dynamically based on the YAML configuration
        processors = {
            entity: PROCESSOR_REGISTRY[entity]
            for entity in runtime.entities
        }
                
        
        # -------------------------
        # KAFKA
        # -------------------------
        producer = Producer(runtime.kafka.config)

        kafka_write = KafkaWriter(
            producer=producer,
            id_calculator=EventIdCalculator(),
            time_calculator=EventTimeCalculator(),
            logger=logger,
            config=runtime.kafka.streaming,
            producer_config=runtime.kafka.config,
            topic_maps=topic_maps,
            protobuf_enabled=runtime.kafka.protobuf_enabled,
            schema_registry_url=runtime.kafka.schema_registry_url,
            protobuf_topic_schemas={
                **{
                    topic_maps.main[entity]: EVM_ENTITY_SCHEMAS[entity]
                    for entity in runtime.entities
                    if entity in EVM_ENTITY_SCHEMAS
                },
                **{
                    topic: DLQ_SCHEMA
                    for topic in topic_maps.dlq.values()
                },
            },
            observability=observability,
        )

        # -------------------------
        # ENGINE
        # -------------------------
        engine = IngestionEngine(
            fetcher=fetcher,
            processors=processors,
            sink=kafka_write,
            topics=main_topics,
            dlq_topics=dlq_topics,
            concurrency=runtime.engine.concurrency,
            logger=logger,
            observability=observability,
        )
        
        # -------------------------
        # RUN PIPELINE
        # -------------------------
        block_source = RealtimeBlockSource(tracker, observability=observability)
        
        await engine.run_stream(block_source)
        
    finally:
        if tracker is not None:
            await tracker.stop()
        elif client is not None:
            await client.close()
        await observability.shutdown()

def cli():
    asyncio.run(main())
    
if __name__ == "__main__":
    asyncio.run(main())
