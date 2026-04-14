import asyncio

from rpcstream.client.jsonrpc import JsonRpcClient
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler

from rpcstream.ingestion.engine import IngestionEngine
from rpcstream.ingestion.fetcher import RpcFetcher
from rpcstream.ingestion.processor import EVMProcessor
from rpcstream.sinks.kafka.producer import KafkaSink

from rpcstream.adapters.evm.identity.event_id_calculator import EventIdCalculator
from rpcstream.adapters.evm.identity.event_time_calculator import EventTimeCalculator

from confluent_kafka import Producer

from rpcstream.utils.config_loader import load_pipeline_config
from rpcstream.utils.topic_builder import build_topic
from rpcstream.utils.logger import get_logger

async def main():
    # -------------------------
    # LOAD YAML CONFIG
    # -------------------------
    config = load_pipeline_config("block_pipeline_backfill.yaml")

    logger = get_logger(config["log"]["name"], config["log"]["level"])
    adapter_type = config["adapter"]["type"]
    pipeline_type = config["pipeline"]["type"]
    chain = config["adapter"]["chain"]
    network = config["adapter"]["network"]
    schemas = config["schema"]["entities"]
    rpc_conf = config["rpc"]

    # -------------------------
    # BUILD TOPICS DYNAMICALLY
    # -------------------------
    TOPICS = {
        schema.rstrip("s"): build_topic(
            adapter_type,
            chain,
            network,
            schema
        )
        for schema in schemas
    }

    # -------------------------
    # RPC
    # -------------------------
    client = JsonRpcClient(rpc_conf["endpoint"], timeout_sec=rpc_conf["inflight"]["timeout_sec"])

    scheduler = AdaptiveRpcScheduler(
        client,
        initial_inflight=rpc_conf["inflight"]["initial"],
        max_inflight=rpc_conf["inflight"]["max"],
    )

    fetcher = RpcFetcher(scheduler, pipeline_type, logger)

    # -------------------------
    # PROCESSOR
    # -------------------------
    processor = EVMProcessor(logger)

    # -------------------------
    # KAFKA
    # -------------------------
    producer = Producer({
        "bootstrap.servers": config["kafka"]["broker"],
        "linger.ms": config["kafka"]["producer"]["linger_ms"],
        "batch.size": config["kafka"]["producer"]["batch_size"],
    })

    kafka_sink = KafkaSink(
        producer=producer,
        id_calculator=EventIdCalculator(),
        time_calculator=EventTimeCalculator(),
        logger=logger,
    )

    # -------------------------
    # ENGINE
    # -------------------------
    engine = IngestionEngine(
        fetcher=fetcher,
        processor=processor,
        sink=kafka_sink,
        topics=TOPICS,
        concurrency=config["engine"]["concurrency"],
        logger=logger,
    )

    try:
        await engine.run_batch(
            config["range"]["start_block"],
            config["range"]["end_block"]
        )

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())