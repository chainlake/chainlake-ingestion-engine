import asyncio

from rpcstream.client.jsonrpc import JsonRpcClient
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler

from rpcstream.ingestion.engine import IngestionEngine
from rpcstream.ingestion.fetcher import RpcFetcher
from rpcstream.ingestion.processor import EVMProcessor
from rpcstream.sinks.kafka.producer import KafkaSink

from rpcstream.adapters.evm.identity.event_id_calculator import EventIdCalculator
from rpcstream.adapters.evm.identity.event_time_calculator import EventTimeCalculator
from rpcstream.metrics.runtime import RuntimeMetrics
from confluent_kafka import Producer


RPC_URL = "http://localhost:30040/main/evm/56"
KAFKA_BROKER = "localhost:30092"

start_block = 90000099
end_bloock = 90000100

CHAIN = "bsc"

TOPICS = {
    "block": f"{CHAIN}.raw_blocks",
    "transaction": f"{CHAIN}.raw_transactions",
}


def block_logger(block_number, latency, queue_wait, tx_count, payload_kb):
    print(
        f"[Block {block_number}] "
        f"latency={latency:.2f}ms "
        f"queue_wait={queue_wait:.2f}ms "
        f"tx={tx_count} "
        f"payload={payload_kb:.1f}KB"
    )


async def main():
    metrics = RuntimeMetrics()

    # ---------------- RPC ----------------
    client = JsonRpcClient(RPC_URL, timeout_sec=5)

    scheduler = AdaptiveRpcScheduler(
        client,
        initial_inflight=10,
        max_inflight=20,
    )

    fetcher = RpcFetcher(scheduler)

    # ---------------- Processor ----------------
    processor = EVMProcessor()

    # ---------------- Kafka ----------------
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    kafka_sink = KafkaSink(
        producer=producer,
        id_calculator=EventIdCalculator(),
        time_calculator=EventTimeCalculator(),
    )

    # ---------------- Engine ----------------
    engine = IngestionEngine(
        fetcher=fetcher,
        processor=processor,
        sink=kafka_sink,
        topics=TOPICS,
        metrics=metrics,
        concurrency=10,
        logger=block_logger
    )

    try:
        await engine.run_batch(start_block, end_bloock)

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())