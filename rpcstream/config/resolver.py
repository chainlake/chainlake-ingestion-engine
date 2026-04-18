from rpcstream.config.builder import build_kafka_config, build_erpc_endpoint, build_topic_maps
from rpcstream.config.profiles.loader import load_kafka_profiles
    
from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class KafkaRuntime:
    config: Dict[str, Any] 
    streaming: any
    
@dataclass
class ClientRuntime:
    base_url: str
    timeout_sec: int
    max_retries: int

@dataclass
class SchedulerRuntime:
    initial_inflight: int
    max_inflight: int
    min_inflight: int
    latency_target_ms: int

@dataclass
class EngineRuntime:
    concurrency: int

@dataclass
class TrackerRuntime:
    poll_interval: float

@dataclass
class PipelineRuntime:
    type: str
    mode: str

@dataclass
class RuntimeConfig:
    kafka: KafkaRuntime
    topic_map: dict
    client: ClientRuntime
    scheduler: SchedulerRuntime
    engine: EngineRuntime
    tracker: TrackerRuntime
    pipeline: PipelineRuntime
    
    
def resolve(cfg) -> RuntimeConfig:

    kafka = KafkaRuntime(
        config=build_kafka_config(cfg),
        streaming=cfg.kafka.streaming,
    )

    client = ClientRuntime(
        base_url=build_erpc_endpoint(cfg),
        timeout_sec=cfg.erpc.timeout_sec,
        max_retries=cfg.erpc.max_retries,
    )

    scheduler = SchedulerRuntime(
        initial_inflight=cfg.erpc.inflight.initial_inflight,
        max_inflight=cfg.erpc.inflight.max_inflight,
        min_inflight=cfg.erpc.inflight.min_inflight,
        latency_target_ms=cfg.erpc.inflight.latency_target_ms,
    )

    engine = EngineRuntime(
        concurrency=cfg.engine.concurrency
    )

    tracker = TrackerRuntime(
        poll_interval=cfg.tracker.poll_interval
    )

    pipeline = PipelineRuntime(
        type=cfg.pipeline.type,
        mode=cfg.pipeline.mode,
    )

    topic_map = build_topic_maps(cfg)

    return RuntimeConfig(
        kafka=kafka,
        topic_map=topic_map,
        client=client,
        scheduler=scheduler,
        engine=engine,
        tracker=tracker,
        pipeline=pipeline,
    )