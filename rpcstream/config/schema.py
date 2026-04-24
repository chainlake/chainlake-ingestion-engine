from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from rpcstream.runtime.observability.config import ObservabilityConfig


class KafkaCommon(BaseModel):
    bootstrap_servers: str
    topic_template: Optional[str] = None


class KafkaProducer(BaseModel):
    linger_ms: int
    batch_size: int


class KafkaStreaming(BaseModel):
    batch_size: int = 100
    flush_interval_ms: int = 20
    queue_maxsize: int = 100


class KafkaConfig(BaseModel):
    profile: str
    common: KafkaCommon
    producer: KafkaProducer
    streaming: KafkaStreaming


class ChainConfig(BaseModel):
    uid: str
    type: str
    name: str
    network: str


class ErpcInflight(BaseModel):
    min_inflight: int
    max_inflight: int
    initial_inflight: int
    latency_target_ms: int


class ErpcConfig(BaseModel):
    project_id: str
    base_url: str
    timeout_sec: int
    max_retries: int
    inflight: ErpcInflight
    

class PipelineConfigModel(BaseModel):
    name: str
    mode: str

class TrackerConfig(BaseModel):
    poll_interval: float


class EngineConfig(BaseModel):
    concurrency: int

class PipelineConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    logLevel: str
    pipeline: PipelineConfigModel
    chain: ChainConfig
    entities: list[str]
    erpc: ErpcConfig
    tracker: TrackerConfig
    engine: EngineConfig
    kafka: KafkaConfig
    observability: ObservabilityConfig = Field(
        default_factory=ObservabilityConfig,
        alias="telemetry",
    )
