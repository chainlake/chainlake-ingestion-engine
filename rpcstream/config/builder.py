# build runtime configs (kafka, rpc)
import os
from .schema import PipelineConfig
from rpcstream.runtime.topic import build_topics, normalize_entity

def build_kafka_config(cfg: PipelineConfig) -> dict:
    kafka = cfg.kafka
    common = kafka.common

    result = {
        "bootstrap.servers": common.bootstrap_servers
    }

    # -------------------------
    # Profile
    # -------------------------
    from rpcstream.config.profiles.loader import load_kafka_profiles

    profiles = load_kafka_profiles()
    profile = profiles.get(kafka.profile, {})
    # -------------------------
    # Security
    # -------------------------
    security = profile.get("security")
    if security:
        result["security.protocol"] = security.get("protocol")
        if "mechanism" in security:
            result["sasl.mechanism"] = security["mechanism"]

    # -------------------------
    # Auth
    # -------------------------
    auth = profile.get("auth")
    if auth:
        username = os.getenv(auth.get("username_env", ""))
        password = os.getenv(auth.get("password_env", ""))

        if username:
            result["sasl.username"] = username
        if password:
            result["sasl.password"] = password

    # -------------------------
    # SSL
    # -------------------------
    ssl = profile.get("ssl")
    if ssl:
        ca_path = os.getenv(ssl.get("ca_path_env", ""))
        if ca_path:
            result["ssl.ca.location"] = ca_path

    # -------------------------
    # Producer tuning
    # -------------------------
    result["linger.ms"] = kafka.producer.linger_ms
    result["batch.size"] = kafka.producer.batch_size

    return result


def build_topic_maps(cfg):
    """
    Convert TopicSet → engine-compatible maps
    """

    topics = {}
    dlq_topics = {}

    for entity in cfg.entities:
        normalized = normalize_entity(entity)
        topic_set = build_topics(cfg, normalized)

        topics[normalized] = topic_set.main
        dlq_topics[normalized] = topic_set.dlq

    return topics, dlq_topics


def build_erpc_endpoint(cfg) -> str:
    chain_type = cfg.chain.type
    chain_id = cfg.chain.uid.split(":")[-1]

    return (
        f"{cfg.erpc.base_url}/"
        f"{cfg.erpc.project_id}/"
        f"{chain_type}/{chain_id}"
    )