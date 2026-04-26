from types import SimpleNamespace

from rpcstream.config.builder import build_erpc_endpoint
from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.naming import build_pipeline_name


def test_load_pipeline_config_resolves_chain_profile_fields(tmp_path):
    config_file = tmp_path / "pipeline.yaml"
    config_file.write_text(
        "\n".join(
            [
                "logLevel: info",
                "pipeline:",
                "  mode: realtime",
                "  start_block: latest",
                "  checkpoint:",
                "    enabled: true",
                "chain:",
                "  name: bsc",
                "  network: mainnet",
                "entities:",
                "  - block",
                "erpc:",
                "  project_id: main",
                "  base_url: http://localhost:30040",
                "  timeout_sec: 10",
                "  max_retries: 1",
                "  inflight:",
                "    min_inflight: 1",
                "    max_inflight: 5",
                "    initial_inflight: 1",
                "    latency_target_ms: 1000",
                "tracker:",
                "  poll_interval: 0.4",
                "engine:",
                "  concurrency: 1",
                "kafka:",
                "  connection:",
                "    bootstrap_servers: localhost:9092",
                "  common: {}",
                "  producer:",
                "    linger_ms: 50",
                "    batch_size: 1024",
                "  streaming: {}",
            ]
        )
    )

    config = load_pipeline_config(str(config_file))

    assert config.chain.uid == "evm:56"
    assert config.chain.type == "evm"
    assert config.chain.name == "bsc"
    assert config.chain.network == "mainnet"
    assert config.pipeline.name == "bsc_mainnet_realtime_checkpointed_latest"


def test_build_pipeline_name_covers_realtime_and_backfill_variants():
    assert build_pipeline_name(
        chain_name="bsc",
        network="mainnet",
        mode="realtime",
        start_block="latest",
        checkpoint_enabled=False,
    ) == "bsc_mainnet_realtime_latest"

    assert build_pipeline_name(
        chain_name="bsc",
        network="mainnet",
        mode="realtime",
        start_block="latest",
        checkpoint_enabled=True,
    ) == "bsc_mainnet_realtime_checkpointed_latest"

    assert build_pipeline_name(
        chain_name="bsc",
        network="mainnet",
        mode="backfill",
        start_block=100,
        end_block=200,
    ) == "bsc_mainnet_backfill_100_200"


def test_build_erpc_endpoint_uses_chain_profile_lookup():
    cfg = SimpleNamespace(
        chain=SimpleNamespace(name="bsc", network="mainnet"),
        erpc=SimpleNamespace(
            base_url="http://localhost:30040",
            project_id="main",
        ),
    )

    assert build_erpc_endpoint(cfg) == "http://localhost:30040/main/evm/56"
