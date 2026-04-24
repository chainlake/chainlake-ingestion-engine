from rpcstream.runtime.observability.provider import build_observability


def init_metrics(runtime):
    return None


def init_telemetry(runtime):
    return build_observability(runtime.observability.config, runtime.pipeline.name)
