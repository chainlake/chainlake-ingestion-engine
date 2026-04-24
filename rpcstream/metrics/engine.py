class _NoOp:
    def add(self, *args, **kwargs):
        pass

    def record(self, *args, **kwargs):
        pass

class EngineMetrics:
    def __init__(self, meter=None):
        if meter is None:
            self.BLOCK_COUNTER = _NoOp()
            self.ROW_COUNTER = _NoOp()
            self.DLQ_COUNTER = _NoOp()
            self.BLOCK_LATENCY = _NoOp()
            self.QUEUE_WAIT = _NoOp()
            self.TOTAL_TIME = _NoOp()
            self.INFLIGHT = _NoOp()
            self.ERROR_COUNTER = _NoOp()
            self.CHAIN_LAG = _NoOp()
            self.INGESTION_LAG = _NoOp()
            return

        # Throughput
        self.BLOCK_COUNTER = meter.create_counter(
            "rpcstream_engine_blocks_total",
        )

        self.ROW_COUNTER = meter.create_counter(
            "rpcstream_engine_rows_total",
        )

        self.DLQ_COUNTER = meter.create_counter(
            "rpcstream_engine_dlq_total",
        )

        # Latency
        self.BLOCK_LATENCY = meter.create_histogram(
            "rpcstream_engine_block_latency_ms",
            unit="ms",
        )

        self.QUEUE_WAIT = meter.create_histogram(
            "rpcstream_engine_queue_wait_ms",
            unit="ms",
        )

        self.TOTAL_TIME = meter.create_histogram(
            "rpcstream_engine_total_time_ms",
            unit="ms",
        )

        # Load
        self.INFLIGHT = meter.create_up_down_counter(
            "rpcstream_engine_inflight",
        )

        # Errors
        self.ERROR_COUNTER = meter.create_counter(
            "rpcstream_engine_errors_total",
        )

        # lag
        self.CHAIN_LAG = meter.create_histogram(
            name="rpcstream_engine_chain_lag",
            description="point-in-time lag at processing moment",
        )

        self.INGESTION_LAG = meter.create_histogram(
            name="rpcstream_engine_ingestion_lag",
            description="TRUE pipeline lag (monotonic)",
        )
