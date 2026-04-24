class _NoOp:
    def add(self, *args, **kwargs):
        pass

    def record(self, *args, **kwargs):
        pass

class KafkaMetrics:
    def __init__(self, meter=None):
        if meter is None:
            self.MESSAGE_COUNTER = _NoOp()
            self.BATCH_COUNTER = _NoOp()
            self.BATCH_LATENCY = _NoOp()
            self.PRODUCE_LATENCY = _NoOp()
            self.QUEUE_SIZE = _NoOp()
            self.BUFFER_RETRY_COUNTER = _NoOp()
            self.DELIVERY_SUCCESS = _NoOp()
            self.DELIVERY_ERROR = _NoOp()
            return

        # Throughput
        self.MESSAGE_COUNTER = meter.create_counter(
            "kafka_messages_total",
        )

        self.BATCH_COUNTER = meter.create_counter(
            "kafka_batches_total",
        )

        # Latency
        self.BATCH_LATENCY = meter.create_histogram(
            "kafka_batch_latency_ms",
            unit="ms",
        )

        self.PRODUCE_LATENCY = meter.create_histogram(
            "kafka_produce_latency_ms",
            unit="ms",
        )

        # Backpressure
        self.QUEUE_SIZE = meter.create_up_down_counter(
            "kafka_queue_size",
        )

        self.BUFFER_RETRY_COUNTER = meter.create_counter(
            "kafka_buffer_retries_total",
        )

        # Reliability
        self.DELIVERY_SUCCESS = meter.create_counter(
            "kafka_delivery_success_total",
        )

        self.DELIVERY_ERROR = meter.create_counter(
            "kafka_delivery_errors_total",
        )
