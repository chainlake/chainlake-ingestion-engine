from opentelemetry import metrics

meter = metrics.get_meter("rpcstream.kafka")

# Throughput
MESSAGE_COUNTER = meter.create_counter(
    "kafka_messages_total",
)

BATCH_COUNTER = meter.create_counter(
    "kafka_batches_total",
)

# Latency
BATCH_LATENCY = meter.create_histogram(
    "kafka_batch_latency_ms",
    unit="ms",
)

PRODUCE_LATENCY = meter.create_histogram(
    "kafka_produce_latency_ms",
    unit="ms",
)

# Backpressure
QUEUE_SIZE = meter.create_up_down_counter(
    "kafka_queue_size",
)

BUFFER_RETRY_COUNTER = meter.create_counter(
    "kafka_buffer_retries_total",
)

# Reliability
DELIVERY_SUCCESS = meter.create_counter(
    "kafka_delivery_success_total",
)

DELIVERY_ERROR = meter.create_counter(
    "kafka_delivery_errors_total",
)