class _NoOp:
    def add(self, *args, **kwargs):
        pass

    def record(self, *args, **kwargs):
        pass


class ClientMetrics:
    def __init__(self, meter=None):
        if meter is None:
            self.REQUEST_COUNTER = _NoOp()
            self.REQUEST_SUBMITTED_COUNTER = _NoOp()
            self.INFLIGHT_GAUGE = _NoOp()
            self.RETRY_COUNTER = _NoOp()
            self.LATENCY_HISTOGRAM = _NoOp()
            return

        self.REQUEST_COUNTER = meter.create_counter(
            "rpc_requests_total"
        )

        self.REQUEST_SUBMITTED_COUNTER = meter.create_counter(
            "rpc_submitted_total"
        )

        self.INFLIGHT_GAUGE = meter.create_up_down_counter(
            "rpc_inflight"
        )

        self.RETRY_COUNTER = meter.create_counter(
            "rpc_retries_total"
        )

        self.LATENCY_HISTOGRAM = meter.create_histogram(
            "rpc_latency_ms", unit="ms"
        )
