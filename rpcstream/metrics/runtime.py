import time
import statistics

class RuntimeMetrics:
    def __init__(self):
        self.start_ts = time.time()

        self.latencies = []
        self.queue_waits = []
        self.payload_sizes = []
        self.tx_counts = []

        self.errors = 0

    def record(self, latency, queue_wait, payload_size, tx_count):
        self.latencies.append(latency)
        self.queue_waits.append(queue_wait)
        self.payload_sizes.append(payload_size)
        self.tx_counts.append(tx_count)

    def record_error(self):
        self.errors += 1

    def summary(self):
        elapsed = time.time() - self.start_ts

        def avg(x): return statistics.mean(x) if x else 0

        return {
            "elapsed_sec": elapsed,
            "requests": len(self.latencies) + self.errors,
            "success": len(self.latencies),
            "errors": self.errors,
            "rps": (len(self.latencies) / elapsed) if elapsed else 0,
            "avg_latency": avg(self.latencies),
            "p95_latency": percentile(self.latencies, 95),
            "avg_payload_kb": avg(self.payload_sizes) / 1024,
            "avg_tx": avg(self.tx_counts),
        }


def percentile(data, p):
    if not data:
        return 0
    data = sorted(data)
    k = int(len(data) * p / 100)
    return data[min(k, len(data) - 1)]