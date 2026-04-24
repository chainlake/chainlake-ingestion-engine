class MetricHandle:
    def __init__(self, meter):
        self._meter = meter
        self._cache = {}

    def counter(self, name):
        if name not in self._cache:
            self._cache[name] = self._meter.create_counter(name)
        return self._cache[name]

    def histogram(self, name):
        if name not in self._cache:
            self._cache[name] = self._meter.create_histogram(name)
        return self._cache[name]

    def up_down(self, name):
        if name not in self._cache:
            self._cache[name] = self._meter.create_up_down_counter(name)
        return self._cache[name]