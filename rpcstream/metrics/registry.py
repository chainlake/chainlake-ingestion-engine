from typing import Dict


class _NoOp:
    def add(self, *args, **kwargs): pass
    def record(self, *args, **kwargs): pass


class MetricsRegistry:
    def __init__(self):
        self._enabled = False
        self._meters: Dict[str, object] = {}
        self._meter_provider = None

    def init(self, enabled: bool, meter_provider=None):
        self._enabled = enabled
        self._meter_provider = meter_provider

    def register(self, name: str, builder):
        """
        builder: function(meter) -> metrics object (class)
        """
        if not self._enabled or self._meter_provider is None:
            self._meters[name] = builder(None)  # pass None → NoOp mode
            return

        meter = self._meter_provider.get_meter(name)
        self._meters[name] = builder(meter)

    def get(self, name: str):
        return self._meters[name]


# global singleton
registry = MetricsRegistry()
