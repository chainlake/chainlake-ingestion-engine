import time

from rpcstream.adapters.evm.identity.event_time_calculator import EventTimeCalculator


def test_calculate_ingest_timestamp_tracks_unix_epoch_time():
    calculator = EventTimeCalculator()

    before = int(time.time() * 1000)
    observed = calculator.calculate_ingest_timestamp()
    after = int(time.time() * 1000)

    assert before <= observed <= after
