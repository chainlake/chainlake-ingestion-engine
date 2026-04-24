from rpcstream.scheduler.base import BaseScheduler


def test_base_scheduler_clamps_runtime_bounds():
    scheduler = BaseScheduler(
        min_inflight=0,
        max_inflight=0,
        initial_inflight=0,
        latency_target_ms=1000,
    )

    assert scheduler.min_inflight == 1
    assert scheduler.max_inflight == 1
    assert scheduler.current_limit == 1


def test_base_scheduler_keeps_initial_within_min_max():
    scheduler = BaseScheduler(
        min_inflight=3,
        max_inflight=5,
        initial_inflight=10,
        latency_target_ms=1000,
    )

    assert scheduler.min_inflight == 3
    assert scheduler.max_inflight == 5
    assert scheduler.current_limit == 5
