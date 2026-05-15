from time import perf_counter_ns, time_ns


def utc_now_ns() -> int:
    """Wall-clock nanoseconds since Unix epoch."""

    return time_ns()


def monotonic_now_ns() -> int:
    """Monotonic nanoseconds for latency measurement."""

    return perf_counter_ns()
