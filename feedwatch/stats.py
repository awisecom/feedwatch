"""Latency and freshness statistics.

Two different questions, two different numbers:

* delivery lag: how late did each update arrive? (receive time - event time)
* data age: right now, how old is the newest value I hold? (now - event time)

Polling every few seconds can show a small delivery lag on the request that
happens to land right after a cache refresh, while the data age seen by
everything downstream is still seconds. Data age is the number that matters
for decisions, so it is sampled continuously, not only when data arrives.
"""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Sequence

LAG_BUCKETS: tuple[float, ...] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    15.0,
    30.0,
)


class Window:
    """The last `size` samples, with exact quantiles on demand."""

    def __init__(self, size: int = 4096) -> None:
        self._samples: deque[float] = deque(maxlen=size)
        self.count = 0

    def add(self, value: float) -> None:
        self._samples.append(value)
        self.count += 1

    def __len__(self) -> int:
        return len(self._samples)

    def values(self) -> list[float]:
        return list(self._samples)

    def quantile(self, q: float) -> float | None:
        return quantile(self._samples, q)

    def summary(self) -> dict[str, float | int | None]:
        data = sorted(self._samples)
        return {
            "count": self.count,
            "p50": quantile(data, 0.50, presorted=True),
            "p90": quantile(data, 0.90, presorted=True),
            "p99": quantile(data, 0.99, presorted=True),
            "max": data[-1] if data else None,
        }


def quantile(values: Sequence[float] | deque[float], q: float, presorted: bool = False) -> float | None:
    """Nearest-rank quantile: always a value that was actually observed."""
    if not values:
        return None
    data = values if presorted else sorted(values)
    rank = max(1, math.ceil(q * len(data)))
    return data[rank - 1]


class Histogram:
    """Cumulative histogram in the Prometheus sense."""

    def __init__(self, buckets: Sequence[float] = LAG_BUCKETS) -> None:
        self.buckets = tuple(sorted(buckets))
        self._counts = [0] * (len(self.buckets) + 1)  # last slot is +Inf
        self.sum = 0.0
        self.count = 0

    def observe(self, value: float) -> None:
        self.sum += value
        self.count += 1
        for i, upper in enumerate(self.buckets):
            if value <= upper:
                self._counts[i] += 1
                return
        self._counts[-1] += 1

    def cumulative(self) -> list[tuple[str, int]]:
        """(le, cumulative count) pairs, ending with +Inf == count."""
        out: list[tuple[str, int]] = []
        running = 0
        for upper, n in zip(self.buckets, self._counts, strict=False):
            running += n
            out.append((format_float(upper), running))
        out.append(("+Inf", running + self._counts[-1]))
        return out


def format_float(v: float) -> str:
    return repr(v) if not v.is_integer() else f"{v:.1f}"
