from __future__ import annotations

import pytest

from feedwatch.backoff import Backoff
from feedwatch.staleness import Freshness, StaleDetector
from feedwatch.stats import Histogram, Window, quantile


def test_quantile_is_nearest_rank() -> None:
    data = [float(i) for i in range(1, 101)]
    assert quantile(data, 0.50) == 50.0
    assert quantile(data, 0.99) == 99.0
    assert quantile(data, 1.0) == 100.0
    assert quantile([], 0.5) is None


def test_window_keeps_the_last_n_samples() -> None:
    w = Window(3)
    for v in (1.0, 2.0, 3.0, 4.0, 5.0):
        w.add(v)
    assert w.values() == [3.0, 4.0, 5.0]
    assert w.count == 5
    assert w.summary()["max"] == 5.0


def test_histogram_is_cumulative_and_ends_with_inf() -> None:
    h = Histogram((0.1, 1.0))
    for v in (0.05, 0.5, 2.0):
        h.observe(v)
    assert h.cumulative() == [("0.1", 1), ("1.0", 2), ("+Inf", 3)]
    assert h.count == 3
    assert h.sum == pytest.approx(2.55)


def test_freshness_transitions() -> None:
    d = StaleDetector(stale_after=2.0, dead_after=10.0)
    assert d.update("ALFA", None, 0.0) is None
    t = d.update("ALFA", 0.5, 1.0)
    assert t is not None and (t.before, t.after) == (Freshness.UNKNOWN, Freshness.LIVE)
    assert d.update("ALFA", 1.9, 2.0) is None  # still live: no event
    t = d.update("ALFA", 2.5, 3.0)
    assert t is not None and t.after is Freshness.STALE
    t = d.update("ALFA", 11.0, 4.0)
    assert t is not None and t.after is Freshness.DEAD
    t = d.update("ALFA", 0.3, 5.0)
    assert t is not None and (t.before, t.after) == (Freshness.DEAD, Freshness.LIVE)
    assert d.since("ALFA") == 5.0


def test_thresholds_must_make_sense() -> None:
    with pytest.raises(ValueError):
        StaleDetector(stale_after=5.0, dead_after=2.0)


def test_backoff_doubles_up_to_the_cap_and_resets() -> None:
    b = Backoff(base=0.5, cap=4.0, rand=lambda: 1.0)
    assert [b.next_delay() for _ in range(6)] == [0.5, 1.0, 2.0, 4.0, 4.0, 4.0]
    b.reset()
    assert b.next_delay() == 0.5


def test_backoff_jitter_stays_inside_the_ceiling() -> None:
    b = Backoff(base=1.0, cap=8.0)
    for _ in range(50):
        ceiling = b.ceiling()
        assert 0.0 <= b.next_delay() <= ceiling


def test_backoff_survives_thousands_of_attempts() -> None:
    b = Backoff(cap=10.0, rand=lambda: 1.0)
    for _ in range(5000):
        assert b.next_delay() <= 10.0
