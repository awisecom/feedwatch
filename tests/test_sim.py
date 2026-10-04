from __future__ import annotations

from feedwatch.model import Level
from feedwatch.sim import Market, SimConfig, Stall


def test_the_confirmed_version_follows_one_slot_later() -> None:
    m = Market(SimConfig(symbols=("ALFA",), seed=1))
    first = m.step(0.0, 0.0)
    assert [(u.seq, u.level) for u in first] == [(1, Level.PROVISIONAL)]
    second = m.step(0.4, 0.4)
    assert [(u.seq, u.level) for u in second] == [(1, Level.CONFIRMED), (2, Level.PROVISIONAL)]
    assert second[0].source_ts == first[0].source_ts  # event time, not confirmation time


def test_the_running_total_equals_the_sum_of_confirmed_quantities() -> None:
    m = Market(SimConfig(symbols=("ALFA", "BRAVO"), seed=2))
    confirmed = []
    for i in range(200):
        confirmed += [u for u in m.step(i * 0.4, i * 0.4) if u.level is Level.CONFIRMED]
    for symbol in ("ALFA", "BRAVO"):
        total = sum(u.qty for u in confirmed if u.symbol == symbol)
        assert abs(total - m.cum_qty[symbol]) < 1e-6
        assert m.snapshot(symbol).cum_qty == m.cum_qty[symbol]


def test_history_serves_recent_ranges_and_refuses_old_ones() -> None:
    m = Market(SimConfig(symbols=("ALFA",), history=5, seed=3))
    for i in range(20):
        m.step(i * 0.4, i * 0.4)
    last = m.confirmed["ALFA"].seq
    assert m.updates("ALFA", 1, 3) is None  # rolled out of the history
    found = m.updates("ALFA", last - 2, last)
    assert found is not None and [u.seq for u in found] == [last - 2, last - 1, last]


def test_a_stalled_symbol_publishes_nothing() -> None:
    m = Market(SimConfig(symbols=("ALFA", "BRAVO"), stalls=(Stall("ALFA", 1.0, 2.0),), seed=4))
    during = m.step(1.5, 1.5)
    assert {u.symbol for u in during if u.level is Level.PROVISIONAL} == {"BRAVO"}
    after = m.step(3.5, 3.5)
    assert "ALFA" in {u.symbol for u in after if u.level is Level.PROVISIONAL}


def test_stall_parses_from_the_command_line() -> None:
    assert Stall.parse("DELTA:20:10") == Stall("DELTA", 20.0, 10.0)


def test_revisions_happen_at_the_configured_rate() -> None:
    m = Market(SimConfig(symbols=("ALFA",), revise_prob=0.5, seed=5))
    provisional: dict[int, float] = {}
    revised = total = 0
    for i in range(400):
        for u in m.step(i * 0.4, i * 0.4):
            if u.level is Level.PROVISIONAL:
                provisional[u.seq] = u.price
            else:
                total += 1
                revised += provisional[u.seq] != u.price
    assert 0.4 < revised / total < 0.6
