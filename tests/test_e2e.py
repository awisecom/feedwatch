"""The whole data path in one process: simulator, network, watcher."""

from __future__ import annotations

import asyncio
import contextlib
import xml.etree.ElementTree as ET
from collections import defaultdict

import pytest
from conftest import FAST

from feedwatch.compare import render_svg, run_compare
from feedwatch.model import Snapshot
from feedwatch.sim import SimConfig, Simulator, Stall, bound_port, serve
from feedwatch.sink import MemorySink
from feedwatch.watcher import WatchConfig, Watcher


async def _scenario(
    seconds: float, freeze: bool = True, **sim: object
) -> tuple[Watcher, dict[str, Snapshot], MemorySink]:
    """Run simulator and watcher for `seconds`. With `freeze`, then stop the source
    and give the watcher up to 8 s to catch up with it."""
    simulator = Simulator(SimConfig(**{"seed": 3, **FAST, **sim}))  # type: ignore[arg-type]
    runner = await serve(simulator, "127.0.0.1", 0)
    sink = MemorySink()
    watcher = Watcher(
        WatchConfig(
            f"http://127.0.0.1:{bound_port(runner)}",
            idle_timeout=0.6,
            healthy_after=0.3,
            sample_every=0.02,
            stale_after=0.5,
            dead_after=2.0,
        ),
        sink,
    )
    task = asyncio.create_task(watcher.run())
    loop = asyncio.get_running_loop()
    try:
        await asyncio.sleep(seconds)
        if freeze:
            simulator.paused = True
            truth = simulator.truth()
            deadline = loop.time() + 8.0
            while loop.time() < deadline:
                books = watcher.books
                if all(s in books and books[s].seq == t.seq and not books[s].buffer for s, t in truth.items()):
                    break
                await asyncio.sleep(0.05)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await runner.cleanup()
    return watcher, simulator.truth(), sink


def test_confirmed_state_matches_the_source_exactly_despite_drops_and_disconnects() -> None:
    watcher, truth, sink = asyncio.run(_scenario(3.0, drop_prob=0.05, disconnect_every=0.7))

    for symbol, snap in truth.items():
        book = watcher.books[symbol]
        assert book.seq == snap.seq, symbol
        assert book.cum_qty == pytest.approx(snap.cum_qty, abs=1e-6), symbol

    assert watcher.events["gaps"] > 0  # the faults really happened
    assert watcher.events["backfills"] > 0
    assert watcher.events["connects"] >= 2

    # Every confirmed event after the baseline was stored exactly once, without holes.
    seqs: dict[str, list[int]] = defaultdict(list)
    for row in sink.rows:
        seqs[row.symbol].append(row.seq)
    for symbol, got in seqs.items():
        assert got == list(range(got[0], got[0] + len(got))), symbol
    assert any(row.via == "backfill" for row in sink.rows)


def test_a_symbol_that_goes_quiet_is_reported_stale_then_live_again() -> None:
    _, _, sink = asyncio.run(_scenario(3.2, freeze=False, seed=4, stalls=(Stall("DELTA", 1.0, 1.2),)))
    changes = [(t.before.value, t.after.value) for t in sink.transitions if t.symbol == "DELTA"]
    assert ("live", "stale") in changes
    assert ("stale", "live") in changes or ("dead", "live") in changes
    others = [t for t in sink.transitions if t.symbol != "DELTA" and t.after.value != "live"]
    assert others == []  # nobody else went stale


def test_the_push_feed_is_fresher_than_polling() -> None:
    res = asyncio.run(
        run_compare(3.0, SimConfig(seed=5, **FAST), poll_interval=0.25, sample_every=0.02, stale_after=0.5)
    )
    poll, provisional, confirmed = res.rows
    assert poll.p50 is not None and confirmed.p95 is not None and provisional.p50 is not None
    assert confirmed.p50 is not None
    assert confirmed.p95 < poll.p50
    assert provisional.p50 < confirmed.p50  # confirmation costs one slot
    ET.fromstring(render_svg(res))  # the chart is well-formed SVG
