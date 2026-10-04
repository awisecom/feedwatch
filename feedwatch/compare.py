"""`feedwatch compare`: reproduce the original finding with numbers.

Starts the simulator and two watchers against it in one process, one polling
the aggregator the old way and one on the push feed, samples the data age of
every symbol ten times a second, and reports the distribution for each.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from typing import Any

from feedwatch.sim import SimConfig, Simulator, bound_port, serve
from feedwatch.stats import Window, quantile
from feedwatch.svg import Series, line_chart
from feedwatch.watcher import WatchConfig, Watcher


@dataclass(slots=True)
class Row:
    name: str
    samples: int
    p50: float | None
    p95: float | None
    p99: float | None
    max: float | None

    @classmethod
    def of(cls, name: str, window: Window) -> Row:
        values = sorted(window.values())
        return cls(
            name,
            len(values),
            quantile(values, 0.50, presorted=True),
            quantile(values, 0.95, presorted=True),
            quantile(values, 0.99, presorted=True),
            values[-1] if values else None,
        )


@dataclass(slots=True)
class CompareResult:
    seconds: float
    symbols: int
    rows: list[Row]
    trace_symbol: str
    poll_trace: list[tuple[float, float | None]] = field(default_factory=list)
    stream_trace: list[tuple[float, float | None]] = field(default_factory=list)
    stream_events: dict[str, int] = field(default_factory=dict)
    poll_events: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "seconds": self.seconds,
            "symbols": self.symbols,
            "data_age_s": [_row(r) for r in self.rows],
            "stream_events": self.stream_events,
            "poll_events": self.poll_events,
        }


def _row(r: Row) -> dict[str, Any]:
    return {"name": r.name, "samples": r.samples, "p50": r.p50, "p95": r.p95, "p99": r.p99, "max": r.max}


async def run_compare(
    seconds: float = 30.0,
    sim_cfg: SimConfig | None = None,
    poll_interval: float = 2.0,
    sample_every: float = 0.1,
    stale_after: float = 2.0,
) -> CompareResult:
    sim = Simulator(sim_cfg or SimConfig())
    runner = await serve(sim, "127.0.0.1", 0)
    upstream = f"http://127.0.0.1:{bound_port(runner)}"
    trace = sim.cfg.symbols[0]
    common: dict[str, Any] = {
        "sample_every": sample_every,
        "stale_after": stale_after,
        "dead_after": max(10.0, stale_after * 5),
        "window": 1_000_000,
        "trace_symbol": trace,
    }
    stream = Watcher(WatchConfig(upstream, mode="stream", **common))
    poll = Watcher(WatchConfig(upstream, mode="poll", poll_interval=poll_interval, **common))
    poll.started = stream.started
    tasks = [asyncio.create_task(w.run()) for w in (stream, poll)]
    try:
        await asyncio.sleep(seconds)
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t
        await runner.cleanup()

    return CompareResult(
        seconds=seconds,
        symbols=len(sim.cfg.symbols),
        rows=[
            Row.of(f"polling the aggregator every {poll_interval:g} s", poll.age["confirmed"]),
            Row.of("push feed, provisional", stream.age["provisional"]),
            Row.of("push feed, confirmed", stream.age["confirmed"]),
        ],
        trace_symbol=trace,
        poll_trace=poll.trace,
        stream_trace=stream.trace,
        stream_events=dict(stream.events),
        poll_events=dict(poll.events),
    )


def fmt_s(v: float | None) -> str:
    if v is None:
        return "-"
    return f"{v:.2f} s" if v < 1 else f"{v:.1f} s"


def render_table(res: CompareResult) -> str:
    width = max(len(r.name) for r in res.rows)
    lines = [
        f"data age over {res.seconds:g} s, {res.symbols} symbols, sampled every 100 ms",
        "",
        f"{'':<{width}}  {'p50':>8}  {'p95':>8}  {'p99':>8}  {'max':>8}",
    ]
    for r in res.rows:
        lines.append(f"{r.name:<{width}}  {fmt_s(r.p50):>8}  {fmt_s(r.p95):>8}  {fmt_s(r.p99):>8}  {fmt_s(r.max):>8}")
    ev = res.stream_events
    lines += [
        "",
        f"push feed: {ev.get('updates_confirmed', 0)} confirmed updates, {ev.get('gaps', 0)} gaps, "
        f"{ev.get('revisions', 0)} provisional values revised by their confirmation",
    ]
    return "\n".join(lines)


def render_svg(res: CompareResult) -> str:
    stream_p50 = res.rows[2].p50
    poll_p50 = res.rows[0].p50
    return line_chart(
        [
            Series(res.rows[0].name, "polling", res.poll_trace, slot=2),
            Series("push feed, confirmed values", "push feed", res.stream_trace, slot=1),
        ],
        title=f"Data age of {res.trace_symbol}: polling vs push feed",
        subtitle=f"Simulated upstream, {res.seconds:g} s. Median data age: polling {fmt_s(poll_p50)}, "
        f"push feed {fmt_s(stream_p50)} (confirmed values only).",
        x_label="seconds",
        threshold=(2.0, "stale threshold, 2 s"),
    )
