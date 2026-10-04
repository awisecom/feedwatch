"""Command line: feedwatch sim | run | compare."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import signal
from collections.abc import Awaitable, Callable
from pathlib import Path

from feedwatch import __version__
from feedwatch.compare import render_svg, render_table, run_compare
from feedwatch.sim import SimConfig, Simulator, Stall
from feedwatch.sim import serve as serve_sim
from feedwatch.sink import NullSink, PostgresSink, Sink
from feedwatch.status import serve as serve_status
from feedwatch.watcher import WatchConfig, Watcher

log = logging.getLogger("feedwatch")


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="feedwatch", description=__doc__)
    p.add_argument("--version", action="version", version=f"feedwatch {__version__}")
    p.add_argument("--log-level", default="info", choices=["debug", "info", "warning", "error"])
    sub = p.add_subparsers(dest="command", required=True)

    sim = sub.add_parser("sim", help="run the upstream simulator (push feed, REST, aggregator)")
    sim.add_argument("--host", default="127.0.0.1")
    sim.add_argument("--port", type=int, default=8765)
    sim.add_argument("--slot", type=float, default=0.4, help="seconds between events per symbol")
    sim.add_argument("--revise", type=float, default=0.02, help="chance a confirmation differs from the provisional")
    sim.add_argument("--drop", type=float, default=0.0, help="chance a confirmed message is lost on the way (gaps)")
    sim.add_argument("--disconnect-every", type=float, default=0.0, help="close every connection every N seconds")
    sim.add_argument("--latency", type=float, default=0.03, help="one-way network delay, seconds")
    sim.add_argument("--cache-min", type=float, default=2.0, help="aggregator cache lifetime, lower bound")
    sim.add_argument("--cache-max", type=float, default=15.0, help="aggregator cache lifetime, upper bound")
    sim.add_argument(
        "--stall",
        action="append",
        default=[],
        metavar="SYMBOL:AFTER:FOR",
        help="symbol goes quiet AFTER seconds in, FOR seconds (repeatable)",
    )
    sim.add_argument("--seed", type=int)

    run = sub.add_parser("run", help="watch an upstream, serve the board, write to PostgreSQL")
    run.add_argument("--upstream", required=True, help="base URL of the upstream, e.g. http://localhost:8765")
    run.add_argument("--mode", choices=["stream", "poll"], default="stream")
    run.add_argument("--symbols", help="comma separated; default: everything published")
    run.add_argument("--host", default="127.0.0.1")
    run.add_argument("--port", type=int, default=8080)
    run.add_argument("--dsn", default=os.environ.get("FEEDWATCH_DSN"), help="PostgreSQL DSN (or FEEDWATCH_DSN)")
    run.add_argument("--stale-after", type=float, default=2.0)
    run.add_argument("--dead-after", type=float, default=10.0)
    run.add_argument("--poll-interval", type=float, default=2.0)

    cmp = sub.add_parser("compare", help="polling vs push feed, side by side, with numbers")
    cmp.add_argument("--seconds", type=float, default=30.0)
    cmp.add_argument("--poll-interval", type=float, default=2.0)
    cmp.add_argument("--seed", type=int, default=7)
    cmp.add_argument("--svg", type=Path, help="also write a chart of one symbol's data age")
    cmp.add_argument("--json", action="store_true", help="print JSON instead of a table")
    return p


async def _until_signalled(main: Awaitable[object], cleanup: Callable[[], Awaitable[None]]) -> None:
    """Run `main` until it ends or SIGINT/SIGTERM arrives, then clean up."""
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    task = asyncio.ensure_future(main)
    stopper = asyncio.ensure_future(stop.wait())
    try:
        await asyncio.wait({task, stopper}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for t in (task, stopper):
            t.cancel()
        for t in (task, stopper):
            with contextlib.suppress(asyncio.CancelledError):
                await t
        await cleanup()


async def _sim(args: argparse.Namespace) -> int:
    cfg = SimConfig(
        slot=args.slot,
        revise_prob=args.revise,
        drop_prob=args.drop,
        disconnect_every=args.disconnect_every,
        latency=args.latency,
        cache_min=args.cache_min,
        cache_max=args.cache_max,
        stalls=tuple(Stall.parse(s) for s in args.stall),
        seed=args.seed,
    )
    runner = await serve_sim(Simulator(cfg), args.host, args.port)
    log.info("simulator on http://%s:%d  (ws /ws, /snapshot, /updates, /aggregated)", args.host, args.port)
    await _until_signalled(asyncio.Event().wait(), runner.cleanup)
    return 0


async def _run(args: argparse.Namespace) -> int:
    sink: Sink = PostgresSink(args.dsn) if args.dsn else NullSink()
    watcher = Watcher(
        WatchConfig(
            upstream=args.upstream,
            mode=args.mode,
            symbols=tuple(args.symbols.split(",")) if args.symbols else None,
            stale_after=args.stale_after,
            dead_after=args.dead_after,
            poll_interval=args.poll_interval,
        ),
        sink,
    )
    runner = await serve_status(watcher, args.host, args.port)
    log.info(
        "%s mode against %s, board on http://%s:%d, %s",
        args.mode,
        args.upstream,
        args.host,
        args.port,
        "writing to PostgreSQL" if args.dsn else "no database",
    )
    await _until_signalled(watcher.run(), runner.cleanup)
    return 0


async def _compare(args: argparse.Namespace) -> int:
    logging.getLogger("feedwatch").setLevel(logging.WARNING)  # the table is the output
    res = await run_compare(args.seconds, SimConfig(seed=args.seed), poll_interval=args.poll_interval)
    print(json.dumps(res.as_dict(), indent=2) if args.json else render_table(res))
    if args.svg:
        args.svg.write_text(render_svg(res), encoding="utf-8")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    handler = {"sim": _sim, "run": _run, "compare": _compare}[args.command]
    try:
        return asyncio.run(handler(args))
    except KeyboardInterrupt:
        return 130
