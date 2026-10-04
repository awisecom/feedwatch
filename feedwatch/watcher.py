"""The watcher: a source feeds it, it keeps confirmed state, measures
everything, repairs gaps, and hands confirmed rows to the sink."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter, deque
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import aiohttp

from feedwatch.book import Result, SymbolBook
from feedwatch.model import Snapshot, Update
from feedwatch.sink import NullSink, Sink, TickRow
from feedwatch.sources import PollSource, StreamSource
from feedwatch.staleness import StaleDetector, Transition
from feedwatch.stats import Histogram, Window

log = logging.getLogger("feedwatch.watcher")

CHANNELS = ("provisional", "confirmed", "poll")


@dataclass(slots=True)
class WatchConfig:
    upstream: str
    mode: str = "stream"  # "stream" (push feed) or "poll" (the old aggregator path)
    symbols: tuple[str, ...] | None = None  # None: everything the upstream publishes
    stale_after: float = 2.0
    dead_after: float = 10.0
    poll_interval: float = 2.0
    sample_every: float = 0.1
    idle_timeout: float = 3.0
    healthy_after: float = 10.0
    window: int = 4096
    trace_symbol: str | None = None  # record a data-age time series for this symbol


class Watcher:
    def __init__(self, cfg: WatchConfig, sink: Sink | None = None) -> None:
        if cfg.mode not in ("stream", "poll"):
            raise ValueError(f"unknown mode {cfg.mode!r}")
        self.cfg = cfg
        self.sink: Sink = sink or NullSink()
        self.books: dict[str, SymbolBook] = {s: SymbolBook(s) for s in cfg.symbols or ()}
        self.stale = StaleDetector(cfg.stale_after, cfg.dead_after)
        self.lag = {c: Window(cfg.window) for c in CHANNELS}
        self.lag_hist = {c: Histogram() for c in CHANNELS}
        self.age = {"provisional": Window(cfg.window), "confirmed": Window(cfg.window)}
        self.age_now: dict[str, dict[str, float | None]] = {}
        self.events: Counter[str] = Counter()
        self.transitions: deque[Transition] = deque(maxlen=200)
        self.trace: list[tuple[float, float | None]] = []
        self.connected = False
        self.last_message_at: float | None = None
        self.started = time.time()
        self._resync: asyncio.Queue[str] = asyncio.Queue()
        self._resync_pending: set[str] = set()

    # -- running -----------------------------------------------------------

    async def run(self) -> None:
        async with aiohttp.ClientSession() as session, asyncio.TaskGroup() as tg:
            if self.cfg.mode == "stream":
                cfg = self.cfg
                source = StreamSource(cfg.upstream, cfg.symbols, cfg.idle_timeout, cfg.healthy_after)
                tg.create_task(source.run(session, self))
                tg.create_task(self._resync_worker(session))
            else:
                tg.create_task(PollSource(self.cfg.upstream, self.cfg.poll_interval).run(session, self))
            tg.create_task(self._sampler())
            tg.create_task(self.sink.run())

    # -- events from the sources ---------------------------------------------

    def on_connected(self) -> None:
        self.connected = True
        self.events["connects"] += 1
        for symbol, book in self.books.items():
            if book.needs_baseline():
                self._request_resync(symbol)

    def on_disconnected(self) -> None:
        if self.connected:
            self.events["disconnects"] += 1
        self.connected = False

    def on_heartbeat(self, recv_ts: float, seqs: dict[str, Any] | None = None) -> None:
        self.last_message_at = recv_ts
        for symbol, seq in (seqs or {}).items():
            book = self.books.get(symbol)
            if book is not None and book.note_upstream_seq(int(seq)):
                self.events["gaps"] += 1
                log.info("%s: heartbeat says seq %s exists, we have %d: backfilling", symbol, seq, book.seq)
                self._request_resync(symbol)

    def on_update(self, u: Update, recv_ts: float) -> None:
        self.last_message_at = recv_ts
        channel = u.level.value
        self._observe_lag(channel, recv_ts - u.source_ts)
        self.events[f"updates_{channel}"] += 1

        book = self._book(u.symbol)
        result = book.apply(u)
        if result is Result.GAP:
            self.events["gaps"] += 1
            log.info("%s: gap after seq %d (got %d), backfilling", u.symbol, book.seq, u.seq)
            self._request_resync(u.symbol)
        elif result is Result.DUPLICATE:
            self.events["duplicates"] += 1
        elif result is Result.BUFFERED and book.needs_baseline():
            self._request_resync(u.symbol)
        self._flush(book, recv_ts)

    def on_poll(self, snapshots: Iterable[Snapshot], recv_ts: float) -> None:
        self.last_message_at = recv_ts
        self.events["polls"] += 1
        for snap in snapshots:
            self._observe_lag("poll", recv_ts - snap.source_ts)
            self._book(snap.symbol).apply_snapshot(snap)

    def count(self, event: str, n: int = 1) -> None:
        self.events[event] += n

    # -- gap repair ---------------------------------------------------------

    def _request_resync(self, symbol: str) -> None:
        if symbol not in self._resync_pending:
            self._resync_pending.add(symbol)
            self._resync.put_nowait(symbol)

    async def _resync_worker(self, session: aiohttp.ClientSession) -> None:
        """Close gaps: backfill the missing range first, snapshot when it's gone upstream."""
        base = self.cfg.upstream.rstrip("/")
        timeout = aiohttp.ClientTimeout(total=5)
        while True:
            symbols = [await self._resync.get()]
            while not self._resync.empty():
                symbols.append(self._resync.get_nowait())
            need_snapshot: list[str] = []
            try:
                for symbol in symbols:
                    book = self.books[symbol]
                    if book.needs_baseline():
                        need_snapshot.append(symbol)
                        continue
                    missing = book.missing()
                    if missing is None:
                        continue
                    first, last = missing
                    params = {"symbol": symbol, "from": str(first), "to": str(last)}
                    async with session.get(f"{base}/updates", params=params, timeout=timeout) as resp:
                        if resp.status == 410:
                            self.events["backfill_gone"] += 1
                            need_snapshot.append(symbol)
                            continue
                        resp.raise_for_status()
                        updates = [Update.from_wire(d) for d in await resp.json()]
                    used = book.apply_backfill(updates)
                    self.events["backfilled_updates"] += used
                    self.events["backfills"] += 1
                    self._flush(book, time.time(), backfilled={u.seq for u in updates})
                    if used < last - first + 1:
                        need_snapshot.append(symbol)
                if need_snapshot:
                    params = {"symbols": ",".join(need_snapshot)}
                    async with session.get(f"{base}/snapshot", params=params, timeout=timeout) as resp:
                        resp.raise_for_status()
                        snaps = [Snapshot.from_wire(d) for d in await resp.json()]
                    for snap in snaps:
                        book = self._book(snap.symbol)
                        if book.apply_snapshot(snap):
                            self.events["snapshots"] += 1
                        self._flush(book, time.time())
            except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
                self.events["resync_errors"] += 1
                log.warning("resync of %s failed: %s", ",".join(symbols), exc)
                await asyncio.sleep(0.5)
            finally:
                for symbol in symbols:
                    self._resync_pending.discard(symbol)
            for symbol in symbols:
                book = self.books[symbol]
                if book.needs_baseline() or book.missing() is not None:
                    self._request_resync(symbol)

    # -- sampling: data age and freshness ----------------------------------

    async def _sampler(self) -> None:
        while True:
            self.sample(time.time())
            await asyncio.sleep(self.cfg.sample_every)

    def sample(self, now: float) -> None:
        for symbol, book in self.books.items():
            conf = now - book.source_ts if book.source_ts is not None else None
            prov = now - book.prov_ts if book.prov_ts is not None else None
            self.age_now[symbol] = {"confirmed": conf, "provisional": prov}
            if conf is not None:
                self.age["confirmed"].add(conf)
            if prov is not None:
                self.age["provisional"].add(prov)
            transition = self.stale.update(symbol, conf, now)
            if transition is not None:
                self.transitions.append(transition)
                self.events[f"to_{transition.after.value}"] += 1
                self.sink.freshness(transition)
                if transition.before.value != "unknown":
                    log.info(
                        "%s: %s -> %s (data age %.1f s)",
                        symbol,
                        transition.before.value,
                        transition.after.value,
                        conf or 0.0,
                    )
            if symbol == self.cfg.trace_symbol:
                self.trace.append((now - self.started, conf))

    # -- helpers ------------------------------------------------------------

    def _book(self, symbol: str) -> SymbolBook:
        book = self.books.get(symbol)
        if book is None:
            book = self.books[symbol] = SymbolBook(symbol)
        return book

    def _observe_lag(self, channel: str, lag: float) -> None:
        self.lag[channel].add(lag)
        self.lag_hist[channel].observe(max(0.0, lag))

    def _flush(self, book: SymbolBook, now: float, backfilled: set[int] | None = None) -> None:
        committed = book.take_committed()
        if committed:
            via = backfilled or set()
            self.sink.ticks(
                [
                    TickRow(u.symbol, u.seq, u.price, u.qty, u.source_ts, now, "backfill" if u.seq in via else "stream")
                    for u in committed
                ]
            )
        revisions = book.take_revisions()
        if revisions:
            self.events["revisions"] += len(revisions)
            self.sink.revisions(revisions)

    # -- reporting ------------------------------------------------------------

    def status(self) -> dict[str, Any]:
        now = time.time()
        symbols = []
        for symbol in sorted(self.books):
            book = self.books[symbol]
            ages = self.age_now.get(symbol, {})
            symbols.append(
                {
                    "symbol": symbol,
                    "freshness": self.stale.state(symbol).value,
                    "price": book.price,
                    "seq": book.seq,
                    "cum_qty": round(book.cum_qty, 6),
                    "age_s": ages.get("confirmed"),
                    "provisional_price": book.prov_price,
                    "provisional_age_s": ages.get("provisional"),
                    "gaps": book.stats.gaps,
                    "revisions": book.stats.revisions,
                    "backfilled": book.stats.backfilled,
                    "snapshots": book.stats.snapshots,
                    "duplicates": book.stats.duplicates,
                    "buffered": len(book.buffer),
                }
            )
        return {
            "mode": self.cfg.mode,
            "upstream": self.cfg.upstream,
            "uptime_s": round(now - self.started, 1),
            "connected": self.connected if self.cfg.mode == "stream" else None,
            "last_message_age_s": None if self.last_message_at is None else round(now - self.last_message_at, 3),
            "thresholds": {"stale_after_s": self.cfg.stale_after, "dead_after_s": self.cfg.dead_after},
            "lag": {c: w.summary() for c, w in self.lag.items() if w.count},
            "age": {c: w.summary() for c, w in self.age.items() if w.count},
            "events": dict(sorted(self.events.items())),
            "sink": self.sink.stats(),
            "symbols": symbols,
        }
