"""Upstream simulator.

Plays both sides of the original problem so the whole data path runs on a
laptop, without any venue, key or account:

* a push feed (WebSocket): a provisional update per symbol every slot
  (400 ms by default), and the confirmed version one slot later;
* the source's own REST endpoints: /snapshot (current confirmed state) and
  /updates (recent confirmed history, for closing gaps);
* an aggregator endpoint (/aggregated) that serves prices through a cache
  refreshed every 2 to 15 seconds: the polling setup this replaced.

Faults can be switched on to exercise the watcher: dropped messages (gaps),
forced disconnects, network latency, and symbols that go quiet (stale data).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import random
import time
from collections import deque
from dataclasses import dataclass, field

from aiohttp import WSMsgType, web

from feedwatch.model import Level, Snapshot, Update

log = logging.getLogger("feedwatch.sim")

DEFAULT_SYMBOLS = ("ALFA", "BRAVO", "CHARLIE", "DELTA", "ECHO", "FOXTROT")


@dataclass(frozen=True, slots=True)
class Stall:
    """Symbol publishes nothing from `after` seconds for `duration` seconds."""

    symbol: str
    after: float
    duration: float

    @classmethod
    def parse(cls, text: str) -> Stall:
        symbol, after, duration = text.split(":")
        return cls(symbol, float(after), float(duration))


@dataclass(slots=True)
class SimConfig:
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS
    slot: float = 0.4  # seconds between events per symbol
    confirm_slots: int = 1  # confirmed version follows this many slots later
    revise_prob: float = 0.02  # chance the confirmed version differs
    drop_prob: float = 0.0  # chance a confirmed message never reaches a client
    disconnect_every: float = 0.0  # seconds; 0 = never
    latency: float = 0.03  # one-way network delay, seconds
    jitter: float = 0.01
    cache_min: float = 2.0  # aggregator cache lifetime, seconds
    cache_max: float = 15.0
    history: int = 2_000  # confirmed updates kept per symbol for /updates
    heartbeat: float = 1.0
    stalls: tuple[Stall, ...] = ()
    seed: int | None = None


@dataclass(slots=True)
class _Pending:
    confirm_at: int
    update: Update
    final_price: float
    final_qty: float


class Market:
    """Ground truth. One random walk per symbol, advanced slot by slot. No I/O."""

    def __init__(self, cfg: SimConfig) -> None:
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)
        self.price = {s: round(50.0 + 25.0 * i, 2) for i, s in enumerate(cfg.symbols)}
        self.seq = dict.fromkeys(cfg.symbols, 0)
        self.cum_qty = dict.fromkeys(cfg.symbols, 0.0)
        self.confirmed: dict[str, Snapshot] = {}
        self.history: dict[str, deque[Update]] = {s: deque(maxlen=cfg.history) for s in cfg.symbols}
        self._pending: deque[_Pending] = deque()
        self.slot = 0

    def stalled(self, symbol: str, elapsed: float) -> bool:
        return any(st.symbol == symbol and st.after <= elapsed < st.after + st.duration for st in self.cfg.stalls)

    def step(self, now: float, elapsed: float) -> list[Update]:
        """Advance one slot. Returns the messages to publish, in order."""
        self.slot += 1
        out: list[Update] = []

        while self._pending and self._pending[0].confirm_at <= self.slot:
            p = self._pending.popleft()
            u = p.update
            c = Update(u.symbol, u.seq, Level.CONFIRMED, p.final_price, p.final_qty, u.source_ts)
            self.cum_qty[u.symbol] = round(self.cum_qty[u.symbol] + c.qty, 6)
            self.confirmed[u.symbol] = Snapshot(u.symbol, c.seq, c.price, self.cum_qty[u.symbol], c.source_ts)
            self.history[u.symbol].append(c)
            out.append(c)

        for symbol in self.cfg.symbols:
            if self.stalled(symbol, elapsed):
                continue
            self.seq[symbol] += 1
            drift = math.exp(self.rng.gauss(0.0, 0.0015))
            price = round(self.price[symbol] * drift, 4)
            qty = round(self.rng.expovariate(1 / 5.0), 3)
            final_price, final_qty = price, qty
            if self.rng.random() < self.cfg.revise_prob:
                final_price = round(price * (1 + self.rng.choice((-1, 1)) * self.rng.uniform(0.001, 0.004)), 4)
                final_qty = round(qty * self.rng.uniform(0.5, 1.5), 3)
            self.price[symbol] = final_price
            u = Update(symbol, self.seq[symbol], Level.PROVISIONAL, price, qty, now)
            self._pending.append(_Pending(self.slot + self.cfg.confirm_slots, u, final_price, final_qty))
            out.append(u)
        return out

    def snapshot(self, symbol: str) -> Snapshot:
        return self.confirmed.get(symbol) or Snapshot(symbol, 0, self.price[symbol], 0.0, time.time())

    def updates(self, symbol: str, first: int, last: int) -> list[Update] | None:
        """Confirmed updates first..last, or None if part of that range is no longer kept."""
        hist = self.history[symbol]
        if first > last:
            return []
        if not hist or hist[0].seq > first:
            return None
        return [u for u in hist if first <= u.seq <= last]


@dataclass(eq=False)
class _Client:
    ws: web.WebSocketResponse
    symbols: frozenset[str]
    queue: asyncio.Queue[tuple[float, str]] = field(default_factory=asyncio.Queue)
    last_due: float = 0.0
    dropped: int = 0


class Simulator:
    def __init__(self, cfg: SimConfig | None = None) -> None:
        self.cfg = cfg or SimConfig()
        self.market = Market(self.cfg)
        self.rng = random.Random(None if self.cfg.seed is None else self.cfg.seed + 1)
        self.clients: set[_Client] = set()
        self.paused = False
        self.started = time.time()
        self._agg: list[Snapshot] = []
        self._agg_at = 0.0
        self._tasks: list[asyncio.Task[None]] = []
        self.connections = 0

    # -- HTTP --------------------------------------------------------------

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/ws", self._ws)
        app.router.add_get("/snapshot", self._snapshot)
        app.router.add_get("/updates", self._updates)
        app.router.add_get("/aggregated", self._aggregated)
        app.on_startup.append(self._start)
        app.on_cleanup.append(self._stop)
        return app

    async def _start(self, _app: web.Application) -> None:
        self.started = time.time()
        self._tasks = [asyncio.create_task(self._clock()), asyncio.create_task(self._aggregator())]
        if self.cfg.disconnect_every > 0:
            self._tasks.append(asyncio.create_task(self._disconnector()))

    async def _stop(self, _app: web.Application) -> None:
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t
        for c in list(self.clients):
            await c.ws.close()

    async def _ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        try:
            hello = await ws.receive_json(timeout=5)
        except (TimeoutError, ValueError, TypeError):
            await ws.close(code=4000, message=b"expected a subscribe message")
            return ws
        wanted = frozenset(hello.get("symbols") or self.cfg.symbols)
        client = _Client(ws, wanted & frozenset(self.cfg.symbols))
        self.clients.add(client)
        self.connections += 1
        sender = asyncio.create_task(self._send_loop(client))
        try:
            async for msg in ws:
                if msg.type is WSMsgType.ERROR:
                    break
        finally:
            self.clients.discard(client)
            sender.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sender
        return ws

    async def _snapshot(self, request: web.Request) -> web.Response:
        names = request.query.get("symbols")
        symbols = names.split(",") if names else list(self.cfg.symbols)
        unknown = [s for s in symbols if s not in self.market.price]
        if unknown:
            raise web.HTTPNotFound(text=f"unknown symbols: {','.join(unknown)}")
        return web.json_response([self.market.snapshot(s).to_wire() for s in symbols])

    async def _updates(self, request: web.Request) -> web.Response:
        try:
            symbol = request.query["symbol"]
            first, last = int(request.query["from"]), int(request.query["to"])
        except (KeyError, ValueError):
            raise web.HTTPBadRequest(text="need symbol, from and to") from None
        if symbol not in self.market.history:
            raise web.HTTPNotFound(text=f"unknown symbol: {symbol}")
        found = self.market.updates(symbol, first, last)
        if found is None:
            raise web.HTTPGone(text="range is older than the retained history: use /snapshot")
        return web.json_response([u.to_wire() for u in found])

    async def _aggregated(self, _request: web.Request) -> web.Response:
        return web.json_response({"cached_at": self._agg_at, "prices": [s.to_wire() for s in self._agg]})

    # -- background tasks --------------------------------------------------

    async def _clock(self) -> None:
        loop = asyncio.get_running_loop()
        start = loop.time()
        n = 0
        while True:
            n += 1
            await asyncio.sleep(max(0.0, start + n * self.cfg.slot - loop.time()))
            if self.paused:
                continue
            now = time.time()
            messages = self.market.step(now, now - self.started)
            for client in list(self.clients):
                self._enqueue(client, messages, loop.time())

    def _enqueue(self, client: _Client, messages: list[Update], now: float) -> None:
        for u in messages:
            if u.symbol not in client.symbols:
                continue
            if u.level is Level.CONFIRMED and self.rng.random() < self.cfg.drop_prob:
                client.dropped += 1
                continue
            due = now + self.cfg.latency + self.rng.uniform(0, self.cfg.jitter)
            client.last_due = max(client.last_due, due)  # jitter must not reorder
            client.queue.put_nowait((client.last_due, json.dumps(u.to_wire())))

    async def _send_loop(self, client: _Client) -> None:
        loop = asyncio.get_running_loop()
        try:
            while not client.ws.closed:
                try:
                    due, text = await asyncio.wait_for(client.queue.get(), timeout=self.cfg.heartbeat)
                except TimeoutError:
                    # The heartbeat carries the last confirmed seq per symbol, so a client
                    # that lost the final messages before a quiet spell can still tell.
                    last = {s: self.market.confirmed[s].seq for s in client.symbols if s in self.market.confirmed}
                    await client.ws.send_str(json.dumps({"type": "heartbeat", "ts": time.time(), "seq": last}))
                    continue
                await asyncio.sleep(max(0.0, due - loop.time()))
                await client.ws.send_str(text)
        except (ConnectionError, RuntimeError):
            pass  # client went away mid-send

    async def _aggregator(self) -> None:
        while True:
            self._agg = [self.market.snapshot(s) for s in self.cfg.symbols if s in self.market.confirmed]
            self._agg_at = time.time()
            if not self._agg:  # nothing confirmed yet: look again next slot
                await asyncio.sleep(self.cfg.slot)
                continue
            await asyncio.sleep(self.rng.uniform(self.cfg.cache_min, self.cfg.cache_max))

    async def _disconnector(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.disconnect_every)
            for c in list(self.clients):
                await c.ws.close(code=1012, message=b"simulated restart")

    # -- control (tests, compare) -----------------------------------------

    def truth(self) -> dict[str, Snapshot]:
        return {s: self.market.snapshot(s) for s in self.cfg.symbols}


async def serve(sim: Simulator, host: str, port: int) -> web.AppRunner:
    runner = web.AppRunner(sim.app(), access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return runner


def bound_port(runner: web.AppRunner) -> int:
    """The port a runner actually listens on (useful with port=0)."""
    for address in runner.addresses:
        return int(address[1])
    raise RuntimeError("runner is not listening")
