"""Where data comes from: the push feed, or the old polling path."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING

import aiohttp

from feedwatch.backoff import Backoff
from feedwatch.model import Snapshot, Update

if TYPE_CHECKING:
    from feedwatch.watcher import Watcher

log = logging.getLogger("feedwatch.source")


class StreamSource:
    """WebSocket client: subscribe, read, and reconnect with backoff when the feed dies.

    A connection is declared dead after `idle_timeout` seconds without any
    message. The upstream sends a heartbeat every second, so silence means a
    broken path even when TCP still looks open, which a half-dead NAT or a
    stalled proxy produces regularly.
    """

    def __init__(
        self,
        upstream: str,
        symbols: tuple[str, ...] | None,
        idle_timeout: float = 3.0,
        healthy_after: float = 10.0,
    ) -> None:
        self.url = upstream.rstrip("/") + "/ws"
        self.symbols = symbols
        self.idle_timeout = idle_timeout
        self.healthy_after = healthy_after  # a connection that lived this long resets the backoff

    async def run(self, session: aiohttp.ClientSession, watcher: Watcher) -> None:
        backoff = Backoff()
        loop = asyncio.get_running_loop()
        while True:
            opened = loop.time()
            try:
                async with session.ws_connect(self.url, autoping=True) as ws:
                    await ws.send_json({"op": "subscribe", "symbols": list(self.symbols or ())})
                    watcher.on_connected()
                    while True:
                        msg = await ws.receive(timeout=self.idle_timeout)
                        if msg.type is not aiohttp.WSMsgType.TEXT:
                            break
                        self._handle(json.loads(msg.data), watcher)
                        if loop.time() - opened > self.healthy_after:
                            backoff.reset()
            except (aiohttp.ClientError, TimeoutError, ConnectionError) as exc:
                log.info("feed connection lost: %s", type(exc).__name__)
            watcher.on_disconnected()
            await asyncio.sleep(backoff.next_delay())

    @staticmethod
    def _handle(data: dict[str, object], watcher: Watcher) -> None:
        recv = time.time()
        kind = data.get("type")
        if kind == "update":
            watcher.on_update(Update.from_wire(data), recv)
        elif kind == "heartbeat":
            seqs = data.get("seq")
            watcher.on_heartbeat(recv, seqs if isinstance(seqs, dict) else {})


class PollSource:
    """The path this project replaced: ask an aggregator for prices every few seconds."""

    def __init__(self, upstream: str, interval: float = 2.0) -> None:
        self.url = upstream.rstrip("/") + "/aggregated"
        self.interval = interval

    async def run(self, session: aiohttp.ClientSession, watcher: Watcher) -> None:
        loop = asyncio.get_running_loop()
        timeout = aiohttp.ClientTimeout(total=5)
        while True:
            started = loop.time()
            try:
                async with session.get(self.url, timeout=timeout) as resp:
                    resp.raise_for_status()
                    body = await resp.json()
                watcher.on_poll([Snapshot.from_wire(p) for p in body["prices"]], time.time())
            except (aiohttp.ClientError, TimeoutError, KeyError, ValueError) as exc:
                watcher.count("poll_errors")
                log.info("poll failed: %s", exc)
            await asyncio.sleep(max(0.0, self.interval - (loop.time() - started)))
