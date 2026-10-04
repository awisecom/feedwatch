from __future__ import annotations

import asyncio
import re

import aiohttp

from feedwatch.model import Level, Snapshot, Update
from feedwatch.status import make_app, render_metrics
from feedwatch.watcher import WatchConfig, Watcher

SAMPLE = re.compile(r"^[a-z_]+(\{[^}]*\})? -?[0-9.e+-]+$")


def _watcher_with_data() -> Watcher:
    w = Watcher(WatchConfig("http://upstream.invalid"))
    w._book("ALFA").apply_snapshot(Snapshot("ALFA", 0, 100.0, 0.0, 1000.0))
    w.on_update(Update("ALFA", 1, Level.PROVISIONAL, 100.2, 1.0, 1000.0), recv_ts=1000.05)
    w.on_update(Update("ALFA", 1, Level.CONFIRMED, 100.1, 1.0, 1000.0), recv_ts=1000.45)
    w.sample(1000.5)
    return w


def test_metrics_follow_the_exposition_format() -> None:
    text = render_metrics(_watcher_with_data())
    assert "# TYPE feedwatch_delivery_lag_seconds histogram" in text
    assert 'feedwatch_delivery_lag_seconds_bucket{channel="confirmed",le="+Inf"} 1' in text
    assert 'feedwatch_freshness{symbol="ALFA",state="live"} 1' in text
    assert 'feedwatch_events_total{event="revisions"} 1' in text
    for line in text.splitlines():
        assert line.startswith("# ") or SAMPLE.match(line), line


def test_status_reports_confirmed_and_provisional_side_by_side() -> None:
    st = _watcher_with_data().status()
    (alfa,) = st["symbols"]
    assert alfa["price"] == 100.1  # confirmed
    assert alfa["provisional_price"] == 100.2
    assert alfa["revisions"] == 1
    assert alfa["freshness"] == "live"


def test_http_endpoints() -> None:
    async def go() -> tuple[int, dict[str, object], str, str]:
        runner = aiohttp.web.AppRunner(make_app(_watcher_with_data()))
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"http://127.0.0.1:{port}/status") as r:
                    status, body = r.status, await r.json()
                async with s.get(f"http://127.0.0.1:{port}/metrics") as r:
                    metrics = await r.text()
                async with s.get(f"http://127.0.0.1:{port}/") as r:
                    page = await r.text()
        finally:
            await runner.cleanup()
        return status, body, metrics, page

    status, body, metrics, page = asyncio.run(go())
    assert status == 200 and body["mode"] == "stream"
    assert "feedwatch_connected 0" in metrics
    assert "<title>feedwatch</title>" in page
