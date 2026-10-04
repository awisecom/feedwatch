"""HTTP side of the watcher: an at-a-glance board, JSON status and Prometheus metrics."""

from __future__ import annotations

from aiohttp import web

from feedwatch.staleness import Freshness
from feedwatch.watcher import Watcher

WATCHER_KEY = web.AppKey("watcher", Watcher)


def make_app(watcher: Watcher) -> web.Application:
    app = web.Application()
    app[WATCHER_KEY] = watcher
    app.router.add_get("/", _board)
    app.router.add_get("/status", _status)
    app.router.add_get("/metrics", _metrics)
    return app


async def _status(request: web.Request) -> web.Response:
    return web.json_response(request.app[WATCHER_KEY].status())


async def _metrics(request: web.Request) -> web.Response:
    return web.Response(text=render_metrics(request.app[WATCHER_KEY]), content_type="text/plain", charset="utf-8")


async def _board(_request: web.Request) -> web.Response:
    return web.Response(text=BOARD_HTML, content_type="text/html", charset="utf-8")


def _label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render_metrics(w: Watcher) -> str:
    """Prometheus text exposition format, version 0.0.4."""
    out: list[str] = []

    def family(name: str, kind: str, help_text: str) -> None:
        out.append(f"# HELP {name} {help_text}")
        out.append(f"# TYPE {name} {kind}")

    family("feedwatch_delivery_lag_seconds", "histogram", "Receive time minus event time, per update.")
    for channel, hist in w.lag_hist.items():
        if not hist.count:
            continue
        for le, n in hist.cumulative():
            out.append(f'feedwatch_delivery_lag_seconds_bucket{{channel="{channel}",le="{le}"}} {n}')
        out.append(f'feedwatch_delivery_lag_seconds_sum{{channel="{channel}"}} {hist.sum:.6f}')
        out.append(f'feedwatch_delivery_lag_seconds_count{{channel="{channel}"}} {hist.count}')

    family("feedwatch_data_age_seconds", "gauge", "Now minus the event time of the newest value held.")
    for symbol, ages in sorted(w.age_now.items()):
        for level, age in ages.items():
            if age is not None:
                out.append(f'feedwatch_data_age_seconds{{symbol="{_label(symbol)}",level="{level}"}} {age:.3f}')

    family("feedwatch_freshness", "gauge", "1 for the current freshness state of each symbol, 0 otherwise.")
    for symbol in sorted(w.books):
        current = w.stale.state(symbol)
        for state in Freshness:
            labels = f'symbol="{_label(symbol)}",state="{state.value}"'
            out.append(f"feedwatch_freshness{{{labels}}} {int(state is current)}")

    family("feedwatch_events_total", "counter", "Feed events: updates, gaps, backfills, snapshots, revisions.")
    for event, n in sorted(w.events.items()):
        out.append(f'feedwatch_events_total{{event="{_label(event)}"}} {n}')

    family("feedwatch_connected", "gauge", "1 while the push feed is connected.")
    out.append(f"feedwatch_connected {int(w.connected)}")

    sink = w.sink.stats()
    family("feedwatch_sink_rows_written_total", "counter", "Rows committed to the database.")
    out.append(f"feedwatch_sink_rows_written_total {sink.get('written', 0)}")
    family("feedwatch_sink_rows_dropped_total", "counter", "Rows dropped because the queue was full (database down).")
    out.append(f"feedwatch_sink_rows_dropped_total {sink.get('dropped', 0)}")
    family("feedwatch_sink_queue_rows", "gauge", "Rows waiting to be written.")
    out.append(f"feedwatch_sink_queue_rows {sink.get('queued', 0)}")
    return "\n".join(out) + "\n"


BOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>feedwatch</title>
<style>
  :root {
    --bg: #0d1117; --panel: #161b22; --line: #30363d; --text: #e6edf3; --muted: #8b949e;
    --live: #3fb950; --stale: #d29922; --dead: #f85149; --unknown: #6e7681;
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--text);
         font: 14px/1.4 ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif; }
  header { display: flex; flex-wrap: wrap; gap: 8px 24px; align-items: baseline;
           padding: 16px 24px; border-bottom: 1px solid var(--line); }
  h1 { font-size: 18px; margin: 0; letter-spacing: .02em; }
  .mode { color: var(--muted); }
  .kpis { display: flex; flex-wrap: wrap; gap: 6px 16px; margin-left: auto; color: var(--muted); }
  .kpis b { color: var(--text); font-variant-numeric: tabular-nums; font-weight: 600; }
  main { display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 12px; padding: 20px 24px; }
  .tile { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 14px 16px;
          border-top: 3px solid var(--unknown); }
  .tile.live { border-top-color: var(--live); } .tile.stale { border-top-color: var(--stale); }
  .tile.dead { border-top-color: var(--dead); }
  .row { display: flex; justify-content: space-between; align-items: baseline; }
  .sym { font-weight: 700; letter-spacing: .04em; }
  .pill { font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: .06em;
          padding: 2px 8px; border-radius: 999px; background: var(--line); color: var(--muted); }
  .live .pill { background: rgba(63,185,80,.15); color: var(--live); }
  .stale .pill { background: rgba(210,153,34,.15); color: var(--stale); }
  .dead .pill { background: rgba(248,81,73,.15); color: var(--dead); }
  .price { font-size: 26px; font-weight: 600; margin: 10px 0 2px; font-variant-numeric: tabular-nums; }
  .age { font-variant-numeric: tabular-nums; }
  .small { color: var(--muted); font-size: 12px; font-variant-numeric: tabular-nums; }
  .prov { margin-top: 8px; }
  footer { padding: 0 24px 20px; color: var(--muted); font-size: 12px; }
</style>
</head>
<body>
<header>
  <h1>feedwatch</h1><span class="mode" id="mode">connecting</span>
  <div class="kpis" id="kpis"></div>
</header>
<main id="tiles"></main>
<footer id="foot"></footer>
<script>
const fmt = (s) => s == null ? "-" : s < 1 ? (s * 1000).toFixed(0) + " ms" : s.toFixed(1) + " s";
const price = (p) => p == null ? "-" : p.toFixed(2);
async function tick() {
  try {
    const st = await (await fetch("status", {cache: "no-store"})).json();
    const feed = st.connected ? "push feed, connected" : "push feed, reconnecting";
    document.getElementById("mode").textContent = st.mode === "stream" ? feed : "polling " + st.upstream;
    const ev = st.events, lag = st.lag;
    const kpis = st.mode === "stream"
      ? [["provisional p50", fmt(lag.provisional?.p50)], ["confirmed p50", fmt(lag.confirmed?.p50)],
         ["confirmed p99", fmt(lag.confirmed?.p99)], ["gaps", ev.gaps || 0], ["backfilled", ev.backfilled_updates || 0],
         ["reconnects", Math.max(0, (ev.connects || 0) - 1)], ["rows written", st.sink.written ?? 0]]
      : [["poll delivery p50", fmt(lag.poll?.p50)], ["poll p99", fmt(lag.poll?.p99)], ["polls", ev.polls || 0]];
    document.getElementById("kpis").innerHTML = kpis.map(([k, v]) => `<span>${k} <b>${v}</b></span>`).join("");
    document.getElementById("tiles").innerHTML = st.symbols.map((s) => `
      <section class="tile ${s.freshness}">
        <div class="row"><span class="sym">${s.symbol}</span><span class="pill">${s.freshness}</span></div>
        <div class="price">${price(s.price)}</div>
        <div class="row"><span class="age">data age ${fmt(s.age_s)}</span><span class="small">seq ${s.seq}</span></div>
        <div class="small prov">provisional ${price(s.provisional_price)} &middot; ${fmt(s.provisional_age_s)}</div>
        <div class="small">gaps ${s.gaps} &middot; backfilled ${s.backfilled} &middot; revised ${s.revisions}</div>
      </section>`).join("");
    document.getElementById("foot").textContent =
      `stale after ${st.thresholds.stale_after_s} s, dead after ${st.thresholds.dead_after_s} s. ` +
      `Prices come from confirmed data only; provisional values are shown for reference.`;
  } catch (e) {
    document.getElementById("mode").textContent = "status unavailable";
  }
}
tick();
setInterval(tick, 500);
</script>
</body>
</html>
"""


async def serve(watcher: Watcher, host: str, port: int) -> web.AppRunner:
    runner = web.AppRunner(make_app(watcher), access_log=None)
    await runner.setup()
    await web.TCPSite(runner, host, port).start()
    return runner
