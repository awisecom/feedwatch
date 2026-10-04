"""Against a real PostgreSQL. Skipped unless FEEDWATCH_TEST_DSN is set (CI sets it)."""

from __future__ import annotations

import asyncio
import contextlib
import os

import pytest
from conftest import FAST

from feedwatch.book import Revision
from feedwatch.sim import SimConfig, Simulator, bound_port, serve
from feedwatch.sink import PostgresSink, TickRow, apply_schema
from feedwatch.staleness import Freshness, Transition
from feedwatch.watcher import WatchConfig, Watcher

DSN = os.environ.get("FEEDWATCH_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="set FEEDWATCH_TEST_DSN to run against PostgreSQL")


async def _fresh_connection():  # type: ignore[no-untyped-def]
    asyncpg = pytest.importorskip("asyncpg")
    conn = await asyncpg.connect(DSN)
    await conn.execute("DROP VIEW IF EXISTS lag_per_minute; DROP TABLE IF EXISTS ticks, revisions, freshness_changes")
    await apply_schema(conn)
    await apply_schema(conn)  # idempotent
    return conn


def test_batches_are_written_once_even_when_replayed() -> None:
    async def go() -> None:
        conn = await _fresh_connection()
        try:
            sink = PostgresSink(str(DSN), batch=100)
            t0 = 999_960.0  # on a minute boundary, so all 250 rows land in one lag_per_minute row
            rows = [TickRow("ALFA", i, 100.0 + i, 1.0, t0 + i / 10, t0 + i / 10 + 0.5, "stream") for i in range(1, 251)]
            sink.ticks(rows)
            await sink.flush(conn)
            sink.ticks(rows[:50])  # a replay after reconnect or backfill
            await sink.flush(conn)
            assert await conn.fetchval("SELECT count(*) FROM ticks") == 250
            assert sink.written == 250

            sink.revisions([Revision("ALFA", 3, 101.0, 100.5, 2.0, 2.0)])
            sink.freshness(Transition("ALFA", Freshness.LIVE, Freshness.STALE, 2.5, 1003.0))
            await sink.flush(conn)
            assert await conn.fetchval("SELECT count(*) FROM revisions") == 1
            assert await conn.fetchval("SELECT to_state FROM freshness_changes") == "stale"

            (row,) = await conn.fetch("SELECT ticks, p50_s FROM lag_per_minute WHERE symbol = 'ALFA'")
            assert row["ticks"] == 250
            assert row["p50_s"] == pytest.approx(0.5, abs=1e-3)
        finally:
            await conn.close()

    asyncio.run(go())


def test_a_full_queue_drops_the_oldest_rows_and_counts_them() -> None:
    sink = PostgresSink("postgresql://unused", max_queued=10)
    sink.ticks([TickRow("ALFA", i, 1.0, 1.0, 0.0, 0.0, "stream") for i in range(1, 16)])
    assert sink.dropped == 5
    assert sink.stats()["queued"] == 10


def test_the_watcher_streams_into_postgres() -> None:
    async def go() -> None:
        conn = await _fresh_connection()
        sim = Simulator(SimConfig(seed=9, drop_prob=0.05, **FAST))
        runner = await serve(sim, "127.0.0.1", 0)
        sink = PostgresSink(str(DSN), flush_every=0.1)
        watcher = Watcher(WatchConfig(f"http://127.0.0.1:{bound_port(runner)}", sample_every=0.02), sink)
        task = asyncio.create_task(watcher.run())
        try:
            await asyncio.sleep(2.5)
            sim.paused = True
            await asyncio.sleep(1.5)  # catch up and flush
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await runner.cleanup()
        try:
            stored = await conn.fetchval("SELECT count(*) FROM ticks")
            dupes = await conn.fetchval("SELECT count(*) - count(DISTINCT (symbol, seq)) FROM ticks")
            holes = await conn.fetchval(
                """SELECT count(*) FROM (
                       SELECT seq, lead(seq) OVER (PARTITION BY symbol ORDER BY seq) AS next FROM ticks
                   ) s WHERE next <> seq + 1"""
            )
            vias = {r["via"] for r in await conn.fetch("SELECT DISTINCT via FROM ticks")}
        finally:
            await conn.close()
        assert stored == sink.written > 100
        assert dupes == 0
        assert holes == 0  # gaps were backfilled, not skipped
        assert vias == {"stream", "backfill"}

    asyncio.run(go())
