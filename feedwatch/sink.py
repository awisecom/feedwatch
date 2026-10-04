"""Where confirmed data goes: PostgreSQL, or nowhere.

The sink must never slow the feed down. `ticks()` is called on the hot path
and only appends to an in-memory queue; a background task writes batches.

Backpressure policy, decided up front instead of discovered in an outage:
if the database is down long enough for the queue to fill, the OLDEST rows
are dropped and counted. Fresh data beats complete data for a live monitor;
the counter makes the loss visible instead of silent.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import resources
from typing import Any, Protocol

from feedwatch.backoff import Backoff
from feedwatch.book import Revision
from feedwatch.staleness import Transition

log = logging.getLogger("feedwatch.sink")

TICK_COLUMNS = ("symbol", "seq", "price", "qty", "source_ts", "applied_ts", "via")
SCHEMA_LOCK = 0x46454544  # pg_advisory_xact_lock key, so two sinks never race on DDL


@dataclass(frozen=True, slots=True)
class TickRow:
    symbol: str
    seq: int
    price: float
    qty: float
    source_ts: float
    applied_ts: float
    via: str  # "stream" | "backfill"

    def record(self) -> tuple[Any, ...]:
        return (
            self.symbol,
            self.seq,
            self.price,
            self.qty,
            datetime.fromtimestamp(self.source_ts, UTC),
            datetime.fromtimestamp(self.applied_ts, UTC),
            self.via,
        )


class Sink(Protocol):
    def ticks(self, rows: list[TickRow]) -> None: ...
    def revisions(self, revisions: list[Revision]) -> None: ...
    def freshness(self, transition: Transition) -> None: ...
    async def run(self) -> None: ...
    def stats(self) -> dict[str, Any]: ...


class MemorySink:
    """Keeps everything in lists. For tests and for runs without a database."""

    def __init__(self) -> None:
        self.rows: list[TickRow] = []
        self.revision_rows: list[Revision] = []
        self.transitions: list[Transition] = []

    def ticks(self, rows: list[TickRow]) -> None:
        self.rows.extend(rows)

    def revisions(self, revisions: list[Revision]) -> None:
        self.revision_rows.extend(revisions)

    def freshness(self, transition: Transition) -> None:
        self.transitions.append(transition)

    async def run(self) -> None:
        return None

    def stats(self) -> dict[str, Any]:
        return {"kind": "memory", "written": len(self.rows), "dropped": 0, "queued": 0}


class NullSink(MemorySink):
    """Discards rows (keeps only counts)."""

    def __init__(self) -> None:
        super().__init__()
        self.count = 0

    def ticks(self, rows: list[TickRow]) -> None:
        self.count += len(rows)

    def revisions(self, revisions: list[Revision]) -> None:
        return None

    def freshness(self, transition: Transition) -> None:
        return None

    def stats(self) -> dict[str, Any]:
        return {"kind": "none", "written": 0, "dropped": 0, "queued": 0, "seen": self.count}


class PostgresSink:
    def __init__(
        self,
        dsn: str,
        batch: int = 500,
        flush_every: float = 0.25,
        max_queued: int = 100_000,
    ) -> None:
        self.dsn = dsn
        self.batch = batch
        self.flush_every = flush_every
        self.max_queued = max_queued
        self._rows: deque[TickRow] = deque()
        self._revisions: deque[Revision] = deque(maxlen=10_000)
        self._transitions: deque[Transition] = deque(maxlen=10_000)
        self._wake = asyncio.Event()
        self.written = 0
        self.dropped = 0
        self.errors = 0
        self.connected = False
        self.last_error: str | None = None

    # -- hot path ----------------------------------------------------------

    def ticks(self, rows: list[TickRow]) -> None:
        for row in rows:
            if len(self._rows) >= self.max_queued:
                self._rows.popleft()
                self.dropped += 1
            self._rows.append(row)
        if len(self._rows) >= self.batch:
            self._wake.set()

    def revisions(self, revisions: list[Revision]) -> None:
        self._revisions.extend(revisions)

    def freshness(self, transition: Transition) -> None:
        self._transitions.append(transition)

    def stats(self) -> dict[str, Any]:
        return {
            "kind": "postgres",
            "connected": self.connected,
            "written": self.written,
            "dropped": self.dropped,
            "queued": len(self._rows),
            "errors": self.errors,
            "last_error": self.last_error,
        }

    # -- background writer -------------------------------------------------

    async def run(self) -> None:
        import asyncpg  # optional dependency: pip install "feedwatch[postgres]"

        backoff = Backoff(base=0.5, cap=15.0)
        while True:
            try:
                conn = await asyncpg.connect(self.dsn)
            except (OSError, asyncpg.PostgresError) as exc:
                await self._failed(exc, backoff)
                continue
            self.connected = True
            backoff.reset()
            try:
                await apply_schema(conn)
                while True:
                    await self._wait_for_work()
                    await self.flush(conn)
            except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError) as exc:
                await self._failed(exc, backoff)
            finally:
                self.connected = False
                await conn.close()

    async def _failed(self, exc: BaseException, backoff: Backoff) -> None:
        self.errors += 1
        self.last_error = f"{type(exc).__name__}: {exc}"
        delay = backoff.next_delay()
        log.warning(
            "postgres unavailable (%s), retrying in %.1f s, %d rows queued", self.last_error, delay, len(self._rows)
        )
        await asyncio.sleep(delay)

    async def _wait_for_work(self) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._wake.wait(), timeout=self.flush_every)
        self._wake.clear()

    async def flush(self, conn: Any) -> None:
        """Write what is queued. Rows leave the queue only after their transaction commits."""
        while self._rows:
            batch = [self._rows[i] for i in range(min(self.batch, len(self._rows)))]
            async with conn.transaction():
                # COPY into a session-local staging table, then one INSERT ... ON CONFLICT:
                # bulk-load speed, and replays after a reconnect or backfill stay idempotent.
                await conn.execute(
                    "CREATE TEMP TABLE IF NOT EXISTS ticks_in (LIKE ticks INCLUDING DEFAULTS) ON COMMIT DELETE ROWS"
                )
                await conn.copy_records_to_table("ticks_in", records=[r.record() for r in batch], columns=TICK_COLUMNS)
                status = await conn.execute(
                    "INSERT INTO ticks SELECT * FROM ticks_in ON CONFLICT (symbol, seq) DO NOTHING"
                )
            self.written += int(status.split()[-1])
            for row in batch:  # the cap may have dropped some of them meanwhile
                if self._rows and self._rows[0] is row:
                    self._rows.popleft()

        if self._revisions:
            revs = list(self._revisions)
            await conn.executemany(
                """INSERT INTO revisions
                       (symbol, seq, provisional_price, confirmed_price, provisional_qty, confirmed_qty)
                   VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (symbol, seq) DO NOTHING""",
                [
                    (r.symbol, r.seq, r.provisional_price, r.confirmed_price, r.provisional_qty, r.confirmed_qty)
                    for r in revs
                ],
            )
            for _ in revs:
                self._revisions.popleft()

        if self._transitions:
            changes = list(self._transitions)
            await conn.executemany(
                "INSERT INTO freshness_changes (symbol, from_state, to_state, age_s, at) VALUES ($1, $2, $3, $4, $5)",
                [(t.symbol, t.before.value, t.after.value, t.age, datetime.fromtimestamp(t.at, UTC)) for t in changes],
            )
            for _ in changes:
                self._transitions.popleft()


def schema_sql() -> str:
    return resources.files("feedwatch").joinpath("schema.sql").read_text(encoding="utf-8")


async def apply_schema(conn: Any) -> None:
    async with conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock($1)", SCHEMA_LOCK)
        await conn.execute(schema_sql())
