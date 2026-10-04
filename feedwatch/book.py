"""Per-symbol state built from confirmed reads only.

This is the part that has to be right. The rules:

* Confirmed updates are applied strictly in sequence: seq N+1 after N, once.
* A duplicate (seq <= current) is counted and ignored.
* A jump (seq > current + 1) is a gap: later updates are buffered, the
  missing range is fetched (backfill), then the buffer drains in order.
  When the upstream no longer has that range, a snapshot replaces the state.
* Provisional updates never touch confirmed state. When the confirmed
  version of a provisional update differs, that is counted as a revision:
  exactly the case that produced wrong state when provisional data was trusted.

Pure logic, no I/O: the watcher feeds it and acts on the results.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import StrEnum

from feedwatch.model import Level, Snapshot, Update

MAX_BUFFER = 10_000  # confirmed updates held while a gap is open
MAX_PROVISIONAL = 1_000  # provisional updates kept for revision checks


class Result(StrEnum):
    APPLIED = "applied"
    PROVISIONAL = "provisional"
    DUPLICATE = "duplicate"
    BUFFERED = "buffered"
    GAP = "gap"


@dataclass(slots=True)
class Revision:
    symbol: str
    seq: int
    provisional_price: float
    confirmed_price: float
    provisional_qty: float
    confirmed_qty: float


@dataclass(slots=True)
class BookStats:
    applied: int = 0
    duplicates: int = 0
    gaps: int = 0
    backfilled: int = 0
    snapshots: int = 0
    revisions: int = 0


@dataclass(slots=True)
class SymbolBook:
    symbol: str
    seq: int = 0
    price: float | None = None
    cum_qty: float = 0.0
    source_ts: float | None = None
    synced: bool = False  # True once a baseline snapshot has been applied

    prov_seq: int = 0
    prov_price: float | None = None
    prov_ts: float | None = None
    upstream_seq: int = 0  # highest confirmed seq the upstream says exists (from heartbeats)

    stats: BookStats = field(default_factory=BookStats)
    buffer: dict[int, Update] = field(default_factory=dict)
    _provisional: dict[int, Update] = field(default_factory=dict)
    _committed: list[Update] = field(default_factory=list)
    _revisions: list[Revision] = field(default_factory=list)

    # -- input -------------------------------------------------------------

    def apply(self, u: Update) -> Result:
        if u.level is Level.PROVISIONAL:
            return self._apply_provisional(u)

        if self.synced and u.seq <= self.seq:
            self.stats.duplicates += 1
            return Result.DUPLICATE

        if not self.synced or self.buffer or u.seq != self.seq + 1:
            new_gap = self.synced and not self.buffer and u.seq > self.seq + 1
            if len(self.buffer) >= MAX_BUFFER:
                # The gap has been open far too long: start over from a snapshot.
                self.buffer.clear()
                self.synced = False
            self.buffer[u.seq] = u
            if new_gap:
                self.stats.gaps += 1
                return Result.GAP
            return Result.BUFFERED

        self._commit(u)
        self._drain()
        return Result.APPLIED

    def apply_backfill(self, updates: Iterable[Update]) -> int:
        """Apply confirmed updates fetched to close a gap. Returns how many were used."""
        used = 0
        for u in sorted(updates, key=lambda x: x.seq):
            if u.level is not Level.CONFIRMED or u.seq != self.seq + 1:
                continue
            self._commit(u)
            self.stats.backfilled += 1
            used += 1
        self._drain()
        return used

    def apply_snapshot(self, snap: Snapshot) -> bool:
        """Adopt a snapshot as the new baseline. Ignored if it is older than the state."""
        if self.synced and snap.seq <= self.seq:
            self._drain()
            return False
        self.seq = snap.seq
        self.price = snap.price
        self.cum_qty = snap.cum_qty
        self.source_ts = snap.source_ts
        self.synced = True
        self.stats.snapshots += 1
        self._provisional = {s: p for s, p in self._provisional.items() if s > self.seq}
        self._drain()
        return True

    def note_upstream_seq(self, seq: int) -> bool:
        """Record the latest confirmed seq the upstream reports. Returns True when that
        reveals updates we never received: a loss at the tail, with nothing after it
        to expose the gap."""
        if seq <= self.upstream_seq:
            return False
        self.upstream_seq = seq
        if self.synced and not self.buffer and seq > self.seq:
            self.stats.gaps += 1
            return True
        return False

    # -- queries -----------------------------------------------------------

    def missing(self) -> tuple[int, int] | None:
        """The seq range a backfill has to fetch, or None if nothing is missing."""
        if not self.synced:
            return None
        if self.buffer:
            first = min(self.buffer)
            return (self.seq + 1, first - 1) if first > self.seq + 1 else None
        if self.upstream_seq > self.seq:
            return self.seq + 1, self.upstream_seq
        return None

    def needs_baseline(self) -> bool:
        return not self.synced

    def take_committed(self) -> list[Update]:
        """Confirmed updates applied since the last call (for the sink)."""
        out, self._committed = self._committed, []
        return out

    def take_revisions(self) -> list[Revision]:
        out, self._revisions = self._revisions, []
        return out

    # -- internals ---------------------------------------------------------

    def _apply_provisional(self, u: Update) -> Result:
        if u.seq > self.seq:
            self._provisional[u.seq] = u
            if len(self._provisional) > MAX_PROVISIONAL:
                del self._provisional[min(self._provisional)]
            if u.seq > self.prov_seq:
                self.prov_seq, self.prov_price, self.prov_ts = u.seq, u.price, u.source_ts
        return Result.PROVISIONAL

    def _commit(self, u: Update) -> None:
        prov = self._provisional.pop(u.seq, None)
        if prov is not None and (prov.price != u.price or prov.qty != u.qty):
            self.stats.revisions += 1
            self._revisions.append(Revision(u.symbol, u.seq, prov.price, u.price, prov.qty, u.qty))
        self.seq = u.seq
        self.price = u.price
        self.cum_qty += u.qty
        self.source_ts = u.source_ts
        self.stats.applied += 1
        self._committed.append(u)
        for s in [s for s in self._provisional if s <= self.seq]:
            del self._provisional[s]

    def _drain(self) -> None:
        while (nxt := self.buffer.pop(self.seq + 1, None)) is not None:
            self._commit(nxt)
        for s in [s for s in self.buffer if s <= self.seq]:
            del self.buffer[s]
