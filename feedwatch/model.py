"""Wire types shared by the simulator and the watcher."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class Level(StrEnum):
    """How final a piece of data is.

    PROVISIONAL arrives first and may still be revised. CONFIRMED arrives one
    step later and never changes. Anything that is stored or acted on is built
    from CONFIRMED data only; PROVISIONAL is for display, marked as such.
    """

    PROVISIONAL = "provisional"
    CONFIRMED = "confirmed"


@dataclass(frozen=True, slots=True)
class Update:
    """One event for one symbol: the price after it and the quantity traded in it.

    `seq` increases by exactly 1 per event and symbol, so a missing number is a
    missed event. `qty` is a delta: the running total (`cum_qty`) is only right
    if every confirmed event is applied exactly once.
    """

    symbol: str
    seq: int
    level: Level
    price: float
    qty: float
    source_ts: float  # when the event happened, upstream clock (unix seconds)

    def to_wire(self) -> dict[str, Any]:
        return {
            "type": "update",
            "symbol": self.symbol,
            "seq": self.seq,
            "level": self.level.value,
            "price": self.price,
            "qty": self.qty,
            "ts": self.source_ts,
        }

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> Update:
        return cls(
            symbol=str(d["symbol"]),
            seq=int(d["seq"]),
            level=Level(d["level"]),
            price=float(d["price"]),
            qty=float(d["qty"]),
            source_ts=float(d["ts"]),
        )


@dataclass(frozen=True, slots=True)
class Snapshot:
    """Confirmed state of one symbol as of `seq`."""

    symbol: str
    seq: int
    price: float
    cum_qty: float
    source_ts: float

    def to_wire(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "seq": self.seq,
            "price": self.price,
            "cum_qty": self.cum_qty,
            "ts": self.source_ts,
        }

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> Snapshot:
        return cls(
            symbol=str(d["symbol"]),
            seq=int(d["seq"]),
            price=float(d["price"]),
            cum_qty=float(d["cum_qty"]),
            source_ts=float(d["ts"]),
        )
