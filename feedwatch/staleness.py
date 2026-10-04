"""Stale-data detection.

A feed that silently stops looks exactly like a quiet market: the last price
just stays on the screen. So every symbol has an explicit state derived from
the age of its newest confirmed value, and every change of state is an event.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Freshness(StrEnum):
    UNKNOWN = "unknown"  # no data yet
    LIVE = "live"
    STALE = "stale"
    DEAD = "dead"


@dataclass(frozen=True, slots=True)
class Transition:
    symbol: str
    before: Freshness
    after: Freshness
    age: float | None
    at: float


class StaleDetector:
    def __init__(self, stale_after: float = 2.0, dead_after: float = 10.0) -> None:
        if not 0 < stale_after < dead_after:
            raise ValueError("need 0 < stale_after < dead_after")
        self.stale_after = stale_after
        self.dead_after = dead_after
        self._state: dict[str, Freshness] = {}
        self._since: dict[str, float] = {}

    def classify(self, age: float | None) -> Freshness:
        if age is None:
            return Freshness.UNKNOWN
        if age <= self.stale_after:
            return Freshness.LIVE
        if age <= self.dead_after:
            return Freshness.STALE
        return Freshness.DEAD

    def update(self, symbol: str, age: float | None, now: float) -> Transition | None:
        """Feed the current data age; returns a Transition when the state changed."""
        after = self.classify(age)
        before = self._state.get(symbol, Freshness.UNKNOWN)
        if after is before and symbol in self._state:
            return None
        self._state[symbol] = after
        self._since[symbol] = now
        if after is before:
            return None
        return Transition(symbol, before, after, age, now)

    def state(self, symbol: str) -> Freshness:
        return self._state.get(symbol, Freshness.UNKNOWN)

    def since(self, symbol: str) -> float | None:
        return self._since.get(symbol)
