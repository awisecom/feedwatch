"""Reconnect delays: exponential with full jitter.

Full jitter (a random delay between 0 and the exponential cap) keeps a fleet
of clients that lost the same upstream from reconnecting in lockstep.
"""

from __future__ import annotations

import random
from collections.abc import Callable


class Backoff:
    def __init__(
        self,
        base: float = 0.25,
        cap: float = 10.0,
        rand: Callable[[], float] = random.random,
    ) -> None:
        self.base = base
        self.cap = cap
        self.attempt = 0
        self._rand = rand

    def ceiling(self) -> float:
        return min(self.cap, self.base * 2.0 ** min(self.attempt, 32))

    def next_delay(self) -> float:
        delay = self._rand() * self.ceiling()
        self.attempt += 1
        return delay

    def reset(self) -> None:
        self.attempt = 0
