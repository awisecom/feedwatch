from __future__ import annotations

from typing import Any

# Fast timings for tests that run the simulator: same shapes as the defaults
# (slot, confirmation delay, aggregator cache), scaled down about eight times.
FAST: dict[str, Any] = {
    "slot": 0.05,
    "latency": 0.005,
    "jitter": 0.002,
    "cache_min": 0.3,
    "cache_max": 1.2,
    "heartbeat": 0.2,
}
