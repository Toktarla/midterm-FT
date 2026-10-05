"""Software fault-tolerance primitives used by the gateway and payment service.

Retry delays, timeouts, and the circuit breaker live here. Idempotency,
checkpointing, and rollback live next to the payment state machine because
they are part of that transaction, not a generic wrapper.
"""

from __future__ import annotations

import threading
import time


class CircuitOpen(Exception):
    def __init__(self, service: str) -> None:
        super().__init__(service)
        self.service = service


class CircuitBreaker:
    """Closed, open, then one half-open trial after the cool-down."""

    def __init__(self, name: str, threshold: int = 3, reset_s: float = 4.0) -> None:
        self.name = name
        self.threshold = threshold
        self.reset_s = reset_s
        self._failures = 0
        self._state = "closed"
        self._opened_at = 0.0
        self._lock = threading.Lock()

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    def allow(self) -> bool:
        with self._lock:
            if self._state == "open":
                if time.monotonic() - self._opened_at >= self.reset_s:
                    self._state = "half_open"
                    return True
                return False
            return True

    def success(self) -> None:
        with self._lock:
            self._failures = 0
            self._state = "closed"

    def failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._state == "half_open" or self._failures >= self.threshold:
                self._state = "open"
                self._opened_at = time.monotonic()

    def reset(self) -> None:
        with self._lock:
            self._failures = 0
            self._state = "closed"
            self._opened_at = 0.0

    def snapshot(self) -> dict[str, str | int]:
        with self._lock:
            return {"state": self._state, "failures": self._failures}


def backoff_s(attempt: int, base_s: float = 0.05) -> float:
    """Exponential backoff: base, 2*base, 4*base, ..."""
    return base_s * (2**attempt)
