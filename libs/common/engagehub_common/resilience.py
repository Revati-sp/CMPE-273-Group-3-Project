"""Timeouts, retries with exponential backoff and a circuit breaker."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

from engagehub_common.metrics import CIRCUIT_OPEN

T = TypeVar("T")


def backoff_delay(attempt: int, base: float = 0.2, cap: float = 10.0) -> float:
    """Exponential backoff with full jitter in the top half of the range."""
    ceiling = min(cap, base * (2**attempt))
    return ceiling / 2 + random.uniform(0, ceiling / 2)


async def retry_async(
    func: Callable[[], Awaitable[T]],
    *,
    attempts: int = 30,
    base: float = 0.5,
    cap: float = 5.0,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    on_retry: Callable[[int, BaseException], None] | None = None,
) -> T:
    for attempt in range(attempts):
        try:
            return await func()
        except retry_on as exc:
            if attempt == attempts - 1:
                raise
            if on_retry:
                on_retry(attempt, exc)
            await asyncio.sleep(backoff_delay(attempt, base, cap))
    raise AssertionError("unreachable")


class CircuitOpenError(RuntimeError):
    pass


class CircuitBreaker:
    """Fail fast after repeated failures; allow a probe after the reset timeout."""

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        reset_seconds: float = 15.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_seconds = reset_seconds
        self._clock = clock
        self._failures = 0
        self._opened_at: float | None = None

    @property
    def state(self) -> str:
        if self._opened_at is None:
            return "closed"
        return "half_open" if self._clock() - self._opened_at >= self.reset_seconds else "open"

    async def call(self, func: Callable[[], Awaitable[T]]) -> T:
        if self.state == "open":
            raise CircuitOpenError(f"circuit {self.name} is open")
        try:
            result = await func()
        except Exception:
            self._failures += 1
            if self.state == "half_open" or self._failures >= self.failure_threshold:
                self._opened_at = self._clock()
                CIRCUIT_OPEN.labels(self.name).inc()
            raise
        self._failures = 0
        self._opened_at = None
        return result
