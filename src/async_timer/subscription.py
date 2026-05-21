"""Buffered per-consumer tick stream — `Subscription`.

Each subscriber owns an `asyncio.Queue` of every tick from
subscribe-time, so a consumer slower than the tick rate doesn't miss
ticks (unlike the Timer's single-shot fanout).

    async with timer.subscribe() as feed:
        async for value in feed:
            await slow_work(value)

`maxsize=0` (default) is unbounded; `maxsize>0` drops oldest + logs
when full. Closed by `async with` exit, explicit `close()`, or the
upstream Timer ending. Target exceptions re-raise from `__anext__`.
"""

import asyncio
import logging
import typing

T = typing.TypeVar("T")
logger = logging.getLogger(__name__)


class _StreamEnd:
    """End-of-stream sentinel (distinct from `None`)."""


_STREAM_END: typing.Final = _StreamEnd()


class _StreamExc:
    """Wraps an upstream exception into a queue item."""

    __slots__ = ("exc",)

    def __init__(self, exc: BaseException):
        self.exc = exc


_QueueItem = typing.Union[T, _StreamEnd, _StreamExc]


class Subscription(typing.Generic[T]):
    """Async-iterable feed of every tick. Use `timer.subscribe(...)`."""

    _queue: "asyncio.Queue[_QueueItem[T]]"
    _maxsize: int
    _closed: bool
    _name: typing.Optional[str]
    # Cleanup hook set by Timer.subscribe; cleared after first invocation.
    _unregister: typing.Optional[typing.Callable[["Subscription[T]"], None]]
    # Counts both producer-side (queue-full) and consumer-side (drop_oldest) drops.
    dropped_count: int

    def __init__(
        self,
        maxsize: int = 0,
        *,
        name: typing.Optional[str] = None,
    ):
        if maxsize < 0:
            raise ValueError(f"maxsize must be >= 0, got {maxsize!r}")
        self._queue = asyncio.Queue(maxsize=maxsize)
        self._maxsize = maxsize
        self._closed = False
        self._name = name
        self._unregister = None
        self.dropped_count = 0

    # ------------------------------------------------------------------
    # Producer-side API (called by Timer)
    # ------------------------------------------------------------------

    def _push_value(self, value: T) -> None:
        """Push a tick. If bounded and full, drop the oldest first."""
        if self._closed:
            return
        if self._maxsize and self._queue.full():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:  # pragma: no cover
                pass
            self.dropped_count += 1
            logger.warning(
                "Subscription %s: queue full (maxsize=%d), dropped oldest "
                "tick (total drops: %d)",
                self._name or "<unnamed>",
                self._maxsize,
                self.dropped_count,
            )
        self._queue.put_nowait(value)

    def _push_exception(self, exc: BaseException) -> None:
        """Push upstream exception and close. Evicts a buffered value
        if bounded and full — exception delivery wins over buffered data."""
        if self._closed:
            return
        self._closed = True
        if self._maxsize and self._queue.full():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:  # pragma: no cover
                pass
        self._queue.put_nowait(_StreamExc(exc))

    def _push_end(self) -> None:
        """Signal end-of-stream. Evicts if bounded and full."""
        if self._closed:
            return
        self._closed = True
        if self._maxsize and self._queue.full():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:  # pragma: no cover
                pass
        self._queue.put_nowait(_STREAM_END)

    # ------------------------------------------------------------------
    # Consumer-side API
    # ------------------------------------------------------------------

    @property
    def qsize(self) -> int:
        """Buffered item count (includes any sentinel). Load signal."""
        return self._queue.qsize()

    def drop_oldest(self, n: int = 1) -> int:
        """Consumer-side load-shedding: drop up to `n` oldest values.

        Returns the count actually dropped. Stops at the first
        end-of-stream / exception sentinel so termination signals are
        never lost. Each drop counts toward `dropped_count`.
        """
        if n < 0:
            raise ValueError(f"n must be >= 0, got {n!r}")
        dropped = 0
        for _ in range(n):
            try:
                head = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if isinstance(head, (_StreamEnd, _StreamExc)):
                # Sentinel — restore queue order and stop.
                # asyncio.Queue has no head-insert: drain everything,
                # re-put sentinel first, then re-put the tail.
                tail: typing.List[_QueueItem[T]] = []
                while True:
                    try:
                        tail.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                self._queue.put_nowait(head)
                for item in tail:
                    self._queue.put_nowait(item)
                break
            dropped += 1
            self.dropped_count += 1
        return dropped

    def close(self) -> None:
        """Stop receiving ticks. Idempotent — second call is a no-op."""
        if self._closed:
            return
        self._closed = True
        if self._unregister is not None:
            unregister = self._unregister
            self._unregister = None
            unregister(self)
        try:
            self._queue.put_nowait(_STREAM_END)
        except asyncio.QueueFull:  # pragma: no cover - defensive
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            self._queue.put_nowait(_STREAM_END)

    async def __aenter__(self) -> "Subscription[T]":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __aiter__(self) -> "Subscription[T]":
        return self

    async def __anext__(self) -> T:
        item = await self._queue.get()
        if isinstance(item, _StreamEnd):
            raise StopAsyncIteration
        if isinstance(item, _StreamExc):
            raise item.exc
        return item
