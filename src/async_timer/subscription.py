"""Per-consumer queue-based tick subscription.

Complements `Timer`'s built-in `join()` / `wait()` / `async for` API
(which is single-shot fan-out and may drop intermediate ticks under a
slow consumer) with a *buffered* stream: each `Subscription` owns its
own queue, so it sees **every tick from the moment it subscribed**,
even if consumption is slower than the tick rate.

Usage
-----

    async with timer.subscribe() as feed:
        async for value in feed:
            await slow_work(value)   # never misses a tick

`maxsize` controls the underlying queue:

* `maxsize=0` (default): unbounded queue — safe for steady-state
  consumers that occasionally fall behind; will leak memory if the
  consumer permanently can't keep up.
* `maxsize>0`: bounded queue. When full, the *oldest* buffered tick is
  dropped to make room and a warning is logged on the timer's logger.
  Use this when "stay current at all costs" beats "deliver every tick".

A `Subscription` is closed by:

* exiting its `async with` block,
* the underlying `Timer` ending (target raised, target's generator
  exhausted, or `cancel()` called),
* calling `subscription.close()` explicitly.

Once closed, iteration ends cleanly with `StopAsyncIteration`. If the
timer ended because the target raised, that exception is re-raised
from the subscriber's `__anext__` (so subscribers learn about target
failures rather than silently exiting).
"""

import asyncio
import logging
import typing

T = typing.TypeVar("T")
logger = logging.getLogger(__name__)


class _StreamEnd:
    """Sentinel pushed into a subscription queue when the upstream timer
    ends. Distinct from a `None` value so subscriptions over `Timer[None]`
    still work correctly."""


_STREAM_END: typing.Final = _StreamEnd()


class _StreamExc:
    """Wrapper used to ship an upstream target exception into a
    subscription queue without confusing it with a value of type T."""

    __slots__ = ("exc",)

    def __init__(self, exc: BaseException):
        self.exc = exc


_QueueItem = typing.Union[T, _StreamEnd, _StreamExc]


class Subscription(typing.Generic[T]):
    """An async-iterable feed of every tick a `Timer` produces while
    this subscription is open.

    Don't construct directly — use `timer.subscribe(...)`.
    """

    _queue: "asyncio.Queue[_QueueItem[T]]"
    _maxsize: int
    _closed: bool
    _name: typing.Optional[str]
    # Owning timer's per-subscription cleanup hook, set by Timer.subscribe.
    _unregister: typing.Optional[typing.Callable[["Subscription[T]"], None]]
    # Drops counter for observability (incremented when bounded queue is
    # full and the oldest tick is evicted).
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
        """Push a tick value. If bounded and full, drop the oldest."""
        if self._closed:
            return
        if self._maxsize and self._queue.full():
            # Drop oldest to make room. `get_nowait` is safe because
            # we're on the loop thread and the queue is non-empty
            # (full == maxsize > 0 in v3.10+).
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:  # pragma: no cover - shouldn't happen
                pass
            self.dropped_count += 1
            logger.warning(
                "Subscription %s: queue full (maxsize=%d), dropped oldest "
                "tick (total drops: %d)",
                self._name or "<unnamed>",
                self._maxsize,
                self.dropped_count,
            )
        # put_nowait can't raise QueueFull here because we just made room.
        self._queue.put_nowait(value)

    def _push_exception(self, exc: BaseException) -> None:
        """Push an upstream exception and close the stream."""
        if self._closed:
            return
        self._closed = True
        # Make room if needed; exception delivery is more important than
        # any pending buffered values.
        if self._maxsize and self._queue.full():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:  # pragma: no cover
                pass
        self._queue.put_nowait(_StreamExc(exc))

    def _push_end(self) -> None:
        """Signal end-of-stream from upstream."""
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
        """Approximate number of buffered items currently in the queue.

        Includes any end-of-stream / exception sentinels (so a closed
        subscription with no buffered values still reports `1`).
        Useful as a load signal — if `qsize` grows steadily, the
        consumer is falling behind and may want to call `drop_oldest()`
        to shed load explicitly.
        """
        return self._queue.qsize()

    def drop_oldest(self, n: int = 1) -> int:
        """Discard up to `n` oldest buffered values from the queue.

        Returns the number actually dropped (may be less than `n` if
        the queue had fewer than `n` items). Lets a slow consumer
        implement its own load-shedding policy when it notices the
        queue growing — e.g. "if qsize > 100, drop the oldest 50 so
        I catch up to recent data."

        End-of-stream / exception sentinels at the queue head are NOT
        dropped — they are preserved so the consumer still learns about
        stream termination. The drop scan stops at the first sentinel.

        `dropped_count` is incremented for each value actually dropped,
        so the metric reflects both producer-side and consumer-side
        drops uniformly.
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
                # Put the sentinel back at the head and stop — we don't
                # want to swallow termination signals just to shed load.
                # asyncio.Queue is FIFO with no head-insert primitive,
                # so we rebuild: pull everything, put sentinel first,
                # then everything else back.
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
        """Stop receiving ticks. Idempotent. Any pending iteration will
        terminate cleanly on its next `__anext__`."""
        if self._closed:
            # Still call the unregister hook in case close() was reached
            # via a path that flipped `_closed` (e.g. _push_end) without
            # unregistering — defensive idempotency.
            pass
        self._closed = True
        if self._unregister is not None:
            unregister = self._unregister
            self._unregister = None
            unregister(self)
        # Wake any pending __anext__ with end-of-stream.
        # put_nowait may raise if unbounded queue is somehow full, but
        # unbounded queues don't get full.
        try:
            self._queue.put_nowait(_STREAM_END)
        except asyncio.QueueFull:  # pragma: no cover - bounded + full
            # Drop oldest and retry. This is a defensive path.
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
