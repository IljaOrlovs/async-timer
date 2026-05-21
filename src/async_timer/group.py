"""`TimerGroup` — lifespan helper for managing a set of timers together.

Example
-------

    async with async_timer.TimerGroup() as group:
        group.add(async_timer.Timer(5, refresh_db))
        group.add(async_timer.Timer(60, prune_cache))
        yield  # both timers running

    # Both timers are cancelled by the time we exit the `async with`.
"""

import asyncio
import logging
import typing

from .timer import Timer

logger = logging.getLogger(__name__)
T = typing.TypeVar("T", bound=Timer)


class TimerGroup:
    """A collection of `Timer` objects with a shared lifecycle.

    Timers added via `add()` before `__aenter__` are started together
    on context entry. Timers added *while* the group is active are
    started immediately (if not already running). On `__aexit__`,
    every timer in the group is cancelled concurrently.

    A `TimerGroup` may be re-entered as long as all its timers support
    restart (i.e. were not constructed with `cancel_aws`).
    """

    timers: typing.List[Timer]
    _active: bool

    def __init__(self, timers: typing.Iterable[Timer] = ()):
        self.timers = list(timers)
        self._active = False

    def add(self, timer: T) -> T:
        """Add a timer to the group. Returns the timer for chaining.

        If the group is already active, the timer is started immediately
        (unless it is already running).
        """
        self.timers.append(timer)
        if self._active and not timer.is_running():
            timer.start()
        return timer

    def __iter__(self) -> typing.Iterator[Timer]:
        return iter(self.timers)

    def __len__(self) -> int:
        return len(self.timers)

    def __contains__(self, timer: object) -> bool:
        return timer in self.timers

    async def __aenter__(self) -> "TimerGroup":
        self._active = True
        for t in self.timers:
            if not t.is_running():
                t.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._active = False
        await self.cancel_all()

    async def cancel_all(self):
        """Cancel every timer in the group concurrently and await
        cleanup of all of them.

        Exceptions raised by any individual `cancel()` are caught and
        logged so that a failure in one timer's shutdown doesn't leave
        sibling timers half-cancelled.
        """
        if not self.timers:
            return
        results = await asyncio.gather(
            *(t.cancel() for t in self.timers),
            return_exceptions=True,
        )
        for timer, result in zip(self.timers, results):
            if isinstance(result, BaseException):
                logger.exception(
                    "TimerGroup: cancelling %r raised %s",
                    timer,
                    type(result).__name__,
                    exc_info=result,
                )
