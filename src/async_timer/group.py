"""Lifespan helper to start/cancel a set of timers together.

async with async_timer.TimerGroup() as group:
    group.add(async_timer.Timer(5, refresh_db))
    group.add(async_timer.Timer(60, prune_cache))
    yield  # both running; both cancelled on exit
"""

import asyncio
import logging
import typing

from .timer import Timer

logger = logging.getLogger(__name__)
T = typing.TypeVar("T", bound=Timer)


class TimerGroup:
    """Timers with a shared lifecycle.

    Timers added before `__aenter__` start on entry; timers added
    while active start immediately. `__aexit__` cancels all
    concurrently. Re-entry works if no member uses `cancel_aws`.
    """

    timers: typing.List[Timer]
    _active: bool

    def __init__(self, timers: typing.Iterable[Timer] = ()):
        self.timers = list(timers)
        self._active = False

    def add(self, timer: T) -> T:
        """Add a timer (and start it if the group is active). Returns it."""
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
        started: typing.List[Timer] = []
        try:
            for t in self.timers:
                if not t.is_running():
                    t.start()
                    started.append(t)
        except BaseException:
            # Partial-start failure: cancel what started, re-raise.
            # (__aexit__ won't run when __aenter__ raises.)
            self._active = False
            await asyncio.gather(
                *(t.cancel() for t in started),
                return_exceptions=True,
            )
            raise
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._active = False
        await self.cancel_all()

    async def cancel_all(self):
        """Cancel all timers concurrently. Individual failures are logged."""
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
