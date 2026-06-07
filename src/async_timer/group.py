"""Lifespan helper to start/cancel a set of timers together.

async with async_timer.TimerGroup() as group:
    group.add(async_timer.Timer(5, refresh_db))
    group.add(async_timer.Timer(60, prune_cache))
    yield  # both running; both cancelled on exit
"""

import asyncio
import logging
import typing

from ._common import _resolve_threadsafe_loop, _run_threadsafe
from .timer import Timer

logger = logging.getLogger(__name__)
T = typing.TypeVar("T", bound=Timer)

# Public alias for the per-member result type returned by `wait()` /
# `trigger()`. When `return_exceptions=False` each entry is
# `(timer, last_rv)`; when True, the second element may be a
# `BaseException` instead.
GroupResult = typing.List[typing.Tuple[Timer, typing.Any]]
# Backwards-compat alias.
WaitResult = GroupResult


class TimerGroup:
    """Timers with a shared lifecycle.

    Timers added before `start()` / `__aenter__` start on entry; timers
    added while active start immediately. `cancel_all()` / `__aexit__`
    cancel all concurrently. Re-entry works if no member uses
    `cancel_aws`.
    """

    timers: typing.List[Timer]
    name: typing.Optional[str]
    _active: bool
    # Bound at start(); used by cancel_threadsafe() to marshal from
    # non-loop threads back to the loop the group runs on.
    _loop: typing.Optional[asyncio.AbstractEventLoop] = None

    def __init__(
        self,
        timers: typing.Iterable[Timer] = (),
        *,
        name: typing.Optional[str] = None,
    ):
        self.timers = list(timers)
        self.name = name
        self._logger = logger.getChild(name) if name else logger
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

    def __repr__(self) -> str:
        name_part = f" name={self.name!r}" if self.name else ""
        return (
            f"<{self.__class__.__name__}{name_part}"
            f" members={len(self.timers)} active={self._active}>"
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start every non-running member. Idempotent.

        On a member's `start()` failure, already-started members are
        scheduled for cancellation and the exception propagates. Binds
        to the current loop for `cancel_threadsafe()`.
        """
        if self._active:
            return
        loop = asyncio.get_running_loop()
        self._loop = loop
        self._active = True
        started: typing.List[Timer] = []
        try:
            for t in self.timers:
                if not t.is_running():
                    t.start()
                    started.append(t)
        except BaseException:
            self._active = False
            # Can't await here — schedule cleanup on the loop and
            # propagate. (Callers using __aenter__ get a synchronous
            # cleanup path; see __aenter__.)
            for t in started:
                loop.create_task(t.cancel())
            raise

    def is_running(self) -> bool:
        """True if the group is active and every member is running.

        Vacuously True for an empty active group; False for an
        inactive group regardless of contents.
        """
        if not self._active:
            return False
        return all(t.is_running() for t in self.timers)

    async def __aenter__(self) -> "TimerGroup":
        self._active = True
        loop = asyncio.get_running_loop()
        self._loop = loop
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
        self._active = False
        if not self.timers:
            return
        results = await asyncio.gather(
            *(t.cancel() for t in self.timers),
            return_exceptions=True,
        )
        for timer, result in zip(self.timers, results):
            if isinstance(result, BaseException):
                self._logger.exception(
                    "TimerGroup: cancelling %r raised %s",
                    timer,
                    type(result).__name__,
                    exc_info=result,
                )

    # ------------------------------------------------------------------
    # Group-level operations
    # ------------------------------------------------------------------

    async def wait(
        self,
        /,
        hit_count: typing.Optional[int] = None,
        hits: typing.Optional[int] = None,
        timeout: typing.Optional[float] = None,
        return_exceptions: bool = False,
    ) -> GroupResult:
        """AND-combined :meth:`Timer.wait` across every member.

        Returns ``[(timer, last_rv), ...]`` in iteration order. Empty
        group returns ``[]``. ``timeout`` is a whole-group bound that
        raises ``TimeoutError`` and cancels pending per-member waits.
        ``return_exceptions`` mirrors ``asyncio.gather`` — entries may
        carry a ``BaseException`` in place of ``last_rv``.
        """
        if not self.timers:
            return []
        members = list(self.timers)
        coros = [
            t.wait(hit_count=hit_count, hits=hits)
            for t in members
        ]
        gather = asyncio.gather(*coros, return_exceptions=return_exceptions)
        if timeout is None:
            results = await gather
        else:
            results = await asyncio.wait_for(gather, timeout=timeout)
        return list(zip(members, results))

    async def trigger(
        self,
        *,
        timeout: typing.Optional[float] = None,
        return_exceptions: bool = False,
    ) -> GroupResult:
        """Concurrent :meth:`Timer.trigger` across every member — fires
        each target now and collects results. Cache-invalidate-all
        pattern.

        Returns ``[(timer, rv), ...]`` in iteration order. Each member's
        schedule resumes from the trigger moment (re-anchored for
        ``fixed_rate``). ``timeout`` and ``return_exceptions`` mirror
        :meth:`wait`. A non-running member raises ``TimerNotRunningError``
        from its own ``trigger()``.
        """
        if not self.timers:
            return []
        members = list(self.timers)
        coros = [t.trigger() for t in members]
        gather = asyncio.gather(*coros, return_exceptions=return_exceptions)
        if timeout is None:
            results = await gather
        else:
            results = await asyncio.wait_for(gather, timeout=timeout)
        return list(zip(members, results))

    # ------------------------------------------------------------------
    # Cross-thread control
    # ------------------------------------------------------------------

    def cancel_threadsafe(self, timeout: typing.Optional[float] = None) -> None:
        """Thread-safe `cancel_all()`. Blocks until done.

        Raises `ThreadsafeDispatchError` from the group's own loop
        thread (use ``await cancel_all()`` there), before start, or
        after the bound loop closes. On ``timeout`` (seconds) exceeded
        raises ``TimeoutError``; cancellation may still complete on the
        loop asynchronously.
        """
        loop = _resolve_threadsafe_loop(
            self._loop,
            owner="TimerGroup",
            async_alternative="await cancel_all()",
        )
        return _run_threadsafe(
            self.cancel_all(),
            loop,
            timeout=timeout,
            timeout_message=(
                f"cancel_threadsafe: cancellation did not complete within "
                f"{timeout}s (it may still complete on the loop)"
            ),
        )
