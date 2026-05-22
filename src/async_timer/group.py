"""Lifespan helper to start/cancel a set of timers together.

async with async_timer.TimerGroup() as group:
    group.add(async_timer.Timer(5, refresh_db))
    group.add(async_timer.Timer(60, prune_cache))
    yield  # both running; both cancelled on exit
"""

import asyncio
import concurrent.futures
import logging
import typing

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
        """Start every non-running member.

        Idempotent: members already running are left alone. If any
        member's `start()` raises, the members that did start are
        scheduled for cancellation and the original exception
        propagates. Binds the group to the current running loop so
        `cancel_threadsafe()` works from other threads.
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
        """Wait until every member timer satisfies the hit-count condition.

        Per-member semantics match :meth:`Timer.wait`. Group semantics
        are AND-combined: this returns when *all* members are satisfied.
        Empty group returns ``[]`` immediately.

        Args:
            hit_count: absolute hit-count target applied to each member.
            hits: additional ticks each member must produce from now.
            timeout: wall-clock upper bound on the whole-group wait,
                seconds. Raises ``TimeoutError`` if exceeded; pending
                per-member waits are cancelled.
            return_exceptions: if False (default), the first member
                exception propagates and the rest are cancelled. If
                True, exceptions are placed in the result list in place
                of the per-member value (mirrors ``asyncio.gather``).

        Returns:
            ``[(timer, last_rv), ...]`` in iteration order. With
            ``return_exceptions=True`` an entry's second element may be
            a ``BaseException`` instead of ``last_rv``.
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
        """Fire every member's target now and collect their results.

        Concurrent fan-out of :meth:`Timer.trigger` across every
        member. Each member's regular schedule resumes from the trigger
        moment (re-anchored for ``fixed_rate``). Empty group returns
        ``[]`` immediately.

        Common use: cache-invalidate-all — force every refresh timer
        in the group to re-fetch right now without restarting them.

        Args:
            timeout: wall-clock upper bound on the whole-group trigger,
                seconds. Raises ``TimeoutError`` if exceeded.
            return_exceptions: if False (default), the first member
                exception propagates. If True, exceptions appear in the
                result list (mirrors ``asyncio.gather``). Note that a
                member that is not currently running raises
                ``RuntimeError`` from its own ``trigger()``.

        Returns:
            ``[(timer, rv), ...]`` in iteration order. ``rv`` is the
            value returned by that member's target.
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
        """Thread-safe `cancel_all()`. Blocks until cancellation completes.

        Use from a non-loop thread (signal handlers, sync REST endpoints,
        worker threads). Raises ``RuntimeError`` if called from the
        group's own loop thread (use ``await cancel_all()`` instead),
        if the group has not been started, or if the bound loop is
        closed.

        ``timeout`` (seconds) bounds the wait. If exceeded, raises
        ``TimeoutError``; the cancellation may still complete on the
        loop asynchronously.
        """
        loop = self._loop
        if loop is None:
            raise RuntimeError(
                "TimerGroup: cannot dispatch — group has not been started "
                "yet (no event loop bound). Call start() first."
            )
        if loop.is_closed():
            raise RuntimeError(
                "TimerGroup: target event loop is closed; cannot dispatch "
                "cross-thread call."
            )
        try:
            current = asyncio.get_running_loop()
        except RuntimeError:
            current = None
        if current is loop:
            raise RuntimeError(
                "TimerGroup: called from the group's own event loop "
                "thread. Use `await cancel_all()` instead."
            )
        fut = asyncio.run_coroutine_threadsafe(self.cancel_all(), loop)
        try:
            return fut.result(timeout=timeout)
        except concurrent.futures.TimeoutError as err:
            fut.cancel()
            raise TimeoutError(
                f"cancel_threadsafe: cancellation did not complete within "
                f"{timeout}s (it may still complete on the loop)"
            ) from err
