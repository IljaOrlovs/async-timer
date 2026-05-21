"""The `Timer` class and its supporting `FanoutRv` result broadcaster.

`Timer` drives a `TimerPacemaker` (which decides *when* a tick fires) and
a `Caller` (which decides *what* a tick produces from the user's target),
broadcasting each tick's result to every coroutine waiting via `join()`,
`wait()`, or `async for`.
"""

import asyncio
import logging
import time
import typing

import async_timer
from async_timer.pacemaker import PacemakerMode

logger = logging.getLogger(__name__)
T = typing.TypeVar("T")
TimerMainTaskT = typing.Union[
    typing.Callable[[], T],
    typing.Callable[[], typing.Coroutine[typing.Any, typing.Any, T]],
    typing.Callable[[], typing.AsyncGenerator[T, typing.Any]],
    typing.Callable[[], typing.Generator[T, typing.Any, typing.Any]],
    typing.AsyncGenerator[T, typing.Any],
    typing.Generator[T, typing.Any, typing.Any],
]
TimerCallbackT = typing.Callable[["Timer[T]", TimerMainTaskT[T]], None]


class FanoutRv(typing.Generic[T]):
    """An object that shares a result across all waiters.

    All mutating operations are synchronous: `set_result`/`set_exception`/
    `cancel` on futures are non-blocking and list append/clear are atomic
    in CPython. Only `wait()` is a coroutine — it returns a future that
    callers await for the next posted result.
    """

    futures: typing.List[asyncio.Future]
    _closed: bool

    def __init__(self):
        self.futures = []
        self._closed = False

    async def wait(self) -> T:
        """Wait for result to be posted."""
        if self._closed:
            raise asyncio.CancelledError("Fanout is closed")
        future = asyncio.get_running_loop().create_future()
        self.futures.append(future)
        return await future

    def send_result(self, result: T):
        for future in self.futures:
            if not future.done():
                future.set_result(result)
        self.futures.clear()

    def send_exception(self, exc: BaseException):
        for future in self.futures:
            if not future.done():
                future.set_exception(exc)
        self.futures.clear()

    def cancel(self):
        self._closed = True
        for future in self.futures:
            if not future.done():
                future.cancel()
        self.futures.clear()


def _noop_cb(*_, **__):
    pass


def _default_main_loop_exception_callback(*_, **__):
    """Default exc_cb: log the in-flight target exception.

    Does NOT re-raise. Re-raising would propagate the exception out of
    the timer task into the asyncio loop, which then emits a duplicate
    "Task exception was never retrieved" warning. The loop's `finally`
    has already cleaned up by the time exc_cb runs, so the task ends
    naturally regardless.
    """
    logger.exception("An unexpected exception in the timer loop.")


class Timer(typing.Generic[T]):
    """The main Timer object"""

    pacemaker: "async_timer.pacemaker.TimerPacemaker"
    hit_count: int = 0  # Number of times the timer has run so far
    target_caller: "async_timer.target_caller.Caller[T]"

    name: typing.Optional[str]
    result_fanout: FanoutRv[T]
    main_task: typing.Optional[asyncio.Task] = None
    exception_callback: TimerCallbackT[T]
    cancel_callback: TimerCallbackT[T]
    last_result: typing.Optional[T] = None
    last_tick_at: typing.Optional[float] = None  # time.monotonic() of last tick

    def __init__(
        self,
        delay: float,
        target: TimerMainTaskT[T],
        exc_cb: TimerCallbackT[T] = _default_main_loop_exception_callback,
        cancel_cb: TimerCallbackT[T] = _noop_cb,
        cancel_aws: typing.Union[typing.Sequence[typing.Awaitable], None] = None,
        start: bool = False,
        *,
        mode: PacemakerMode = "fixed_delay",
        initial_delay: float = 0.0,
        jitter: float = 0.0,
        name: typing.Optional[str] = None,
    ):
        """Create the Timer object.

        Parameters:
            `delay` - number of seconds between timer invocations.
            `target` - the callable, coroutine function, generator,
                async generator, or callable returning any of those
                that the timer will invoke each tick. The first tick
                fires immediately on `start()`; subsequent ticks are
                spaced by `delay`.
            `exc_cb` - callback the timer will call if `target` raises.
                Default logs the exception via the per-timer logger.
                After exc_cb runs, the timer task ends and `cancel_cb`
                fires.
            `cancel_cb` - callback the timer will call when the timer
                task ends for any reason (explicit cancel, target
                exhaustion via StopIteration, exception, or
                cancel_aws firing).
            `cancel_aws` - a sequence of awaitables; the timer stops
                as soon as any one of them resolves (or raises — the
                raised exception is logged). These awaitables are
                single-shot and the Timer cannot be restarted after
                being constructed with them.
            `start` - if True, calls `start()` immediately. Requires
                a running event loop.
            `mode` - "fixed_delay" (default; next tick fires `delay`
                after the previous one finishes) or "fixed_rate" (ticks
                are anchored to a wall-clock schedule; missed slots are
                skipped with a warning log).
            `initial_delay` - seconds to wait before the first tick.
                Default 0 (first tick fires immediately on start).
            `jitter` - fraction in [0, 1]. Each per-tick sleep is
                perturbed by ±jitter × sleep to avoid thundering-herd.
            `name` - optional identifier used in the timer's repr and
                in the per-timer logger. Useful when an app runs many
                timers concurrently.
        """
        self.name = name
        self._logger = logger.getChild(name) if name else logger
        self.pacemaker = self._create_pacemaker(
            delay, mode=mode, initial_delay=initial_delay, jitter=jitter
        )
        self.target_caller = async_timer.target_caller.Caller[T](target)
        self.result_fanout = FanoutRv()
        self.exception_callback = exc_cb
        self.cancel_callback = cancel_cb
        # cancel_aws are single-shot awaitables — track whether the user
        # passed any so we can fail loudly on restart instead of silently
        # losing them. Stored as a pending list and registered with the
        # pacemaker on first start() (not in __init__), so module-scope
        # use (e.g. `@every(..., cancel_aws=[...])`) works without a
        # running event loop at decoration time.
        self._pending_cancel_aws: typing.Optional[typing.List[typing.Awaitable]] = (
            list(cancel_aws) if cancel_aws else None
        )
        self._had_cancel_aws: bool = bool(cancel_aws)
        # Separate from main_task (which `cancel()` clears) — survives
        # across cancel/restart cycles so start() can detect "this is a
        # restart, not the first run".
        self._has_been_started: bool = False
        if start:
            self.start()

    def _create_pacemaker(
        self,
        delay: float,
        *,
        mode: PacemakerMode = "fixed_delay",
        initial_delay: float = 0.0,
        jitter: float = 0.0,
    ) -> "async_timer.pacemaker.TimerPacemaker":
        """Hook for subclasses that need a different pacemaker class."""
        return async_timer.pacemaker.TimerPacemaker(
            delay, mode=mode, initial_delay=initial_delay, jitter=jitter
        )

    @property
    def delay(self) -> float:
        """A shorthand to access timer firing delay"""
        return self.pacemaker.delay

    def set_delay(self, new_delay: float):
        """Change the delay."""
        self.pacemaker.delay = new_delay

    def start(self):
        """Schedule the timer to run.

        Calling start() after cancel() restarts the timer with fresh
        pacemaker, fanout, and target-caller state. Raises RuntimeError
        on restart if the original construction used `cancel_aws`, since
        those awaitables are single-shot and would be silently lost.
        """
        if self.is_running():
            raise RuntimeError("Already running")
        is_restart = self._has_been_started
        if is_restart and self._had_cancel_aws:
            raise RuntimeError(
                "Cannot restart a Timer that was constructed with "
                "cancel_aws: those awaitables are single-shot and have "
                "already been consumed. Construct a new Timer instead."
            )
        self.pacemaker._reset()
        self.result_fanout = FanoutRv()
        if is_restart:
            self.target_caller.reset()
        # Now that we know a running loop is available, arm any deferred
        # cancel_aws awaitables registered at construction time.
        if self._pending_cancel_aws is not None:
            self.pacemaker.stop_on(self._pending_cancel_aws)
            self._pending_cancel_aws = None
        loop = asyncio.get_running_loop()  # there MUST be a running loop
        self.main_task = loop.create_task(self._loop_callback_routine())
        self._has_been_started = True

    def is_running(self) -> bool:
        """Return `True` if the timer is currently scheduled"""
        return (self.main_task is not None) and (not self.main_task.done())

    async def __aenter__(self) -> "Timer[T]":
        self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cancel()

    def __aiter__(self) -> typing.AsyncIterator[T]:
        return self

    async def join(self) -> T:
        """Wait for the next tick of the timer and return its result.

        Raises `asyncio.CancelledError` if the timer is not running, or
        if it stops (naturally or via cancel) while we're waiting.
        """
        if not self.is_running():
            raise asyncio.CancelledError("The timer is not running.")
        return await self.result_fanout.wait()

    async def wait(
        self,
        /,
        hit_count: typing.Optional[int] = None,
        hits: typing.Optional[int] = None,
        timeout: typing.Optional[float] = None,
    ) -> typing.Optional[T]:
        """
        Wait for the timer to reach certain hit count
            or wait for a certain number of hits.

        Can raise `asyncio.TimeoutError` if there was a wait condition
            and timeout specified and the wait did not manage to hit
            the condition in time

        Waits for the timer to stop if neither parameter is present.

        Returns the last generated result IF there was a need to wait.
        Returns `None` otherwise.
        """
        start_time = time.monotonic()
        timeout_left = timeout
        infinite_wait = False
        if hit_count is not None:
            target_hit_count = max(0, hit_count)
        elif hits is not None:
            target_hit_count = self.hit_count + max(0, hits)
        else:
            target_hit_count = 0
            infinite_wait = True
        need_to_wait_for = target_hit_count - self.hit_count
        last_rv = None
        try:
            while infinite_wait or need_to_wait_for > 0:
                last_rv = await asyncio.wait_for(self.join(), timeout_left)
                need_to_wait_for -= 1
                if timeout is not None:
                    time_passed = time.monotonic() - start_time
                    timeout_left = timeout - time_passed
        except (asyncio.CancelledError, asyncio.TimeoutError):
            # Cancelled/Timeout error is what we were waiting
            # for in the infinite wait mode
            if infinite_wait:
                pass
            else:
                raise
        return last_rv

    async def __anext__(self) -> T:
        try:
            return await self.join()
        except asyncio.CancelledError as err:
            raise StopAsyncIteration() from err

    async def _loop_callback_routine(self):
        try:
            async for _ in self.pacemaker:
                try:
                    rv = await self.target_caller.next()
                except StopAsyncIteration:
                    break
                except Exception as err:
                    self.result_fanout.send_exception(err)
                    self.exception_callback(self, self.target_caller.target)
                    break
                else:
                    self.last_result = rv
                    self.last_tick_at = time.monotonic()
                    self.result_fanout.send_result(rv)
                self.hit_count += 1
        finally:
            # Main loop finished - cancel all watchers
            self.result_fanout.cancel()
            self.cancel_callback(self, self.target_caller.target)

    async def trigger(self) -> T:
        """Fire the target now, then resume the regular schedule.

        Returns the result of the triggered tick. Useful for "refresh
        on demand, then go back to the periodic schedule" patterns.

        Raises `RuntimeError` if the timer is not currently running,
        or `asyncio.CancelledError` if the timer stops while the
        triggered tick is in flight.
        """
        if not self.is_running():
            raise RuntimeError("Cannot trigger a Timer that is not running")
        # Register the join() waiter *before* nudging the pacemaker, so
        # we don't miss the resulting tick.
        wait = asyncio.ensure_future(self.result_fanout.wait())
        self.pacemaker.trigger()
        try:
            return await wait
        finally:
            # If a naturally-scheduled tick fired between our waiter
            # registration and the pacemaker noticing the trigger, our
            # `wait` is already resolved but the trigger event is still
            # set — clear it so the pacemaker doesn't fire a phantom
            # extra tick on its next iteration.
            self.pacemaker._trigger_evt.clear()

    async def cancel(self):
        """Unschedule the timer.

        Awaits the underlying task so that by the time this returns,
        the cancel callback has fired and waiters have been resolved.

        Safe to call from inside `cancel_cb`, `exc_cb`, or the target
        itself — in that case the awaiting-the-task step is skipped
        (it would deadlock on `current_task`).
        """
        if not self.main_task:
            return
        task = self.main_task
        self.main_task = None
        self.pacemaker.stop()
        task.cancel()
        if asyncio.current_task() is task:
            # Self-cancel from inside the timer's own task — cannot await
            # ourselves. The cancellation has been scheduled; the task's
            # own `finally` will run cleanup on the next yield.
            return
        try:
            await task
        except BaseException:
            # The task's own callbacks already saw any exception;
            # the caller of cancel() should not get it raised here.
            pass

    async def stop(self):
        """An alias to `cancel()`"""
        return await self.cancel()

    def __repr__(self) -> str:
        name_part = f" name={self.name!r}" if self.name else ""
        return (
            f"<{self.__class__.__name__}{name_part}"
            f" target={self.target_caller.target!r}"
            f" delay={self.delay!r}"
            f" hit_count={self.hit_count!r}"
            f" exception_callback={self.exception_callback!r}"
            f" cancel_callback={self.cancel_callback!r}"
            ">"
        )
