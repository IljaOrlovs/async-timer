"""`Timer` and its result broadcaster `FanoutRv`.

Two delivery models for tick results:

* `join()` / `wait()` / `async for self` — single-shot broadcast.
  A tick is delivered only to consumers awaiting at that instant; busy
  consumers miss intermediate ticks. Use for "latest cached value"
  patterns.

* `subscribe()` (see `subscription.py`) — per-consumer queue. Buffers
  every tick from subscribe-time. Use when you need every tick.

Target exceptions are sticky on the fanout: late-arriving waiters see
the exception, not a generic `CancelledError`.
"""

import asyncio
import concurrent.futures
import logging
import time
import typing
import weakref

import async_timer
from async_timer.pacemaker import PacemakerMode
from async_timer.subscription import Subscription

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
    """Single-shot result broadcaster.

    `send_result(v)` resolves every currently-awaiting `wait()` with
    `v` and clears the list. Consumers not awaiting at that instant
    miss the value. `send_exception()` is sticky: late waiters also
    see the exception.
    """

    futures: typing.List[asyncio.Future]
    _closed: bool
    _close_exc: typing.Optional[BaseException]

    def __init__(self):
        self.futures = []
        self._closed = False
        self._close_exc = None

    async def wait(self) -> T:
        """Wait for result to be posted."""
        if self._closed:
            if self._close_exc is not None:
                raise self._close_exc
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
        # Sticky close — late waiters see exc rather than CancelledError.
        self._closed = True
        self._close_exc = exc

    def cancel(self):
        if self._closed:
            return  # preserve any sticky exception from send_exception
        self._closed = True
        for future in self.futures:
            if not future.done():
                future.cancel()
        self.futures.clear()


def _noop_cb(*_, **__):
    pass


def _default_main_loop_exception_callback(*_, **__):
    """Default exc_cb: log the target exception. Does not re-raise."""
    logger.exception("An unexpected exception in the timer loop.")


class Timer(typing.Generic[T]):
    """Periodically invoke `target` and broadcast each result.

    Read results via `join()`, `wait()`, `async for self`, polling
    `last_result`, or per-consumer `subscribe()` (see module docstring
    on delivery models).
    """

    pacemaker: "async_timer.pacemaker.TimerPacemaker"
    hit_count: int = 0  # successful ticks so far
    target_caller: "async_timer.target_caller.Caller[T]"

    name: typing.Optional[str]
    result_fanout: FanoutRv[T]
    main_task: typing.Optional[asyncio.Task] = None
    exception_callback: TimerCallbackT[T]
    cancel_callback: TimerCallbackT[T]
    last_result: typing.Optional[T] = None
    last_tick_at: typing.Optional[float] = None  # time.monotonic() of last tick
    # WeakSet — dropped subscriptions get GC'd and auto-removed.
    _subscriptions: "weakref.WeakSet[Subscription[T]]"
    # Bound at start(); used by *_threadsafe methods to marshal calls
    # from non-loop threads back to the loop the timer runs on.
    _loop: typing.Optional[asyncio.AbstractEventLoop] = None

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
        """
        Args:
            delay: seconds between ticks.
            target: callable / coroutine fn / (async) generator /
                callable returning any of those. First tick fires
                immediately on `start()`.
            exc_cb: called on target exception. After it runs the task
                ends and `cancel_cb` fires. Default: log only.
            cancel_cb: called when the task ends, for any reason.
            cancel_aws: awaitables that stop the timer when any
                resolves (or raises — exception is logged).
                Single-shot: prevents restart.
            start: call `start()` immediately. Needs a running loop.
            mode: `"fixed_delay"` (next tick fires `delay` after the
                previous one *finishes*) or `"fixed_rate"` (anchored to
                a wall-clock schedule; missed slots are skipped+logged).
            initial_delay: seconds before the first tick (default 0).
            jitter: per-tick sleep perturbation, fraction in [0, 1].
            name: identifier for repr and per-timer logger.
        """
        self.name = name
        self._logger = logger.getChild(name) if name else logger
        self.pacemaker = self._create_pacemaker(
            delay, mode=mode, initial_delay=initial_delay, jitter=jitter
        )
        self.target_caller = async_timer.target_caller.Caller[T](target)
        self.result_fanout = FanoutRv()
        self._subscriptions = weakref.WeakSet()
        self.exception_callback = exc_cb
        self.cancel_callback = cancel_cb
        # Deferred to start() so module-scope use without a running loop
        # (e.g. `@every(..., cancel_aws=[...])`) doesn't crash.
        self._pending_cancel_aws: typing.Optional[typing.List[typing.Awaitable]] = (
            list(cancel_aws) if cancel_aws else None
        )
        self._had_cancel_aws: bool = bool(cancel_aws)
        # Survives cancel/restart cycles so start() can detect restarts.
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
        """Current tick interval (seconds)."""
        return self.pacemaker.delay

    def set_delay(self, new_delay: float):
        """Change `delay`. Takes effect on the next sleep.

        Single attribute write; safe to call from any thread.
        """
        if new_delay < 0:
            raise ValueError(f"delay must be >= 0, got {new_delay!r}")
        self.pacemaker.delay = new_delay

    def start(self):
        """Schedule the timer.

        Restart after cancel is supported (fresh pacemaker, fanout, and
        target-caller state). Raises `RuntimeError` if the timer was
        built with `cancel_aws` — those are single-shot.
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
        # Arm deferred cancel_aws (loop is required for ensure_future).
        if self._pending_cancel_aws is not None:
            self.pacemaker.stop_on(self._pending_cancel_aws)
            self._pending_cancel_aws = None
        loop = asyncio.get_running_loop()
        self._loop = loop  # bind for *_threadsafe methods
        # Inform any pre-existing subscriptions of the loop binding so
        # their close_threadsafe() knows where to dispatch.
        for sub in list(self._subscriptions):
            sub._loop = loop
        self.main_task = loop.create_task(self._loop_callback_routine())
        self._has_been_started = True

    def is_running(self) -> bool:
        """True if the timer task is scheduled and not done."""
        return (self.main_task is not None) and (not self.main_task.done())

    async def __aenter__(self) -> "Timer[T]":
        self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cancel()

    def __aiter__(self) -> typing.AsyncIterator[T]:
        return self

    async def join(self) -> T:
        """Await the next tick and return its result.

        Raises `asyncio.CancelledError` if the timer is not running, or
        stops while we wait.
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
        """Wait for a hit-count condition or until the timer stops.

        Args:
            hit_count: wait until `self.hit_count` reaches this absolute value.
            hits: wait for this many additional ticks.
            timeout: wall-clock upper bound, seconds.

        `hit_count` and `hits` are mutually exclusive (`hit_count` wins
        if both are given).

        ┌─────────────────────────┬─────────────┬───────────────────────────┐
        │ Wait condition          │ Timeout     │ Outcome                   │
        ├─────────────────────────┼─────────────┼───────────────────────────┤
        │ hit_count or hits set   │ not reached │ returns last tick result  │
        │ hit_count or hits set   │ exceeded    │ raises TimeoutError       │
        │ neither (idle wait)     │ not given   │ blocks until timer stops  │
        │ neither (idle wait)     │ given       │ returns last_rv (no raise)│
        └─────────────────────────┴─────────────┴───────────────────────────┘

        For TimeoutError on idle wait, use `hits=1`, or
        `await asyncio.wait_for(timer.wait(), timeout=T)`.

        Returns `None` if no tick happened during the wait.
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
                    timeout_left = timeout - (time.monotonic() - start_time)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            # Idle wait treats both as "we're done waiting".
            if not infinite_wait:
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
                    # Snapshot: WeakSet may shrink mid-iter if a sub is GC'd.
                    for sub in list(self._subscriptions):
                        sub._push_exception(err)
                    self.exception_callback(self, self.target_caller.target)
                    break
                else:
                    self.last_result = rv
                    self.last_tick_at = time.monotonic()
                    self.result_fanout.send_result(rv)
                    for sub in list(self._subscriptions):
                        sub._push_value(rv)
                self.hit_count += 1
        finally:
            self.result_fanout.cancel()
            # End-of-stream for any sub that didn't already get _push_exception.
            for sub in list(self._subscriptions):
                sub._push_end()
            self._subscriptions.clear()
            self.cancel_callback(self, self.target_caller.target)

    def subscribe(
        self,
        maxsize: int = 0,
        *,
        name: typing.Optional[str] = None,
    ) -> "Subscription[T]":
        """Open a buffered per-consumer feed.

        Unlike `join()`/`wait()`/`async for self`, a Subscription does
        NOT drop intermediate ticks when the consumer is slow — each
        subscriber owns its own queue.

        Args:
            maxsize: queue bound. 0 (default) is unbounded; otherwise
                oldest is dropped and a warning logged when full.
            name: identifier shown in the drop-warning log.

        Returns a `Subscription` — an async context manager and async
        iterator over every tick from now until close. Target
        exceptions re-raise from the subscriber's `__anext__`.

            async with timer.subscribe() as feed:
                async for value in feed:
                    await process(value)
        """
        sub: Subscription[T] = Subscription(maxsize=maxsize, name=name)
        sub._unregister = self._unsubscribe
        sub._loop = self._loop  # may be None if Timer not yet started
        self._subscriptions.add(sub)
        return sub

    def _unsubscribe(self, sub: "Subscription[T]") -> None:
        # discard: no-op if already GC-reaped or double-closed.
        self._subscriptions.discard(sub)

    async def trigger(self) -> T:
        """Fire the target now and return its result.

        Raises `RuntimeError` if not running, or `CancelledError` if
        the timer stops while the triggered tick is in flight.
        """
        if not self.is_running():
            raise RuntimeError("Cannot trigger a Timer that is not running")
        # Register the waiter before nudging the pacemaker.
        wait = asyncio.ensure_future(self.result_fanout.wait())
        self.pacemaker.trigger()
        try:
            return await wait
        finally:
            # Clear stale trigger flag if a natural tick beat us to the punch
            # (prevents a phantom extra tick on the next pacemaker iteration).
            self.pacemaker._trigger_evt.clear()

    async def cancel(self):
        """Stop the timer; await full cleanup before returning.

        Safe to call from inside `target`, `exc_cb`, or `cancel_cb` —
        a self-cancel skips the `await task` step (would deadlock).
        """
        if not self.main_task:
            return
        task = self.main_task
        self.main_task = None
        self.pacemaker.stop()
        task.cancel()
        if asyncio.current_task() is task:
            return  # self-cancel: task's own `finally` will clean up
        try:
            await task
        except BaseException:
            pass  # task callbacks already saw any exception

    async def stop(self):
        """Alias for `cancel()`."""
        return await self.cancel()

    # ------------------------------------------------------------------
    # Cross-thread control
    # ------------------------------------------------------------------

    def _check_threadsafe_call(
        self, async_alternative: str
    ) -> asyncio.AbstractEventLoop:
        """Guard for *_threadsafe methods.

        Returns the bound loop. Raises with a clear, actionable message
        if the timer hasn't been started, the loop is dead, or we're
        being called from the loop's own thread.
        """
        loop = self._loop
        if loop is None:
            raise RuntimeError(
                f"{type(self).__name__}: cannot dispatch — timer has not "
                f"been started yet (no event loop bound). Call start() first."
            )
        if loop.is_closed():
            raise RuntimeError(
                f"{type(self).__name__}: target event loop is closed; "
                f"cannot dispatch cross-thread call."
            )
        try:
            current = asyncio.get_running_loop()
        except RuntimeError:
            current = None
        if current is loop:
            raise RuntimeError(
                f"{type(self).__name__}: called from the timer's own event "
                f"loop thread. Use `{async_alternative}` instead."
            )
        return loop

    def cancel_threadsafe(self, timeout: typing.Optional[float] = None) -> None:
        """Thread-safe `cancel()`. Blocks until cancellation completes.

        Use from a non-loop thread (signal handlers, sync REST endpoints,
        worker threads). Raises `RuntimeError` if called from the
        timer's own loop thread; use `await cancel()` there instead.

        `timeout` (seconds) bounds the wait. If exceeded, raises
        `TimeoutError`; the cancellation may still complete on the loop
        asynchronously.
        """
        loop = self._check_threadsafe_call("await timer.cancel()")
        fut = asyncio.run_coroutine_threadsafe(self.cancel(), loop)
        try:
            return fut.result(timeout=timeout)
        except concurrent.futures.TimeoutError as err:
            fut.cancel()
            raise TimeoutError(
                f"cancel_threadsafe: cancellation did not complete within "
                f"{timeout}s (it may still complete on the loop)"
            ) from err

    def trigger_threadsafe(self, timeout: typing.Optional[float] = None) -> T:
        """Thread-safe `trigger()`. Blocks and returns the tick's value.

        See `cancel_threadsafe` for cross-thread semantics.
        """
        loop = self._check_threadsafe_call("await timer.trigger()")
        fut = asyncio.run_coroutine_threadsafe(self.trigger(), loop)
        try:
            return fut.result(timeout=timeout)
        except concurrent.futures.TimeoutError as err:
            fut.cancel()
            raise TimeoutError(
                f"trigger_threadsafe: tick did not arrive within {timeout}s "
                f"(the trigger may still fire on the loop)"
            ) from err

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
