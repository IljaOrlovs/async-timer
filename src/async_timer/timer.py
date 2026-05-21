"""Utility async io functions"""

import asyncio
import logging
import time
import typing

import async_timer

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
    """An object that shares a result actoss all waiters"""

    lock: asyncio.Lock
    futures: typing.List[asyncio.Future]

    def __init__(self):
        self.futures = []
        self.lock = asyncio.Lock()

    async def wait(self) -> T:
        """Wait for result to be posted"""
        future = asyncio.get_running_loop().create_future()
        async with self.lock:
            self.futures.append(future)
        return await future

    async def send_result(self, result: T):
        async with self.lock:
            for future in self.futures:
                future.set_result(result)
            self.futures.clear()

    async def send_exception(self, exc: Exception):
        async with self.lock:
            for future in self.futures:
                future.set_exception(exc)
            self.futures.clear()

    async def cancel(self):
        async with self.lock:
            for future in self.futures:
                future.cancel()
            self.futures.clear()


def _noop_cb(*_, **__):
    pass


def _default_main_loop_exception_callback(*_, **__):
    logger.exception("An unexpected exception in the timer loop.")
    raise


class Timer(typing.Generic[T]):
    """The main Timer object"""

    pacemaker: "async_timer.pacemaker.TimerPacemaker"
    hit_count: int = 0  # Number of times the timer has run so far
    target: TimerMainTaskT[T]

    result_fanout: FanoutRv[T]
    main_task: typing.Optional[asyncio.Task] = None
    exception_callback: TimerCallbackT[T]
    cancel_callback: TimerCallbackT[T]

    def __init__(
        self,
        delay: float,
        target: TimerMainTaskT[T],
        exc_cb: TimerCallbackT[T] = _default_main_loop_exception_callback,
        cancel_cb: TimerCallbackT[T] = _noop_cb,
        cancel_aws: typing.Union[typing.Sequence[typing.Awaitable], None] = None,
        start: bool = False,
    ):
        """Create the Timer object.

        Parameters:
            `delay` - number of seconds between timer incovations
            `target` - the callable the timer will be invoking at the `delay` period
            `exc_cb` - a callback that the timer will call on exception
            `cancel_cb` - callback the timer will call at cancellation
            `cancel_aws` - a list of awaitables, where any
                            one resolving cancels the timer
        """
        self.pacemaker = async_timer.pacemaker.TimerPacemaker(delay)
        self.target_caller = async_timer.target_caller.Caller(target)
        self.result_fanout = FanoutRv()
        self.exception_callback = exc_cb
        self.cancel_callback = cancel_cb
        if cancel_aws:
            self.pacemaker.stop_on(list(cancel_aws))
        if start:
            self.start()

    @property
    def delay(self) -> float:
        """A shorthand to access timer firing delay"""
        return self.pacemaker.delay

    def set_delay(self, new_delay: float):
        """Change the delay."""
        self.pacemaker.delay = new_delay

    def start(self):
        """Schedule the timer to run."""
        if self.main_task:
            raise RuntimeError("Already running")
        else:
            loop = asyncio.get_running_loop()  # there MUST be a running loop
            self.main_task = loop.create_task(self._loop_callback_routine())

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
        """Wait for the next tick of the timer"""
        if not self.is_running():
            raise asyncio.CancelledError("The timer is not running.")
        return (
            await self.result_fanout.wait()
        )  # this can raise `asyncio.CancelledError`

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
                    await self.result_fanout.send_exception(err)
                    self.exception_callback(self, self.target_caller.target)
                    break
                else:
                    await self.result_fanout.send_result(rv)
                self.hit_count += 1
        finally:
            # Main loop finished - cancel all watchers
            await self.result_fanout.cancel()
            self.cancel_callback(self, self.target_caller.target)

    async def cancel(self):
        """Unshedule the timer"""
        if self.main_task:
            self.main_task.cancel()
            await self.result_fanout.cancel()
            self.pacemaker.stop()
            self.main_task = None

    async def stop(self):
        """An alias to `cancel()`"""
        return await self.cancel()

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} target={self.target_caller.target!r}"
            f" delay={self.delay!r}"
            f" hit_count={self.hit_count!r}"
            f" exception_callback={self.exception_callback!r}"
            f" cancel_callback={self.cancel_callback!r}"
            ">"
        )
