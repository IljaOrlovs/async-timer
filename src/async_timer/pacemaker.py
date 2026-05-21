import asyncio
import logging
import typing

logger = logging.getLogger(__name__)


class TimerPacemaker:
    """Async-iterable that yields once per `delay` seconds.

    The first iteration fires immediately (no leading sleep) so the
    target function runs at startup; subsequent iterations sleep for
    `delay` before yielding.

    Iteration ends — `StopAsyncIteration` is raised — when either
    `stop()` is called explicitly or one of the awaitables registered
    via `stop_on()` resolves.

    `_reset()` is provided so a single instance can be re-used across
    `Timer.start()` / `Timer.cancel()` cycles.
    """

    delay: float
    _first_iter: bool = True
    _running: bool = True
    _cancel_futs: typing.List[asyncio.futures.Future]
    _cancel_evt: asyncio.Event

    def __init__(self, delay: float):
        self.delay = delay
        self._cancel_futs = []
        self._cancel_evt = asyncio.Event()

    def stop_on(self, aws: typing.Sequence[typing.Awaitable]):
        """Register awaitables that, when any one resolves or raises,
        will stop this pacemaker.

        Requires a running event loop (uses `asyncio.ensure_future`).
        The wrapped futures are tracked on `_cancel_futs` and cancelled
        on `stop()`. Awaitables passed here are single-shot — they are
        cleared on stop and not re-armed by `_reset()`.
        """
        for el in aws:
            fut = asyncio.ensure_future(el)
            fut.add_done_callback(self._on_cancel_fut_done)
            self._cancel_futs.append(fut)

    def _on_cancel_fut_done(self, fut: asyncio.Future):
        # Consume any exception so asyncio does not emit
        # "exception was never retrieved" warnings — but surface it
        # via logging so it isn't silently swallowed.
        if not fut.cancelled():
            exc = fut.exception()
            if exc is not None:
                logger.warning(
                    "cancel_aws awaitable %r raised %s; treating as stop signal",
                    fut,
                    exc,
                    exc_info=exc,
                )
        self.stop()

    def stop(self):
        """Stop the iterator."""
        if not self._running:
            return
        self._running = False
        self._cancel_evt.set()
        for fut in self._cancel_futs:
            if not fut.done():
                fut.cancel()
        self._cancel_futs.clear()

    def _reset(self):
        """Reset state so the iterator can be re-used after stop()."""
        self._first_iter = True
        self._running = True
        if self._cancel_evt.is_set():
            self._cancel_evt = asyncio.Event()

    def __aiter__(self):
        """Return the iterator (this object is its own iterator)."""
        return self

    async def __anext__(self):
        # Do not sleep at the first iter
        # (so the timer hits the target function at startup)
        if not self._running:
            raise StopAsyncIteration()
        elif self._first_iter:
            self._first_iter = False
        else:
            try:
                await self._try_wait(self.delay)
            except StopAsyncIteration:
                self.stop()
                raise
        return None

    async def _try_wait(self, delay: float):
        """Try waiting for the `delay`.

        Raises `StopAsyncIteration` if the sleep was cancelled
        """
        try:
            await asyncio.wait_for(self._cancel_evt.wait(), timeout=delay)
        except asyncio.TimeoutError:
            # Sleep succeeded
            return None
        # the cancel event was triggered if no timeout was raised
        assert self._cancel_evt.is_set()
        # Signal end-of-iteration to the consumer (`async for`).
        raise StopAsyncIteration()
