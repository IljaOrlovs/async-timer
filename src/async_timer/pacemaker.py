import asyncio
import dataclasses
import logging
import typing

logger = logging.getLogger(__name__)


@dataclasses.dataclass()
class ConfigurationChanged:
    """An internal object that is returned when internal pacemaker state has changed"""


class TimerPacemaker:
    """A helper object that controls the timers' iterations."""

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
        """The core funtionality - return the iterator"""
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
        # the cancel event was triggered if no timout was raised
        assert self._cancel_evt.is_set()
        # So, raise StopIteration
        raise StopAsyncIteration()
