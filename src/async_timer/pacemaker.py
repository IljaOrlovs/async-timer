import asyncio
import dataclasses
import typing


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
            fut.add_done_callback(lambda _fut: self.stop())
            self._cancel_futs.append(fut)

    def stop(self):
        """Stop the iterator."""
        for fut in self._cancel_futs:
            fut.cancel()
        self._cancel_futs.clear()
        self._cancel_evt.set()
        self._running = False

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
