import asyncio
import unittest.mock

import async_timer


class MockPacemaker(async_timer.pacemaker.TimerPacemaker):
    """Pacemaker that yields one tick per event-loop iteration instead
    of actually sleeping.

    The "sleep" is replaced with an `AsyncMock` so tests can introspect
    call counts and arguments — e.g.
    `pacemaker.sleep.assert_called_with(expected_delay)`. The cancel
    event is re-checked after each await so an in-flight stop takes
    effect immediately without producing an extra tick.
    """

    sleep: unittest.mock.AsyncMock

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sleep = unittest.mock.AsyncMock(name="mock-timer-sleep")

    async def _try_wait(self, delay: float):
        if self._cancel_evt.is_set():
            raise StopAsyncIteration()

        await self._sleep_until_next_loop_iter()
        if self._cancel_evt.is_set():
            raise StopAsyncIteration()

        await self.sleep(delay)
        if self._cancel_evt.is_set():
            raise StopAsyncIteration()

    async def _sleep_until_next_loop_iter(self):
        """Awaiting this function will release on the next async loop iteration"""
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        loop.call_soon(lambda: fut.set_result(42))
        await fut


class MockTimer(async_timer.Timer):
    """Test-friendly `Timer` subclass that swaps the real pacemaker for
    `MockPacemaker`.

    Use it in unit tests where you don't want real wall-clock delays
    but still want the full `Timer` API (`join`, `wait`, `async for`,
    `cancel_aws`, callbacks, etc.).
    """

    pacemaker: MockPacemaker

    def _create_pacemaker(self, delay: float) -> MockPacemaker:
        return MockPacemaker(delay)
