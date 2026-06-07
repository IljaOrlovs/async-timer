import asyncio
import unittest.mock

import async_timer


class MockPacemaker(async_timer.pacemaker.TimerPacemaker):
    """Pacemaker that yields per loop-iter instead of sleeping.

    `sleep` is an `AsyncMock` — tests can assert call args/counts.
    Cancel/trigger are re-checked after each await so in-flight
    signals take effect without producing an extra tick.
    """

    sleep: unittest.mock.AsyncMock

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sleep = unittest.mock.AsyncMock(name="mock-timer-sleep")

    async def _try_wait(self, delay: float) -> bool:
        if self._cancel_evt.is_set():
            raise StopAsyncIteration()
        if self._trigger_evt.is_set():
            self._trigger_evt.clear()
            return True

        await self._sleep_until_next_loop_iter()
        if self._cancel_evt.is_set():
            raise StopAsyncIteration()
        if self._trigger_evt.is_set():
            self._trigger_evt.clear()
            return True

        await self.sleep(delay)
        if self._cancel_evt.is_set():
            raise StopAsyncIteration()
        if self._trigger_evt.is_set():
            self._trigger_evt.clear()
            return True
        return False

    async def _sleep_until_next_loop_iter(self):
        """Yield to the scheduler for one loop iteration."""
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        loop.call_soon(lambda: fut.set_result(42))
        await fut


class MockTimer(async_timer.Timer):
    """Timer subclass using `MockPacemaker` — no real sleeps. Full Timer API.

    Use in tests to drive a Timer's tick loop without wall-clock waits.
    The mocked sleep yields to the scheduler each iteration, so
    `await timer.wait(hits=N)` resolves as soon as the loop has a
    chance to run N times.

        from mock_async_timer import MockTimer

        async def test_periodic_refresh():
            calls = 0
            def tick():
                nonlocal calls
                calls += 1
            async with MockTimer(0.1, tick) as t:
                await t.wait(hits=3)
            assert calls == 3
    """

    pacemaker: MockPacemaker

    def _create_pacemaker(self, delay: float, **kwargs) -> MockPacemaker:
        return MockPacemaker(delay, **kwargs)
