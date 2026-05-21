"""Regression tests for MockPacemaker/MockTimer fixes."""

import asyncio

import pytest

import async_timer
import mock_async_timer
from mock_async_timer.timer import MockPacemaker


@pytest.mark.asyncio
async def test_mock_pacemaker_stops_immediately_mid_iteration():
    """stop() called mid-_try_wait must not yield one extra tick."""
    pm = MockPacemaker(delay=10_000)
    hits = []

    async def _consume():
        async for _ in pm:
            hits.append(1)
            if len(hits) == 3:
                # Schedule a stop in the next event loop tick — it will fire
                # while _try_wait is awaiting self.sleep().
                asyncio.get_running_loop().call_soon(pm.stop)

    await asyncio.wait_for(_consume(), timeout=1.0)
    # No extra hit after stop is observed.
    assert hits == [1, 1, 1]


@pytest.mark.asyncio
async def test_mock_timer_uses_mock_pacemaker_directly():
    """MockTimer.pacemaker is the live, used pacemaker — not a leftover."""
    timer = mock_async_timer.MockTimer(target=lambda: 1, delay=10_000, start=True)
    try:
        await timer.join()
        # If pacemaker were the real one, the test would hang on the
        # delay=10_000 sleep.
        await timer.join()
        assert timer.pacemaker.sleep.await_count >= 1
    finally:
        await timer.cancel()


@pytest.mark.asyncio
async def test_mock_timer_cancel_aws_no_duplicate_callbacks():
    """cancel_aws on a MockTimer should register a stop-callback once, not
    twice. The original bug had fromPacemaker copy the futs *and* call
    stop_on again, doubling done-callbacks on each user awaitable."""
    evt = asyncio.Event()
    timer = mock_async_timer.MockTimer(
        target=lambda: 1,
        delay=10_000,
        cancel_aws=[evt.wait()],
        start=True,
    )
    # Exactly one cancel-fut is registered (not two).
    assert len(timer.pacemaker._cancel_futs) == 1
    fut = timer.pacemaker._cancel_futs[0]
    # The fut has exactly one done-callback (the pacemaker's), not two.
    # (asyncio Futures expose `_callbacks` as a list of (cb, ctx) tuples.)
    assert len(fut._callbacks) == 1

    evt.set()
    await asyncio.sleep(0.01)
    await timer.cancel()
    assert not timer.is_running()


@pytest.mark.asyncio
async def test_mock_timer_restart():
    """Restart should work on MockTimer too."""
    hits = []
    timer = mock_async_timer.MockTimer(
        target=lambda: hits.append(1), delay=10_000, start=True
    )
    await timer.join()
    await timer.cancel()
    first = len(hits)

    timer.start()
    await timer.join()
    await timer.cancel()
    assert len(hits) > first


@pytest.mark.asyncio
async def test_mock_pacemaker_stops_during_sleep_await():
    """Cover the third `_cancel_evt` check in MockPacemaker._try_wait —
    the one *after* `await self.sleep(delay)`. Triggered by a sleep mock
    whose side_effect sets the cancel event."""
    pm = MockPacemaker(delay=10_000)
    hits = []

    async def _sleep_side_effect(_delay):
        # Stop the pacemaker during the sleep itself, so the post-sleep
        # check is what raises StopAsyncIteration.
        pm._cancel_evt.set()

    pm.sleep.side_effect = _sleep_side_effect

    async for _ in pm:
        hits.append(1)

    # First iter fires (no sleep), then the second iter awaits sleep which
    # sets the event, and the post-sleep check stops us.
    assert hits == [1]


@pytest.mark.asyncio
async def test_mock_pacemaker_is_subclass_of_real():
    """Hook-based construction means MockTimer.pacemaker is the only one ever
    created — no orphan TimerPacemaker hanging around with stop-callbacks."""
    timer = mock_async_timer.MockTimer(target=lambda: 1, delay=1)
    assert isinstance(timer.pacemaker, MockPacemaker)
    assert isinstance(timer.pacemaker, async_timer.pacemaker.TimerPacemaker)
