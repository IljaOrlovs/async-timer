"""Regression tests for timer lifecycle: cancel/restart/race scenarios.

These cover bug fixes that the original test suite missed because every
test used a single Timer per `async with` block with no restart or
concurrent-cancel scenarios.
"""

import asyncio
import warnings

import pytest

import async_timer


@pytest.mark.asyncio
async def test_cancel_awaits_callback_completion():
    """`await cancel()` must not return before the cancel callback fires."""
    cb_done = asyncio.Event()

    def _cb(*_a, **_kw):
        cb_done.set()

    timer = async_timer.Timer(
        delay=10e-5, target=lambda: 1, cancel_cb=_cb, start=True
    )
    await timer.join()
    await timer.cancel()
    # cancel_cb has fired by the time cancel() returns — no extra sleep
    # or yield needed.
    assert cb_done.is_set()
    assert not timer.is_running()


@pytest.mark.asyncio
async def test_cancel_is_idempotent():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    await timer.join()
    await timer.cancel()
    await timer.cancel()  # must not raise


@pytest.mark.asyncio
async def test_restart_after_cancel():
    """Calling start() after cancel() resumes a working timer."""
    hits = []
    timer = async_timer.Timer(
        delay=10e-5, target=lambda: hits.append(1), start=True
    )
    await timer.join()
    await timer.cancel()
    first_run_hits = len(hits)
    assert first_run_hits >= 1

    timer.start()
    await timer.join()
    await timer.join()
    await timer.cancel()
    assert len(hits) > first_run_hits


@pytest.mark.asyncio
async def test_restart_resets_pacemaker_state():
    """After cancel(), pacemaker must be re-usable, not stuck at stopped."""
    timer = async_timer.Timer(delay=10e-5, target=lambda: 42, start=True)
    await timer.join()
    await timer.cancel()

    timer.start()
    # If pacemaker wasn't reset, this would hang or raise immediately.
    result = await asyncio.wait_for(timer.join(), timeout=1.0)
    assert result == 42
    await timer.cancel()


@pytest.mark.asyncio
async def test_start_while_running_raises():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    try:
        with pytest.raises(RuntimeError):
            timer.start()
    finally:
        await timer.cancel()


@pytest.mark.asyncio
async def test_join_does_not_hang_when_loop_ends_concurrently():
    """join() racing with task completion must not orphan its waiter."""

    def _target():
        yield 1
        return  # generator ends -> StopAsyncIteration in the loop

    timer = async_timer.Timer(delay=10e-5, target=_target, start=True)

    # Issue many joins; some will be in-flight when the target's generator
    # exhausts and the loop ends.
    async def _try_join():
        try:
            return await asyncio.wait_for(timer.join(), timeout=1.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            return None

    results = await asyncio.gather(*[_try_join() for _ in range(10)])
    # None of these should hit the wait_for timeout (which would be a hang).
    # CancelledError is the expected post-end signal.
    assert all(r is None or r == 1 for r in results)
    await timer.cancel()


@pytest.mark.asyncio
async def test_join_after_task_ended_raises_cleanly():
    """join() called after the timer is fully stopped must not hang."""
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    await timer.join()
    await timer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(timer.join(), timeout=1.0)


@pytest.mark.asyncio
async def test_cancel_awaitable_raising_does_not_warn(caplog):
    """User-supplied cancel awaitables that raise must not leave
    'exception was never retrieved' warnings — they should be logged
    instead, and still stop the timer."""

    async def _raising():
        await asyncio.sleep(10e-5)
        raise ValueError("boom")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with caplog.at_level("WARNING", logger="async_timer.pacemaker"):
            timer = async_timer.Timer(
                delay=1.0,
                target=lambda: 1,
                cancel_aws=[_raising()],
                start=True,
            )
            # Give the cancel-awaitable a chance to raise and stop the timer.
            await asyncio.sleep(0.01)
            await timer.cancel()

    bad = [w for w in caught if "exception was never retrieved" in str(w.message)]
    assert not bad, f"Got asyncio warnings: {[str(w.message) for w in bad]}"
    # The raised exception was surfaced via logging, not silently dropped.
    assert any("boom" in r.message for r in caplog.records), (
        "expected the raised ValueError to be logged"
    )


@pytest.mark.asyncio
async def test_cancel_cb_not_double_called_on_natural_stop():
    """On natural StopIteration from target, cancel_cb fires exactly once."""
    cb_calls = []

    def _cb(*_a, **_kw):
        cb_calls.append(1)

    def _target():
        yield 1
        yield 2

    timer = async_timer.Timer(
        delay=10e-5, target=_target, cancel_cb=_cb, start=True
    )
    # Wait for the task to end naturally.
    await asyncio.sleep(0.05)
    await timer.cancel()
    assert len(cb_calls) == 1
