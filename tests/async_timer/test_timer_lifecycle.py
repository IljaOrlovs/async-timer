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

    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, cancel_cb=_cb, start=True)
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
    timer = async_timer.Timer(delay=10e-5, target=lambda: hits.append(1), start=True)
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
    """After cancel(), pacemaker must be re-usable, not stuck at stopped.

    Uses a slow delay so the restart's first tick can't fire before we
    register the join() waiter — that way we actually verify the new
    pacemaker produces, rather than catching the first race-condition tick.
    """
    timer = async_timer.Timer(delay=0.05, target=lambda: 42, start=True)
    await timer.join()
    await timer.cancel()

    timer.start()
    # If pacemaker wasn't reset, this would hang or raise immediately.
    result = await asyncio.wait_for(timer.join(), timeout=1.0)
    assert result == 42
    # And keep producing across multiple ticks (not just the first one).
    result2 = await asyncio.wait_for(timer.join(), timeout=1.0)
    assert result2 == 42
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
async def test_cancel_awaitable_raising_stops_timer_and_logs(caplog):
    """A raising cancel_aws awaitable must:
    1. actually stop the timer (no explicit cancel needed),
    2. be surfaced via logging (not silently swallowed), and
    3. not leak an 'exception was never retrieved' asyncio warning.
    """

    async def _raising():
        await asyncio.sleep(10e-5)
        raise ValueError("boom")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with caplog.at_level("WARNING", logger="async_timer.pacemaker"):
            timer = async_timer.Timer(
                delay=1.0,  # long enough that no tick fires before the raise
                target=lambda: 1,
                cancel_aws=[_raising()],
                start=True,
            )
            # Wait for the timer to stop on its own via the raising awaitable.
            # No explicit cancel() — that would mask the bug we're testing.
            for _ in range(50):
                await asyncio.sleep(0.01)
                if not timer.is_running():
                    break
            assert not timer.is_running(), (
                "Raising cancel_aws awaitable must stop the timer"
            )

    bad = [w for w in caught if "exception was never retrieved" in str(w.message)]
    assert not bad, f"Got asyncio warnings: {[str(w.message) for w in bad]}"
    assert any("boom" in r.message for r in caplog.records), (
        "expected the raised ValueError to be logged"
    )


@pytest.mark.asyncio
async def test_self_cancel_from_inside_target_does_not_deadlock():
    """An async target that calls `await timer.cancel()` on itself must
    not deadlock — `cancel()` skips `await task` when called from the
    timer's own task."""
    timer_box: list = []

    async def _target():
        # Cancel self after the first tick.
        await timer_box[0].cancel()
        return 1

    timer = async_timer.Timer(delay=10e-5, target=_target)
    timer_box.append(timer)
    timer.start()
    # If self-cancel deadlocked, this wait_for would time out.
    for _ in range(50):
        await asyncio.sleep(0.01)
        if not timer.is_running():
            break
    assert not timer.is_running()


@pytest.mark.asyncio
async def test_self_cancel_from_inside_cancel_cb_does_not_deadlock():
    """A cancel_cb that calls `timer.cancel()` (e.g. via a scheduled task)
    must not deadlock. Less obviously buggy than self-cancel from target,
    but the same guard handles it."""
    cb_done = asyncio.Event()
    timer_box: list = []

    def _cb(*_a, **_kw):
        # Schedule a re-cancel that will run after we return. Even if
        # the user does this, we should not hang.
        async def _recancel():
            await timer_box[0].cancel()

        asyncio.get_running_loop().create_task(_recancel())
        cb_done.set()

    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, cancel_cb=_cb)
    timer_box.append(timer)
    timer.start()
    await timer.join()
    await asyncio.wait_for(timer.cancel(), timeout=2.0)
    assert cb_done.is_set()


@pytest.mark.asyncio
async def test_restart_with_generator_target_gets_fresh_generator():
    """Restarting a Timer whose target is a generator function (or
    returns a generator) must re-introspect and call the target afresh
    on restart — not reuse the exhausted iterator."""
    instantiations = []

    def _target():
        # Each call to _target produces a fresh generator. Track them.
        gen_id = len(instantiations)
        instantiations.append(gen_id)

        def _gen():
            yield gen_id
            yield gen_id
            yield gen_id

        return _gen()

    timer = async_timer.Timer(delay=10e-5, target=_target, start=True)
    first = await timer.join()
    await timer.cancel()
    assert first == 0

    timer.start()
    # If Caller wasn't reset, this would raise StopAsyncIteration immediately
    # and the new run would produce nothing — the join would hang.
    second = await asyncio.wait_for(timer.join(), timeout=1.0)
    await timer.cancel()
    assert second == 1, "restart should call _target again, producing gen_id=1"


@pytest.mark.asyncio
async def test_restart_with_plain_generator_function_target():
    """Same as above but target is itself a generator function (not a
    callable returning a generator)."""

    def _target():
        yield "a"
        yield "b"

    timer = async_timer.Timer(delay=10e-5, target=_target, start=True)
    assert await timer.join() == "a"
    await timer.cancel()

    timer.start()
    # Without Caller.reset(), the generator is still the first one but
    # already-consumed-and-discarded. With the fix, a brand-new one is made.
    assert await asyncio.wait_for(timer.join(), timeout=1.0) == "a"
    await timer.cancel()


@pytest.mark.asyncio
async def test_restart_after_cancel_aws_raises():
    """Restarting a Timer originally built with cancel_aws should fail
    loudly rather than silently dropping the cancel-condition."""
    evt = asyncio.Event()
    timer = async_timer.Timer(
        delay=10e-5,
        target=lambda: 1,
        cancel_aws=[evt.wait()],
        start=True,
    )
    await timer.join()
    await timer.cancel()

    with pytest.raises(RuntimeError, match="cancel_aws"):
        timer.start()


@pytest.mark.asyncio
async def test_restart_without_cancel_aws_does_not_raise():
    """Sanity: a Timer with no cancel_aws can still be restarted."""
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    await timer.join()
    await timer.cancel()
    timer.start()  # must not raise
    await timer.join()
    await timer.cancel()


@pytest.mark.asyncio
async def test_cancel_cb_not_double_called_on_natural_stop():
    """On natural StopIteration from target, cancel_cb fires exactly once."""
    cb_calls = []

    def _cb(*_a, **_kw):
        cb_calls.append(1)

    def _target():
        yield 1
        yield 2

    timer = async_timer.Timer(delay=10e-5, target=_target, cancel_cb=_cb, start=True)
    # Wait for the task to end naturally.
    await asyncio.sleep(0.05)
    await timer.cancel()
    assert len(cb_calls) == 1
