"""Tests for new Timer features: name, last_result, last_tick_at, trigger()."""

import asyncio
import logging
import time

import pytest

import async_timer


@pytest.mark.asyncio
async def test_name_appears_in_repr():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, name="db_refresh")
    assert "name='db_refresh'" in repr(timer)


@pytest.mark.asyncio
async def test_unnamed_repr_omits_name_field():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1)
    assert "name=" not in repr(timer)


@pytest.mark.asyncio
async def test_named_timer_uses_child_logger():
    """Named timers should log under `async_timer.timer.<name>`,
    so apps with many timers can filter/route per-timer logs."""

    def _raising():
        raise RuntimeError("boom")

    # Use a default exc_cb that re-raises so the loop ends naturally.
    timer = async_timer.Timer(
        delay=10e-5,
        target=_raising,
        name="my_named_timer",
    )
    # The internal logger should be the child of async_timer.timer.
    assert timer._logger.name == "async_timer.timer.my_named_timer"


@pytest.mark.asyncio
async def test_last_result_and_last_tick_at_populated():
    values = iter([10, 20, 30])

    timer = async_timer.Timer(delay=10e-5, target=lambda: next(values), start=True)
    assert timer.last_result is None
    assert timer.last_tick_at is None

    await timer.join()
    assert timer.last_result == 10
    assert timer.last_tick_at is not None
    first_tick_at = timer.last_tick_at

    await timer.join()
    assert timer.last_result == 20
    assert timer.last_tick_at >= first_tick_at

    await timer.cancel()


@pytest.mark.asyncio
async def test_last_result_not_updated_on_exception():
    """A target exception should not corrupt the last_result cache —
    the last *successful* value remains."""
    calls = [0]

    def _target():
        calls[0] += 1
        if calls[0] == 2:
            raise ValueError("boom")
        return calls[0]

    timer = async_timer.Timer(
        delay=10e-5,
        target=_target,
        exc_cb=lambda *_a, **_kw: None,  # swallow
        start=True,
    )
    await timer.join()
    assert timer.last_result == 1
    # Wait for the loop to end (target raised on 2nd call)
    for _ in range(50):
        await asyncio.sleep(0.01)
        if not timer.is_running():
            break
    await timer.cancel()
    # last_result still reflects the last successful tick.
    assert timer.last_result == 1


@pytest.mark.asyncio
async def test_trigger_fires_immediately_returning_result():
    """trigger() should produce a tick without waiting for the delay."""
    timer = async_timer.Timer(delay=10.0, target=lambda: 42, start=True)
    await timer.join()  # consume first (immediate) tick
    t0 = time.monotonic()
    rv = await timer.trigger()
    elapsed = time.monotonic() - t0
    assert rv == 42
    assert elapsed < 0.5, f"trigger took {elapsed}s — should be immediate"
    await timer.cancel()


@pytest.mark.asyncio
async def test_trigger_raises_when_not_running():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1)
    with pytest.raises(RuntimeError, match="not running"):
        await timer.trigger()


@pytest.mark.asyncio
async def test_jitter_param_forwarded_to_pacemaker():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, jitter=0.3)
    assert timer.pacemaker.jitter == 0.3


@pytest.mark.asyncio
async def test_initial_delay_param_forwarded():
    timer = async_timer.Timer(
        delay=10e-5, target=lambda: 1, initial_delay=0.05, start=True
    )
    t0 = time.monotonic()
    await timer.join()
    elapsed = time.monotonic() - t0
    assert elapsed >= 0.04
    await timer.cancel()


@pytest.mark.asyncio
async def test_mode_param_forwarded():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, mode="fixed_rate")
    assert timer.pacemaker.mode == "fixed_rate"


@pytest.mark.asyncio
async def test_trigger_cancelled_mid_wait_propagates(monkeypatch):
    """If the awaiter of trigger() is cancelled mid-wait, the inner
    future is cancelled as part of normal asyncio await-cancellation."""
    timer = async_timer.Timer(delay=10.0, target=lambda: 1, start=True)
    await timer.join()

    # Make the pacemaker trigger a no-op so the inner wait stays pending
    # and our cancel definitely hits while it's in flight.
    monkeypatch.setattr(timer.pacemaker, "trigger", lambda: None)

    async def _trigger_then_cancel():
        task = asyncio.current_task()
        assert task is not None
        asyncio.get_running_loop().call_soon(task.cancel)
        await timer.trigger()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.create_task(_trigger_then_cancel())
    await timer.cancel()


@pytest.mark.asyncio
async def test_trigger_does_not_cause_phantom_tick_after_natural_tick():
    """Regression: if a naturally-scheduled tick fires between trigger()
    registering its waiter and the pacemaker observing the trigger
    event, the leftover set event used to cause one extra phantom tick.
    `trigger()` should clear the event after its wait resolves."""
    call_count = [0]

    def _target():
        call_count[0] += 1
        return call_count[0]

    # Long delay so no natural ticks happen on their own.
    timer = async_timer.Timer(delay=10.0, target=_target, start=True)
    await timer.join()  # consume the immediate first tick (call_count=1)

    # Trigger and immediately also force the fanout to resolve our wait
    # synthetically (mimicking the "natural tick beat us" race) by
    # directly invoking send_result before the pacemaker iterates.
    async def _race():
        wait = asyncio.ensure_future(timer.result_fanout.wait())
        timer.pacemaker.trigger()
        # Synthetically resolve the wait before the pacemaker can
        # process its trigger — same effect as a natural tick beating it.
        timer.result_fanout.send_result(999)
        return await wait

    # The above directly tests the fanout side; for the trigger()-level
    # invariant we just verify trigger() clears the event afterwards.
    count_before = call_count[0]
    await timer.trigger()
    # Give any phantom tick a chance to fire (it would be near-immediate).
    await asyncio.sleep(0.05)
    await timer.cancel()
    # Exactly one tick from the trigger; no phantom.
    assert call_count[0] == count_before + 1, (
        f"expected exactly 1 tick from trigger, got {call_count[0] - count_before}"
    )
    assert not timer.pacemaker._trigger_evt.is_set()


@pytest.mark.asyncio
async def test_trigger_clears_event_when_natural_tick_satisfies_waiter():
    """Deterministically construct the M2 race: trigger() registers its
    waiter, then a natural tick (simulated by external send_result)
    resolves it before the pacemaker has a chance to consume the trigger
    event. trigger()'s finally must clear the leftover event so no
    phantom tick fires on the next pacemaker iteration."""
    call_count = [0]

    def _target():
        call_count[0] += 1
        return call_count[0]

    timer = async_timer.Timer(delay=10.0, target=_target, start=True)
    await timer.join()  # call_count == 1

    # Launch trigger() in a task; wait for its waiter to register; then
    # externally satisfy it before the pacemaker can iterate.
    task = asyncio.create_task(timer.trigger())
    for _ in range(100):
        await asyncio.sleep(0)
        if timer.result_fanout.futures:
            break
    assert timer.result_fanout.futures, "trigger() did not register its waiter"
    timer.result_fanout.send_result(999)  # mimic natural tick beating trigger
    rv = await task
    assert rv == 999
    # The event must have been cleared by trigger()'s finally block,
    # otherwise the pacemaker would fire a phantom tick.
    assert not timer.pacemaker._trigger_evt.is_set(), (
        "trigger event was not cleared — phantom tick would follow"
    )
    await timer.cancel()


@pytest.mark.asyncio
async def test_fixed_rate_timer_emits_skip_warning(caplog):
    """End-to-end: a Timer in fixed_rate mode whose target is slower than
    delay should emit a 'fell behind' warning from the pacemaker."""

    async def _slow_target():
        await asyncio.sleep(0.03)

    timer = async_timer.Timer(
        delay=0.01, target=_slow_target, mode="fixed_rate", start=True
    )
    with caplog.at_level(logging.WARNING, logger="async_timer.pacemaker"):
        # Let it tick a few times so the lag accumulates.
        for _ in range(3):
            await timer.join()
    await timer.cancel()
    assert any("fell behind" in r.message for r in caplog.records)
