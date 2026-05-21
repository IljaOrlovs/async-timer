"""Regression tests for issues found in the clean-slate audit.

Each test names the corresponding finding (#1..#4) so the link between
the audit and the fix is visible.
"""

import asyncio
import logging
import warnings

import pytest

import async_timer

# ---------------------------------------------------------------------------
# Finding #1: @every(..., cancel_aws=[...]) at module scope must not crash.
# Before the fix, Timer.__init__ called pacemaker.stop_on() which calls
# asyncio.ensure_future() — a RuntimeError at import time (no running loop).
# ---------------------------------------------------------------------------


def _construct_timer_with_cancel_aws_outside_loop():
    """Helper executed at *module* (sync) scope — no running event loop."""
    # `Event.wait()` returns a coroutine. Pre-fix this would crash on the
    # ensure_future inside pacemaker.stop_on. Post-fix it's deferred to start().
    evt = asyncio.Event()
    return async_timer.Timer(delay=1.0, target=lambda: 1, cancel_aws=[evt.wait()]), evt


# Run at *test-module-import* time (not inside a coroutine). If the bug
# existed, this would raise during pytest's collection of this file.
_PRECONSTRUCTED_TIMER, _PRECONSTRUCTED_EVT = (
    _construct_timer_with_cancel_aws_outside_loop()
)


def test_timer_with_cancel_aws_constructible_outside_async_context():
    """Sanity: a Timer with cancel_aws can be constructed at module
    scope (no running loop). The pacemaker registration is deferred
    until start()."""
    # If we got here, construction at module-import time succeeded.
    assert _PRECONSTRUCTED_TIMER is not None
    assert not _PRECONSTRUCTED_TIMER.is_running()
    # Close the pending coroutine to silence "never awaited" warnings;
    # the test only needed to prove construction works.
    if _PRECONSTRUCTED_TIMER._pending_cancel_aws:
        for aw in _PRECONSTRUCTED_TIMER._pending_cancel_aws:
            aw.close()  # type: ignore[union-attr]
        _PRECONSTRUCTED_TIMER._pending_cancel_aws = None


@pytest.mark.asyncio
async def test_deferred_cancel_aws_still_works_when_started():
    """The deferred cancel_aws must actually fire when its event resolves."""
    evt = asyncio.Event()
    timer = async_timer.Timer(
        delay=10.0,  # long enough no natural tick fires
        target=lambda: 1,
        cancel_aws=[evt.wait()],
    )
    timer.start()
    await timer.join()  # consume immediate first tick
    evt.set()
    for _ in range(50):
        await asyncio.sleep(0.01)
        if not timer.is_running():
            break
    assert not timer.is_running()
    await timer.cancel()


@pytest.mark.asyncio
async def test_every_decorator_with_cancel_aws_at_module_scope_works():
    """End-to-end: @every at module scope can use cancel_aws. We can't
    *actually* declare a module-scope decorator inside a test, but we
    simulate it by constructing the DecoratedTimer outside any running
    loop via direct invocation."""
    from async_timer.decorators import every

    evt = asyncio.Event()
    # `every()(...)` is what `@every(...)` desugars to. The construction
    # itself must not need a running loop.
    decorator = every(1.0, cancel_aws=[evt.wait()])
    # In Python 3.13 we already have a running loop here, but the bug
    # was specifically in pacemaker.stop_on being called from __init__,
    # which now doesn't happen.

    async def my_target():
        return 1

    decorated = decorator(my_target)
    assert not decorated.is_running()
    # Start it and verify cancel_aws still fires.
    decorated.start()
    await decorated.join()
    evt.set()
    for _ in range(50):
        await asyncio.sleep(0.01)
        if not decorated.is_running():
            break
    assert not decorated.is_running()


# ---------------------------------------------------------------------------
# Finding #2: trigger() must realign the fixed_rate schedule.
# Before the fix, the docstring claimed realignment but the code did not
# update _start_time, so the next tick would catch up to the original slot.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_realigns_fixed_rate_schedule():
    """After trigger() in fixed_rate mode, the *next* scheduled tick
    should fire one `delay` after the triggered tick, not catch up to
    the original schedule."""
    import time

    delay = 0.05
    timer = async_timer.Timer(
        delay=delay,
        target=lambda: time.monotonic(),
        mode="fixed_rate",
        start=True,
    )
    t0 = await timer.join()  # tick 0, anchor

    # Wait a small fraction of `delay`, then trigger early.
    await asyncio.sleep(delay * 0.2)
    t_trigger = await timer.trigger()
    # The triggered tick happened soon after our sleep above:
    assert t_trigger - t0 < delay * 0.7, "trigger() did not fire early"

    # Now: without re-anchoring, the next scheduled tick would fire at
    # `t0 + delay` (i.e. very soon, ~0.04s after t_trigger).
    # With re-anchoring, it should fire ~delay after t_trigger.
    t_next = await timer.join()
    await timer.cancel()

    actual_gap = t_next - t_trigger
    # Allow generous slack; key is that it's not near-zero.
    assert actual_gap >= delay * 0.6, (
        f"next tick fired only {actual_gap:.3f}s after trigger "
        f"(expected ~{delay:.3f}s — schedule was not realigned)"
    )


# ---------------------------------------------------------------------------
# Finding #3: default exc_cb must NOT re-raise.
# Before the fix, re-raising caused asyncio to emit a duplicate
# "Task exception was never retrieved" warning AND log it twice.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_default_exc_cb_does_not_propagate_or_double_log(caplog):
    """Default exc_cb logs the target exception once and lets the task
    end cleanly — no duplicate warnings."""

    def _raising():
        raise ValueError("target boom")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with caplog.at_level(logging.ERROR, logger="async_timer.timer"):
            timer = async_timer.Timer(delay=10e-5, target=_raising, start=True)
            # Wait for the loop to end naturally.
            for _ in range(50):
                await asyncio.sleep(0.01)
                if not timer.is_running():
                    break
            await timer.cancel()

    # The exception was logged exactly once.
    target_logs = [r for r in caplog.records if "target boom" in str(r.exc_info)]
    assert len(target_logs) == 1, (
        f"expected exactly one log of the target exception, got {len(target_logs)}"
    )
    # No "Task exception was never retrieved" warnings from asyncio.
    bad = [w for w in caught if "never retrieved" in str(w.message)]
    assert not bad, f"got asyncio warnings: {[str(w.message) for w in bad]}"


# ---------------------------------------------------------------------------
# Finding #4: TimerGroup.__aenter__ must clean up partially-started timers
# on failure. Before the fix, if one timer's start() raised, the earlier
# ones leaked because __aexit__ never ran.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timer_group_cleans_up_on_partial_start_failure():
    """If a timer's start() raises mid-way through __aenter__, the
    already-started timers must be cancelled so they don't leak."""
    good_timer = async_timer.Timer(delay=10e-5, target=lambda: 1)

    # Construct a timer that will raise on start() — re-use the
    # cancel_aws + restart guard: built with cancel_aws, then we
    # mark it as already-started so start() raises RuntimeError.
    evt = asyncio.Event()
    bad_timer = async_timer.Timer(
        delay=10e-5, target=lambda: 1, cancel_aws=[evt.wait()]
    )
    bad_timer._has_been_started = True  # spoof prior run

    group = async_timer.TimerGroup([good_timer, bad_timer])

    with pytest.raises(RuntimeError, match="cancel_aws"):
        async with group:
            pass  # should never reach here

    # good_timer must NOT be left running.
    assert not good_timer.is_running(), (
        "TimerGroup leaked good_timer after bad_timer failed to start"
    )
    # Close bad_timer's never-armed cancel_aws coroutine to silence warning.
    if bad_timer._pending_cancel_aws:
        for aw in bad_timer._pending_cancel_aws:
            aw.close()  # type: ignore[union-attr]
        bad_timer._pending_cancel_aws = None
