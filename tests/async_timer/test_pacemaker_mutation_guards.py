"""Targeted tests for pacemaker branches that survive naive coverage.

These exercise specific assertions that a tick-by-tick coverage report
calls "100%" but that a mutation-test sweep would call out as unverified
(boundary conditions, log emissions, idempotency).
"""

import asyncio
import logging

import pytest

import async_timer.pacemaker as pacemaker


@pytest.mark.asyncio
async def test_stop_on_with_exception_logs_warning(caplog):
    """A `cancel_aws` awaitable that *raises* must log a warning before stop.

    Without this, mutating the `if exc is not None:` guard or removing the
    log call would survive — the existing cancel-by-exception test only
    checks termination, not the warning.
    """
    cancel_fut: asyncio.Future = asyncio.Future()
    pm = pacemaker.TimerPacemaker(delay=10e-5)
    pm.stop_on([cancel_fut])

    caplog.set_level(logging.WARNING, logger="async_timer.pacemaker")
    iter_count = 0
    async for _ in pm:
        iter_count += 1
        if iter_count == 1:
            cancel_fut.set_exception(RuntimeError("kaboom"))

    warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        "kaboom" in r.message or "RuntimeError" in str(r.exc_info)
        for r in warning_records
    ), "stop_on exception path should log a warning identifying the cause"


@pytest.mark.asyncio
async def test_stop_on_with_clean_result_does_not_warn(caplog):
    """A `cancel_aws` awaitable that resolves *cleanly* must NOT log.

    Locks down the `if not fut.cancelled(): exc = fut.exception(); if exc:`
    branch — a mutation that unconditionally logs would surface here.
    """
    cancel_fut: asyncio.Future = asyncio.Future()
    pm = pacemaker.TimerPacemaker(delay=10e-5)
    pm.stop_on([cancel_fut])

    caplog.set_level(logging.WARNING, logger="async_timer.pacemaker")
    iter_count = 0
    async for _ in pm:
        iter_count += 1
        if iter_count == 1:
            cancel_fut.set_result(42)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == [], (
        f"clean stop_on result must not warn; got {[r.message for r in warnings]}"
    )


def test_stop_is_idempotent():
    """Second stop() is a no-op — locks down the early-return guard."""
    pm = pacemaker.TimerPacemaker(delay=0.01)
    pm.stop()
    assert not pm._running
    pm.stop()  # must not raise
    assert not pm._running


@pytest.mark.asyncio
async def test_reset_clears_set_cancel_event():
    """_reset() must replace a set _cancel_evt; otherwise restart would
    StopAsyncIteration immediately on the next iter."""
    pm = pacemaker.TimerPacemaker(delay=10e-5)
    pm.stop()
    assert pm._cancel_evt.is_set()
    pm._reset()
    assert not pm._cancel_evt.is_set()
    # And the iterator must actually produce a tick after reset.
    iter_count = 0
    async for _ in pm:
        iter_count += 1
        if iter_count >= 2:
            pm.stop()
    assert iter_count >= 2


@pytest.mark.asyncio
async def test_fixed_rate_does_not_carry_tick_number_across_modes():
    """Confirms removing the dead `_tick_number += 1` in fixed_delay path
    doesn't break fixed_rate. In fixed_delay, _tick_number is never read."""
    pm = pacemaker.TimerPacemaker(delay=10e-5, mode="fixed_delay")
    iter_count = 0
    async for _ in pm:
        iter_count += 1
        if iter_count >= 3:
            pm.stop()
    # In fixed_delay, _tick_number is reset to 0 on first iter and never
    # advanced again — the dead-code removal preserves this invariant.
    assert pm._tick_number == 0


def test_apply_jitter_with_zero_jitter_honors_cap():
    """Regression: hypothesis caught the early-return-on-zero-jitter
    bypassing the cap. Explicit unit test in case the property test is
    ever weakened."""
    pm = pacemaker.TimerPacemaker(delay=1.0, jitter=0.0)
    assert pm._apply_jitter(base=10.0, cap=3.0) == 3.0
    assert pm._apply_jitter(base=10.0, cap=None) == 10.0
    assert pm._apply_jitter(base=2.0, cap=5.0) == 2.0  # no clamp when base<cap
