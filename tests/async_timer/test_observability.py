"""Tests for layer-1 observability additions.

Covers:
  * `Timer.exception_count` / `last_exception` / `last_exception_at`
    counters wired into the loop's exception path.
  * `extra={"event": "async_timer.*", ...}` payload on every WARNING /
    EXCEPTION log call — JSON-log handlers read these as structured
    fields without parsing message strings.
"""

import asyncio
import logging
import time

import pytest

import async_timer
from async_timer.subscription import Subscription


@pytest.mark.asyncio
async def test_exception_telemetry_is_recorded():
    """One target exception → counter=1, last_exception set, timestamp set."""
    err = RuntimeError("boom")

    def explode():
        raise err

    t0 = time.monotonic()
    timer: async_timer.Timer = async_timer.Timer(0.001, explode, start=True)
    with pytest.raises(RuntimeError, match="boom"):
        await timer.wait()  # FanoutRv re-raises the sticky target exception
    await timer.cancel()
    assert timer.exception_count == 1
    assert timer.last_exception is err
    assert timer.last_exception_at is not None
    assert timer.last_exception_at >= t0


@pytest.mark.asyncio
async def test_exception_telemetry_cumulative_across_restarts():
    """Counters accumulate across cancel/restart — same semantics as hit_count."""
    err1 = RuntimeError("first")
    err2 = RuntimeError("second")
    errors = iter([err1, err2])

    def explode():
        raise next(errors)

    timer: async_timer.Timer = async_timer.Timer(0.001, explode)
    timer.start()
    with pytest.raises(RuntimeError, match="first"):
        await timer.wait()
    await timer.cancel()
    assert timer.exception_count == 1
    assert timer.last_exception is err1

    timer.start()  # restart
    with pytest.raises(RuntimeError, match="second"):
        await timer.wait()
    await timer.cancel()
    assert timer.exception_count == 2
    assert timer.last_exception is err2


def test_exception_telemetry_defaults_zero_before_start():
    """A freshly-constructed Timer has the default zero/None values."""
    timer: async_timer.Timer = async_timer.Timer(1.0, lambda: 42)
    assert timer.exception_count == 0
    assert timer.last_exception is None
    assert timer.last_exception_at is None


@pytest.mark.asyncio
async def test_fixed_rate_skip_has_structured_extras(caplog):
    """The fall-behind warning carries `event`, `skipped_ticks`, `delay_s`,
    `behind_s` as structured fields so JSON-log handlers don't have to
    regex the message string."""
    import async_timer.pacemaker as pacemaker

    pm = pacemaker.TimerPacemaker(delay=0.001, mode="fixed_rate")
    pm._start_time = time.monotonic() - 5.0  # five-second backlog
    pm._tick_number = 0

    caplog.set_level(logging.WARNING, logger="async_timer.pacemaker")
    pm._compute_fixed_rate_wait()

    skip_records = [
        r
        for r in caplog.records
        if getattr(r, "event", None) == "async_timer.fixed_rate_skip"
    ]
    assert len(skip_records) == 1
    rec = skip_records[0]
    assert rec.skipped_ticks >= 1
    assert rec.delay_s == pytest.approx(0.001)
    assert rec.behind_s > 0


@pytest.mark.asyncio
async def test_cancel_aws_raise_has_structured_extras(caplog):
    """`cancel_aws` exception warning carries `event` + `exception_type`."""
    import async_timer.pacemaker as pacemaker

    cancel_fut: asyncio.Future = asyncio.Future()
    pm = pacemaker.TimerPacemaker(delay=10e-5)
    pm.stop_on([cancel_fut])

    caplog.set_level(logging.WARNING, logger="async_timer.pacemaker")
    iter_count = 0
    async for _ in pm:
        iter_count += 1
        if iter_count == 1:
            cancel_fut.set_exception(RuntimeError("from-test"))

    raise_records = [
        r
        for r in caplog.records
        if getattr(r, "event", None) == "async_timer.cancel_aws_raised"
    ]
    assert len(raise_records) == 1
    assert raise_records[0].exception_type == "RuntimeError"


def test_subscription_drop_has_structured_extras(caplog):
    """The bounded-queue drop warning carries `event` + `subscription_name`
    + `dropped_count`."""
    caplog.set_level(logging.WARNING, logger="async_timer.subscription")
    sub: Subscription[int] = Subscription(maxsize=2, name="metrics")
    for v in range(5):
        sub._push_value(v)

    drop_records = [
        r
        for r in caplog.records
        if getattr(r, "event", None) == "async_timer.subscription_drop"
    ]
    assert drop_records, "expected at least one drop warning"
    last = drop_records[-1]
    assert last.subscription_name == "metrics"
    assert last.maxsize == 2
    assert last.dropped_count >= 1


@pytest.mark.asyncio
async def test_default_exc_cb_uses_named_logger_and_extras(caplog):
    """Naming a Timer scopes the exception log to `async_timer.timer.<name>`
    and the record carries `event`, `timer_name`, `hit_count` extras."""

    def explode():
        raise ValueError("named-boom")

    caplog.set_level(logging.ERROR)
    t: async_timer.Timer = async_timer.Timer(
        0.001, explode, name="cache-warmup", start=True
    )
    with pytest.raises(ValueError, match="named-boom"):
        await t.wait()
    await t.cancel()

    matching = [
        r
        for r in caplog.records
        if getattr(r, "event", None) == "async_timer.target_exception"
    ]
    assert matching, "default exc_cb should emit a structured-extras record"
    rec = matching[0]
    assert rec.timer_name == "cache-warmup"
    assert rec.name == "async_timer.timer.cache-warmup"
    assert rec.hit_count == t.hit_count
