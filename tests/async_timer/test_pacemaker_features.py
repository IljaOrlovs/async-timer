"""Tests for the new pacemaker options:
initial_delay, jitter, mode (fixed_rate vs fixed_delay), and trigger()."""

import asyncio
import logging
import time

import pytest

from async_timer.pacemaker import TimerPacemaker


@pytest.mark.asyncio
async def test_initial_delay_delays_first_tick():
    pm = TimerPacemaker(delay=10.0, initial_delay=0.05)
    t0 = time.monotonic()
    aiter = pm.__aiter__()
    await aiter.__anext__()
    elapsed = time.monotonic() - t0
    assert 0.04 <= elapsed < 0.5
    pm.stop()


@pytest.mark.asyncio
async def test_initial_delay_zero_fires_immediately():
    pm = TimerPacemaker(delay=10.0, initial_delay=0.0)
    t0 = time.monotonic()
    aiter = pm.__aiter__()
    await aiter.__anext__()
    elapsed = time.monotonic() - t0
    assert elapsed < 0.05
    pm.stop()


def test_invalid_jitter_rejected():
    with pytest.raises(ValueError, match="jitter"):
        TimerPacemaker(delay=1.0, jitter=1.5)
    with pytest.raises(ValueError, match="jitter"):
        TimerPacemaker(delay=1.0, jitter=-0.1)


def test_invalid_initial_delay_rejected():
    with pytest.raises(ValueError, match="initial_delay"):
        TimerPacemaker(delay=1.0, initial_delay=-1.0)


def test_invalid_mode_rejected():
    with pytest.raises(ValueError, match="mode"):
        TimerPacemaker(delay=1.0, mode="cron")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_jitter_applied_within_bounds():
    """With jitter=0.5, sleeps should fall in [delay*0.5, delay*1.5]."""
    pm = TimerPacemaker(delay=0.02, jitter=0.5)
    aiter = pm.__aiter__()
    await aiter.__anext__()  # first tick — no sleep
    samples = []
    for _ in range(10):
        t0 = time.monotonic()
        await aiter.__anext__()
        samples.append(time.monotonic() - t0)
    pm.stop()
    # All within bounds (allow generous slack for scheduler latency).
    assert all(0.0 <= s <= 0.05 for s in samples), samples
    # And not all identical (jitter actually varies).
    assert len({round(s, 4) for s in samples}) > 1


@pytest.mark.asyncio
async def test_fixed_rate_mode_skips_when_behind(caplog):
    """If consumer processing of a tick takes longer than `delay`, the
    fixed_rate pacemaker should skip the missed slot(s) and log."""
    pm = TimerPacemaker(delay=0.02, mode="fixed_rate")
    aiter = pm.__aiter__()

    t0 = time.monotonic()
    await aiter.__anext__()  # tick 1 (anchors schedule)
    # Simulate slow processing of tick 1 — long enough to miss tick 2.
    await asyncio.sleep(0.07)
    with caplog.at_level(logging.WARNING, logger="async_timer.pacemaker"):
        await aiter.__anext__()  # should skip ahead, not sleep again
    pm.stop()
    elapsed = time.monotonic() - t0
    # We should have landed at slot ~0.08 (4*delay), not slot 0.04 (2*delay).
    assert elapsed >= 0.06
    assert any("fell behind" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_fixed_delay_mode_does_not_skip():
    """fixed_delay mode just waits `delay` after the previous tick,
    regardless of wall-clock drift; no skip warnings."""
    pm = TimerPacemaker(delay=0.02, mode="fixed_delay")
    aiter = pm.__aiter__()
    await aiter.__anext__()
    await asyncio.sleep(0.07)  # "slow processing"
    t0 = time.monotonic()
    await aiter.__anext__()
    elapsed = time.monotonic() - t0
    # Slept the full delay even though we were "behind".
    assert 0.015 <= elapsed < 0.1
    pm.stop()


@pytest.mark.asyncio
async def test_trigger_wakes_in_progress_sleep():
    pm = TimerPacemaker(delay=10.0)
    aiter = pm.__aiter__()
    await aiter.__anext__()  # first tick is free

    async def _delayed_trigger():
        await asyncio.sleep(0.01)
        pm.trigger()

    asyncio.get_running_loop().create_task(_delayed_trigger())
    t0 = time.monotonic()
    await aiter.__anext__()  # should return ~immediately after trigger
    elapsed = time.monotonic() - t0
    assert elapsed < 0.5, f"trigger did not wake sleep promptly ({elapsed:.2f}s)"
    pm.stop()


@pytest.mark.asyncio
async def test_trigger_evt_is_consumed_so_next_sleep_is_normal():
    """After trigger() fires once, the next __anext__ should sleep
    normally, not return immediately."""
    pm = TimerPacemaker(delay=0.02)
    aiter = pm.__aiter__()
    await aiter.__anext__()
    pm.trigger()
    await aiter.__anext__()  # consumes trigger, returns ~immediately
    t0 = time.monotonic()
    await aiter.__anext__()  # should sleep ~0.02s
    elapsed = time.monotonic() - t0
    assert elapsed >= 0.015
    pm.stop()


@pytest.mark.asyncio
async def test_reset_clears_trigger_event():
    pm = TimerPacemaker(delay=0.02)
    pm.trigger()
    pm.stop()
    pm._reset()
    assert not pm._trigger_evt.is_set()


@pytest.mark.asyncio
async def test_stop_during_initial_delay():
    """Stopping mid-initial_delay should end iteration cleanly."""
    pm = TimerPacemaker(delay=1.0, initial_delay=10.0)
    aiter = pm.__aiter__()

    async def _delayed_stop():
        await asyncio.sleep(0.01)
        pm.stop()

    asyncio.get_running_loop().create_task(_delayed_stop())
    with pytest.raises(StopAsyncIteration):
        await aiter.__anext__()


@pytest.mark.asyncio
async def test_jitter_clamps_negative_to_zero(monkeypatch):
    """Even with max negative jitter the resulting wait is never < 0."""
    pm = TimerPacemaker(delay=0.01, jitter=1.0)
    # Force the random sample to -1 (max negative): delta = 0.01 * 1 * -1 = -0.01
    # base + delta = 0 — at the boundary; force a value that would go below 0.
    monkeypatch.setattr("random.uniform", lambda _a, _b: -2.0)
    out = pm._apply_jitter(0.01)
    assert out == 0.0


@pytest.mark.asyncio
async def test_jitter_respects_cap(monkeypatch):
    """When `cap` is provided, jitter never pushes the wait above it."""
    pm = TimerPacemaker(delay=0.01, jitter=1.0)
    monkeypatch.setattr("random.uniform", lambda _a, _b: 5.0)
    out = pm._apply_jitter(0.01, cap=0.012)
    assert out == 0.012


@pytest.mark.asyncio
async def test_zero_wait_path_yields_to_other_tasks(monkeypatch):
    """When the computed per-tick wait is exactly 0 (max negative jitter
    collapses delay to 0), pacemaker still yields via `asyncio.sleep(0)`
    rather than hot-looping."""
    pm = TimerPacemaker(delay=0.01, jitter=1.0)
    aiter = pm.__aiter__()
    await aiter.__anext__()  # first tick, no sleep
    # Force jitter to produce wait==0.
    monkeypatch.setattr("random.uniform", lambda _a, _b: -1.0)
    await aiter.__anext__()  # exercises the wait_for<=0 branch
    pm.stop()
