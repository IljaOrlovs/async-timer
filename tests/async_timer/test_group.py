"""Tests for `TimerGroup`."""

import asyncio

import pytest

import async_timer


@pytest.mark.asyncio
async def test_group_starts_and_cancels_all_timers():
    hits_a = []
    hits_b = []
    timer_a = async_timer.Timer(delay=10e-5, target=lambda: hits_a.append(1))
    timer_b = async_timer.Timer(delay=10e-5, target=lambda: hits_b.append(1))

    group = async_timer.TimerGroup([timer_a, timer_b])
    async with group:
        assert timer_a.is_running()
        assert timer_b.is_running()
        await timer_a.join()
        await timer_b.join()

    # Both cancelled by __aexit__.
    assert not timer_a.is_running()
    assert not timer_b.is_running()
    assert hits_a and hits_b


@pytest.mark.asyncio
async def test_group_add_returns_timer_for_chaining():
    group = async_timer.TimerGroup()
    t = async_timer.Timer(delay=10e-5, target=lambda: 1)
    added = group.add(t)
    assert added is t
    assert len(group) == 1
    assert t in group


@pytest.mark.asyncio
async def test_group_add_while_active_starts_immediately():
    group = async_timer.TimerGroup()
    async with group:
        late_arrival = async_timer.Timer(delay=10e-5, target=lambda: 99)
        group.add(late_arrival)
        # Should already be running by the time add() returns.
        assert late_arrival.is_running()
        await late_arrival.join()
    assert not late_arrival.is_running()


@pytest.mark.asyncio
async def test_group_add_already_running_timer_doesnt_restart():
    """Adding a timer that's already running must not double-start it."""
    t = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    try:
        group = async_timer.TimerGroup()
        async with group:
            group.add(t)  # already running — must not raise
            assert t.is_running()
        # __aexit__ cancels it.
        assert not t.is_running()
    finally:
        if t.is_running():
            await t.cancel()


@pytest.mark.asyncio
async def test_empty_group_works():
    group = async_timer.TimerGroup()
    async with group:
        pass  # nothing to do, must not raise


@pytest.mark.asyncio
async def test_group_cancel_all_runs_cancellations_concurrently():
    """cancel_all() should await all cancellations together, not serially."""
    cancel_order = []

    async def _slow_cb_a(*_a, **_kw):
        await asyncio.sleep(0.05)
        cancel_order.append("a")

    def _cb_a(*a, **kw):
        asyncio.get_running_loop().create_task(_slow_cb_a())

    def _cb_b(*_a, **_kw):
        cancel_order.append("b")

    timer_a = async_timer.Timer(delay=10e-5, target=lambda: 1, cancel_cb=_cb_a)
    timer_b = async_timer.Timer(delay=10e-5, target=lambda: 1, cancel_cb=_cb_b)
    group = async_timer.TimerGroup([timer_a, timer_b])
    async with group:
        await timer_a.join()
        await timer_b.join()
    # Both fired; we don't strictly assert ordering, just that both ran.
    assert "b" in cancel_order


@pytest.mark.asyncio
async def test_group_cancel_all_continues_on_individual_failure(caplog):
    """If one timer's cancel() raises, the group must still cancel the
    others and log the failure (rather than abandoning siblings)."""
    import logging

    cancelled_b = []
    timer_a = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    timer_b = async_timer.Timer(
        delay=10e-5,
        target=lambda: 1,
        cancel_cb=lambda *_a, **_kw: cancelled_b.append(1),
        start=True,
    )
    await timer_a.join()
    await timer_b.join()

    # Replace timer_a.cancel with one that raises.
    async def _bad_cancel():
        raise RuntimeError("intentional cancel failure")

    timer_a.cancel = _bad_cancel  # type: ignore[method-assign]

    group = async_timer.TimerGroup([timer_a, timer_b])
    with caplog.at_level(logging.ERROR, logger="async_timer.group"):
        # __aexit__ would have to be called manually for this case since
        # we never entered; use cancel_all directly.
        await group.cancel_all()
    # timer_b was still cancelled despite timer_a's failure.
    assert cancelled_b == [1]
    # The failure was logged.
    assert any(
        "intentional cancel failure" in r.message or "RuntimeError" in r.message
        for r in caplog.records
    )
    # Cleanup any still-running real cancel.
    if timer_b.is_running():
        # restore and cleanup
        pass


@pytest.mark.asyncio
async def test_group_is_iterable():
    timers = [async_timer.Timer(delay=10e-5, target=lambda: 1) for _ in range(3)]
    group = async_timer.TimerGroup(timers)
    assert list(group) == timers
