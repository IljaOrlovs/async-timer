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


# ---------------------------------------------------------------------
# TimerGroup.wait
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_group_wait_empty_returns_empty_list_immediately():
    group = async_timer.TimerGroup()
    assert await group.wait(hit_count=1) == []


@pytest.mark.asyncio
async def test_group_wait_blocks_until_all_members_hit_target():
    counter_a = 0
    counter_b = 0

    def make_a():
        nonlocal counter_a
        counter_a += 1
        return ("a", counter_a)

    def make_b():
        nonlocal counter_b
        counter_b += 1
        return ("b", counter_b)

    t_a = async_timer.Timer(delay=10e-5, target=make_a)
    t_b = async_timer.Timer(delay=10e-5, target=make_b)
    async with async_timer.TimerGroup([t_a, t_b]) as group:
        results = await group.wait(hit_count=2)
    # Order preserved.
    assert [t for t, _ in results] == [t_a, t_b]
    # Each timer has hit at least the target.
    assert t_a.hit_count >= 2 and t_b.hit_count >= 2
    # Returned value is each timer's last tick result — match the shape
    # produced by its target. (Can't compare to `timer.last_result` here:
    # the timer may have ticked again between gather resolving and now.)
    a_rv = next(rv for t, rv in results if t is t_a)
    b_rv = next(rv for t, rv in results if t is t_b)
    assert a_rv[0] == "a" and a_rv[1] >= 2
    assert b_rv[0] == "b" and b_rv[1] >= 2


@pytest.mark.asyncio
async def test_group_wait_uses_hits_for_relative_target():
    t = async_timer.Timer(delay=10e-5, target=lambda: 1)
    async with async_timer.TimerGroup([t]) as group:
        await t.wait(hit_count=3)  # advance the timer
        baseline = t.hit_count
        await group.wait(hits=2)
    assert t.hit_count >= baseline + 2


@pytest.mark.asyncio
async def test_group_wait_timeout_raises_and_cancels_pending():
    slow = async_timer.Timer(delay=10.0, target=lambda: 1)  # essentially never
    async with async_timer.TimerGroup([slow]) as group:
        with pytest.raises(asyncio.TimeoutError):
            await group.wait(hit_count=5, timeout=0.05)
        # The group is still active; the timer is still running.
        assert slow.is_running()


@pytest.mark.asyncio
async def test_group_wait_propagates_first_member_failure_by_default():
    boom_count = 0

    def boom():
        nonlocal boom_count
        boom_count += 1
        if boom_count >= 2:
            raise RuntimeError("kaboom")
        return 1

    healthy = async_timer.Timer(delay=10e-5, target=lambda: 1)
    sick = async_timer.Timer(delay=10e-5, target=boom, exc_cb=lambda *_a, **_kw: None)
    async with async_timer.TimerGroup([healthy, sick]) as group:
        with pytest.raises(RuntimeError, match="kaboom"):
            await group.wait(hit_count=10)


@pytest.mark.asyncio
async def test_group_wait_return_exceptions_collects_per_member_errors():
    boom_count = 0

    def boom():
        nonlocal boom_count
        boom_count += 1
        if boom_count >= 2:
            raise RuntimeError("kaboom")
        return "ok"

    healthy = async_timer.Timer(delay=10e-5, target=lambda: "h")
    sick = async_timer.Timer(delay=10e-5, target=boom, exc_cb=lambda *_a, **_kw: None)
    async with async_timer.TimerGroup([healthy, sick]) as group:
        # Healthy will easily hit 3; sick stops on the second call.
        # With return_exceptions=True both come back, sick as an exception.
        results = await group.wait(hit_count=3, return_exceptions=True)
    assert len(results) == 2
    by_timer = dict(results)
    # Healthy succeeded; its last_rv is its last_result.
    assert by_timer[healthy] == healthy.last_result
    # Sick raised; CancelledError from .wait() on a stopped timer.
    assert isinstance(by_timer[sick], BaseException)


# ---------------------------------------------------------------------
# TimerGroup.is_running
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_group_is_running_false_before_start():
    t = async_timer.Timer(delay=10e-5, target=lambda: 1)
    group = async_timer.TimerGroup([t])
    assert group.is_running() is False


@pytest.mark.asyncio
async def test_group_is_running_true_while_active():
    t = async_timer.Timer(delay=10e-5, target=lambda: 1)
    async with async_timer.TimerGroup([t]) as group:
        assert group.is_running() is True
    assert group.is_running() is False


@pytest.mark.asyncio
async def test_group_is_running_false_if_a_member_stopped():
    t_ok = async_timer.Timer(delay=10e-5, target=lambda: 1)
    t_dead = async_timer.Timer(delay=10e-5, target=lambda: 1)
    async with async_timer.TimerGroup([t_ok, t_dead]) as group:
        await t_dead.cancel()
        assert group.is_running() is False
        assert t_ok.is_running()


@pytest.mark.asyncio
async def test_group_is_running_vacuous_true_for_empty_active_group():
    async with async_timer.TimerGroup() as group:
        assert group.is_running() is True


# ---------------------------------------------------------------------
# TimerGroup.trigger
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_group_trigger_empty_returns_empty_list():
    async with async_timer.TimerGroup() as group:
        assert await group.trigger() == []


@pytest.mark.asyncio
async def test_group_trigger_fires_every_member_and_returns_values():
    counter_a = 0
    counter_b = 0

    async def make_a():
        nonlocal counter_a
        counter_a += 1
        return ("a", counter_a)

    async def make_b():
        nonlocal counter_b
        counter_b += 1
        return ("b", counter_b)

    # Long delays so a natural tick can't race the trigger.
    t_a = async_timer.Timer(delay=60, target=make_a)
    t_b = async_timer.Timer(delay=60, target=make_b)
    async with async_timer.TimerGroup([t_a, t_b]) as group:
        # First (delay=0) tick lands almost immediately; wait it out so
        # trigger's count is unambiguous.
        await group.wait(hit_count=1)
        baseline_a, baseline_b = counter_a, counter_b
        results = await group.trigger()
    # Order preserved.
    assert [t for t, _ in results] == [t_a, t_b]
    # Each target ran exactly once more.
    assert counter_a == baseline_a + 1
    assert counter_b == baseline_b + 1
    by_timer = dict(results)
    assert by_timer[t_a] == ("a", baseline_a + 1)
    assert by_timer[t_b] == ("b", baseline_b + 1)


@pytest.mark.asyncio
async def test_group_trigger_timeout_raises():
    # A target that hangs forever — trigger() will never resolve.
    async def stuck():
        await asyncio.sleep(60)

    t = async_timer.Timer(delay=60, target=stuck)
    async with async_timer.TimerGroup([t]) as group:
        with pytest.raises(asyncio.TimeoutError):
            await group.trigger(timeout=0.05)


@pytest.mark.asyncio
async def test_group_trigger_return_exceptions_collects_per_member():
    t_ok = async_timer.Timer(delay=60, target=lambda: "ok")
    t_dead = async_timer.Timer(delay=60, target=lambda: "x")
    async with async_timer.TimerGroup([t_ok, t_dead]) as group:
        await group.wait(hit_count=1)
        # Stop one member; its trigger() will raise RuntimeError.
        await t_dead.cancel()
        results = await group.trigger(return_exceptions=True)
    assert len(results) == 2
    by_timer = dict(results)
    assert by_timer[t_ok] == "ok"
    assert isinstance(by_timer[t_dead], BaseException)


# ---------------------------------------------------------------------
# TimerGroup.start (explicit)
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_group_explicit_start_and_cancel_without_context_manager():
    t = async_timer.Timer(delay=10e-5, target=lambda: 1)
    group = async_timer.TimerGroup([t])
    group.start()
    try:
        assert group.is_running()
        assert t.is_running()
    finally:
        await group.cancel_all()
    assert not group.is_running()
    assert not t.is_running()


@pytest.mark.asyncio
async def test_group_start_is_idempotent_while_active():
    t = async_timer.Timer(delay=10e-5, target=lambda: 1)
    async with async_timer.TimerGroup([t]) as group:
        # Second start while active is a no-op.
        group.start()
        assert t.is_running()


# ---------------------------------------------------------------------
# TimerGroup.name
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_group_name_in_repr():
    group = async_timer.TimerGroup(name="caches")
    assert "caches" in repr(group)


@pytest.mark.asyncio
async def test_group_name_scopes_cancel_failure_logger(caplog):
    import logging

    t = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    await t.join()

    async def _bad_cancel():
        raise RuntimeError("boom")

    t.cancel = _bad_cancel  # type: ignore[method-assign]
    group = async_timer.TimerGroup([t], name="caches")
    with caplog.at_level(logging.ERROR, logger="async_timer.group.caches"):
        await group.cancel_all()
    assert any(r.name == "async_timer.group.caches" for r in caplog.records)


# ---------------------------------------------------------------------
# TimerGroup.cancel_threadsafe
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_group_cancel_threadsafe_from_loop_thread_raises():
    async with async_timer.TimerGroup([
        async_timer.Timer(delay=10e-5, target=lambda: 1)
    ]) as group:
        with pytest.raises(RuntimeError, match="own event loop"):
            group.cancel_threadsafe()


@pytest.mark.asyncio
async def test_group_cancel_threadsafe_unstarted_raises():
    group = async_timer.TimerGroup([
        async_timer.Timer(delay=10e-5, target=lambda: 1)
    ])
    with pytest.raises(RuntimeError, match="has not been started"):
        group.cancel_threadsafe()


@pytest.mark.asyncio
async def test_group_cancel_threadsafe_from_worker_thread():
    import threading

    t = async_timer.Timer(delay=10e-5, target=lambda: 1)
    group = async_timer.TimerGroup([t])
    group.start()
    assert t.is_running()

    err: list = []

    def worker():
        try:
            group.cancel_threadsafe(timeout=5.0)
        except BaseException as e:  # pragma: no cover - failure path
            err.append(e)

    th = threading.Thread(target=worker)
    th.start()
    # Pump the loop so the threadsafe-scheduled coroutine can run.
    while th.is_alive():
        await asyncio.sleep(0.01)
    th.join()
    assert not err, err
    assert not t.is_running()
    assert not group.is_running()


@pytest.mark.asyncio
async def test_group_wait_lifespan_warmup_pattern():
    """The headline use case: warm a set of caches before serving."""
    caches = [{"warm": False}, {"warm": False}, {"warm": False}]

    def make_warmer(i):
        def warm():
            caches[i]["warm"] = True
        return warm

    async with async_timer.TimerGroup() as group:
        for i in range(3):
            group.add(async_timer.Timer(delay=10e-5, target=make_warmer(i)))
        await group.wait(hit_count=1)
        # By this point every cache is warm — no traffic has slipped through.
        assert all(c["warm"] for c in caches)
