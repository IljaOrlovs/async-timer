"""Tests for the @every decorator."""

import asyncio

import pytest

import async_timer
from async_timer.decorators import DecoratedTimer


@pytest.mark.asyncio
async def test_every_wraps_function_into_timer():
    @async_timer.every(10e-5)
    async def refresh():
        return "fresh"

    assert isinstance(refresh, DecoratedTimer)
    assert isinstance(refresh, async_timer.Timer)


@pytest.mark.asyncio
async def test_every_preserves_original_callable_as_func():
    """The undecorated function must remain directly callable via .func
    so tests can exercise it without involving the timer machinery."""

    call_count = [0]

    @async_timer.every(10e-5)
    async def refresh():
        call_count[0] += 1
        return "fresh"

    # Direct invocation — no timer involved.
    result = await refresh.func()
    assert result == "fresh"
    assert call_count[0] == 1
    # Timer was never started — still callable directly.
    assert not refresh.is_running()


@pytest.mark.asyncio
async def test_every_decorated_timer_works_as_timer():
    @async_timer.every(10e-5)
    async def refresh():
        return 42

    refresh.start()
    try:
        assert await refresh.join() == 42
    finally:
        await refresh.cancel()


@pytest.mark.asyncio
async def test_every_uses_func_name_as_default_timer_name():
    @async_timer.every(10e-5)
    async def my_custom_name():
        return 1

    assert my_custom_name.name == "my_custom_name"
    assert "name='my_custom_name'" in repr(my_custom_name)


@pytest.mark.asyncio
async def test_every_explicit_name_overrides_func_name():
    @async_timer.every(10e-5, name="explicit_override")
    async def my_func():
        return 1

    assert my_func.name == "explicit_override"


@pytest.mark.asyncio
async def test_every_forwards_timer_kwargs():
    @async_timer.every(
        delay=10e-5,
        mode="fixed_rate",
        jitter=0.2,
        initial_delay=0.0,
    )
    async def my_timer():
        return 1

    assert my_timer.pacemaker.mode == "fixed_rate"
    assert my_timer.pacemaker.jitter == 0.2


@pytest.mark.asyncio
async def test_every_preserves_wrapped_function_metadata():
    @async_timer.every(10e-5)
    async def documented():
        """A nicely-documented refresh function."""
        return 1

    # functools.update_wrapper copies __doc__, __name__, etc.
    assert documented.__doc__ == "A nicely-documented refresh function."
    assert getattr(documented, "__name__", None) == "documented"


@pytest.mark.asyncio
async def test_every_with_sync_function():
    """Decorator should also work with a plain sync callable."""
    call_count = [0]

    @async_timer.every(10e-5)
    def sync_target():
        call_count[0] += 1
        return call_count[0]

    sync_target.start()
    try:
        rv = await sync_target.join()
        assert rv >= 1
    finally:
        await sync_target.cancel()


@pytest.mark.asyncio
async def test_every_forwards_exc_cb_and_cancel_cb():
    """The decorator must forward exc_cb and cancel_cb to the Timer."""
    cancel_fired = []
    exc_fired = []

    def _on_cancel(*_a, **_kw):
        cancel_fired.append(1)

    def _on_exc(*_a, **_kw):
        exc_fired.append(1)

    @async_timer.every(10e-5, exc_cb=_on_exc, cancel_cb=_on_cancel)
    async def my_timer():
        return 1

    my_timer.start()
    await my_timer.join()
    await my_timer.cancel()
    assert cancel_fired
    assert not exc_fired  # no exception was raised


@pytest.mark.asyncio
async def test_every_with_cancel_aws():
    """The decorator must forward cancel_aws to the underlying Timer."""
    stop_evt = asyncio.Event()

    @async_timer.every(1.0, cancel_aws=[stop_evt.wait()])
    async def my_timer():
        return 1

    my_timer.start()
    await my_timer.join()
    stop_evt.set()
    for _ in range(50):
        await asyncio.sleep(0.01)
        if not my_timer.is_running():
            break
    await my_timer.cancel()
    assert not my_timer.is_running()
