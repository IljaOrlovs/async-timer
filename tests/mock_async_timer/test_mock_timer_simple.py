import asyncio

import pytest

import mock_async_timer


def test_import():
    hasattr(mock_async_timer, "MockTimer")


@pytest.mark.asyncio
async def test_join_works():
    hit_count = 0

    def _target():
        nonlocal hit_count
        hit_count += 1
        if hit_count >= 1000:
            raise StopIteration

    mock_timer = mock_async_timer.MockTimer(target=_target, delay=10_000, start=True)

    assert mock_timer.is_running()
    await mock_timer.wait(), "Wait for it to exit"

    assert hit_count == 1000
    assert not mock_timer.is_running()
    assert mock_timer.delay == 10_000
    assert mock_timer.pacemaker.sleep.await_count == 999


@pytest.mark.asyncio
async def test_mock_timer_is_coroutine_friendly():
    """Confirm that the mock timer allows for the other async code to run"""
    timer_hit_count = 0

    def _target():
        nonlocal timer_hit_count
        timer_hit_count += 1

    my_hit_count = 0
    async with (
        mock_async_timer.MockTimer(target=_target, delay=10_000),
        mock_async_timer.MockTimer(target=_target, delay=10_000),
    ):
        for _ in range(101):
            await asyncio.sleep(10e-5)
            my_hit_count += 1

    assert timer_hit_count >= 1
    assert my_hit_count >= 1


@pytest.mark.asyncio
async def test_event_listener_works():
    hit_count = 0
    end_evt = asyncio.Event()

    def _target():
        nonlocal hit_count
        hit_count += 1
        if hit_count >= 1000:
            end_evt.set()

    mock_timer = mock_async_timer.MockTimer(
        target=_target, delay=10_000, start=True, cancel_aws=[end_evt.wait()]
    )

    assert mock_timer.is_running()
    await mock_timer.wait(), "Wait for it to exit"

    assert hit_count >= 1000
    assert not mock_timer.is_running()
    assert mock_timer.pacemaker.sleep.await_count == (hit_count - 1)
