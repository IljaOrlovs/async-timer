"""Tests for cross-thread access: read-only attrs + *_threadsafe methods."""

import asyncio
import threading
import time

import pytest

import async_timer


async def _run_in_thread(fn, *args, **kwargs):
    """Run a sync callable on a worker thread; await its result on the loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))


# ---------------------------------------------------------------------------
# A — read-only attrs are safe from any thread (single-attr reads are atomic
# under CPython's GIL).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_only_attrs_safe_from_thread():
    counter = [0]

    def _target():
        counter[0] += 1
        return counter[0]

    timer = async_timer.Timer(
        delay=10e-5, target=_target, name="thread-read", start=True
    )
    await timer.join()  # let one tick happen so last_result is set

    # Read from a thread; values should be sensible (no crash, no None).
    snap = await _run_in_thread(
        lambda: (
            timer.last_result,
            timer.last_tick_at,
            timer.hit_count,
            timer.is_running(),
            timer.delay,
            timer.name,
        )
    )
    last_result, last_tick_at, hit_count, is_running, delay, name = snap
    assert last_result is not None and last_result >= 1
    assert last_tick_at is not None
    assert hit_count >= 1
    assert is_running is True
    assert delay == 10e-5
    assert name == "thread-read"
    await timer.cancel()


@pytest.mark.asyncio
async def test_set_delay_atomic_from_thread():
    """set_delay is a single attribute write — safe from any thread."""
    timer = async_timer.Timer(delay=0.1, target=lambda: 1, start=True)
    await _run_in_thread(timer.set_delay, 0.5)
    assert timer.delay == 0.5
    await timer.cancel()


def test_set_delay_rejects_negative():
    timer = async_timer.Timer(delay=1.0, target=lambda: 1)
    with pytest.raises(ValueError, match="delay must be >= 0"):
        timer.set_delay(-1.0)


def test_construction_rejects_negative_delay():
    with pytest.raises(ValueError, match="delay must be >= 0"):
        async_timer.Timer(delay=-0.1, target=lambda: 1)


# ---------------------------------------------------------------------------
# B — cancel_threadsafe / trigger_threadsafe
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_threadsafe_stops_the_timer():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    await timer.join()
    assert timer.is_running()
    await _run_in_thread(timer.cancel_threadsafe)
    assert not timer.is_running()


@pytest.mark.asyncio
async def test_cancel_threadsafe_is_idempotent():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    await timer.join()
    await _run_in_thread(timer.cancel_threadsafe)
    # Second call: also via thread, must not raise (cancel() itself is idempotent).
    await _run_in_thread(timer.cancel_threadsafe)


@pytest.mark.asyncio
async def test_cancel_threadsafe_from_same_loop_raises():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    await timer.join()
    with pytest.raises(RuntimeError, match="own event loop thread"):
        timer.cancel_threadsafe()  # called from the loop thread
    await timer.cancel()


@pytest.mark.asyncio
async def test_cancel_threadsafe_before_start_raises():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1)
    # Not started — no loop bound.
    with pytest.raises(RuntimeError, match="not been started"):
        await _run_in_thread(timer.cancel_threadsafe)


@pytest.mark.asyncio
async def test_cancel_threadsafe_after_loop_closed_raises():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    await timer.cancel()
    # Simulate loop closure as the timer would see it from outside.
    # We can't actually close the running loop, so spoof the check:
    closed_loop = asyncio.new_event_loop()
    closed_loop.close()
    original_loop = timer._loop
    timer._loop = closed_loop
    try:
        with pytest.raises(RuntimeError, match="loop is closed"):
            await _run_in_thread(timer.cancel_threadsafe)
    finally:
        timer._loop = original_loop


@pytest.mark.asyncio
async def test_trigger_threadsafe_returns_value():
    counter = [0]

    def _target():
        counter[0] += 1
        return counter[0]

    timer = async_timer.Timer(delay=10.0, target=_target, start=True)
    await timer.join()  # consume immediate first tick
    before = counter[0]
    rv = await _run_in_thread(timer.trigger_threadsafe)
    assert rv == before + 1
    assert counter[0] == before + 1
    await timer.cancel()


@pytest.mark.asyncio
async def test_trigger_threadsafe_same_loop_raises():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    await timer.join()
    with pytest.raises(RuntimeError, match="own event loop thread"):
        timer.trigger_threadsafe()
    await timer.cancel()


@pytest.mark.asyncio
async def test_trigger_threadsafe_before_start_raises():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1)
    with pytest.raises(RuntimeError, match="not been started"):
        await _run_in_thread(timer.trigger_threadsafe)


@pytest.mark.asyncio
async def test_trigger_threadsafe_timeout_raises_timeout_error():
    """If the triggered tick doesn't arrive within `timeout`, raise
    TimeoutError with a clear message."""

    async def _slow_target():
        await asyncio.sleep(10.0)

    timer = async_timer.Timer(delay=10.0, target=_slow_target, start=True)
    await asyncio.sleep(0.01)  # let the first tick start (but not finish)
    with pytest.raises(TimeoutError, match="did not arrive within"):
        await _run_in_thread(timer.trigger_threadsafe, 0.05)
    await timer.cancel()


@pytest.mark.asyncio
async def test_cancel_threadsafe_timeout_raises_timeout_error():
    """A cancel that takes longer than the timeout raises TimeoutError."""

    async def _slow_target():
        await asyncio.sleep(10.0)

    # cancel_cb that itself sleeps blocks the cancellation path.
    def _slow_cb(*_a, **_kw):
        time.sleep(0.5)  # blocks the loop thread

    timer = async_timer.Timer(
        delay=10e-5,
        target=_slow_target,
        cancel_cb=_slow_cb,
        start=True,
    )
    await asyncio.sleep(0.01)
    with pytest.raises(TimeoutError, match="did not complete within"):
        await _run_in_thread(timer.cancel_threadsafe, 0.05)
    # Sleep a moment so the still-running cancel can finish cleanly.
    await asyncio.sleep(0.6)


# ---------------------------------------------------------------------------
# Subscription.close_threadsafe
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscription_close_threadsafe_from_thread():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    feed = timer.subscribe()
    await feed.__anext__()
    assert feed in timer._subscriptions

    # Close from a worker thread.
    await _run_in_thread(feed.close_threadsafe)

    # Give the loop one tick to process the call_soon_threadsafe.
    await asyncio.sleep(0)
    assert feed not in timer._subscriptions
    with pytest.raises(StopAsyncIteration):
        await feed.__anext__()
    await timer.cancel()


@pytest.mark.asyncio
async def test_subscription_close_threadsafe_from_same_loop_closes_directly():
    """When called from the loop's own thread, falls through to close()."""
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    feed = timer.subscribe()
    feed.close_threadsafe()  # called from loop thread
    assert feed not in timer._subscriptions
    await timer.cancel()


@pytest.mark.asyncio
async def test_subscription_close_threadsafe_without_loop_falls_back():
    """A subscription whose timer was never started has _loop=None;
    close_threadsafe must just close it directly without raising."""
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1)  # not started
    feed = timer.subscribe()
    assert feed._loop is None
    feed.close_threadsafe()  # must not raise
    assert feed._closed


@pytest.mark.asyncio
async def test_subscribe_before_start_gets_loop_bound_on_start():
    """Subscriptions created before Timer.start() must have their _loop
    populated when start() runs, so close_threadsafe works correctly."""
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1)
    feed = timer.subscribe()
    assert feed._loop is None  # timer not started yet
    timer.start()
    try:
        assert feed._loop is asyncio.get_running_loop()
    finally:
        await timer.cancel()


# ---------------------------------------------------------------------------
# Stress: many threads hammering a single Timer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_many_threads_reading_attrs_does_not_corrupt():
    """N threads concurrently reading state attrs — sanity check that
    nothing crashes and reads always look coherent."""
    counter = [0]

    def _target():
        counter[0] += 1
        return counter[0]

    timer = async_timer.Timer(delay=10e-5, target=_target, start=True)
    await timer.join()

    stop_evt = threading.Event()

    def _reader_loop():
        while not stop_evt.is_set():
            _ = timer.last_result
            _ = timer.hit_count
            _ = timer.delay
            _ = timer.is_running()

    workers = [threading.Thread(target=_reader_loop) for _ in range(8)]
    for w in workers:
        w.start()
    await asyncio.sleep(0.05)  # let producers and consumers race
    stop_evt.set()
    for w in workers:
        w.join(timeout=2.0)
        assert not w.is_alive()
    await timer.cancel()
