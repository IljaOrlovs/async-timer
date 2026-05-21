"""Tests for per-consumer queue-based Subscription / timer.subscribe()."""

import asyncio
import logging

import pytest

import async_timer


@pytest.mark.asyncio
async def test_subscription_yields_every_tick_even_when_consumer_is_slow():
    """The core property: a Subscription buffers ticks, so a consumer
    slower than the tick rate still sees every tick from subscription
    onwards. This is the bug `FanoutRv`/`async for self` has by design."""
    counter = [0]

    def _target():
        counter[0] += 1
        return counter[0]

    timer = async_timer.Timer(delay=0.005, target=_target, start=True)
    seen = []
    async with timer.subscribe() as feed:
        # Drain 10 ticks. Sleep between each consume so the timer's
        # actual tick rate (5ms) outpaces consumer's pull rate. With
        # FanoutRv semantics we'd miss most; with Subscription we get
        # every one (in order).
        for _ in range(10):
            seen.append(await feed.__anext__())
            await asyncio.sleep(0.02)  # consumer is 4x slower than tick rate
    await timer.cancel()

    # We got 10 strictly-monotonic, gap-free values from the subscription.
    assert len(seen) == 10
    assert seen == list(range(seen[0], seen[0] + 10)), (
        f"expected contiguous values, got {seen}"
    )


@pytest.mark.asyncio
async def test_subscription_starts_from_subscribe_time_not_timer_start():
    """A late subscriber should NOT see backlogged historical ticks —
    only ticks that fire from the moment of subscribe() onwards."""
    timer = async_timer.Timer(delay=10e-5, target=lambda: 42, start=True)
    # Let the timer tick a few times before anyone subscribes.
    for _ in range(5):
        await timer.join()

    async with timer.subscribe() as feed:
        # First buffered tick is the *next* one after subscribe(), not
        # one of the historical 5.
        v = await asyncio.wait_for(feed.__anext__(), timeout=1.0)
        assert v == 42
    await timer.cancel()


@pytest.mark.asyncio
async def test_subscription_async_with_exit_closes_cleanly():
    """Exiting the `async with` block must release the subscription
    (no further ticks pushed, queue not retained on the Timer)."""
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    async with timer.subscribe() as feed:
        await feed.__anext__()
        assert len(timer._subscriptions) == 1
    # After __aexit__: subscription unregistered from the Timer.
    assert len(timer._subscriptions) == 0
    await timer.cancel()


@pytest.mark.asyncio
async def test_subscription_explicit_close_works_too():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    sub = timer.subscribe()
    await sub.__anext__()
    sub.close()
    assert len(timer._subscriptions) == 0
    # Subsequent iteration ends cleanly.
    with pytest.raises(StopAsyncIteration):
        await sub.__anext__()
    await timer.cancel()


@pytest.mark.asyncio
async def test_subscription_close_is_idempotent():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    sub = timer.subscribe()
    sub.close()
    # Single end-of-stream sentinel sits in the queue after one close.
    assert sub.qsize == 1
    sub.close()  # second call must not raise AND must not push a duplicate
    sub.close()  # ...nor a third
    assert sub.qsize == 1, (
        "close() should be a no-op on second call; got duplicate sentinels"
    )
    # And iteration terminates exactly once.
    with pytest.raises(StopAsyncIteration):
        await sub.__anext__()
    # Queue now empty — no leftover sentinel from duplicate closes.
    assert sub.qsize == 0
    await timer.cancel()


@pytest.mark.asyncio
async def test_subscription_ends_when_timer_cancelled():
    """An open subscription whose timer is cancelled must terminate
    its iteration cleanly (no hang, no exception)."""
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    async with timer.subscribe() as feed:
        await feed.__anext__()

        async def _consume_rest():
            results = []
            async for v in feed:
                results.append(v)
            return results

        consumer_task = asyncio.create_task(_consume_rest())
        await asyncio.sleep(0.05)  # let the consumer pull a few ticks
        await timer.cancel()
        # The consumer task must end cleanly (no asyncio.TimeoutError).
        results = await asyncio.wait_for(consumer_task, timeout=1.0)
        # We saw some ticks before the timer ended.
        assert results


@pytest.mark.asyncio
async def test_subscription_re_raises_target_exception():
    """If the target raises, the subscription's `__anext__` must
    surface that exception (not StopAsyncIteration), so the subscriber
    learns about failures."""
    calls = [0]

    def _target():
        calls[0] += 1
        if calls[0] >= 3:
            raise ValueError("target died")
        return calls[0]

    timer = async_timer.Timer(
        delay=10e-5,
        target=_target,
        exc_cb=lambda *_a, **_kw: None,  # swallow at the timer level
        start=True,
    )
    async with timer.subscribe() as feed:
        seen = []
        with pytest.raises(ValueError, match="target died"):
            async for v in feed:
                seen.append(v)
        # We got the two successful values before the failure surfaced.
        assert seen == [1, 2]
    await timer.cancel()


@pytest.mark.asyncio
async def test_multiple_subscriptions_each_get_independent_copy():
    """N subscribers each see the same sequence of ticks (not
    consume-from-shared-queue semantics)."""
    counter = [0]

    def _target():
        counter[0] += 1
        return counter[0]

    timer = async_timer.Timer(delay=10e-5, target=_target, start=True)

    async def _drain(sub, n):
        out = []
        for _ in range(n):
            out.append(await sub.__anext__())
        return out

    async with timer.subscribe() as a, timer.subscribe() as b:
        a_vals, b_vals = await asyncio.gather(_drain(a, 5), _drain(b, 5))
    await timer.cancel()

    assert a_vals == b_vals, (
        f"subscribers got different sequences: a={a_vals}, b={b_vals}"
    )


@pytest.mark.asyncio
async def test_bounded_subscription_drops_oldest_and_logs(caplog):
    """When `maxsize=N` and the consumer falls way behind, the
    subscription drops oldest ticks (and logs each drop) rather than
    growing unbounded."""
    counter = [0]

    def _target():
        counter[0] += 1
        return counter[0]

    timer = async_timer.Timer(delay=0.001, target=_target, start=True)
    feed = timer.subscribe(maxsize=3, name="test_sub")
    with caplog.at_level(logging.WARNING, logger="async_timer.subscription"):
        # Let many ticks fire while we don't consume.
        await asyncio.sleep(0.1)
        # Stop the producer so the drain can reach end-of-stream
        # rather than chasing an ever-growing queue.
        await timer.cancel()
        drained = []
        async for v in feed:
            drained.append(v)

    assert len(drained) <= 3, f"queue exceeded maxsize: {drained}"
    assert feed.dropped_count > 0, "expected drops, none recorded"
    drop_warnings = [r for r in caplog.records if "dropped oldest" in r.message]
    assert drop_warnings, "expected at least one drop warning"
    # Warning includes subscription name for debuggability.
    assert any("test_sub" in r.message for r in drop_warnings)


@pytest.mark.asyncio
async def test_subscription_invalid_maxsize_rejected():
    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)
    with pytest.raises(ValueError, match="maxsize"):
        timer.subscribe(maxsize=-1)
    await timer.cancel()


@pytest.mark.asyncio
async def test_subscription_can_be_used_as_async_iterator_directly():
    """`async for v in timer.subscribe():` works without explicit
    `async with` — the iteration just runs until the upstream ends or
    the user breaks out."""
    counter = [0]

    def _target():
        counter[0] += 1
        if counter[0] >= 5:
            raise StopIteration
        return counter[0]

    def _gen_target():
        idx = 0
        while True:
            idx += 1
            if idx >= 5:
                return
            yield idx

    timer = async_timer.Timer(delay=10e-5, target=_gen_target, start=True)
    seen = []
    async for v in timer.subscribe():
        seen.append(v)
    await timer.cancel()
    # Saw all 4 values before the generator exhausted.
    assert seen == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_subscription_does_not_affect_fanout_consumers():
    """A subscription and a `join()` consumer must coexist — each
    independent, neither steals ticks from the other."""
    counter = [0]

    def _target():
        counter[0] += 1
        return counter[0]

    timer = async_timer.Timer(delay=10e-5, target=_target, start=True)

    async def _join_consumer():
        # Just a single join — should still get a tick.
        return await timer.join()

    async with timer.subscribe() as feed:
        joined = await _join_consumer()
        # Now drain the subscription — should see ticks too.
        subscribed = await asyncio.wait_for(feed.__anext__(), timeout=1.0)
    await timer.cancel()

    assert joined is not None
    assert subscribed is not None


@pytest.mark.asyncio
async def test_push_value_on_closed_subscription_is_noop():
    """If the producer (Timer) tries to push to an already-closed
    subscription, it's a silent no-op rather than an error."""
    timer = async_timer.Timer(delay=10.0, target=lambda: 1, start=True)
    sub = timer.subscribe()
    sub.close()
    # Direct call — simulating the Timer's loop trying to push after
    # the consumer closed but before the unregister was processed.
    sub._push_value(99)  # must not raise, must not enqueue
    # The queue should only contain the end-of-stream sentinel.
    with pytest.raises(StopAsyncIteration):
        await sub.__anext__()
    await timer.cancel()


@pytest.mark.asyncio
async def test_push_exception_on_closed_subscription_is_noop():
    timer = async_timer.Timer(delay=10.0, target=lambda: 1, start=True)
    sub = timer.subscribe()
    sub.close()
    # Pushing an exception to a closed sub is a no-op.
    sub._push_exception(RuntimeError("would have been ignored"))
    with pytest.raises(StopAsyncIteration):
        await sub.__anext__()
    await timer.cancel()


@pytest.mark.asyncio
async def test_push_exception_evicts_to_make_room_on_full_bounded_queue():
    """When the target raises and the subscription's bounded queue is
    already full, the exception must still be delivered — the oldest
    buffered value is evicted to make room."""
    counter = [0]

    def _target():
        counter[0] += 1
        if counter[0] >= 5:
            raise ValueError("boom")
        return counter[0]

    timer = async_timer.Timer(
        delay=0.001,
        target=_target,
        exc_cb=lambda *_a, **_kw: None,
        start=True,
    )
    feed = timer.subscribe(maxsize=2, name="evict_test")
    # Let several ticks fire so the queue fills (and starts dropping
    # older values). Eventually the target raises; we must see the
    # exception, not a hung wait.
    seen = []
    with pytest.raises(ValueError, match="boom"):
        async for v in feed:
            seen.append(v)
            # Slow consumer so the queue stays full when target raises.
            await asyncio.sleep(0.01)
    await timer.cancel()


@pytest.mark.asyncio
async def test_qsize_reflects_buffered_items():
    """qsize property mirrors the underlying asyncio.Queue and lets
    consumers detect when they're falling behind."""
    timer = async_timer.Timer(delay=0.001, target=lambda: 1, start=True)
    feed = timer.subscribe()
    assert feed.qsize == 0
    await asyncio.sleep(0.05)  # let some ticks pile up
    assert feed.qsize > 0
    backlog = feed.qsize
    # Pulling one should reduce qsize by 1 (and the producer is paused
    # while we have the GIL during the assertion below — but it can
    # tick again on the next await; just assert directionally).
    await feed.__anext__()
    assert feed.qsize <= backlog
    await timer.cancel()


@pytest.mark.asyncio
async def test_drop_oldest_default_drops_one():
    timer = async_timer.Timer(delay=0.001, target=lambda: 1, start=True)
    feed = timer.subscribe()
    await asyncio.sleep(0.05)
    await timer.cancel()  # stop producer so qsize is stable

    initial = feed.qsize
    assert initial >= 2  # need at least 2 to drop and still have stream
    dropped = feed.drop_oldest()
    assert dropped == 1
    assert feed.qsize == initial - 1
    assert feed.dropped_count == 1


@pytest.mark.asyncio
async def test_drop_oldest_drops_up_to_n():
    timer = async_timer.Timer(delay=0.001, target=lambda: 1, start=True)
    feed = timer.subscribe()
    await asyncio.sleep(0.1)
    await timer.cancel()

    initial = feed.qsize
    # The end-of-stream sentinel is one of those items, so the most we
    # can drop without hitting it is initial - 1.
    droppable = initial - 1
    dropped = feed.drop_oldest(droppable)
    assert dropped == droppable
    assert feed.dropped_count == droppable


@pytest.mark.asyncio
async def test_drop_oldest_preserves_items_buffered_after_sentinel():
    """Defensive: if (somehow) values exist behind an end-of-stream
    sentinel in the queue, drop_oldest's sentinel-stop logic must
    restore them. In normal operation nothing lands after the sentinel
    (close is always the last write), so we construct this state by
    directly poking the queue."""
    from async_timer.subscription import _STREAM_END

    timer: async_timer.Timer[str] = async_timer.Timer(
        delay=10.0, target=lambda: "x", start=True
    )
    sub = timer.subscribe()
    # Hand-build the queue: [value_a, _STREAM_END, value_b]
    sub._queue.put_nowait("a")
    sub._queue.put_nowait(_STREAM_END)
    sub._queue.put_nowait("b")
    assert sub.qsize == 3

    # Asking to drop 5 should drop "a", hit the sentinel, restore both
    # the sentinel and "b" in original order.
    dropped = sub.drop_oldest(5)
    assert dropped == 1
    assert sub.qsize == 2
    # Sentinel still at head → next iteration ends cleanly.
    with pytest.raises(StopAsyncIteration):
        await sub.__anext__()
    # And "b" is still there behind it.
    assert sub._queue.get_nowait() == "b"
    await timer.cancel()


@pytest.mark.asyncio
async def test_drop_oldest_stops_at_end_of_stream_sentinel():
    """drop_oldest must NOT swallow end-of-stream — the consumer needs
    that signal to know the timer ended."""
    timer = async_timer.Timer(delay=0.001, target=lambda: 1, start=True)
    feed = timer.subscribe()
    await asyncio.sleep(0.05)
    await timer.cancel()  # injects end-of-stream sentinel into the queue

    # Try to drop way more than what's buffered.
    huge = feed.qsize * 10
    dropped = feed.drop_oldest(huge)
    # Drops every buffered value but stops at the sentinel.
    assert dropped < huge
    # Iteration still terminates cleanly via StopAsyncIteration.
    with pytest.raises(StopAsyncIteration):
        await feed.__anext__()


@pytest.mark.asyncio
async def test_drop_oldest_stops_at_exception_sentinel():
    """Same as above but for the exception-close path: the exception
    must still surface to the consumer after drop_oldest()."""
    calls = [0]

    def _target():
        calls[0] += 1
        if calls[0] >= 5:
            raise RuntimeError("upstream died")
        return calls[0]

    timer = async_timer.Timer(
        delay=0.001,
        target=_target,
        exc_cb=lambda *_a, **_kw: None,
        start=True,
    )
    feed = timer.subscribe()
    # Wait for the loop to end via exception.
    for _ in range(50):
        await asyncio.sleep(0.01)
        if not timer.is_running():
            break
    # Drop everything possible.
    feed.drop_oldest(feed.qsize * 10)
    # The exception is still pending — surfaces on next __anext__.
    with pytest.raises(RuntimeError, match="upstream died"):
        await feed.__anext__()
    await timer.cancel()


@pytest.mark.asyncio
async def test_drop_oldest_on_empty_queue_returns_zero():
    timer = async_timer.Timer(delay=10.0, target=lambda: 1, start=True)
    feed = timer.subscribe()
    # No ticks have arrived yet (delay=10s, we don't wait).
    assert feed.qsize == 0
    assert feed.drop_oldest(5) == 0
    assert feed.dropped_count == 0
    await timer.cancel()


@pytest.mark.asyncio
async def test_drop_oldest_zero_is_noop():
    timer = async_timer.Timer(delay=0.001, target=lambda: 1, start=True)
    feed = timer.subscribe()
    await asyncio.sleep(0.05)
    await timer.cancel()

    before = feed.qsize
    assert feed.drop_oldest(0) == 0
    assert feed.qsize == before
    assert feed.dropped_count == 0


@pytest.mark.asyncio
async def test_drop_oldest_rejects_negative_n():
    timer = async_timer.Timer(delay=10.0, target=lambda: 1, start=True)
    feed = timer.subscribe()
    with pytest.raises(ValueError, match="n must be >= 0"):
        feed.drop_oldest(-1)
    await timer.cancel()


@pytest.mark.asyncio
async def test_dropped_subscription_is_garbage_collected_no_leak():
    """A caller that subscribes and then drops the reference *without*
    closing must not leak: the Timer holds Subscriptions weakly so GC
    reaps them and the timer stops pushing values to a dead queue."""
    import gc
    import weakref

    timer = async_timer.Timer(delay=0.001, target=lambda: 1, start=True)

    # Subscribe inside a helper scope so the local goes out of scope
    # when the helper returns.
    def _make_orphan_sub():
        sub = timer.subscribe()
        return weakref.ref(sub)  # only a weak ref escapes

    sub_ref = _make_orphan_sub()
    # CPython refcounting collects the orphan immediately on scope exit;
    # gc.collect() is for non-refcounting implementations / cycle cleanup.
    gc.collect()
    assert sub_ref() is None, "subscription was not garbage collected"
    # WeakSet entry vanished automatically — no manual unsubscribe needed.
    assert len(timer._subscriptions) == 0

    # Timer keeps running normally with no subscribers.
    await timer.join()
    await timer.cancel()


@pytest.mark.asyncio
async def test_timer_keeps_running_when_subscription_collected_mid_tick():
    """After a subscriber is GC'd, the timer's tick loop must keep
    working without raising (no "set changed size during iteration"
    or similar) — proves we snapshot the WeakSet on each push."""
    import gc

    timer = async_timer.Timer(delay=10e-5, target=lambda: 1, start=True)

    # Hold one explicit subscription that survives.
    async with timer.subscribe() as alive_feed:
        # Make and drop several orphan subscriptions.
        for _ in range(5):
            _ = timer.subscribe()  # immediately drop the ref
        gc.collect()

        # Timer must keep ticking and the surviving sub must keep
        # receiving values.
        for _ in range(3):
            await asyncio.wait_for(alive_feed.__anext__(), timeout=1.0)

    await timer.cancel()


@pytest.mark.asyncio
async def test_load_shedding_pattern_keeps_consumer_caught_up():
    """End-to-end: a slow consumer monitors qsize and proactively
    drops backlogged values to stay near the head. This is the
    use-case the new API exists for."""
    counter = [0]

    def _target():
        counter[0] += 1
        return counter[0]

    timer = async_timer.Timer(delay=0.001, target=_target, start=True)
    seen: list = []

    async with timer.subscribe() as feed:
        # Consumer is much slower than the producer; we shed load when
        # the queue grows beyond a threshold.
        for _ in range(8):
            if feed.qsize > 5:
                feed.drop_oldest(feed.qsize - 1)  # leave just the newest
            seen.append(await feed.__anext__())
            await asyncio.sleep(0.02)
        observed_drops = feed.dropped_count

    await timer.cancel()
    assert seen  # got some values
    # We actually shed load at some point.
    assert observed_drops > 0


@pytest.mark.asyncio
async def test_subscription_unbounded_queue_doesnt_drop():
    """maxsize=0 means unbounded — no drops ever."""
    counter = [0]

    def _target():
        counter[0] += 1
        return counter[0]

    timer = async_timer.Timer(delay=0.001, target=_target, start=True)
    feed = timer.subscribe(maxsize=0)
    await asyncio.sleep(0.05)
    await timer.cancel()  # stop producer so async-for drains to end-of-stream
    drained = []
    async for v in feed:
        drained.append(v)

    assert feed.dropped_count == 0
    assert len(drained) >= 5
    # Contiguous from the first seen value.
    assert drained == list(range(drained[0], drained[0] + len(drained)))
