import asyncio
import itertools
import time
import typing

import asyncstdlib
import pytest

import async_timer


class TestAsyncFunc:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("stop_method_name", ["stop", "cancel"])
    async def test_api_call(self, count_fn, stop_method_name):
        vals = []
        timer = async_timer.Timer(10e-5, target=lambda: vals.append(count_fn()))
        timer.start()
        await asyncio.sleep(0.5)
        stop_method = getattr(timer, stop_method_name)
        await stop_method()
        after_cancel_vals = tuple(vals)
        await asyncio.sleep(0.1)
        after_sleep_vals = tuple(vals)

        assert len(vals) > 10, len(vals)
        assert tuple(vals) == after_cancel_vals
        assert tuple(vals) == after_sleep_vals

    @pytest.mark.asyncio
    async def test_join_non_running_timer(self, count_fn):
        timer = async_timer.Timer(10e-5, target=lambda: count_fn())
        with pytest.raises(asyncio.CancelledError):
            await timer.join()

    @pytest.mark.asyncio
    async def test_async_ctx_call(self, count_fn):
        vals = []
        async with async_timer.Timer(10e-5, target=lambda: vals.append(count_fn())):
            await asyncio.sleep(0.1)
        after_ctx_vals = tuple(vals)
        await asyncio.sleep(0.1)
        after_sleep_vals = tuple(vals)

        assert len(vals) > 10, len(vals)
        assert tuple(vals) == after_ctx_vals
        assert tuple(vals) == after_sleep_vals

    @pytest.mark.asyncio
    async def test_dual_start(self, count_fn):
        vals = []
        async with async_timer.Timer(
            10e-5, target=lambda: vals.append(count_fn())
        ) as timer:
            with pytest.raises(RuntimeError):
                timer.start()
            assert timer.is_running()
        assert not timer.is_running()

    @pytest.mark.asyncio
    async def test_timer_subscribe_async_fn(self, count_fn):
        vals = []
        async_for_vals = []

        async def _target() -> int:
            val = count_fn()
            await asyncio.sleep(10e-7)
            vals.append(val)
            return val

        async with async_timer.Timer(10e-5, target=_target) as timer:
            async for idx, val in asyncstdlib.enumerate(timer):
                async_for_vals.append(val)
                await asyncio.sleep(0.01)
                if idx > 11:
                    break

        assert len(vals) > 10
        assert len(async_for_vals) > 10
        assert set(async_for_vals).issubset(vals)

    @pytest.mark.asyncio
    async def test_timer_subscribe_sync_fn(self, count_fn):
        vals = []
        async_for_vals = []

        def _target() -> int:
            val = count_fn()
            vals.append(val)
            return val

        async with async_timer.Timer(10e-5, target=_target) as timer:
            async for idx, val in asyncstdlib.enumerate(timer):
                async_for_vals.append(val)
                await asyncio.sleep(0.01)
                if idx > 11:
                    break

        assert len(vals) > 10
        assert len(async_for_vals) > 10
        assert set(async_for_vals).issubset(vals)

    @pytest.mark.asyncio
    async def test_timer_subscribe_sync_generator(self, count_fn):
        vals = []
        async_for_vals = []
        term_evt = asyncio.Event()
        exc_evt = asyncio.Event()

        def _target():
            while True:
                el = count_fn
                vals.append(el)
                yield el

        async with async_timer.Timer(
            10e-5,
            target=_target,
            exc_cb=lambda *a, **kw: exc_evt.set(),
            cancel_cb=lambda *a, **kw: term_evt.set(),
        ) as timer:
            async for idx, val in asyncstdlib.enumerate(timer):
                async_for_vals.append(val)
                await asyncio.sleep(0.01)
                if idx > 11:
                    break

        assert len(vals) > 10
        assert len(async_for_vals) > 10
        assert set(async_for_vals).issubset(vals)
        # cancel_cb fires whenever the timer task ends — including the
        # `__aexit__`-driven cancel that runs when we break out of the loop.
        assert term_evt.is_set(), "cancel_cb fires on __aexit__ cleanup"
        assert not exc_evt.is_set(), "No exceptions"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("direct_generator", [True, False])
    async def test_timer_subscribe_async_generator(self, count_fn, direct_generator):
        vals = []
        async_for_vals = []

        async def _target():
            while True:
                el = count_fn
                vals.append(el)
                yield el
                await asyncio.sleep(10e-4)

        if direct_generator:
            constructor_target = _target()
        else:
            constructor_target = _target

        async with async_timer.Timer(10e-5, target=constructor_target) as timer:
            async for idx, val in asyncstdlib.enumerate(timer):
                async_for_vals.append(val)
                await asyncio.sleep(0.01)
                if idx > 11:
                    break

        assert len(vals) > 10
        assert len(async_for_vals) > 10
        assert set(async_for_vals).issubset(vals)

    @pytest.mark.asyncio
    async def test_timer_exception(self):
        term_evt = asyncio.Event()
        exc_evt = asyncio.Event()
        vals = []

        def _target() -> typing.Iterator[int]:
            for idx in itertools.count():
                vals.append(idx)
                yield idx
                if idx > 10:
                    raise NameError("Something went wrong")

        iter_vals = []
        async with async_timer.Timer(
            10e-5,
            target=_target,
            exc_cb=lambda *a, **kw: exc_evt.set(),
            cancel_cb=lambda *a, **kw: term_evt.set(),
        ) as timer:
            with pytest.raises(NameError) as err:
                async for val in timer:
                    iter_vals.append(val)
                    await asyncio.sleep(10e-10)
            assert "Something went wrong" in str(err)

        assert len(vals) > 10  # target ran past the raise threshold
        # iter_vals count is timing-dependent (FanoutRv drops intermediate
        # ticks if the consumer is mid-sleep when send_result fires); we
        # just assert the consumer observed *some* values and they all
        # came from the target.
        assert iter_vals, "consumer should have observed at least one tick"
        assert set(iter_vals).issubset(vals)
        assert term_evt.is_set(), "The generator did terminate"
        assert exc_evt.is_set(), "There was an exception"

    @pytest.mark.asyncio
    async def test_async_generator_exit(self):
        term_evt = asyncio.Event()
        exc_evt = asyncio.Event()

        async def _target():
            for idx in itertools.count():
                yield idx
                await asyncio.sleep(10e-10)
                if idx >= 20:
                    break

        iter_vals = []
        async with async_timer.Timer(
            10e-5,
            target=_target,
            exc_cb=lambda *a, **kw: exc_evt.set(),
            cancel_cb=lambda *a, **kw: term_evt.set(),
        ) as timer:
            async for val in timer:
                iter_vals.append(val)
                await asyncio.sleep(10e-10)

        # Consumer count is timing-dependent (FanoutRv drops intermediate
        # ticks if the consumer is mid-sleep when send_result fires).
        # Assert the invariants that don't depend on scheduler timing:
        #   - the consumer saw the *last* value (the generator finished),
        #   - every value came from the target's range,
        #   - and the timer task actually consumed all 21 generator yields.
        assert iter_vals
        assert set(iter_vals).issubset(range(21))
        assert 20 in iter_vals
        assert term_evt.is_set(), "The generator did terminate"
        assert not exc_evt.is_set(), "No exceptions"
        assert timer.hit_count == 21

    @pytest.mark.asyncio
    async def test_sync_generator_exit(self):
        def _target():
            for idx in itertools.count():
                yield idx
                if idx >= 20:
                    break

        iter_vals = []
        async with async_timer.Timer(10e-5, target=_target) as timer:
            async for val in timer:
                iter_vals.append(val)
                await asyncio.sleep(10e-20)

        # Consumer count is timing-dependent: FanoutRv drops intermediate
        # ticks if the consumer is mid-sleep when send_result fires. The
        # invariants we actually care about:
        #   - the consumer observed some values,
        #   - every observed value came from the target's range,
        #   - and the generator was exhausted (so the loop ended cleanly,
        #     reaching idx=20 in the target).
        assert iter_vals
        assert set(iter_vals).issubset(range(21))
        assert 20 in iter_vals, (
            "consumer should have seen the last value before the generator stopped"
        )

    @pytest.mark.asyncio
    async def test_wait_for_empty(self, count_fn):
        async with async_timer.Timer(10e-15, target=count_fn) as timer:
            rv = await timer.wait(timeout=0.5)
        assert rv is not None and rv > 3_000

    @pytest.mark.asyncio
    async def test_raises_timeout_on_long_wait(self, count_fn):
        async with async_timer.Timer(10_0000, target=count_fn) as timer:
            with pytest.raises(asyncio.TimeoutError):
                await timer.wait(hit_count=42, timeout=0.5)

    @pytest.mark.asyncio
    async def test_idle_wait_with_timeout_returns_last_rv_does_not_raise(
        self, count_fn
    ):
        """Documented behaviour: `wait(timeout=T)` with no hit_count/hits
        is an idle wait bounded by T — it returns the last seen tick
        result rather than raising TimeoutError.

        Callers that want a TimeoutError instead should use hits=1 or
        wrap with asyncio.wait_for(timer.wait(), timeout=T)."""
        async with async_timer.Timer(10e-15, target=count_fn) as timer:
            # No hit_count / hits — pure idle wait with a 0.1s bound.
            rv = await timer.wait(timeout=0.1)
        # We got a value (timer was ticking fast), not a TimeoutError.
        assert rv is not None

    @pytest.mark.asyncio
    async def test_idle_wait_with_timeout_on_slow_timer_returns_none(self):
        """With a slow timer that never ticks within the timeout window,
        idle-wait-with-timeout returns None (no last_rv yet) rather
        than raising."""
        async with async_timer.Timer(10_000, target=lambda: 42) as timer:
            await timer.join()  # consume the immediate first tick
            # Next tick won't fire for 10_000s. Idle-wait with 0.05s
            # timeout must return cleanly (None, since no tick happened
            # during the wait window).
            rv = await timer.wait(timeout=0.05)
        assert rv is None

    @pytest.mark.asyncio
    async def test_hits_one_with_timeout_raises_on_slow_timer(self):
        """Counterpart to the above: `hits=1, timeout=T` IS a "fail if
        no tick" call and raises TimeoutError. This is the pattern
        callers should use when they want to detect "timer is stuck"."""
        async with async_timer.Timer(10_000, target=lambda: 42) as timer:
            await timer.join()  # first tick
            with pytest.raises(asyncio.TimeoutError):
                await timer.wait(hits=1, timeout=0.05)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "initial_hits, wait_for_delta, exp_rv",
        (
            [0, -100, None],
            [0, 0, None],
            [10, 0, None],
            [10, 1, 10],
            [10, 5, 14],
            [10, 6, 15],
            [10, 1000, 1009],
        ),
    )
    async def test_wait_for_delta(self, count_fn, initial_hits, wait_for_delta, exp_rv):
        async with async_timer.Timer(10e-15, target=count_fn) as timer:
            for _ in range(initial_hits):
                await timer.join()
            timer_rv = await timer.wait(hits=wait_for_delta)
            assert timer_rv == exp_rv

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "initial_hits, wait_for_hits, exp_rv",
        (
            [0, -100, None],
            [0, 0, None],
            [10, 0, None],
            [0, 1, 0],
            [0, 100, 99],
            [70, 100, 99],
        ),
    )
    async def test_wait_for_absolute(
        self, count_fn, initial_hits, wait_for_hits, exp_rv
    ):
        async with async_timer.Timer(10e-15, target=count_fn) as timer:
            for _ in range(initial_hits):
                await timer.join()
            timer_rv = await timer.wait(hit_count=wait_for_hits)
            assert timer_rv == exp_rv

    def test_repr(self):
        timer = async_timer.Timer(10, target="Test Function")  # pyright: ignore[reportArgumentType]
        assert repr(timer).startswith(
            """<Timer target='Test Function' delay=10 hit_count=0 exception_callback="""
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("raise_exception", [True, False])
    @pytest.mark.parametrize("manual_terminate", [True, False])
    async def test_cancel_callback_is_always_called(
        self, raise_exception, manual_terminate
    ):
        term_evt = asyncio.Event()

        def _target():
            for idx in range(100):
                if raise_exception and idx > 50:
                    raise RuntimeError("I am dead now")
                yield idx

        async with async_timer.Timer(
            10e-5,
            target=_target,
            cancel_cb=lambda *a, **kw: term_evt.set(),
        ) as timer:
            try:
                async for val in timer:
                    if val > 55 and manual_terminate:
                        await timer.cancel()
                    await asyncio.sleep(10e-10)
            except RuntimeError:
                pass

        assert term_evt.is_set(), "The generator did terminate"

    @pytest.mark.asyncio
    async def test_change_delay(self, count_fn):
        async with async_timer.Timer(10e-15, target=count_fn) as timer:
            t1 = time.monotonic()
            await timer.join()
            await timer.join()
            t2 = time.monotonic()
            timer.set_delay(0.5)
            await timer.join()
            await timer.join()
            t3 = time.monotonic()
        assert t2 - t1 < 0.1
        assert 0.4 < t3 - t2 < 1
