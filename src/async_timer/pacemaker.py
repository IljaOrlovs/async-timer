import asyncio
import logging
import random
import time
import typing

logger = logging.getLogger(__name__)

PacemakerMode = typing.Literal["fixed_delay", "fixed_rate"]


class TimerPacemaker:
    """Async-iterable that yields once per `delay` seconds.

    Modes:
      * `"fixed_delay"` (default): each tick fires `delay` seconds after
        the previous tick *completes*. Long-running consumers cause the
        schedule to drift.
      * `"fixed_rate"`: ticks are anchored to a wall-clock schedule of
        `t0 + n*delay`. If processing of a tick takes longer than
        `delay`, the missed slot(s) are skipped and a warning is logged.

    Optional `initial_delay` adds a leading sleep before the very first
    tick. Optional `jitter` perturbs each per-tick sleep by ±jitter as
    a fraction of `delay` (e.g. 0.1 = ±10%) to avoid thundering-herd
    in distributed deployments.

    Iteration ends — `StopAsyncIteration` is raised — when either
    `stop()` is called explicitly or one of the awaitables registered
    via `stop_on()` resolves.

    `_reset()` is provided so a single instance can be re-used across
    `Timer.start()` / `Timer.cancel()` cycles.
    """

    delay: float
    mode: PacemakerMode
    initial_delay: float
    jitter: float
    _first_iter: bool = True
    _running: bool = True
    _cancel_futs: typing.List[asyncio.futures.Future]
    _cancel_evt: asyncio.Event
    _trigger_evt: asyncio.Event
    _start_time: typing.Optional[float] = None  # wall-clock anchor for fixed_rate
    # 0-indexed position of the most recently emitted tick. Tick N is
    # scheduled to fire at _start_time + N*delay (so tick 0 = the very
    # first emission, at _start_time exactly).
    _tick_number: int = 0

    def __init__(
        self,
        delay: float,
        *,
        mode: PacemakerMode = "fixed_delay",
        initial_delay: float = 0.0,
        jitter: float = 0.0,
    ):
        if jitter < 0 or jitter > 1:
            raise ValueError(f"jitter must be in [0, 1], got {jitter!r}")
        if initial_delay < 0:
            raise ValueError(f"initial_delay must be >= 0, got {initial_delay!r}")
        if mode not in ("fixed_delay", "fixed_rate"):
            raise ValueError(
                f"mode must be 'fixed_delay' or 'fixed_rate', got {mode!r}"
            )
        self.delay = delay
        self.mode = mode
        self.initial_delay = initial_delay
        self.jitter = jitter
        self._cancel_futs = []
        self._cancel_evt = asyncio.Event()
        self._trigger_evt = asyncio.Event()

    def stop_on(self, aws: typing.Sequence[typing.Awaitable]):
        """Register awaitables that, when any one resolves or raises,
        will stop this pacemaker.

        Requires a running event loop (uses `asyncio.ensure_future`).
        The wrapped futures are tracked on `_cancel_futs` and cancelled
        on `stop()`. Awaitables passed here are single-shot — they are
        cleared on stop and not re-armed by `_reset()`.
        """
        for el in aws:
            fut = asyncio.ensure_future(el)
            fut.add_done_callback(self._on_cancel_fut_done)
            self._cancel_futs.append(fut)

    def _on_cancel_fut_done(self, fut: asyncio.Future):
        # Consume any exception so asyncio does not emit
        # "exception was never retrieved" warnings — but surface it
        # via logging so it isn't silently swallowed.
        if not fut.cancelled():
            exc = fut.exception()
            if exc is not None:
                logger.warning(
                    "cancel_aws awaitable %r raised %s; treating as stop signal",
                    fut,
                    exc,
                    exc_info=exc,
                )
        self.stop()

    def stop(self):
        """Stop the iterator."""
        if not self._running:
            return
        self._running = False
        self._cancel_evt.set()
        for fut in self._cancel_futs:
            if not fut.done():
                fut.cancel()
        self._cancel_futs.clear()

    def trigger(self):
        """Wake any in-progress sleep so the next tick fires immediately.

        Has no effect if the pacemaker is not currently sleeping. In
        `fixed_rate` mode, the wall-clock schedule is re-anchored to
        the moment of the triggered tick, so subsequent ticks fire one
        `delay` apart from the trigger rather than catching up to the
        original schedule.
        """
        self._trigger_evt.set()

    def _reset(self):
        """Reset state so the iterator can be re-used after stop()."""
        self._first_iter = True
        self._running = True
        self._start_time = None
        self._tick_number = 0
        if self._cancel_evt.is_set():
            self._cancel_evt = asyncio.Event()
        if self._trigger_evt.is_set():
            self._trigger_evt = asyncio.Event()

    def __aiter__(self):
        """Return the iterator (this object is its own iterator)."""
        return self

    async def __anext__(self):
        if not self._running:
            raise StopAsyncIteration()
        if self._first_iter:
            self._first_iter = False
            if self.initial_delay > 0:
                try:
                    await self._try_wait(self.initial_delay)
                except StopAsyncIteration:
                    self.stop()
                    raise
            # Anchor the wall-clock schedule at the moment of the
            # first emitted tick (tick 0).
            self._start_time = time.monotonic()
            self._tick_number = 0
            return None

        if self.mode == "fixed_rate":
            wait_for = self._compute_fixed_rate_wait()
        else:
            wait_for = self._apply_jitter(self.delay)
            self._tick_number += 1

        if wait_for <= 0:
            # Triggered tick or back-to-back in fixed_rate; still yield
            # control to other tasks at least once to avoid starvation.
            await asyncio.sleep(0)
            return None
        try:
            was_triggered = await self._try_wait(wait_for)
        except StopAsyncIteration:
            self.stop()
            raise
        if was_triggered and self.mode == "fixed_rate":
            # Re-anchor the fixed-rate schedule to the moment of this
            # triggered tick. Without this, the next iteration would
            # compute its slot from the original anchor and either fire
            # too soon (catching up) or warn about "falling behind".
            self._start_time = time.monotonic()
            self._tick_number = 0
        return None

    def _compute_fixed_rate_wait(self) -> float:
        """Time to sleep before the next fixed-rate slot.

        If we've already missed the next slot (typically because the
        consumer's processing of the previous tick took longer than
        `delay`), log a warning and advance `_tick_number` past every
        slot that is in the past, so the *next* emitted tick lines up
        with a still-future slot. Returns the wait until that slot.
        """
        assert self._start_time is not None
        now = time.monotonic()
        # Next tick to emit is index `_tick_number + 1`.
        target_index = self._tick_number + 1
        next_tick_at = self._start_time + target_index * self.delay
        skipped = 0
        while next_tick_at <= now:
            skipped += 1
            target_index += 1
            next_tick_at = self._start_time + target_index * self.delay
        if skipped:
            logger.warning(
                "fixed_rate pacemaker fell behind: skipping %d tick(s) "
                "(delay=%.3fs, behind by %.3fs)",
                skipped,
                self.delay,
                now - (next_tick_at - skipped * self.delay),
            )
        self._tick_number = target_index
        wait_for = next_tick_at - now
        # Apply jitter within the *remaining* wait (not the full delay).
        # Capping at `wait_for` ensures jitter never pushes the tick
        # past the scheduled slot boundary, which would falsely register
        # as "fell behind" on the next iteration.
        return self._apply_jitter(wait_for, cap=wait_for)

    def _apply_jitter(self, base: float, cap: typing.Optional[float] = None) -> float:
        if self.jitter == 0:
            return base
        delta = base * self.jitter * random.uniform(-1, 1)
        out = base + delta
        if out < 0:
            out = 0.0
        if cap is not None and out > cap:
            out = cap
        return out

    async def _try_wait(self, delay: float) -> bool:
        """Wait for `delay`, or until cancel/trigger fires.

        Raises `StopAsyncIteration` if cancel was signalled.
        Returns `True` if a trigger cut the wait short, `False` on a
        normal timeout.
        """
        cancel_task = asyncio.ensure_future(self._cancel_evt.wait())
        trigger_task = asyncio.ensure_future(self._trigger_evt.wait())
        try:
            done, _pending = await asyncio.wait(
                {cancel_task, trigger_task},
                timeout=delay,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for t in (cancel_task, trigger_task):
                if not t.done():
                    t.cancel()
        if self._cancel_evt.is_set():
            raise StopAsyncIteration()
        if self._trigger_evt.is_set():
            # Consume the trigger so the next sleep is normal again.
            self._trigger_evt.clear()
            return True
        return False
