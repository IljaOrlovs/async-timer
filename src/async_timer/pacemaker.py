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
      * `"fixed_delay"`: next tick fires `delay` after the previous
        one *finishes* (schedule drifts under slow consumers).
      * `"fixed_rate"`: ticks anchored to `t0 + n*delay`; missed
        slots are skipped and a warning is logged.

    `initial_delay` adds a leading sleep before the first tick.
    `jitter` (fraction in [0, 1]) perturbs each per-tick sleep to
    avoid thundering-herd. Iteration ends on `stop()` or when any
    `stop_on()` awaitable resolves. `_reset()` lets one instance be
    reused across Timer start/cancel cycles.
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
    _start_time: typing.Optional[float] = None  # fixed_rate anchor
    # Index of most recently emitted tick (0-based). Tick N is scheduled
    # for _start_time + N*delay.
    _tick_number: int = 0

    def __init__(
        self,
        delay: float,
        *,
        mode: PacemakerMode = "fixed_delay",
        initial_delay: float = 0.0,
        jitter: float = 0.0,
    ):
        if delay < 0:
            raise ValueError(f"delay must be >= 0, got {delay!r}")
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
        """Stop when any registered awaitable resolves (or raises).

        Requires a running loop. Single-shot — cleared on `stop()`,
        not re-armed by `_reset()`.
        """
        for el in aws:
            fut = asyncio.ensure_future(el)
            fut.add_done_callback(self._on_cancel_fut_done)
            self._cancel_futs.append(fut)

    def _on_cancel_fut_done(self, fut: asyncio.Future):
        # Consume the exception (silences asyncio warning) and log it.
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
        """Wake an in-progress sleep so the next tick fires now.

        No-op if not sleeping. In `fixed_rate`, re-anchors the
        schedule from this moment (no catch-up).
        """
        self._trigger_evt.set()

    def _reset(self):
        """Reset for re-iteration after stop()."""
        self._first_iter = True
        self._running = True
        self._start_time = None
        self._tick_number = 0
        if self._cancel_evt.is_set():
            self._cancel_evt = asyncio.Event()
        if self._trigger_evt.is_set():
            self._trigger_evt = asyncio.Event()

    def __aiter__(self):
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
            # Anchor at the first emission (tick 0).
            self._start_time = time.monotonic()
            self._tick_number = 0
            return None

        if self.mode == "fixed_rate":
            wait_for = self._compute_fixed_rate_wait()
        else:
            wait_for = self._apply_jitter(self.delay)
            self._tick_number += 1

        if wait_for <= 0:
            await asyncio.sleep(0)  # always yield to avoid starvation
            return None
        try:
            was_triggered = await self._try_wait(wait_for)
        except StopAsyncIteration:
            self.stop()
            raise
        if was_triggered and self.mode == "fixed_rate":
            # Re-anchor from the trigger moment (don't catch up).
            self._start_time = time.monotonic()
            self._tick_number = 0
        return None

    def _compute_fixed_rate_wait(self) -> float:
        """Sleep needed before the next fixed-rate slot.

        Skips past every slot already in the past (logs once per skip
        batch). Advances `_tick_number` accordingly.
        """
        assert self._start_time is not None
        now = time.monotonic()
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
        # Cap jitter at wait_for so it can't push past the slot boundary.
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
        """Wait `delay` or until cancel/trigger fires.

        Raises `StopAsyncIteration` on cancel. Returns True if cut
        short by a trigger, False on normal timeout.
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
            self._trigger_evt.clear()
            return True
        return False
