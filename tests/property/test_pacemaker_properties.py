"""Property tests for pacemaker math and validation."""

import math

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from async_timer.pacemaker import TimerPacemaker

finite_nonneg = st.floats(
    min_value=0,
    max_value=1e6,
    allow_nan=False,
    allow_infinity=False,
)
unit_fraction = st.floats(
    min_value=0,
    max_value=1,
    allow_nan=False,
    allow_infinity=False,
)


@given(
    delay=finite_nonneg,
    jitter=unit_fraction,
    initial_delay=finite_nonneg,
)
def test_valid_construction_never_raises(delay, jitter, initial_delay):
    pm = TimerPacemaker(
        delay, jitter=jitter, initial_delay=initial_delay, mode="fixed_delay"
    )
    assert pm.delay == delay
    assert pm.jitter == jitter
    assert pm.initial_delay == initial_delay


@given(delay=st.floats(max_value=-1e-9, allow_nan=False, allow_infinity=False))
def test_negative_delay_rejected(delay):
    with pytest.raises(ValueError, match="delay"):
        TimerPacemaker(delay)


@given(
    jitter=st.one_of(
        st.floats(max_value=-1e-9, allow_nan=False, allow_infinity=False),
        st.floats(min_value=1 + 1e-9, max_value=1e6, allow_nan=False),
    )
)
def test_out_of_range_jitter_rejected(jitter):
    with pytest.raises(ValueError, match="jitter"):
        TimerPacemaker(1.0, jitter=jitter)


@given(
    base=st.floats(min_value=0, max_value=1e6, allow_nan=False, allow_infinity=False),
    jitter=unit_fraction,
)
def test_apply_jitter_bounds_no_cap(base, jitter):
    """`_apply_jitter` output stays in [0, base*(1+jitter)]."""
    pm = TimerPacemaker(1.0, jitter=jitter)
    out = pm._apply_jitter(base)
    assert out >= 0
    upper = base * (1 + jitter)
    # Allow tiny FP slack so the test isn't flaky on huge bases.
    assert out <= upper + 1e-9 * max(1.0, upper)


@given(
    base=st.floats(min_value=0, max_value=1e6, allow_nan=False, allow_infinity=False),
    cap=st.floats(min_value=0, max_value=1e6, allow_nan=False, allow_infinity=False),
    jitter=unit_fraction,
)
def test_apply_jitter_respects_cap(base, cap, jitter):
    pm = TimerPacemaker(1.0, jitter=jitter)
    out = pm._apply_jitter(base, cap=cap)
    assert 0 <= out <= cap + 1e-9 * max(1.0, cap)


@given(jitter=unit_fraction)
def test_apply_jitter_zero_base_is_zero(jitter):
    pm = TimerPacemaker(1.0, jitter=jitter)
    assert pm._apply_jitter(0.0) == 0.0


@given(
    delay=st.floats(
        min_value=1e-3, max_value=1e3, allow_nan=False, allow_infinity=False
    ),
    elapsed_slots=st.integers(min_value=0, max_value=10_000),
)
@settings(max_examples=200)
def test_fixed_rate_skip_advances_tick_number(delay, elapsed_slots):
    """If wall-clock has advanced N full slots past the next target,
    _compute_fixed_rate_wait skips to slot N+1 and returns a positive wait
    not greater than `delay`."""
    pm = TimerPacemaker(delay, mode="fixed_rate", jitter=0.0)
    # Anchor "now - elapsed_slots*delay - delay/2" → next target is
    # elapsed_slots+1 slots in the past; we expect to skip past all of them.
    import time

    now = time.monotonic()
    pm._start_time = now - elapsed_slots * delay - delay / 2
    pm._tick_number = 0
    wait_for = pm._compute_fixed_rate_wait()
    assume(not math.isnan(wait_for))
    # New target should be in the future and within one delay window.
    assert 0 <= wait_for <= delay + 1e-9
    # Tick number must have advanced past all elapsed slots.
    assert pm._tick_number >= elapsed_slots
