"""Property tests for the shared validation helpers."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from async_timer._common import _validate_nonnegative, _validate_unit_range


@given(value=st.floats(min_value=0, max_value=1e9, allow_nan=False, allow_infinity=False))
def test_validate_nonnegative_accepts_nonneg(value):
    _validate_nonnegative(value, "x")  # must not raise


@given(value=st.floats(max_value=-1e-12, allow_nan=False, allow_infinity=False))
def test_validate_nonnegative_rejects_negative(value):
    with pytest.raises(ValueError, match="x must be >= 0"):
        _validate_nonnegative(value, "x")


@given(value=st.floats(min_value=0, max_value=1, allow_nan=False, allow_infinity=False))
def test_validate_unit_range_accepts(value):
    _validate_unit_range(value, "j")


@given(
    value=st.one_of(
        st.floats(max_value=-1e-12, allow_nan=False, allow_infinity=False),
        st.floats(min_value=1 + 1e-12, max_value=1e9, allow_nan=False, allow_infinity=False),
    )
)
def test_validate_unit_range_rejects_out_of_range(value):
    with pytest.raises(ValueError, match="j must be in"):
        _validate_unit_range(value, "j")
