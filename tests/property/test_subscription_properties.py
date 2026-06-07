"""Property tests for Subscription drop semantics."""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from async_timer.subscription import Subscription, _StreamEnd, _StreamExc


@given(maxsize=st.integers(max_value=-1))
def test_negative_maxsize_rejected(maxsize):
    with pytest.raises(ValueError, match="maxsize"):
        Subscription(maxsize=maxsize)


@given(n=st.integers(max_value=-1))
def test_negative_drop_n_rejected(n):
    sub: Subscription[int] = Subscription()
    with pytest.raises(ValueError, match="n must be >= 0"):
        sub.drop_oldest(n)


@given(
    pushes=st.lists(st.integers(), min_size=1, max_size=50),
    maxsize=st.integers(min_value=1, max_value=20),
)
@settings(max_examples=100)
def test_bounded_queue_never_exceeds_maxsize(pushes, maxsize):
    """Producer-side drops keep qsize <= maxsize at all times."""
    sub: Subscription[int] = Subscription(maxsize=maxsize)
    for v in pushes:
        sub._push_value(v)
        assert sub.qsize <= maxsize
    assert sub.dropped_count + sub.qsize == len(pushes)


@given(
    pushes=st.lists(st.integers(), min_size=0, max_size=20),
    n=st.integers(min_value=0, max_value=30),
)
@settings(max_examples=100)
def test_drop_oldest_returns_actual_count(pushes, n):
    """`drop_oldest` returns <= min(n, current values), remaining = suffix."""
    sub: Subscription[int] = Subscription()
    for v in pushes:
        sub._push_value(v)
    dropped = sub.drop_oldest(n)
    assert 0 <= dropped <= min(n, len(pushes))
    remaining = []
    while sub.qsize:
        remaining.append(sub._queue.get_nowait())
    assert remaining == pushes[dropped:]


@given(
    pre_pushes=st.lists(st.integers(), min_size=0, max_size=10),
    n=st.integers(min_value=1, max_value=20),
)
@settings(max_examples=100)
def test_drop_oldest_preserves_end_sentinel(pre_pushes, n):
    """End-of-stream sentinel never gets dropped — it's a termination signal."""
    sub: Subscription[int] = Subscription()
    for v in pre_pushes:
        sub._push_value(v)
    sub._push_end()
    dropped = sub.drop_oldest(n)
    assert dropped <= len(pre_pushes)
    tail = []
    while sub.qsize:
        tail.append(sub._queue.get_nowait())
    assert isinstance(tail[-1], _StreamEnd)


@given(
    pre_pushes=st.lists(st.integers(), min_size=0, max_size=10),
    n=st.integers(min_value=1, max_value=20),
)
@settings(max_examples=100)
def test_drop_oldest_preserves_exception_sentinel(pre_pushes, n):
    sub: Subscription[int] = Subscription()
    for v in pre_pushes:
        sub._push_value(v)
    sub._push_exception(RuntimeError("boom"))
    dropped = sub.drop_oldest(n)
    assert dropped <= len(pre_pushes)
    tail = []
    while sub.qsize:
        tail.append(sub._queue.get_nowait())
    assert isinstance(tail[-1], _StreamExc)
