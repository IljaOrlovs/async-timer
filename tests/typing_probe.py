"""Pyright type-inference regression test.

Uses `typing.assert_type` so any regression in the generic-T flow (e.g.
target shape → Timer[T] → join()/wait()/trigger()/last_result) produces
a pyright error rather than a silent type-degradation.

This file is intentionally importable at runtime (the asserts are
type-checker-only, no-ops at runtime) so pytest collection doesn't fail.
"""

# pyright: reportMissingTypeStubs=false

import typing

import async_timer

# typing.assert_type was added in 3.11; fall back to no-op for older.
if typing.TYPE_CHECKING:
    from typing import assert_type
else:

    def assert_type(value, _typ):  # noqa: D401
        return value


# --- sync function returning int ---
def sync_int() -> int:
    return 42


t1 = async_timer.Timer(1.0, sync_int)
assert_type(t1, async_timer.Timer[int])
assert_type(t1.last_result, typing.Optional[int])


# --- async function returning str ---
async def async_str() -> str:
    return "x"


t2 = async_timer.Timer(1.0, async_str)
assert_type(t2, async_timer.Timer[str])


# --- generator yielding float ---
def gen_float() -> typing.Generator[float, None, None]:
    yield 1.5


t3 = async_timer.Timer(1.0, gen_float)
assert_type(t3, async_timer.Timer[float])


# --- async generator yielding bytes ---
async def agen_bytes() -> typing.AsyncGenerator[bytes, None]:
    yield b"x"


t4 = async_timer.Timer(1.0, agen_bytes)
assert_type(t4, async_timer.Timer[bytes])


# --- lambda (very common usage) ---
t5 = async_timer.Timer(1.0, lambda: 7)
assert_type(t5, async_timer.Timer[int])


# --- callable returning a generator ---
def make_gen() -> typing.Generator[int, None, None]:
    yield 1


t6 = async_timer.Timer(1.0, make_gen)
assert_type(t6, async_timer.Timer[int])


# --- decorator preserves return type ---
@async_timer.every(1.0)
async def decorated_int() -> int:
    return 99


assert_type(decorated_int, async_timer.decorators.DecoratedTimer[int])


# --- join/wait/trigger return T ---
async def use(t: "async_timer.Timer[int]") -> None:
    assert_type(await t.join(), int)
    assert_type(await t.wait(), typing.Optional[int])
    assert_type(await t.trigger(), int)


# --- async for over a typed Timer yields T ---
async def iterate(t: "async_timer.Timer[int]") -> None:
    async for val in t:
        assert_type(val, int)
