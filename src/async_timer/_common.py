"""Internal helpers shared across modules.

Not part of the public API: anything here may change without notice.
"""

import asyncio
import concurrent.futures
import typing

from .exceptions import ThreadsafeDispatchError


def _validate_nonnegative(value: float, name: str) -> None:
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got {value!r}")


def _validate_unit_range(value: float, name: str) -> None:
    if value < 0 or value > 1:
        raise ValueError(f"{name} must be in [0, 1], got {value!r}")


def _resolve_threadsafe_loop(
    loop: typing.Optional[asyncio.AbstractEventLoop],
    *,
    owner: str,
    async_alternative: str,
) -> asyncio.AbstractEventLoop:
    """Validate a bound loop for *_threadsafe dispatch.

    Raises `RuntimeError` with an actionable message if the loop is
    unbound, closed, or we're being called from its own thread.
    """
    if loop is None:
        raise ThreadsafeDispatchError(
            f"{owner}: cannot dispatch — has not been started yet (no "
            f"event loop bound). Call start() first."
        )
    if loop.is_closed():
        raise ThreadsafeDispatchError(
            f"{owner}: target event loop is closed; cannot dispatch "
            f"cross-thread call."
        )
    try:
        current = asyncio.get_running_loop()
    except RuntimeError:
        current = None
    if current is loop:
        raise ThreadsafeDispatchError(
            f"{owner}: called from its own event loop thread. "
            f"Use `{async_alternative}` instead."
        )
    return loop


def _run_threadsafe(
    coro: typing.Coroutine,
    loop: asyncio.AbstractEventLoop,
    *,
    timeout: typing.Optional[float],
    timeout_message: str,
) -> typing.Any:
    """Dispatch `coro` to `loop` and block for its result.

    Converts `concurrent.futures.TimeoutError` to builtin `TimeoutError`
    with `timeout_message`, cancelling the in-flight future first.
    """
    fut = asyncio.run_coroutine_threadsafe(coro, loop)
    try:
        return fut.result(timeout=timeout)
    except concurrent.futures.TimeoutError as err:
        fut.cancel()
        raise TimeoutError(timeout_message) from err
