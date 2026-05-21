"""Decorator-style API for building Timers.

Example
-------

    @async_timer.every(5)
    async def refresh_db():
        ...

    # `refresh_db` is now a `Timer` instance. The original callable is
    # available as `refresh_db.func` for direct invocation in tests:
    await refresh_db.func()

    # In an async context, start/cancel like any other Timer:
    refresh_db.start()
    await refresh_db.join()
    await refresh_db.cancel()
"""

import functools
import typing

from .pacemaker import PacemakerMode
from .timer import Timer, TimerCallbackT, TimerMainTaskT

T = typing.TypeVar("T")


class DecoratedTimer(Timer[T]):
    """A `Timer` produced by `@every(...)` that also keeps a reference
    to the original undecorated function on `.func`."""

    func: typing.Any  # the unwrapped callable, for direct test invocation

    def __init__(self, *args, _func, **kwargs):
        super().__init__(*args, **kwargs)
        self.func = _func
        # Preserve standard introspection attributes from the wrapped
        # function so tooling (Sphinx, IDEs, etc.) still works.
        functools.update_wrapper(
            self,  # type: ignore[arg-type]
            _func,
            updated=(),
        )


def every(
    delay: float,
    *,
    exc_cb: typing.Optional[TimerCallbackT] = None,  # type: ignore[type-arg]
    cancel_cb: typing.Optional[TimerCallbackT] = None,  # type: ignore[type-arg]
    cancel_aws: typing.Union[typing.Sequence[typing.Awaitable], None] = None,
    mode: PacemakerMode = "fixed_delay",
    initial_delay: float = 0.0,
    jitter: float = 0.0,
    name: typing.Optional[str] = None,
) -> typing.Callable[[TimerMainTaskT[T]], "DecoratedTimer[T]"]:
    """Wrap a function into a `Timer` that fires it every `delay` seconds.

    The returned object is a `Timer` instance — call `.start()` from
    within a running event loop. The original undecorated function is
    available as `.func` for direct invocation in tests::

        @async_timer.every(5)
        async def refresh_db():
            ...

        # In tests, call the underlying function directly:
        await refresh_db.func()

    All Timer constructor keyword arguments (mode, jitter, etc.) are
    supported.
    """

    def _decorator(func: TimerMainTaskT[T]) -> DecoratedTimer[T]:
        timer_kwargs: dict = {
            "mode": mode,
            "initial_delay": initial_delay,
            "jitter": jitter,
            "name": name or getattr(func, "__name__", None),
        }
        if exc_cb is not None:
            timer_kwargs["exc_cb"] = exc_cb
        if cancel_cb is not None:
            timer_kwargs["cancel_cb"] = cancel_cb
        if cancel_aws is not None:
            timer_kwargs["cancel_aws"] = cancel_aws
        return DecoratedTimer(
            delay=delay,
            target=func,
            _func=func,
            **timer_kwargs,
        )

    return _decorator
