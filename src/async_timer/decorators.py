"""`@every(delay)` decorator that wraps a function into a Timer.

@async_timer.every(5)
async def refresh_db(): ...

await refresh_db.func()    # call the undecorated fn (handy in tests)
refresh_db.start(); await refresh_db.join(); await refresh_db.cancel()
"""

import functools
import typing

from .pacemaker import PacemakerMode
from .timer import Timer, TimerCallbackT, TimerMainTaskT

T = typing.TypeVar("T")


class DecoratedTimer(Timer[T]):
    """Timer produced by `@every(...)`. Original callable on `.func`."""

    func: typing.Any  # undecorated callable

    def __init__(self, *args, _func, **kwargs):
        super().__init__(*args, **kwargs)
        self.func = _func
        # Carry __name__/__doc__/etc. from the wrapped fn for tooling.
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
    """Wrap a function into a Timer firing it every `delay` seconds.

    All Timer kwargs (mode, jitter, etc.) are forwarded. The undecorated
    function is exposed as `.func` on the returned object.
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
