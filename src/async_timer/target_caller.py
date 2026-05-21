"""Target-call dispatch for `Timer`.

Accepts any of: plain callable (sync or async), generator/async
generator function, iterator/generator/async-generator object, or
callable returning one of those. The `Caller` introspects on the
first tick and re-uses the dispatch on subsequent ticks. `reset()`
re-runs introspection (used by `Timer.start()` for restart).
"""

import inspect
import typing
from collections.abc import Iterator

T = typing.TypeVar("T")


class Caller(typing.Generic[T]):
    """Normalises target shape; calls once per tick."""

    target: typing.Any
    get_next_val: typing.Optional[typing.Callable[[], typing.Any]] = None
    first_call: bool = True

    def __init__(self, target):
        self.target = target

    def reset(self):
        """Re-introspect the target on next call (used on restart)."""
        self.get_next_val = None
        self.first_call = True

    def _wrap_generator(self, maybe_gen):
        if inspect.isgenerator(maybe_gen):

            def _lock_sync_gen_ctx():
                return lambda: next(maybe_gen)

            gen_next_val = _lock_sync_gen_ctx()
        elif inspect.isasyncgen(maybe_gen):

            def _lock_async_gen_ctx():
                return lambda: maybe_gen.__anext__()

            gen_next_val = _lock_async_gen_ctx()
        elif isinstance(maybe_gen, Iterator):

            def _lock_iterator_ctx():
                return next(maybe_gen)

            gen_next_val = _lock_iterator_ctx
        else:
            gen_next_val = None
        return gen_next_val

    def _setup(self, target):
        """Pick a dispatch mode and return the first value."""
        self.get_next_val = self._wrap_generator(target)
        if self.get_next_val:
            return self.get_next_val()  # target *is* an iterator
        assert callable(target), "target must be callable"
        target_rv = target()
        self.get_next_val = self._wrap_generator(target_rv)
        if self.get_next_val:
            return self.get_next_val()  # callable returned an iterator
        # Plain callable returning a value each call.
        self.get_next_val = target
        return target_rv

    async def next(self) -> T:
        """Call `target` once."""
        try:
            if self.first_call:
                rv = self._setup(self.target)
                self.first_call = False
            else:
                assert self.get_next_val is not None
                rv = self.get_next_val()
        except StopIteration as _err:
            raise StopAsyncIteration() from _err
        if inspect.isawaitable(rv):
            rv = await rv
        return typing.cast(T, rv)
