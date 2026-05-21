"""This module is responsible for the magic behaviour calling the `target` function."""

import inspect
import typing
from collections.abc import Iterator


class Caller:
    target: typing.Any
    get_next_val: typing.Optional[typing.Callable[[], typing.Any]] = None
    first_call: bool = True

    def __init__(self, target):
        self.target = target

    def reset(self):
        """Reset dispatch state so the original `target` is re-introspected.

        Used by Timer.start() after a previous run, so that a target which
        is (or returns) a generator gets a fresh generator on restart
        instead of reusing the exhausted one.
        """
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
        """Configure `get_next_val` to return next value.

        Return the first such next value.
        """
        self.get_next_val = self._wrap_generator(target)
        if self.get_next_val:
            # `target` is a generator and we now have the
            # `get_next_val`
            return self.get_next_val()
        assert callable(target), "Otherwise target must be callable"
        target_rv = target()
        self.get_next_val = self._wrap_generator(target_rv)
        if self.get_next_val:
            # Tartget is a callable that returned a generator.
            return self.get_next_val()
        # Otherwise, target is just a callable that returns values
        self.get_next_val = target
        return target_rv

    async def next(self) -> typing.Any:
        """Call `target` one more time."""
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
        return rv
