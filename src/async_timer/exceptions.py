"""Public exception types.

All inherit from `RuntimeError` for backwards compatibility — existing
`except RuntimeError` clauses continue to catch them. Catch
`TimerError` to filter only async-timer-originated errors.
"""


class TimerError(RuntimeError):
    """Base class for all async-timer raised RuntimeErrors."""


class TimerNotRunningError(TimerError):
    """Operation requires a running timer (trigger, join on stopped, ...)."""


class TimerAlreadyRunningError(TimerError):
    """start() called while the timer is already running."""


class TimerRestartError(TimerError):
    """Restart attempted on a Timer constructed with single-shot cancel_aws."""


class ThreadsafeDispatchError(TimerError):
    """Cross-thread *_threadsafe call made from the loop's own thread,
    before start, or after the loop closed."""


__all__ = [
    "TimerAlreadyRunningError",
    "TimerError",
    "TimerNotRunningError",
    "TimerRestartError",
    "ThreadsafeDispatchError",
]
