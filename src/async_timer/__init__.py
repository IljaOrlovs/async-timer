from importlib import metadata

from . import (
    decorators,
    exceptions,
    group,
    pacemaker,
    subscription,
    target_caller,
    timer,
)
from .decorators import every
from .exceptions import (
    ThreadsafeDispatchError,
    TimerAlreadyRunningError,
    TimerError,
    TimerNotRunningError,
    TimerRestartError,
)
from .group import TimerGroup
from .subscription import Subscription
from .timer import Timer

__all__ = [
    "Subscription",
    "ThreadsafeDispatchError",
    "Timer",
    "TimerAlreadyRunningError",
    "TimerError",
    "TimerGroup",
    "TimerNotRunningError",
    "TimerRestartError",
    "__version__",
    "decorators",
    "every",
    "exceptions",
    "group",
    "pacemaker",
    "subscription",
    "target_caller",
    "timer",
]

try:
    __version__ = metadata.version("async-timer")
except metadata.PackageNotFoundError:  # pragma: no cover - editable w/o dist
    __version__ = "0.0.0+unknown"

del metadata
