from importlib import metadata

from . import decorators, group, pacemaker, subscription, target_caller, timer
from .decorators import every
from .group import TimerGroup
from .subscription import Subscription
from .timer import Timer

try:
    __version__ = metadata.version("async-timer")
except metadata.PackageNotFoundError:  # pragma: no cover - editable w/o dist
    __version__ = "0.0.0+unknown"

del metadata
