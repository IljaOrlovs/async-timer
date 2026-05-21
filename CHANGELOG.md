# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> Entries before v1.2.0 were reconstructed retroactively from commit history
> and may be less detailed than later entries.

## [Unreleased]

## [1.2.0] - 2026-05-21

A substantial overhaul focused on correctness, observability, and new
delivery patterns. Backwards-compatible: the basic
`Timer(delay, target).start() / .join() / .cancel()` API is unchanged.

### Added

- **`timer.subscribe(maxsize=0, name=None) -> Subscription`** — buffered
  per-consumer feed of every tick (never drops intermediates under slow
  consumers, unlike single-shot `join()`). Async context manager + async
  iterator. Bounded variant drops oldest and logs warning. Exposes
  `qsize`, `dropped_count`, and `drop_oldest(n=1)` for consumer-side
  load shedding.
- **`@async_timer.every(delay)` decorator** — wraps a function into a
  `DecoratedTimer`. Original callable preserved on `.func` for testing.
- **`async_timer.TimerGroup`** — async context manager that starts and
  cancels a set of timers together. Cleans up partially-started timers
  on `__aenter__` failure.
- **Scheduling modes**: `mode="fixed_delay"` (default; existing
  behaviour) and `mode="fixed_rate"` (anchored to wall-clock schedule;
  missed slots are skipped and logged). `trigger()` re-anchors the
  fixed-rate schedule.
- **`initial_delay`** parameter — defer the first tick.
- **`jitter`** parameter (fraction in `[0, 1]`) — per-tick sleep
  perturbation to avoid thundering-herd in distributed deployments.
- **`name`** parameter — appears in `repr()` and scopes a per-timer
  child logger.
- **`await timer.trigger()`** — fire the target now and return its
  result; resumes the regular schedule afterwards.
- **`timer.last_result` / `timer.last_tick_at`** — non-blocking access
  to the most recent successful tick.
- **Restart support**: `start()` works after `cancel()` with fresh
  pacemaker, fanout, and target-caller state. Raises a clear error if
  the original construction used `cancel_aws`.
- **Cross-thread control**: `timer.cancel_threadsafe(timeout=None)`,
  `timer.trigger_threadsafe(timeout=None)`, and
  `subscription.close_threadsafe()` — marshal calls back to the timer's
  event loop. Defensive guards for unstarted timer, closed loop,
  same-loop misuse, and timeout, each with a clear error message.
- **`async_timer.__version__`** exposed via `importlib.metadata`.
- **Generic typing** flows correctly through `Timer[T]`: `await join()`,
  `await wait()`, `await trigger()`, `async for v in timer`, and
  `last_result` all resolve to the target's return type under Pyright.
- `LICENSE` file (MIT) at the repo root, bundled in the wheel.

### Changed

- **`Timer.cancel()` now awaits the underlying task** before returning,
  so by the time `await cancel()` completes the `cancel_callback` has
  fired and waiters are resolved. Safe to call from inside the target
  or callbacks (self-cancel detected; deadlock avoided).
- **Default `exc_cb` logs only, no longer re-raises** (eliminates the
  duplicate "Task exception was never retrieved" warning).
- **Tighter API contract for `wait()`**: documented behaviour matrix
  covering all `(condition × timeout)` combinations.
- **Pyright** is now run in CI alongside `ruff check` / `ruff format
  --check` / `pytest` on every matrix Python version (3.9–3.13).
- Build system migrated from Poetry to **PDM**; SCM-derived versioning
  from git tags via `pdm-backend`.
- Python support: now `>=3.9` (was `>=3.8` since 3.8 is EOL). Tested on
  3.9, 3.10, 3.11, 3.12, 3.13.
- Project metadata: `Development Status :: 5 - Production/Stable`,
  `Framework :: AsyncIO`, `Typing :: Typed` classifiers; expanded
  keywords; PyPI sidebar URLs (Homepage / Issues / Changelog).

### Fixed

- **Restart-after-cancel cluster**: pacemaker reset on restart,
  generator targets get a fresh iterator, `cancel_aws` restart attempts
  fail loudly instead of silently dropping the cancel condition.
- **Self-cancel deadlock**: `cancel()` called from within `target`,
  `exc_cb`, or `cancel_cb` no longer hangs awaiting the current task.
- **`FanoutRv` sticky exceptions**: target exceptions raised while no
  consumer is registered are now stored on the fanout's close-state,
  so late-arriving waiters see the exception instead of a generic
  `CancelledError`.
- **Fixed-rate off-by-one**: the 2nd tick fired at `2*delay` instead
  of `1*delay` after the first.
- **Fixed-rate jitter slot overflow**: jitter could push a tick past
  its scheduled slot, triggering spurious "fell behind" warnings.
  Jitter is now capped at the remaining wait so this can't happen.
- **`trigger()` phantom tick**: if a naturally-scheduled tick fired
  between trigger's waiter registration and the pacemaker noticing the
  trigger event, a redundant extra tick fired on the next iteration.
- **`TimerGroup.__aenter__` partial-start leak**: if any timer's
  `start()` raised, the already-started ones leaked because `__aexit__`
  never ran. Now cancelled before re-raising.
- **`cancel_aws` at module scope**: `@every(..., cancel_aws=[...])`
  evaluated outside a running loop no longer crashes; registration is
  deferred until `start()`.
- **`Subscription` GC leak**: timer now holds subscriptions weakly so
  dropped subscriptions don't accumulate.
- **`Subscription.close()` is fully idempotent** — second call no
  longer pushes a duplicate end-of-stream sentinel.

### Removed

- Re-raise behaviour of the default `exc_cb` (see Changed).

## [1.1.6] - 2024-01-24

### Added

- `Timer.set_delay(new_delay)` to change the tick interval after construction.

## [1.1.5] - 2024-01-24

### Fixed

- `MockTimer` termination semantics.

## [1.1.4] - 2024-01-24

### Changed

- `MockTimer` made friendlier for async test patterns.

## [1.1.3] - 2024-01-24

### Fixed

- Packaging: the `mock_async_timer` module was missing from the
  v1.1.2 distribution.

## [1.1.2] - 2024-01-24

### Added

- `mock_async_timer.MockTimer` — test-friendly Timer subclass that
  replaces real sleeps with an `AsyncMock`. ([#1])

[#1]: https://github.com/VRGhost/async-timer/pull/1

## [1.1.1] - 2024-01-23

### Changed

- More permissive `target` interface (accepts a wider range of
  callable/generator shapes).

## [1.1.0] - 2024-01-23

### Added

- `cancel_aws` constructor parameter — the timer stops as soon as any
  registered awaitable resolves.

## [1.0.3] - 2023-12-13

### Added

- `Timer.wait(hit_count=..., hits=..., timeout=...)` for hit-count
  conditioned and bounded waits.

## [1.0.2] - 2023-12-13

### Added

- Callback hooks (`exc_cb`, `cancel_cb`).

## [1.0.1] - 2023-12-11

### Changed

- Adjusted PyPI classifiers and package metadata.

## [1.0.0] - 2023-12-11

Initial public release.

### Added

- Project description and packaging metadata.

## [0.0.1] - 2023-12-11

First tagged release.

[Unreleased]: https://github.com/VRGhost/async-timer/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/VRGhost/async-timer/compare/v1.1.6...v1.2.0
[1.1.6]: https://github.com/VRGhost/async-timer/compare/v1.1.5...v1.1.6
[1.1.5]: https://github.com/VRGhost/async-timer/compare/v1.1.4...v1.1.5
[1.1.4]: https://github.com/VRGhost/async-timer/compare/v1.1.3...v1.1.4
[1.1.3]: https://github.com/VRGhost/async-timer/compare/v1.1.2...v1.1.3
[1.1.2]: https://github.com/VRGhost/async-timer/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/VRGhost/async-timer/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/VRGhost/async-timer/compare/v1.0.3...v1.1.0
[1.0.3]: https://github.com/VRGhost/async-timer/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/VRGhost/async-timer/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/VRGhost/async-timer/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/VRGhost/async-timer/compare/v0.0.1...v1.0.0
[0.0.1]: https://github.com/VRGhost/async-timer/releases/tag/v0.0.1
