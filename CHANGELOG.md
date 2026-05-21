# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> Entries before v1.2.0 were reconstructed retroactively from commit history
> and may be less detailed than later entries.

## [Unreleased]

## [1.2.0] - 2026-05-21

Backwards-compatible overhaul. The basic
`Timer(delay, target).start() / .join() / .cancel()` API is unchanged.

### Added

- `timer.subscribe(maxsize=0, name=None)` — buffered per-consumer feed
  that doesn't drop intermediate ticks. Async context manager + async
  iterator. Bounded variant drops oldest + logs. Exposes `qsize`,
  `dropped_count`, `drop_oldest(n=1)`.
- `@async_timer.every(delay)` decorator. Wrapped fn preserved on `.func`.
- `async_timer.TimerGroup` — start/cancel a set of timers together;
  cleans up partial-start failures.
- `mode="fixed_delay"` (default) or `"fixed_rate"` (anchored to
  wall-clock; missed slots skipped + logged). `trigger()` re-anchors
  the fixed-rate schedule.
- `initial_delay` — defer the first tick.
- `jitter` (fraction in `[0, 1]`) — per-tick sleep perturbation.
- `name` — appears in `repr()` and scopes a per-timer child logger.
- `await timer.trigger()` — fire now, return the tick's value.
- `timer.last_result` / `timer.last_tick_at` — non-blocking latest.
- Restart support: `start()` after `cancel()` works (fresh state).
  Raises if `cancel_aws` was used (those are single-shot).
- Cross-thread control: `cancel_threadsafe`, `trigger_threadsafe`,
  `subscription.close_threadsafe`. Defensive guards with clear error
  messages for unstarted / closed-loop / same-loop / timeout.
- `async_timer.__version__` via `importlib.metadata`.
- Generic typing flows correctly through `Timer[T]` under Pyright.
- `LICENSE` (MIT) at repo root; bundled in the wheel.

### Changed

- `Timer.cancel()` now awaits the underlying task — by the time it
  returns, `cancel_callback` has fired and waiters are resolved.
  Safe from inside the target/callbacks (self-cancel detected).
- Default `exc_cb` logs only, no longer re-raises.
- `wait()` behaviour formally documented with a `(condition × timeout)`
  matrix.
- CI runs `ruff check`, `ruff format --check`, and `pyright` alongside
  `pytest` on every supported Python (3.9–3.13).
- Build system: Poetry → PDM; SCM versioning from git tags.
- Python support: `>=3.9` (was `>=3.8`; EOL).
- Metadata: `Development Status :: 5 - Production/Stable`,
  `Framework :: AsyncIO`, `Typing :: Typed`; PyPI sidebar URLs.

### Fixed

- Restart after cancel: pacemaker reset, fresh generator on
  generator-targets, fails loudly if original used `cancel_aws`.
- Self-cancel deadlock when calling `cancel()` from inside
  `target` / `exc_cb` / `cancel_cb`.
- Target exceptions raised with no waiters are now sticky on the
  fanout — late waiters see the exception instead of `CancelledError`.
- Fixed-rate off-by-one: 2nd tick used to fire at `2*delay` instead
  of `1*delay` after the first.
- Fixed-rate jitter could push a tick past its slot, triggering
  spurious "fell behind" warnings. Jitter is now capped at the
  remaining wait.
- `trigger()` no longer fires a phantom extra tick when racing with
  a naturally-scheduled one.
- `TimerGroup.__aenter__` no longer leaks already-started timers if a
  later timer's `start()` raises.
- `@every(..., cancel_aws=[...])` at module scope no longer crashes —
  registration deferred until `start()`.
- Subscription GC leak: timer now holds subscriptions weakly.
- `Subscription.close()` is fully idempotent (no duplicate sentinel).

### Removed

- Re-raise from the default `exc_cb` (see Changed).

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
