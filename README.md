# async-timer

The missing Python async timer.

[![Tests](docs/badges/tests.svg)](docs/badges/tests.svg)
[![Coverage](docs/badges/coverage.svg)](docs/badges/coverage.svg)
[![CI](https://github.com/IljaOrlovs/async-timer/actions/workflows/main.yml/badge.svg)](https://github.com/IljaOrlovs/async-timer/actions/workflows/main.yml)

## The problem

The obvious way to run something on an interval in asyncio:

```python
async def refresh():
    while True:
        await do_the_thing()
        await asyncio.sleep(5)
```

This works until it doesn't:

* **It drifts.** `sleep(5)` runs *after* `do_the_thing()` finishes, so
  a 2-second call gives you a 7-second period. Drift compounds.
* **It has no cancellation story.** Cancelling the wrapping task
  during `do_the_thing()` may interrupt mid-write; you need explicit
  shielding and cleanup to handle this safely.
* **The first call isn't observable.** Want to wait until the cache is
  populated before serving traffic? You have to bolt on an `Event`.
* **There's nowhere for consumers to subscribe.** If something else
  wants the latest value (or every value), you're hand-rolling a queue
  or a fanout.
* **It doesn't compose.** Running ten of these on startup and
  cancelling them together on shutdown needs a `TaskGroup` plus
  bookkeeping.

`async-timer` is what you actually want: a recurring async call with
proper lifecycle, two delivery models, and the operational knobs
(fixed-rate vs fixed-delay, jitter, trigger-now, cross-thread control)
that real production code ends up needing.

Zero runtime dependencies. Python 3.9+.

## Install

```bash
pip install async-timer
```

## The 30-second example: FastAPI cache warmup

```python
import contextlib
import time
import uvicorn
from fastapi import FastAPI
import async_timer

DB_CACHE = {"initialised": False}

async def refresh_db():
    DB_CACHE.update(initialised=True, cur_value=time.time())

@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    async with async_timer.Timer(delay=5, target=refresh_db) as timer:
        await timer.wait(hit_count=1)   # block startup until the first refresh
        yield                           # serve traffic; timer keeps refreshing

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"db_cache": DB_CACHE}
```

Two lines do the heavy lifting:

* `async with Timer(...) as timer` starts on enter, cancels on exit
  (and *awaits* cleanup — no orphan task).
* `await timer.wait(hit_count=1)` gates startup on the first
  successful refresh.

More recipes: [docs/recipes/](docs/recipes/). Runnable scripts:
[examples/](examples/).

## Features

* **Zero runtime dependencies.**
* **Any callable shape.** Sync or async functions, generators, async
  generators, or callables returning any of those.
* **Two delivery models.** `join()` / `wait()` / `async for self` is
  single-shot fan-out (latest value, may drop intermediate ticks under
  slow consumers). `subscribe()` gives each consumer a buffered queue
  (every tick, optional `maxsize` for bounded drop-oldest).
* **Scheduling modes.** `fixed_delay` (default; next tick fires
  `delay` after the previous one finishes) or `fixed_rate` (anchored
  to wall clock; missed slots skipped + logged). Optional
  `initial_delay` and `jitter`.
* **Trigger on demand.** `await timer.trigger()` fires now and
  resumes the schedule.
* **Last-value cache.** `timer.last_result` / `timer.last_tick_at` —
  no blocking.
* **Cancel anytime.** Explicit `cancel()` or constructor `cancel_aws`
  (awaitables that stop the timer when they resolve). `await cancel()`
  waits for cleanup before returning; safe from inside the
  target/callbacks.
* **Restartable.** `start()` after `cancel()` works (raises if
  `cancel_aws` was used — those are single-shot).
* **Decorator.** `@async_timer.every(5)` wraps a function into a
  Timer; original on `.func`.
* **Groups.** `TimerGroup()` starts/cancels a set of timers together.
* **Named.** `name="db_refresh"` shows in `repr()` and scopes the
  logger.
* **Test-friendly.** `mock_async_timer.MockTimer` replaces real
  sleeps with an `AsyncMock`.

## When to use this — and when not to

`async-timer` is for **in-process recurring work driven by asyncio**:
cache refresh, periodic polling, metrics sampling, heartbeats,
fan-out feeds. Its model is one process, one loop, many timers.

| You want | Reach for |
| --- | --- |
| Periodic work in an asyncio app | **`async-timer`** |
| Cron-style wall-clock scheduling ("every Monday 9am") | [`APScheduler`](https://github.com/agronholm/apscheduler), [`aiocron`](https://github.com/gawel/aiocron) |
| Background *jobs* with retries, queues, persistence | [`arq`](https://github.com/python-arq/arq), [`dramatiq`](https://github.com/Bogdanp/dramatiq), Celery + beat |
| Recurring tasks across many processes | A scheduler + broker (Celery beat, arq cron) |
| One-shot `setTimeout`-equivalent | `loop.call_later` (stdlib) |

If you need durability or cross-process coordination, you need a
broker. `async-timer` doesn't try to be that.

## More examples

### `join()`

```python
import asyncio
import async_timer

async def main():
    timer = async_timer.Timer(12, target=lambda: 42)
    timer.start()
    val = await timer.join()   # 42, after the first tick
    await timer.cancel()

asyncio.run(main())
```

### `async for`

```python
import asyncio, time
import async_timer

async def main():
    async with async_timer.Timer(14, target=time.time) as timer:
        async for t in timer:
            print(t)   # current time every 14 seconds

asyncio.run(main())
```

### Decorator

```python
import async_timer

@async_timer.every(5, mode="fixed_rate", name="db_refresh")
async def refresh_db():
    ...

await refresh_db.func()   # call the undecorated fn (tests)

async def main():
    refresh_db.start()
    await refresh_db.join()
    await refresh_db.cancel()
```

### `TimerGroup`

```python
import async_timer

async def lifespan():
    async with async_timer.TimerGroup(name="caches") as group:
        group.add(async_timer.Timer(5, target=refresh_db))
        group.add(async_timer.Timer(60, target=prune_cache))
        # Block until every cache has been populated at least once,
        # then serve traffic.
        await group.wait(hit_count=1)
        yield   # both running; both cancelled on exit
```

`TimerGroup` mirrors `Timer`'s surface across a set of timers; each
group method fans out to its members (AND-combined) and returns
`[(timer, rv), ...]` in iteration order:

* `group.wait(hit_count=...)` / `wait(hits=...)` — block until every
  member satisfies the condition.
* `group.trigger()` — fire every member's target now (cache-invalidate-all).
* `group.is_running()` — True iff active and every member is running.
* `group.start()` / `await group.cancel_all()` — explicit lifecycle
  for use outside `async with`.
* `group.cancel_threadsafe(timeout=5.0)` — cancel from a non-loop
  thread (signal handler, sync REST endpoint, worker thread).

Each of `wait()` and `trigger()` accepts `timeout=` (whole-group
wall-clock bound) and `return_exceptions=True` (per-member errors
appear in the result list instead of propagating, mirroring
`asyncio.gather`).

### Trigger now

```python
async def force_refresh(timer):
    return await timer.trigger()
```

### Latest value, no blocking

```python
@async_timer.every(5)
async def refresh_db():
    return await db.fetch()

def get_cached():
    return refresh_db.last_result   # None until the first tick
```

### Every-tick delivery via `subscribe()`

`join()` / `async for self` drop ticks under slow consumers (single-shot
fan-out). Use `subscribe()` when you need every tick:

```python
async with timer.subscribe() as feed:
    async for value in feed:
        await log_it(value)        # never misses a tick from subscribe-time
        await asyncio.sleep(3.0)   # even though the consumer is slow
```

Bounded queue (drop oldest + log when full):

```python
async with timer.subscribe(maxsize=10, name="metrics-sink") as feed:
    async for value in feed:
        await slow_export(value)
```

Multiple subscribers each get an independent copy:

```python
async with timer.subscribe() as a, timer.subscribe() as b:
    ...
```

Consumer-side load shedding:

```python
async with timer.subscribe() as feed:
    async for value in feed:
        if feed.qsize > 100:
            feed.drop_oldest(feed.qsize - 1)   # keep only the newest
            log.warning("shed %d ticks", feed.dropped_count)
        await slow_export(value)
```

`drop_oldest()` never swallows end-of-stream / exception sentinels.
Target exceptions re-raise from the subscriber's iteration.

## Thread safety

A `Timer` runs in a single asyncio event loop. Most state-mutating
operations must be called from the loop's thread. The following are
explicitly safe to use from any thread:

**Read-only attributes** (atomic under CPython's GIL):

* `timer.last_result`, `timer.last_tick_at`, `timer.hit_count`
* `timer.is_running()`, `timer.delay`, `timer.name`
* `subscription.qsize`, `subscription.dropped_count`

**`set_delay(new_delay)`** is a single attribute write — safe from any
thread; takes effect on the next sleep.

**Cross-thread control methods** — marshal the operation back to the
timer's loop and block for completion:

```python
# From a sync REST handler, signal handler, worker thread, etc.:
timer.cancel_threadsafe(timeout=5.0)        # raises TimeoutError if exceeded
result = timer.trigger_threadsafe(timeout=5.0)
feed.close_threadsafe()
```

These raise `RuntimeError` with a clear message if called from the
timer's own loop thread (use `await cancel()` / `await trigger()`
instead), or if the timer has not been started yet, or if the bound
event loop has been closed.

Anything else (`subscribe()`, awaiting `join()` / `wait()`, iterating
`async for` over the timer or a subscription, reading from a
subscription queue) must happen on the loop's thread. From other
threads, use `asyncio.run_coroutine_threadsafe(coro, loop)` to
dispatch.

## License

MIT.
