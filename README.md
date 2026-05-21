# Async timer

This package provides an async timer object, that should have been part of batteries.

[![Tests](docs/badges/tests.svg)](docs/badges/tests.svg)
[![Coverage](docs/badges/coverage.svg)](docs/badges/coverage.svg)

[![CI](https://github.com/VRGhost/async-timer/actions/workflows/main.yml/badge.svg)](https://github.com/VRGhost/async-timer/actions/workflows/main.yml)

## Purpose

Sometimes, you need a way to make something happen over and over again at certain times, like updating information or sending reminders. That's where Async Timer comes in. It lets you set up these repeated actions easily.

This package is particularly useful for tasks like automatically updating caches in the background without disrupting the primary application's workflow.

## Features

* **Zero Dependencies**: Written entirely in Python, Async Timer operates independently without needing any external libraries.
* **Versatility in Callables**: It accommodates various callable types, including:
  * Synchronous functions
  * Asynchronous functions
  * Synchronous generators
  * Asynchronous generators
* **Wait for the Next Tick**: You can set it up so your program waits for the timer to do its thing, and then continues. `await timer.wait(hits=N, timeout=T)` raises on timeout when waiting for a specific number of ticks; `await timer.wait(timeout=T)` (no hit condition) is a bounded *idle* wait that returns the last seen value without raising — useful for "let the timer settle, but don't hang forever" patterns.
* **Keep Getting Updates**: You can use it in a loop to keep getting updates every time the timer goes off.
* **Cancel anytime**: The timer object can be stopped at any time either explicitly by calling `stop()`/`cancel()` method OR it can stop automatically on an awaitable resolving (the `cancel_aws` constructor argument). `await cancel()` waits for cleanup to complete before returning, and is safe to call from inside the target or its callbacks.
* **Restartable**: Calling `start()` after `cancel()` resumes the timer with fresh pacemaker, fanout, and target-caller state (generator targets get a fresh generator). Restart is rejected with a clear error if the original construction used `cancel_aws`, since those awaitables are single-shot.
* **Scheduling modes**: Choose between `fixed_delay` (next tick fires `delay` seconds *after* the previous tick finishes) and `fixed_rate` (ticks anchored to a wall-clock schedule; missed slots are skipped and logged).
* **Initial delay and jitter**: `initial_delay=N` lets you defer the first tick; `jitter=0.1` perturbs each per-tick sleep by ±10 % to avoid thundering-herd in distributed deployments.
* **Trigger on demand**: `await timer.trigger()` fires the target immediately and resumes the regular schedule; great for "refresh on user request, then go back to periodic" patterns.
* **Last value cache**: `timer.last_result` / `timer.last_tick_at` let consumers grab the most recent value without blocking on `join()`.
* **Decorator API**: `@async_timer.every(5)` wraps a function into a `Timer` in one line; the undecorated function is preserved on `.func` for direct invocation in tests.
* **Timer groups**: `async with async_timer.TimerGroup(): ...` starts and cancels a collection of timers together.
* **Named timers**: `name="db_refresh"` shows up in `repr()` and scopes the timer's logger, so multi-timer apps have readable logs.
* **Test friendly**: The package provides an additional `mock_async_timer.MockTimer` class with mocked sleep function to aid in your testing.

## Requirements

Python 3.9 or newer.

## Installation

```bash
pip install async-timer
```

## Example Usage

### FastAPI

This snippet starts fastapi webserver with the `refresh_db` function being executed every 5 seconds, refresing a shared `DB_CACHE` object.

```python

import contextlib
import time

import uvicorn
from fastapi import FastAPI

import async_timer

DB_CACHE = {"initialised": False}


async def refresh_db():
    global DB_CACHE
    DB_CACHE |= {"initialised": True, "cur_value": time.time()}


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    async with async_timer.Timer(delay=5, target=refresh_db) as timer:
        await timer.wait(hit_count=1)  # block until the timer triggers at least once
        yield


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"message": "Hello World", "db_cache": DB_CACHE}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

```

### join()
```python
import asyncio
import async_timer

async def main():
    timer = async_timer.Timer(12, target=lambda: 42)
    timer.start()
    val = await timer.join()  # `val` will be set to 42 after the first tick
    await timer.cancel()

asyncio.run(main())
```

### async for loop
```python
import asyncio
import time
import async_timer

async def main():
    async with async_timer.Timer(14, target=time.time) as timer:
        async for time_rv in timer:
            print(f"{time_rv=}")  # Prints current time every 14 seconds

asyncio.run(main())
```

### Decorator API

```python
import async_timer

@async_timer.every(5, mode="fixed_rate", name="db_refresh")
async def refresh_db():
    ...

# In tests, the undecorated function is directly callable:
await refresh_db.func()

# In production, start/cancel like any other Timer:
async def main():
    refresh_db.start()
    await refresh_db.join()
    await refresh_db.cancel()
```

### TimerGroup

```python
import async_timer

async def lifespan():
    async with async_timer.TimerGroup() as group:
        group.add(async_timer.Timer(5, target=refresh_db))
        group.add(async_timer.Timer(60, target=prune_cache))
        yield  # both timers running; both cancelled on exit
```

### Trigger on demand

```python
async def handle_force_refresh(timer):
    # Fire the target *now* and resume the periodic schedule.
    return await timer.trigger()
```

### Read last value without blocking

```python
@async_timer.every(5)
async def refresh_db():
    return await db.fetch()

# Elsewhere in the app:
def get_cached_value():
    return refresh_db.last_result  # `None` until the first tick fires
```