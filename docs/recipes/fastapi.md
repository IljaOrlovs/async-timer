# FastAPI: warm a cache before serving traffic

A common shape: an app needs a refreshed snapshot of some upstream data
(feature flags, rate limits, pricing) and shouldn't accept traffic
until the first refresh succeeds.

```python
import contextlib
import time
import uvicorn
from fastapi import FastAPI
import async_timer

DB_CACHE: dict = {"initialised": False}

async def refresh_db() -> None:
    DB_CACHE.update(initialised=True, cur_value=time.time())

@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    async with async_timer.Timer(delay=5, target=refresh_db, name="db_refresh") as timer:
        await timer.wait(hit_count=1)   # block startup until the first refresh
        yield                           # serve traffic; timer keeps refreshing

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"db_cache": DB_CACHE}
```

## What the pieces do

* `async with Timer(...) as timer` — starts the timer on entry, cancels
  it on exit. The `__aexit__` awaits cleanup, so by the time `lifespan`
  returns there is no orphan task.
* `await timer.wait(hit_count=1)` — blocks until the target has run at
  least once. Lifespan does not yield to FastAPI's request loop, so no
  request ever sees `DB_CACHE["initialised"] == False`.
* `name="db_refresh"` — scopes the timer's logger to
  `async_timer.db_refresh`, which makes log filtering easy.

## When to reach for `subscribe()`

The example above re-reads `DB_CACHE` on every request — that's the
"latest cached value" pattern, and the timer's default fanout suits it.
If instead you want to *react* to each tick (e.g. push the new snapshot
to connected websockets), open a per-consumer feed:

```python
async with timer.subscribe() as feed:
    async for snapshot in feed:
        await broadcast(snapshot)
```

## Warming several caches at once

When startup depends on more than one cache, switch to `TimerGroup` and
its `wait()`:

```python
@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    async with async_timer.TimerGroup() as group:
        group.add(async_timer.Timer(5, target=refresh_db, name="db"))
        group.add(async_timer.Timer(30, target=refresh_flags, name="flags"))
        group.add(async_timer.Timer(60, target=refresh_pricing, name="pricing"))
        await group.wait(hit_count=1)   # AND across all members
        yield
```

`group.wait(hit_count=1)` blocks until *every* member has produced at
least one tick, so no request can land while any cache is still empty.

## Runnable version

See [`examples/fastapi_lifespan.py`](../../examples/fastapi_lifespan.py)
for the single-timer pattern, and
[`examples/fastapi_multi_cache_warmup.py`](../../examples/fastapi_multi_cache_warmup.py)
for the `TimerGroup.wait()` version.
