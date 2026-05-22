# Redis heartbeat with TTL

Liveness pattern: every process writes a key with a short TTL on an
interval. If the process dies, the key expires; anything watching for
membership notices the absence.

```python
import asyncio
import os
import socket
import time
import redis.asyncio as redis
import async_timer

HOST = socket.gethostname()
KEY = f"heartbeat:{HOST}:{os.getpid()}"
TTL_SECONDS = 15

async def main() -> None:
    client = redis.from_url("redis://localhost:6379")

    async def beat() -> None:
        await client.set(KEY, str(time.time()), ex=TTL_SECONDS)

    async with async_timer.Timer(delay=5, target=beat, mode="fixed_rate",
                                 name="heartbeat") as timer:
        await timer.wait(hit_count=1)
        try:
            await asyncio.Event().wait()
        finally:
            await client.delete(KEY)
            await client.aclose()

asyncio.run(main())
```

## Pick TTL > 2 × delay

The TTL has to outlast the worst-case gap between beats — one missed
beat plus jitter plus the next attempt's network round-trip. `delay=5`
with `TTL_SECONDS=15` gives roughly three missed beats of headroom
before peers consider you gone, which is comfortable for most
deployments.

## Why `fixed_rate`

A slow SET round-trip shouldn't push subsequent beats later — that
would compound and eventually exceed TTL. `fixed_rate` keeps the
cadence anchored to the wall clock.

## Don't catch exceptions inside `beat()`

By default a target exception logs and stops the timer. That's the
correct behaviour for a heartbeat — a process that can no longer
write to Redis should *stop* claiming liveness, not pretend it's fine.
The TTL then expires naturally.

## Runnable version

See [`examples/redis_heartbeat.py`](../../examples/redis_heartbeat.py).
