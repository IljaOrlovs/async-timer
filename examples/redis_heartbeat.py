"""Service heartbeat into Redis with a TTL — a common liveness pattern.

Run:
    pip install redis async-timer
    python examples/redis_heartbeat.py

If the process dies, the key expires and another component can notice.
`fixed_rate` keeps the heartbeat on a stable cadence regardless of how
long the SET round-trip takes.
"""

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
        print(f"heartbeating to {KEY} every 5s; ctrl-C to stop")
        try:
            await asyncio.Event().wait()
        finally:
            await client.delete(KEY)
            await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
