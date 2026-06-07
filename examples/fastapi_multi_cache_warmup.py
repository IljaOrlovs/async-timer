"""FastAPI lifespan: warm several caches before serving traffic.

Run:
    pip install fastapi uvicorn async-timer
    python examples/fastapi_multi_cache_warmup.py

`TimerGroup.wait(hit_count=1)` blocks the lifespan until every member
timer has produced at least one tick — no request can hit the app
while any cache is still empty.
"""

import contextlib
import time

import uvicorn
from fastapi import FastAPI

import async_timer

DB_CACHE: dict = {}
FLAGS_CACHE: dict = {}
PRICING_CACHE: dict = {}


async def refresh_db():
    DB_CACHE["ts"] = time.time()


async def refresh_flags():
    FLAGS_CACHE["ts"] = time.time()


async def refresh_pricing():
    PRICING_CACHE["ts"] = time.time()


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    async with async_timer.TimerGroup() as group:
        group.add(async_timer.Timer(5, target=refresh_db, name="db"))
        group.add(async_timer.Timer(30, target=refresh_flags, name="flags"))
        group.add(async_timer.Timer(60, target=refresh_pricing, name="pricing"))
        # Single await — all three caches populated before yield.
        await group.wait(hit_count=1)
        yield


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"db": DB_CACHE, "flags": FLAGS_CACHE, "pricing": PRICING_CACHE}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
