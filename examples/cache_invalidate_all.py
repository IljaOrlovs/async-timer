"""Cache-invalidate-all via TimerGroup.trigger().

A FastAPI admin endpoint that forces every refresh timer in the group
to re-fetch right now, without restarting them. Regular schedule
resumes from the trigger moment.

Run:
    pip install fastapi uvicorn async-timer
    python examples/cache_invalidate_all.py
    curl -X POST http://localhost:8000/admin/refresh-all
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


GROUP = async_timer.TimerGroup(name="caches")


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    GROUP.add(async_timer.Timer(60, target=refresh_db, name="db"))
    GROUP.add(async_timer.Timer(300, target=refresh_flags, name="flags"))
    GROUP.add(async_timer.Timer(900, target=refresh_pricing, name="pricing"))
    async with GROUP:
        await GROUP.wait(hit_count=1)
        yield


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"db": DB_CACHE, "flags": FLAGS_CACHE, "pricing": PRICING_CACHE}


@app.post("/admin/refresh-all")
async def refresh_all():
    """Force every cache in the group to refresh now."""
    results = await GROUP.trigger(timeout=10.0, return_exceptions=True)
    return {
        "refreshed": [
            {"name": t.name, "ok": not isinstance(rv, BaseException)}
            for t, rv in results
        ]
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
