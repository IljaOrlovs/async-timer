"""FastAPI lifespan: warm a cache before serving, refresh on an interval.

Run:
    pip install fastapi uvicorn async-timer
    python examples/fastapi_lifespan.py

The `await timer.wait(hit_count=1)` line is the trick — the app does not
start accepting requests until the first refresh has populated the cache.
"""

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
        await timer.wait(hit_count=1)
        yield


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"db_cache": DB_CACHE}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
