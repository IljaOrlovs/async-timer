"""Starlette lifespan: same pattern as FastAPI, without the framework sugar.

Run:
    pip install starlette uvicorn async-timer
    python examples/starlette_lifespan.py
"""

import contextlib
import time

import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

import async_timer

CACHE: dict = {}


async def refresh() -> None:
    CACHE["ts"] = time.time()


@contextlib.asynccontextmanager
async def lifespan(_app):
    async with async_timer.Timer(delay=5, target=refresh, name="cache") as timer:
        await timer.wait(hit_count=1)
        yield


async def root(_request):
    return JSONResponse(CACHE)


app = Starlette(lifespan=lifespan, routes=[Route("/", root)])


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
