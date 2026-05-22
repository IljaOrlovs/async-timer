"""aiohttp cleanup context: run a periodic task for the app's lifetime.

Run:
    pip install aiohttp async-timer
    python examples/aiohttp_cleanup_ctx.py

Cleanup contexts in aiohttp are the right home for background tasks tied
to app lifetime — the framework awaits both setup and teardown for you.
"""

import time

from aiohttp import web

import async_timer

CACHE: dict = {}


async def refresh() -> None:
    CACHE["ts"] = time.time()


async def timer_ctx(app: web.Application):
    async with async_timer.Timer(delay=5, target=refresh, name="cache") as timer:
        await timer.wait(hit_count=1)
        app["refresh_timer"] = timer
        yield


async def handle(request: web.Request) -> web.Response:
    return web.json_response(CACHE)


def make_app() -> web.Application:
    app = web.Application()
    app.cleanup_ctx.append(timer_ctx)
    app.router.add_get("/", handle)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host="0.0.0.0", port=8000)
