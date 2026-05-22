# aiohttp: cleanup context for app-lifetime tasks

aiohttp's `cleanup_ctx` is the framework's blessed home for background
tasks that should live as long as the app does. It awaits both the
setup half (before `yield`) and the teardown half (after), so you get
the same guarantees as FastAPI's lifespan.

```python
import time
from aiohttp import web
import async_timer

CACHE: dict = {}

async def refresh() -> None:
    CACHE["ts"] = time.time()

async def timer_ctx(app: web.Application):
    async with async_timer.Timer(delay=5, target=refresh, name="cache") as timer:
        await timer.wait(hit_count=1)
        app["refresh_timer"] = timer   # available to handlers if needed
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
```

## Why `cleanup_ctx` and not `on_startup` / `on_cleanup`

Two separate hooks force you to keep the timer in module scope and
remember to clean it up symmetrically. `cleanup_ctx` is a single async
generator: the `Timer`'s own `async with` handles cancellation, so a
crash in setup or teardown can't leave a stranded task.

## Runnable version

See [`examples/aiohttp_cleanup_ctx.py`](../../examples/aiohttp_cleanup_ctx.py).
