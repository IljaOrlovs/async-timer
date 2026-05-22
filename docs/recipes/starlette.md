# Starlette: lifespan without the FastAPI sugar

Same pattern as the [FastAPI recipe](fastapi.md); Starlette just takes
the lifespan as a constructor arg.

```python
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
```

The same notes apply — `async with` for cleanup, `wait(hit_count=1)` to
gate startup.

## Runnable version

See [`examples/starlette_lifespan.py`](../../examples/starlette_lifespan.py).
