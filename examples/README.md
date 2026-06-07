# Examples

Self-contained runnable scripts. Each file is one pattern. See
[`docs/recipes/`](../docs/recipes/) for narrative walk-throughs of the same patterns.

| File | What it shows |
| --- | --- |
| [`fastapi_lifespan.py`](fastapi_lifespan.py) | Warm a cache before the app accepts requests; refresh on an interval. |
| [`fastapi_multi_cache_warmup.py`](fastapi_multi_cache_warmup.py) | `TimerGroup.wait(hit_count=1)` — warm several caches with one await. |
| [`cache_invalidate_all.py`](cache_invalidate_all.py) | `TimerGroup.trigger()` — admin endpoint that force-refreshes every cache. |
| [`starlette_lifespan.py`](starlette_lifespan.py) | Same pattern, framework-minimal. |
| [`aiohttp_cleanup_ctx.py`](aiohttp_cleanup_ctx.py) | Background task tied to aiohttp app lifetime. |
| [`prometheus_metrics.py`](prometheus_metrics.py) | `fixed_rate` sampling with `TimerGroup` for multiple gauges. |
| [`redis_heartbeat.py`](redis_heartbeat.py) | Liveness key with TTL — die quietly, key expires. |
| [`graceful_sigterm.py`](graceful_sigterm.py) | Clean shutdown from SIGTERM/SIGINT via the loop. |
