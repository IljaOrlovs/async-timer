# Recipes

Narrative walk-throughs of common patterns. Each recipe explains the
*why* behind the shape of the code; for the bare runnable script, see
the corresponding file in [`examples/`](../../examples/).

## Web frameworks

* [FastAPI lifespan: warm a cache before serving traffic](fastapi.md)
* [Starlette lifespan](starlette.md)
* [aiohttp cleanup context](aiohttp.md)

## Observability

* [Prometheus periodic metric sampling](prometheus.md)

## Coordination

* [Redis heartbeat with TTL](redis_heartbeat.md)
* [Graceful shutdown on SIGTERM/SIGINT](graceful_shutdown.md)
