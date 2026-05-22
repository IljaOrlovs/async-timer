# Prometheus: periodic metric sampling

Prometheus scrapes pull from the process, but the *values* a gauge
exposes often need to be computed on a schedule (queue depths, cache
sizes, in-flight counters that aren't tracked event-by-event). A timer
does the sampling; the HTTP endpoint just reads the latest value.

```python
import asyncio
import random
from prometheus_client import Gauge, start_http_server
import async_timer

QUEUE_DEPTH = Gauge("queue_depth", "Items in the work queue")
INFLIGHT = Gauge("inflight_requests", "Currently in-flight requests")

async def sample_queue() -> None:
    QUEUE_DEPTH.set(random.randint(0, 100))

async def sample_inflight() -> None:
    INFLIGHT.set(random.randint(0, 20))

async def main() -> None:
    start_http_server(9000)
    async with async_timer.TimerGroup() as group:
        group.add(async_timer.Timer(5, target=sample_queue, mode="fixed_rate",
                                    jitter=0.1, name="queue_depth"))
        group.add(async_timer.Timer(5, target=sample_inflight, mode="fixed_rate",
                                    jitter=0.1, name="inflight"))
        await asyncio.Event().wait()

asyncio.run(main())
```

## Why `fixed_rate`

Default `fixed_delay` waits `delay` seconds *after* the previous tick
finishes — sampling drifts forward as samples take measurable time.
For metrics, you usually want a stable wall-clock cadence so adjacent
points are comparable. `fixed_rate` anchors ticks to `t0 + n*delay`;
if the loop falls behind a whole slot it logs a warning and skips
ahead rather than fanning out a burst of catch-up calls.

## Why `jitter`

If many processes sample the same upstream at exactly the same cadence
they hit it in lockstep. `jitter=0.1` smears each sleep by up to ±10%,
breaking the synchronisation without meaningfully shifting the mean
rate.

## Why `TimerGroup`

The group's `__aexit__` cancels every member, even if one of the
timers fails. Keeps a metrics process from leaving stranded tasks on
shutdown.

## Runnable version

See [`examples/prometheus_metrics.py`](../../examples/prometheus_metrics.py).
