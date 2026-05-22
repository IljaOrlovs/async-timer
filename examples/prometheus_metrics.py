"""Periodic metrics emission for Prometheus.

Run:
    pip install prometheus-client async-timer
    python examples/prometheus_metrics.py
    curl localhost:9000/metrics

Uses `fixed_rate` so gauges are sampled on a stable wall-clock grid even
if the scrape target gets backed up. `jitter=0.1` smears the load across
nearby slots to avoid thundering-herd against the source.
"""

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
        await asyncio.Event().wait()  # serve forever


if __name__ == "__main__":
    asyncio.run(main())
