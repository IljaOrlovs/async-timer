# Graceful shutdown on SIGTERM / SIGINT

Kubernetes, systemd, and `docker stop` all send `SIGTERM` and give the
process a grace period to exit before escalating to `SIGKILL`. A
long-running asyncio app needs to translate that signal into a clean
cancel of its background timers.

```python
import asyncio
import signal
import time
import async_timer

async def work() -> float:
    return time.time()

async def main() -> None:
    timer = async_timer.Timer(delay=2, target=work, name="worker")
    timer.start()

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _handle(*_):
        stop.set()

    loop.add_signal_handler(signal.SIGINT, _handle)
    loop.add_signal_handler(signal.SIGTERM, _handle)

    await stop.wait()
    await timer.cancel()

asyncio.run(main())
```

## Two ways to stop

`loop.add_signal_handler` runs the callback on the loop thread, so
just setting an event works. If your signal handling lives outside the
loop entirely — a sync REST handler in a worker thread, a separate
signal-handling thread — use the threadsafe variant instead:

```python
import threading

def stop_in_other_thread():
    timer.cancel_threadsafe(timeout=5.0)

threading.Thread(target=stop_in_other_thread).start()
```

`cancel_threadsafe` marshals the cancel back to the loop and blocks
until cleanup completes (or the timeout fires).

## Why this matters

`await timer.cancel()` is a hard requirement, not a nicety. Skipping
it leaves the timer task in the loop until process exit; if the target
is mid-call (e.g. a Redis SET in flight), the connection pool may not
close cleanly and clients see resets instead of orderly closes.

## Runnable version

See [`examples/graceful_sigterm.py`](../../examples/graceful_sigterm.py).
