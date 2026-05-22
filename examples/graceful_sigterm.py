"""Graceful shutdown on SIGTERM/SIGINT using cancel_threadsafe().

Run:
    pip install async-timer
    python examples/graceful_sigterm.py
    # then ctrl-C, or `kill -TERM <pid>` from another shell

Signal handlers run outside the event loop's normal flow, so reaching
across to the timer needs the threadsafe entry point. `cancel_threadsafe`
marshals the cancel back to the loop's thread and blocks until cleanup
is done — useful when the framework needs the process to exit cleanly.
"""

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
        # Runs on the loop thread (signal handler), so just set the event.
        stop.set()

    loop.add_signal_handler(signal.SIGINT, _handle)
    loop.add_signal_handler(signal.SIGTERM, _handle)

    print("running; ctrl-C to stop")
    await stop.wait()
    print("shutting down…")
    await timer.cancel()
    print(f"clean exit after {timer.hit_count} ticks")


if __name__ == "__main__":
    asyncio.run(main())
