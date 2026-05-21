# Async timer

This package provides an async timer object, that should have been part of batteries.

[![Tests](docs/badges/tests.svg)](docs/badges/tests.svg)
[![Coverage](docs/badges/coverage.svg)](docs/badges/coverage.svg)

[![CI](https://github.com/VRGhost/async-timer/actions/workflows/main.yml/badge.svg)](https://github.com/VRGhost/async-timer/actions/workflows/main.yml)

## Purpose

Sometimes, you need a way to make something happen over and over again at certain times, like updating information or sending reminders. That's where Async Timer comes in. It lets you set up these repeated actions easily.

This package is particularly useful for tasks like automatically updating caches in the background without disrupting the primary application's workflow.

## Features

* **Zero Dependencies**: Written entirely in Python, Async Timer operates independently without needing any external libraries.
* **Versatility in Callables**: It accommodates various callable types, including:
  * Synchronous functions
  * Asynchronous functions
  * Synchronous generators
  * Asynchronous generators
* **Wait for the Next Tick**: You can set it up so your program waits for the timer to do its thing, and then continues.
* **Keep Getting Updates**: You can use it in a loop to keep getting updates every time the timer goes off.
* **Cancel anytime**: The timer object can be stopped at any time either explicitly by calling `stop()`/`cancel()` method OR it can stop automatically on an awaitable resolving (the `cancel_aws` constructor argument). `await cancel()` waits for cleanup to complete before returning, and is safe to call from inside the target or its callbacks.
* **Restartable**: Calling `start()` after `cancel()` resumes the timer with fresh pacemaker, fanout, and target-caller state (generator targets get a fresh generator). Restart is rejected with a clear error if the original construction used `cancel_aws`, since those awaitables are single-shot.
* **Test friendly**: The package provides an additional `mock_async_timer.MockTimer` class with mocked sleep function to aid in your testing

## Requirements

Python 3.9 or newer.

## Installation

```bash
pip install async-timer
```

## Example Usage

### FastAPI

This snippet starts fastapi webserver with the `refresh_db` function being executed every 5 seconds, refresing a shared `DB_CACHE` object.

```python

import contextlib
import time

import uvicorn
from fastapi import FastAPI

import async_timer

DB_CACHE = {"initialised": False}


async def refresh_db():
    global DB_CACHE
    DB_CACHE |= {"initialised": True, "cur_value": time.time()}


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    async with async_timer.Timer(delay=5, target=refresh_db) as timer:
        await timer.wait(hit_count=1)  # block until the timer triggers at least once
        yield


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"message": "Hello World", "db_cache": DB_CACHE}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

```

### join()
```python
import asyncio
import async_timer

async def main():
    timer = async_timer.Timer(12, target=lambda: 42)
    timer.start()
    val = await timer.join()  # `val` will be set to 42 after the first tick
    await timer.cancel()

asyncio.run(main())
```

### async for loop
```python
import asyncio
import time
import async_timer

async def main():
    async with async_timer.Timer(14, target=time.time) as timer:
        async for time_rv in timer:
            print(f"{time_rv=}")  # Prints current time every 14 seconds

asyncio.run(main())
```