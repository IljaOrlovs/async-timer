import pytest

import async_timer.target_caller as target_caller


@pytest.mark.asyncio
async def test_sync_fn(count_fn):
    caller = target_caller.Caller(target=count_fn)

    assert await caller.next() == 0
    assert await caller.next() == 1
    assert await caller.next() == 2


@pytest.mark.asyncio
async def test_async_fn(async_count_fn):
    caller = target_caller.Caller(target=async_count_fn)

    assert await caller.next() == 0
    assert await caller.next() == 1
    assert await caller.next() == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("called", [True, False])
async def test_sync_gen_callable(count_gen, called):
    if called:
        inp = count_gen()
    else:
        inp = count_gen
    caller = target_caller.Caller(target=inp)

    assert await caller.next() == 0
    assert await caller.next() == 1
    assert await caller.next() == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("called", [True, False])
async def test_async_gen_callable(async_gen, called):
    if called:
        inp = async_gen()
    else:
        inp = async_gen
    caller = target_caller.Caller(target=inp)

    assert await caller.next() == 0
    assert await caller.next() == 1
    assert await caller.next() == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("called", [True, False])
async def test_iterable(count_iterable, called):
    if called:
        inp = count_iterable()
    else:
        inp = count_iterable
    caller = target_caller.Caller(target=inp)

    assert await caller.next() == 0
    assert await caller.next() == 1
    assert await caller.next() == 2
