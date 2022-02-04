import asyncio
from typing import Optional, TypeVar

__version__ = "0.1.0"

T = TypeVar("T")

class Channel:
    def __init__(self, queue_size: Optional[int] = None):
        # Stores the work to be done
        self._q = asyncio.Queue(maxsize=queue_size if queue_size else 0)

        # Has cancel been called?
        self._cancel = asyncio.Event()

        # Has stop been called?
        self._stopped = asyncio.Event()
        
        # Has the cleanup been completed?
        self._done = asyncio.Event()

        self._active_tasks = 0
        self._active_tasks_lock = asyncio.Lock()

    async def _increment_active(self):
        async with self._active_tasks_lock:
            self._active_tasks += 1

    async def _decrement_active(self):
        async with self._active_tasks_lock:
            self._active_tasks -= 1

    def __aiter__(self):
        return self

    async def __anext__(self) -> T:
        # Check if already done
        if self._cancel.is_set() or self._done.is_set():
            raise StopAsyncIteration

        # Check if stopped and the queue is empty
        if self._stopped.is_set() and self._q.empty():
            raise StopAsyncIteration
        # ...otherwise, more data is available

        # Increment the active tasks
        await self._increment_active()

        # Create coroutines to wait for data and cancellation
        cencel_waiter = self._cancel.wait()
        next_item = self._q.get()

        # Wait for the first to return from either data or cancellation
        done, pending = await asyncio.wait(
            [next_item, cencel_waiter], 
            return_when=asyncio.FIRST_COMPLETED
        )

        # Decrement the active tasks
        await self._decrement_active()

        # If the cancellation event was returned, raise a CancelledError
        if cancel_waiter in done:
            # Send a cancellation signal to the active task
            asyncio.create_task(next_item).cancel()

            # Stop iterating
            raise StopAsyncIteration

        self._q.task_done()

        # Otherwise, return the data
        return next_item.result()
            

    async def send(self, data: T):
        """Send `data` to the channel."""
        await self._q.put(data)

    async def cancel(self):
        """Hard stop"""
        self._cancel.set()
        self._done.set()

    async def _drain(self):
        await self._q.join()

    async def done(self):
        """Soft stop"""
        self._stopped.set()
        self._done.set()


async def do(i: int, c: Channel):
    async for d in c:
        print(f"{i} received {d}")
        await asyncio.sleep(i)
        print(f"{i} completed {d}")


async def main():
    c = Channel()

    # Create 5 workers
    for i in range(5):
        asyncio.create_task(do(i, c))

    # Send data to the channel
    for i in range(30):
        await c.send(i)

    # Wait for the tasks to complete
    await c.done()


asyncio.run(main())
