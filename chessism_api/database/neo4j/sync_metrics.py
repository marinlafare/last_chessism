"""Bounded publication timings, without recording SQL/Cypher or payload data."""
from collections import defaultdict
import time


class TimedQueries:
    def __init__(self, transaction):
        self.transaction = transaction
        self.seconds = defaultdict(float)

    async def run(self, name, query, **parameters):
        started = time.monotonic()
        try:
            result = await self.transaction.run(query, **parameters)
        except BaseException:
            self.seconds[name] += time.monotonic() - started
            raise
        return _TimedResult(result, self.seconds, name, started)


class _TimedResult:
    """Measure through result consumption, not just Bolt query submission."""
    def __init__(self, result, seconds, name, started):
        self.result = result
        self.seconds = seconds
        self.name = name
        self.started = started
        self.recorded = False

    async def _consume(self, method, *args, **kwargs):
        try:
            return await method(*args, **kwargs)
        finally:
            if not self.recorded:
                self.seconds[self.name] += time.monotonic() - self.started
                self.recorded = True

    async def data(self, *args, **kwargs):
        return await self._consume(self.result.data, *args, **kwargs)

    async def single(self, *args, **kwargs):
        return await self._consume(self.result.single, *args, **kwargs)

    async def consume(self):
        return await self._consume(self.result.consume)
