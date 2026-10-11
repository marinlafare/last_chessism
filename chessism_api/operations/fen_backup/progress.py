"""Heartbeat even while PostgreSQL is finding the first export page."""
import asyncio
from contextlib import suppress
import json
import logging
import time

logger = logging.getLogger(__name__)


class Progress:
    def __init__(self, ctx, kind):
        self.redis = ctx.get('redis')
        self.started = time.monotonic()
        self.value = {'job_id': str(ctx.get('job_id') or kind), 'kind': kind,
                      'total': 0, 'processed': 0, 'failed': 0, 'bytes': 0,
                      'phase': 'starting', 'detail': 'Starting backup operation.', 'result': None}

    async def emit(self):
        self.value.update(updated_at=time.time(), elapsed_seconds=round(time.monotonic() - self.started, 1))
        if self.redis:
            try:
                await self.redis.set('chessism:job_progress:' + self.value['job_id'],
                                     json.dumps(self.value), ex=86400)
            except Exception:
                logger.warning('Could not update FEN-backup progress', exc_info=True)

    async def change(self, **values):
        self.value.update(values)
        await self.emit()

    async def heartbeat(self):
        while True:
            await asyncio.sleep(2)
            await self.emit()

    async def __aenter__(self):
        await self.emit()
        self.task = asyncio.create_task(self.heartbeat())
        return self

    async def __aexit__(self, kind, error, tb):
        self.task.cancel()
        with suppress(asyncio.CancelledError):
            await self.task
        if error:
            await self.change(phase='failed', failed=1, detail=str(error) or 'Backup interrupted; last published backup is unchanged.')
