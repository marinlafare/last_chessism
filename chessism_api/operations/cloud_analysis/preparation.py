"""Short preparation steps and buffered, native-column performance records."""
from contextlib import contextmanager
from datetime import datetime, timezone
import logging
import time

from sqlalchemy import text
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudPhaseTiming

logger = logging.getLogger(__name__)


async def refresh_claim_statistics():
    # Empty -> tens of thousands of claims can happen before autovacuum wakes.
    # A stale one-row estimate causes a quadratic materialized anti-join on the
    # next selection. Refresh ONLY this small table, not the enormous Fen table.
    # Separate transaction also covers controller restarts between reservations.
    async with AsyncDBSession() as session, session.begin():
        await session.execute(text('ANALYZE cloud_fen_claim'))


class PreparationTimings:
    """Buffer a handful of rows per unit, never write from inside its locks.

    Persist after releasing reservation locks: a second connection inserting a
    timing FK while the first holds a run FOR UPDATE could otherwise deadlock.
    Metrics must never invalidate an already committed, durable reservation.
    """
    def __init__(self):
        self.rows = []

    @contextmanager
    def measure(self, phase):
        started_at, started = datetime.now(timezone.utc), time.monotonic()
        outcome, error = 'complete', None
        try:
            yield
        except BaseException as exc:
            outcome, error = 'failed', f'{type(exc).__name__}: {exc}'[:1000]
            raise
        finally:
            self.rows.append(dict(phase=phase, started_at=started_at,
                finished_at=datetime.now(timezone.utc), duration_seconds=time.monotonic() - started,
                outcome=outcome, error=error))

    async def save(self, job_id, run_id):
        for row in self.rows:
            logger.info('Cloud preparation job=%s run=%s phase=%s seconds=%.3f outcome=%s',
                        job_id, run_id, row['phase'], row['duration_seconds'], row['outcome'])
        try:
            async with AsyncDBSession() as session, session.begin():
                session.add_all([CloudPhaseTiming(job_id=job_id, run_id=run_id, **row) for row in self.rows])
        except Exception:
            logger.exception('Preparation timing persistence failed; reservation state is unchanged')
