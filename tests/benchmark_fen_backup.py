"""Synthetic benchmark only: CHESSISM_FEN_BACKUP_TEST_URL must end in
/chessism_fen_backup_test. Creates/drops a random schema and temporary files.
Run in separate processes with --rows 25000 and --rows 100000 to compare RSS.
"""
import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import resource
import tempfile
import time
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Fen, FenContinuation
from chessism_api.operations.fen_backup import jobs, archive, catalog


async def run(count):
    url = os.environ['CHESSISM_FEN_BACKUP_TEST_URL']
    if not url.endswith('/chessism_fen_backup_test') or not 1 <= count <= 1_000_000:
        raise ValueError('Requires isolated backup-test database and 1–1,000,000 synthetic rows')
    schema = 'fen_backup_bench_' + uuid4().hex
    admin = create_async_engine(url)
    engine = create_async_engine(url, connect_args={'server_settings': {'search_path': schema}})
    AsyncDBSession.configure(bind=engine)
    try:
        async with admin.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with engine.begin() as conn:
            await conn.run_sync(lambda c: Fen.__table__.create(c))
            await conn.run_sync(lambda c: FenContinuation.__table__.create(c))
            # Synthetic identifiers, never positions selected from production.
            await conn.execute(text('''INSERT INTO fen (fen,n_games,moves_counter,score,next_moves,analyzed_at)
                SELECT repeat('synthetic-',6)||i,1,'{}',i%200,'e2e4',now()-interval '1 day'
                FROM generate_series(1,:n) AS i'''), {'n': count})
            await conn.execute(text("INSERT INTO fen_continuation (fen_fen,rank,move,score) SELECT fen,1,'e2e4',score FROM fen"))
            await conn.execute(text('ANALYZE fen'))
            await conn.execute(text('ANALYZE fen_continuation'))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            phases = []
            for phase in ('base', 'delta', 'unchanged'):
                if phase == 'delta':
                    async with engine.begin() as conn:
                        await conn.execute(text('''UPDATE fen SET score=score+1,analyzed_at=now()
                            WHERE fen IN (SELECT fen FROM fen ORDER BY fen LIMIT 1000)'''))
                start = time.monotonic()
                result = await jobs.create({}, root)
                phases.append({'phase': phase, 'seconds': round(time.monotonic()-start, 3),
                               'exported': result['exported_records'], 'new_bytes': result['new_bytes'],
                               'parts': result['parts']})
                if phase == 'base':
                    base = catalog.load(root, result['filename'])['files']
            if any(archive.sha256(root / p['file']) != p['sha256'] for p in base):
                raise AssertionError('Incremental update modified its base')
            print(json.dumps({'synthetic_fens': count, 'peak_rss_mib': round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024, 1),
                              'phases': phases}, indent=2), flush=True)
    finally:
        await engine.dispose()
        async with admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=25000)
    asyncio.run(run(parser.parse_args().rows))
