"""Real progress-query regression against synthetic, disposable PostgreSQL."""
import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
import time
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import urlparse

import asyncpg
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from chessism_api.database.neo4j import sync_ingestion as progress
from chessism_api.database.postgresql.relational_storage import RelationalStorage


async def main():
    url = os.environ['GRAPH_PROGRESS_TEST_DB']
    if urlparse(url).hostname != 'graph-progress-test-db':
        raise RuntimeError('Use only the disposable graph-progress-test-db.')
    conn = await asyncpg.connect(url)
    engine = create_async_engine(url.replace('postgresql://', 'postgresql+asyncpg://'))
    factory = async_sessionmaker(engine)
    layout = RelationalStorage('public', 'archive', 'reads', 'relational_v3')
    now = datetime.now(timezone.utc)
    run = SimpleNamespace(player_name='Sample', status='complete',
                          started_at=now-timedelta(minutes=10), finished_at=now)
    try:
        await conn.execute('''CREATE SCHEMA graph_sync;
          CREATE TABLE graph_sync.control(id bool PRIMARY KEY,enabled bool,heartbeat timestamptz);
          INSERT INTO graph_sync.control VALUES(true,true,now());
          CREATE TABLE graph_sync.event(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            kind text,ids bigint[],offset_items int DEFAULT 0,completed_at timestamptz,
            error text,created_at timestamptz,batch_id uuid);
          CREATE INDEX ON graph_sync.event(created_at);
          CREATE TABLE player(id bigint PRIMARY KEY,player_name text UNIQUE);
          CREATE TABLE game_player(game_link bigint,player_id bigint);
          CREATE INDEX ON game_player(player_id,game_link);
          CREATE TABLE game_fen_association(game_link bigint,fen_id bigint);
          CREATE INDEX ON game_fen_association(fen_id);
          CREATE INDEX ON game_fen_association(game_link);
          INSERT INTO player VALUES(1,'sample'),(2,'other');
          INSERT INTO game_player VALUES(10,1),(11,1),(12,1),(20,2);
          INSERT INTO game_fen_association VALUES(10,100),(11,100),(12,101),(20,102);
        ''')

        async def event(kind, ids, offset=0, done=False, error=None, at=None, batch=None):
            return await conn.fetchval('''INSERT INTO graph_sync.event
              (kind,ids,offset_items,completed_at,error,created_at,batch_id)
              VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id''',
              kind, ids, offset, now-timedelta(seconds=1) if done else None,
              error, at or now-timedelta(minutes=5), batch)

        async def read():
            with patch.object(progress, 'AsyncDBSession', factory), patch.object(progress, 'configured_storage', return_value=layout):
                return await progress.ingestion_progress(run)

        # Mixed players, duplicate entities/events, offsets and retries must
        # retain the original semantics: all events for an entity must finish.
        await event('game', [10,20,11,12], offset=3)
        repeat = await event('game', [10], error='temporary failure')
        await event('score', [100,101,102], offset=1)
        await event('score', [100], done=True)
        await event('player', [1,2], done=True)
        await event('game', [999], at=run.started_at-timedelta(seconds=1))
        await event('game', [998], at=run.finished_at+timedelta(seconds=1))
        await event('game', [997], batch='00000000-0000-0000-0000-000000000001')
        result = await read()
        assert (result['processed'],result['total'],result['status']) == (3,6,'retrying'), result
        assert '1 / 3 games' in result['detail'] and '1 / 2 evaluations' in result['detail']
        await conn.execute('UPDATE graph_sync.event SET completed_at=$1,error=NULL WHERE id=$2',now,repeat)
        assert (await read())['processed'] == 4
        await conn.execute('UPDATE graph_sync.event SET completed_at=$1,error=NULL',now)
        result = await read()
        assert (result['processed'],result['total'],result['status']) == (6,6,'complete'), result
        run.status='running';run.finished_at=None
        assert (await read())['status'] == 'waiting'
        run.status='complete';run.finished_at=now
        await conn.execute('UPDATE graph_sync.control SET enabled=false')
        assert await read() is None
        await conn.execute('UPDATE graph_sync.control SET enabled=true')

        # Production-sized poll: 17,485 games, 31,202 scores and 1.75M links.
        # Multiple game links per score previously repeated the full game CTE.
        await conn.execute('''TRUNCATE graph_sync.event,game_player,game_fen_association;
          INSERT INTO game_player SELECT i,1 FROM generate_series(1,17485) i;
          INSERT INTO game_player SELECT i+17485,2 FROM generate_series(1,17485) i;
          INSERT INTO game_fen_association SELECT ((i-1)%17485)+1,((i-1)%31202)+1
            FROM generate_series(1,1748500) i;
          ANALYZE game_player; ANALYZE game_fen_association;
        ''')
        for kind, total in [('game',17485),('score',31202)]:
            for start in range(1,total+1,1000):
                await event(kind,list(range(start,min(start+1000,total+1))))
        await conn.execute('ANALYZE graph_sync.event')
        started = time.perf_counter()
        result = await read()
        print(json.dumps({'case':'production_sized_progress','seconds':time.perf_counter()-started,
                          'processed':result['processed'],'total':result['total']}),flush=True)
        assert (result['processed'],result['total'],result['status']) == (0,48687,'working'),result
        await conn.execute('UPDATE graph_sync.event SET offset_items=cardinality(ids),completed_at=$1',now)
        result = await read()
        assert result['processed'] == 48687 and result['status'] == 'complete'
        print('PASS: mixed-player scoping, duplicate events, offsets, retries, date/backfill exclusion, live completion and production-sized progress query',flush=True)
    finally:
        await conn.close()
        await engine.dispose()


if __name__ == '__main__':
    asyncio.run(main())
