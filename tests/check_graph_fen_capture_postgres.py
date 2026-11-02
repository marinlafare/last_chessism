"""Synthetic trigger correctness/cost regression; isolated PostgreSQL only."""
import asyncio
import json
import os
import time
from urllib.parse import urlparse

import asyncpg
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from chessism_api.database.neo4j import sync_schema
from chessism_api.database.neo4j.fen_capture import function_sql, upgrade
from chessism_api.database.postgresql.relational_storage import RelationalStorage


OLD_FUNCTION = """CREATE OR REPLACE FUNCTION graph_sync.capture_fen_update()
RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
  IF NOT EXISTS(SELECT 1 FROM graph_sync.control WHERE id AND enabled) THEN RETURN NULL; END IF;
  INSERT INTO graph_sync.event(kind,ids,source_job_id)
  SELECT 'score',array_agg(fen_id ORDER BY fen_id),job_id FROM (
    SELECT n.fen_id,r.job_id,
      (row_number() OVER(PARTITION BY r.job_id ORDER BY n.fen_id)-1)/1000 AS chunk
    FROM new_rows n JOIN old_rows o USING(fen_id)
    LEFT JOIN public.cloud_fen_claim c ON c.fen=n.fen
    LEFT JOIN public.cloud_analysis_run r ON r.id=c.run_id
    WHERE (n.score,n.analysis_source,n.tablebase_wdl,n.tablebase_dtz)
      IS DISTINCT FROM (o.score,o.analysis_source,o.tablebase_wdl,o.tablebase_dtz)
  ) changed GROUP BY job_id,chunk ORDER BY job_id NULLS LAST,chunk;
  RETURN NULL;
END $$;"""


async def main():
    url = os.environ['GRAPH_FEN_TEST_DB']
    if urlparse(url).hostname != 'fen-capture-test-db':
        raise RuntimeError('Use the disposable fen-capture-test-db, never live data.')
    conn = await asyncpg.connect(url)
    engine = create_async_engine(url.replace('postgresql://', 'postgresql+asyncpg://'))
    factory = async_sessionmaker(engine)
    layout = RelationalStorage('public', 'archive', 'reads', 'relational_v3')
    try:
        await conn.execute('''
          CREATE TABLE player(id bigint PRIMARY KEY,player_name text,joined bigint,deleted_at timestamptz);
          CREATE TABLE game(link bigint PRIMARY KEY,white_player_id bigint,black_player_id bigint,
            mode_id smallint,played_at timestamptz,white_result float,black_result float,
            white_str_result text,black_str_result text,n_moves int,fens_done bool);
          CREATE TABLE fen(fen_id bigint PRIMARY KEY,fen text UNIQUE,score integer,
            analysis_source text,tablebase_wdl int,tablebase_dtz int,n_games bigint DEFAULT 0);
          CREATE TABLE game_fen_association(game_link bigint,fen_id bigint);
          CREATE TABLE moves(link bigint);
          INSERT INTO fen(fen_id,fen) SELECT i,'position-'||i FROM generate_series(1,30000) i;
          ANALYZE fen; SET statement_timeout='5s'; SET jit=off;
        ''')

        async def install():
            async with factory() as session, session.begin():
                await sync_schema.install(session, layout)
            await conn.execute('UPDATE graph_sync.control SET enabled=true')

        # Both install variants, including a repeated installation, use the same
        # function as a live function-only upgrade.
        await install()
        await conn.execute('UPDATE fen SET score=10 WHERE fen_id=1')
        assert await conn.fetchval('SELECT ids FROM graph_sync.event') == [1]
        await conn.execute('''TRUNCATE graph_sync.event;
          CREATE TABLE cloud_analysis_run(id text PRIMARY KEY,job_id text);
          CREATE TABLE cloud_fen_claim(fen text PRIMARY KEY,run_id text);
          INSERT INTO cloud_analysis_run VALUES('a','job-a'),('b','job-b');
          INSERT INTO cloud_fen_claim SELECT fen,CASE WHEN fen_id<=1100 THEN 'a' ELSE 'b' END
            FROM fen WHERE fen_id<=2200;
          ANALYZE cloud_fen_claim;
        ''')
        await install()
        await install()
        before_oid = await conn.fetchval("SELECT 'graph_sync.capture_fen_update()'::regprocedure::oid")
        async with factory() as session, session.begin():
            result = await upgrade(session, layout)
        assert result['function_oid'] == before_oid and result['cloud_tracking']
        assert await conn.fetchval("SELECT 'graph_sync.capture_fen_update()'::regprocedure::oid") == before_oid

        # Exercise the actual cached-plan regression: empty ON CONFLICT updates
        # precede a large appearance-only update on the same connection.
        for label, sql in [('old', OLD_FUNCTION), ('fixed', function_sql(layout, cloud_tracking=True))]:
            await conn.execute(sql)
            for _ in range(8):
                await conn.execute('''INSERT INTO fen(fen_id,fen) VALUES(1,'position-1')
                  ON CONFLICT(fen) DO UPDATE SET n_games=fen.n_games WHERE false''')
            started = time.perf_counter()
            try:
                plan = json.loads(await conn.fetchval('''EXPLAIN (ANALYZE,FORMAT JSON)
                  UPDATE fen SET n_games=n_games+1'''))[0]
                measurement = {'triggers': plan.get('Triggers')}
            except asyncpg.QueryCanceledError:
                if label != 'old':
                    raise
                measurement = {'timeout_seconds': 5}
            print(json.dumps({'case': label, 'seconds': time.perf_counter()-started, **measurement}), flush=True)
            assert await conn.fetchval('SELECT count(*) FROM graph_sync.event') == 0

        # Counter/no-op updates must not even READ claims or the event table.
        # Holding exclusive locks proves the early return, not just zero inserts.
        blocker = await asyncpg.connect(url)
        try:
            async with blocker.transaction():
                await blocker.execute('LOCK cloud_fen_claim,cloud_analysis_run,graph_sync.event IN ACCESS EXCLUSIVE MODE')
                await conn.execute("SET lock_timeout='300ms'")
                await conn.execute('UPDATE fen SET n_games=n_games+1')
                await conn.execute('UPDATE fen SET score=score,analysis_source=analysis_source')
                await conn.execute('UPDATE fen SET score=0 WHERE false')
                await conn.execute("SET lock_timeout='0'")
        finally:
            await blocker.close()

        # Mixed changes, unchanged rows, chunking and same-transaction attribution.
        async with conn.transaction():
            await conn.execute('''UPDATE fen SET score=CASE WHEN fen_id<=2300 THEN 20 ELSE score END,
              n_games=n_games+1''')
            await conn.execute('DELETE FROM cloud_fen_claim')
        rows = await conn.fetch('SELECT kind,ids,source_job_id FROM graph_sync.event ORDER BY id')
        by_job = {}
        for row in rows:
            assert row['kind'] == 'score'
            assert 1 <= len(row['ids']) <= 1000
            assert row['ids'] == sorted(row['ids'])
            by_job.setdefault(row['source_job_id'], []).extend(row['ids'])
        assert by_job == {'job-a': list(range(1,1101)), 'job-b': list(range(1101,2201)), None: list(range(2201,2301))}

        # NULL transitions and tablebase/source-only changes remain exact.
        for statement in [
            'UPDATE fen SET score=NULL WHERE fen_id=1',
            "UPDATE fen SET analysis_source='stockfish' WHERE fen_id=1",
            'UPDATE fen SET analysis_source=NULL WHERE fen_id=1',
            'UPDATE fen SET tablebase_wdl=2 WHERE fen_id=1',
            'UPDATE fen SET tablebase_dtz=17 WHERE fen_id=1',
            'UPDATE fen SET tablebase_wdl=NULL,tablebase_dtz=NULL WHERE fen_id=1',
        ]:
            await conn.execute('TRUNCATE graph_sync.event')
            await conn.execute(statement)
            assert await conn.fetchval('SELECT ids FROM graph_sync.event') == [1]
            await conn.execute('TRUNCATE graph_sync.event')
            await conn.execute(statement)
            assert await conn.fetchval('SELECT count(*) FROM graph_sync.event') == 0

        tx = conn.transaction()
        await tx.start()
        await conn.execute('UPDATE fen SET score=99 WHERE fen_id=1')
        assert await conn.fetchval('SELECT ids FROM graph_sync.event') == [1]
        await tx.rollback()
        assert await conn.fetchval('SELECT count(*) FROM graph_sync.event') == 0
        await conn.execute('UPDATE graph_sync.control SET enabled=false')
        await conn.execute('UPDATE fen SET score=99 WHERE fen_id=1')
        assert await conn.fetchval('SELECT count(*) FROM graph_sync.event') == 0
        print('PASS: install variants, cached empty-update plan, no-op isolation, true score/source/tablebase changes, job attribution, chunking, rollback and disabled capture', flush=True)
    finally:
        await conn.close()
        await engine.dispose()


if __name__ == '__main__':
    asyncio.run(main())
