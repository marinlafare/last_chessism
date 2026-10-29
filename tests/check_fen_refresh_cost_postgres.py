"""Synthetic, socket-only regression check for evaluation refresh trigger cost.

No live data, credentials, GPU, or cloud access. Keep both changed-position and
high-fanout unchanged-position cases: tiny fixtures hid the original regression.
"""
import asyncio
import json
import time

from sqlalchemy import text
from chessism_api.database.relational_storage import RelationalStorage
from chessism_api.operations import fen_summary_recovery as recovery
from tests import check_canonical_blunders_postgres as base


async def check(factory, connection, _layout):
    layout = RelationalStorage('cost', 'archive', 'reads', 'relational_v3')
    await connection.execute('''CREATE SCHEMA cost;
        CREATE TABLE cost.fen(fen_id bigint PRIMARY KEY, fen text UNIQUE,
            score integer, analysis_source text, n_games bigint DEFAULT 0);
        CREATE TABLE cost.game_fen_association(game_link bigint,fen_id bigint);
        CREATE INDEX cost_fen_links ON cost.game_fen_association(fen_id);
        CREATE TABLE cost.game_player(game_link bigint);
        CREATE TABLE public.cloud_fen_claim(fen text PRIMARY KEY);
        INSERT INTO cost.fen SELECT i,'position-'||i,24,'stockfish',0
            FROM generate_series(1,20000) i;
        INSERT INTO cost.game_fen_association
            SELECT i/100+1,CASE WHEN i%2=0 THEN 1 ELSE i%20000+1 END
            FROM generate_series(1,400000) i;
        ANALYZE cost.fen; ANALYZE cost.game_fen_association;
        SET statement_timeout='20s'; SET work_mem='32MB'; SET jit=off;
        SET max_parallel_workers_per_gather=0;''')
    async with factory() as s, s.begin():
        await recovery.install(s, layout)

    # Ingestion's ON CONFLICT ... WHERE false fires the UPDATE statement trigger
    # with empty transition tables before its nonempty appearance update.
    for _ in range(8):
        await connection.execute('UPDATE cost.fen SET n_games=n_games WHERE false')

    # Capture the real trigger SQL from the database, not a reimplemented query.
    # EXPLAIN includes the UPDATE's trigger time; no performance ratio assertions
    # because CI hardware varies. The separate no-read guard below is structural.
    started = time.perf_counter()
    plan = json.loads(await connection.fetchval('''EXPLAIN (ANALYZE,FORMAT JSON)
        UPDATE cost.fen SET n_games=n_games+1'''))[0]
    print(json.dumps({'case': 'appearance_only', 'seconds': time.perf_counter()-started,
                      'triggers': plan.get('Triggers')}), flush=True)
    assert await connection.fetchval('SELECT count(*) FROM cost.fen_summary_refresh') == 0

    # Block access to associations from another connection. An irrelevant FEN
    # update must not even try to read them (not merely produce an empty queue).
    async with factory() as blocker, blocker.begin():
        await blocker.execute(text('LOCK cost.game_fen_association IN ACCESS EXCLUSIVE MODE'))
        await connection.execute("SET lock_timeout='300ms'")
        await connection.execute('UPDATE cost.fen SET n_games=n_games+1')
        await connection.execute('UPDATE cost.fen SET score=score,analysis_source=analysis_source')
        await connection.execute('UPDATE cost.fen SET score=0 WHERE false')
        await connection.execute("SET lock_timeout='0'")

    # Mixed rows: only genuine changes queue work; source-only changes, NULLs,
    # cloud reservations, coalescing and transaction rollback remain supported.
    await connection.execute("INSERT INTO public.cloud_fen_claim VALUES('position-2')")
    await connection.execute('''UPDATE cost.fen SET score=CASE fen_id
        WHEN 1 THEN 25 WHEN 2 THEN NULL ELSE score END,n_games=n_games+1''')
    expected = await connection.fetchval('SELECT count(DISTINCT game_link) FROM cost.game_fen_association WHERE fen_id=1')
    assert await connection.fetchval('SELECT count(*) FROM cost.fen_summary_refresh') == expected
    await connection.execute('TRUNCATE cost.fen_summary_refresh')
    await connection.execute('''UPDATE cost.fen SET analysis_source=NULL WHERE fen_id=4''')
    assert await connection.fetchval('SELECT count(*) FROM cost.fen_summary_refresh') == await connection.fetchval(
        'SELECT count(DISTINCT game_link) FROM cost.game_fen_association WHERE fen_id=4')
    await connection.execute('TRUNCATE cost.fen_summary_refresh')
    tx = connection.transaction()
    await tx.start()
    await connection.execute('UPDATE cost.fen SET score=NULL WHERE fen_id=1')
    assert await connection.fetchval('SELECT count(*) FROM cost.fen_summary_refresh') == expected
    await tx.rollback()
    assert await connection.fetchval('SELECT count(*) FROM cost.fen_summary_refresh') == 0
    print('PASS: metadata/no-op/empty updates never read associations; actual score/source changes, cloud claims and rollback retain durable refresh', flush=True)


if __name__ == '__main__':
    base.check = check
    asyncio.run(base.main())
