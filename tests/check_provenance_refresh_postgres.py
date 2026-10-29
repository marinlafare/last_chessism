"""Source-only scheduling, coalescing and atomic handoff; disposable DB only."""
import asyncio
import json
from unittest.mock import AsyncMock
from sqlalchemy import text
from tests import check_canonical_blunders_postgres as base
from chessism_api.operations import fen_summary_recovery as recovery
from chessism_api.operations.canonical_blunders import schema


async def check(factory, connection, layout):
    await connection.execute('''ALTER TABLE candidate.fen ADD COLUMN fen text;
        UPDATE candidate.fen SET fen='fixture-'||fen_id;
        UPDATE candidate.fen SET analysis_source=NULL WHERE fen_id=1;''')
    async with factory() as s, s.begin():
        await schema.install(s, layout)
    never_cpu = AsyncMock(side_effect=AssertionError('Unnecessary CPU refresh'))
    await connection.execute("UPDATE candidate.fen SET analysis_source='stockfish_legacy_assumed' WHERE fen_id=1")
    count = await connection.fetchval('SELECT count(*) FROM candidate.fen_summary_refresh')
    assert count > 0
    assert await connection.fetchval('SELECT bool_and(NOT refresh_engine) FROM candidate.fen_summary_refresh')
    before = await connection.fetchval('SELECT jsonb_agg(to_jsonb(s)) FROM candidate.game_player_engine_summary s')
    done = await recovery.recover_batch(factory=factory, refresh=never_cpu)
    assert done['source_only'] == count and done['engine_refreshed'] == done['pending'] == 0
    never_cpu.assert_not_awaited()
    assert before == await connection.fetchval('SELECT jsonb_agg(to_jsonb(s)) FROM candidate.game_player_engine_summary s')
    assert await connection.fetchval('SELECT count(*) FROM candidate.canonical_blunder_dirty') == count

    # Full reasons must survive a subsequent label-only change, in either order.
    await connection.execute('UPDATE candidate.fen SET score=score+1 WHERE fen_id=1')
    await connection.execute("UPDATE candidate.fen SET analysis_source='stockfish' WHERE fen_id=1")
    assert await connection.fetchval('SELECT bool_and(refresh_engine) FROM candidate.fen_summary_refresh')
    cpu = AsyncMock()
    done = await recovery.recover_batch(factory=factory, refresh=cpu)
    assert done['source_only'] == 0 and done['engine_refreshed'] == count
    cpu.assert_awaited_once()
    await connection.execute("UPDATE candidate.fen SET analysis_source=NULL WHERE fen_id=1")
    await connection.execute('UPDATE candidate.fen SET score=score+1 WHERE fen_id=1')
    assert await connection.fetchval('SELECT bool_and(refresh_engine) FROM candidate.fen_summary_refresh')
    await recovery.recover_batch(factory=factory, refresh=AsyncMock())

    # Same numeric score but newly tablebase is a semantic change, not metadata.
    await connection.execute("UPDATE candidate.fen SET analysis_source='tablebase' WHERE fen_id=1")
    assert await connection.fetchval('SELECT bool_and(refresh_engine) FROM candidate.fen_summary_refresh')
    await recovery.recover_batch(factory=factory, refresh=AsyncMock())
    await connection.execute("UPDATE candidate.fen SET analysis_source='stockfish' WHERE fen_id=1")
    assert await connection.fetchval('SELECT bool_and(refresh_engine) FROM candidate.fen_summary_refresh')
    await recovery.recover_batch(factory=factory, refresh=AsyncMock())

    # Association/ownership writes cannot be masked by a later source label.
    await connection.execute('UPDATE candidate.game_player SET is_white=is_white WHERE game_link=1')
    await connection.execute("UPDATE candidate.fen SET analysis_source='stockfish_legacy_assumed' WHERE fen_id=1")
    assert await connection.fetchval('SELECT refresh_engine FROM candidate.fen_summary_refresh WHERE game_link=1')
    await recovery.recover_batch(factory=factory, refresh=AsyncMock())

    # Failed handoff must roll back both the GPU enqueue and input acknowledgement.
    await connection.execute("UPDATE candidate.fen SET analysis_source='stockfish' WHERE fen_id=1")
    await connection.execute('''CREATE FUNCTION candidate.fail_cuda_enqueue() RETURNS trigger LANGUAGE plpgsql AS
        $$ BEGIN RAISE EXCEPTION 'synthetic handoff failure'; END $$;
        CREATE TRIGGER fail_cuda_enqueue BEFORE INSERT ON candidate.canonical_blunder_dirty
        FOR EACH ROW EXECUTE FUNCTION candidate.fail_cuda_enqueue();''')
    pending = await connection.fetchval('SELECT count(*) FROM candidate.fen_summary_refresh')
    try:
        await recovery.recover_batch(factory=factory, refresh=never_cpu)
    except Exception as error:
        assert 'synthetic handoff failure' in str(error)
    else:
        raise AssertionError('Failed handoff was acknowledged')
    assert await connection.fetchval('SELECT count(*) FROM candidate.fen_summary_refresh') == pending
    await connection.execute('DROP TRIGGER fail_cuda_enqueue ON candidate.canonical_blunder_dirty')
    await recovery.recover_batch(factory=factory, refresh=never_cpu)
    never_cpu.assert_not_awaited()
    print('PASS: source-only bypass, mixed-change precedence, tablebase semantics, ownership, atomic handoff and retry')
    await routing_check(factory, connection)


async def routing_check(factory, connection):
    from chessism_api.operations.evaluation_repair.refresh_routing import route
    from chessism_api.operations.evaluation_repair.legacy_policy import POLICY, SOURCE
    from chessism_api.operations.evaluation_repair.publish import install
    from chessism_api.operations.evaluation_repair.artifacts import digest
    await connection.execute('''CREATE TABLE public.ingestion_pipeline_run(status text,started_at timestamptz,finished_at timestamptz);
        CREATE TABLE public.algorithm_run(status text,created_at timestamptz,started_at timestamptz,finished_at timestamptz);
        CREATE TABLE public.analysis_time(created_at timestamptz);
        CREATE TABLE public.cloud_fen_claim(fen text);
        CREATE TABLE candidate.fen_continuation(fen_id bigint);
        ALTER TABLE public.cloud_analysis_job ADD COLUMN created_at timestamptz,ADD COLUMN updated_at timestamptz;
        TRUNCATE candidate.fen_summary_refresh,candidate.canonical_blunder_dirty;''')
    async with factory() as s, s.begin():
        await install(s)
    await connection.execute('INSERT INTO candidate.fen_summary_refresh(game_link) VALUES(101)')
    async def receipt(name, before, after):
        await connection.execute('INSERT INTO candidate.evaluation_repair_receipt(id,actor,payload) VALUES($1,\'test\',$2)',
            digest(name), json.dumps({'policy':POLICY,'run_id':'fixture','before':before,'after':after}))
    async with connection.transaction():
        await receipt('full', [{'score':12,'analysis_source':None}], [{'score':0,'analysis_source':'tablebase'}])
    async with connection.transaction():
        await connection.execute('''INSERT INTO candidate.fen_summary_refresh(game_link) VALUES(101),(102),(103),(104),(105)
            ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision''')
        await receipt('source', [{'score':24,'analysis_source':None}], [{'score':24,'analysis_source':SOURCE}])
    # A later writer and an errored request must never be downgraded.
    await connection.execute('UPDATE candidate.fen_summary_refresh SET revision=revision+1000 WHERE game_link=104')
    await connection.execute("UPDATE candidate.fen_summary_refresh SET error='preserve',attempts=1 WHERE game_link=105")
    async with factory() as s, s.begin():
        planned = await route(s, 'fixture')
        assert planned['eligible'] == 2 and not planned['applied'], planned
    await connection.execute('''INSERT INTO public.ingestion_pipeline_run
        SELECT 'complete',min(committed_at),max(committed_at) FROM candidate.evaluation_repair_receipt''')
    try:
        async with factory() as s, s.begin():
            await route(s, 'fixture', apply=True)
    except ValueError as error:
        assert 'Overlapping ingestion' in str(error)
    else:
        raise AssertionError('Overlapping historical job was not rejected')
    await connection.execute('DELETE FROM public.ingestion_pipeline_run')
    async with factory() as s, s.begin():
        result = await route(s, 'fixture', apply=True)
        assert result['routed'] == 2 and result['scores_changed'] == 0
    actual = dict(await connection.fetch('SELECT game_link,refresh_engine FROM candidate.fen_summary_refresh'))
    assert actual == {101:True,102:False,103:False,104:True,105:True}, actual
    async with factory() as s, s.begin():
        assert (await route(s, 'fixture', apply=True))['already_applied']
    print('PASS: receipt-backed migration, older/full/later/failed requests preserved, historical overlap refusal, audit and idempotency')


if __name__ == '__main__':
    base.check = check
    asyncio.run(base.main())
