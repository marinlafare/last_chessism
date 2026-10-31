"""Synthetic-only coverage projection and transactional recovery tests."""
import asyncio
import json
from unittest.mock import AsyncMock, patch
from sqlalchemy import text
from tests import check_canonical_blunders_postgres as base
from chessism_api.operations import fen_summary_recovery as recovery
from chessism_api.operations.canonical_blunders import assessment, publication, readers, source
from chessism_api.operations.canonical_blunders.coverage_upgrade import upgrade

original = base.check


async def check(factory, connection, layout):
    await original(factory, connection, layout)
    # Side-specific coverage, odd/even game lengths, and every unknown-ply position.
    async with factory() as s:
        tx = await s.begin()
        records = []
        for length in range(1, 24):
            for unknown in ([], list(range(1, length+1)), [length], [1]):
                counts = {side: sum(p not in unknown for p in range(start, length+1, 2))
                          for side, start in (('white', 1), ('black', 2))}
                records.append({'game_link': 1000+len(records), 'mode': 'blitz', 'status': 'assessed',
                    'white_owner': 'alice', 'black_owner': 'bob', 'white_count': 0 if counts['white'] else None,
                    'black_count': 0 if counts['black'] else None,
                    'white_assessed': counts['white'], 'black_assessed': counts['black'],
                    'flags': {'blunders': [], 'unassessed': unknown}})
        for record in records:
            await s.execute(text('''INSERT INTO candidate.canonical_blunder_game(
                model_id,game_link,revision,mode,status,white_owner,black_owner,white_count,black_count,
                white_assessed,black_assessed,flags,source_sha256) VALUES('coverage-fixture',:game_link,1,
                :mode,:status,:white_owner,:black_owner,:white_count,:black_count,:white_assessed,:black_assessed,
                CAST(:flags AS jsonb),'synthetic')'''), {**record, 'flags': json.dumps(record['flags'])})
        rows = (await s.execute(text("SELECT * FROM candidate.canonical_blunder_assessment WHERE model_id='coverage-fixture' ORDER BY game_link"))).mappings().all()
        for row, record in zip(rows, records):
            for side, parity in (('white', 1), ('black', 0)):
                expected = sum(p % 2 == parity for p in record['flags']['unassessed'])
                assert row[side+'_unassessed'] == expected, (row, record)
                assert row[side+'_complete'] == (expected == 0 and record[side+'_assessed'] > 0)
        await publication.summaries(s, 'coverage-fixture')
        payload = await s.scalar(text("SELECT payload FROM candidate.canonical_blunder_snapshot WHERE model_id='coverage-fixture' AND player_name='alice'"))
        complete = sum(r['white_complete'] for r in rows)
        assert payload['blitz']['counts'] == [complete, 0, 0, 0, 0, 0, 0]
        assert payload['blitz']['partial_games'] > 0 and payload['blitz']['unassessed_games'] > 0
        reference = {'alice': {mode: assessment.empty_mode() for mode in ('bullet', 'blitz', 'rapid')}}
        for row in records:
            assessment.add_record(reference, row)
        assert reference['alice'] == payload
        await tx.rollback()

    # Real compatibility view and saved chart use identical complete-side counts.
    async with factory() as s:
        tx = await s.begin()
        mid = await s.scalar(text('SELECT active_model FROM candidate.canonical_blunder_control'))
        await s.execute(text('''UPDATE candidate.canonical_blunder_game SET status='assessed',white_count=0,
            black_count=0,white_assessed=10,black_assessed=11,flags='{"blunders":[],"unassessed":[1]}' WHERE model_id=:mid AND game_link=1'''), {'mid': mid})
        await publication.summaries(s, mid)
        with patch.dict('os.environ', {'CHESSISM_CANONICAL_BLUNDERS': '1'}):
            saved = await readers.saved(s, 'alice')
            flags = await readers.game_flags(s, 1)
            assert not flags['pending'] and flags['supported']
            assert flags['coverage']['white'] == {'assessed_moves': 10, 'unassessed_moves': 1, 'complete': False, 'detected_blunders': 0}
            assert await s.scalar(text("SELECT blunder_count FROM reads.game_player_engine_summary WHERE game_link=1 AND player_name='alice'")) is None
            games = await s.scalar(text("SELECT count(*) FROM reads.game_player_engine_summary WHERE player_name='alice' AND blunder_count IS NOT NULL"))
            assert games == saved['by_mode']['blitz']['games_count']
            await s.execute(text("UPDATE candidate.canonical_blunder_game SET status='unsupported' WHERE model_id=:mid AND game_link=1"), {'mid': mid})
            flags = await readers.game_flags(s, 1)
            assert not flags['pending'] and not flags['supported']
            await s.execute(text("UPDATE candidate.canonical_blunder_snapshot SET payload='{}' WHERE model_id=:mid"), {'mid': mid})
            assert (await readers.saved(s, 'alice'))['summary_pending']
        await tx.rollback()

    # Upgrade is atomic: neither a new equation nor a source-position write.
    before = await connection.fetchval('SELECT jsonb_agg(to_jsonb(g) ORDER BY game_link) FROM candidate.canonical_blunder_game g')
    async with factory() as s, s.begin():
        result = await upgrade(s)
        assert result['assessment_policy'] == assessment.POLICY and not result['inference_changed']
    assert before == await connection.fetchval('SELECT jsonb_agg(to_jsonb(g) ORDER BY game_link) FROM candidate.canonical_blunder_game g')

    # Make the source fixture support the real coverage refresh and cloud claim guard.
    await connection.execute('''ALTER TABLE candidate.fen ADD COLUMN fen text;
        UPDATE candidate.fen SET fen='synthetic-'||fen_id;
        CREATE TABLE public.cloud_fen_claim(fen text PRIMARY KEY);
        ALTER TABLE candidate.game_analysis_coverage ADD PRIMARY KEY(link);
        ALTER TABLE candidate.game_analysis_coverage ADD COLUMN analyzed_positions int,
          ADD COLUMN unscored_positions int, ADD COLUMN equal_positions int,
          ADD COLUMN small_positions int, ADD COLUMN clear_positions int,
          ADD COLUMN decisive_positions int, ADD COLUMN mate_positions int,
          ADD COLUMN refreshed_at timestamptz;''')
    async with factory() as s, s.begin():
        await recovery.install(s, layout)
    assert await connection.fetchval('SELECT count(*) FROM candidate.fen_summary_refresh') == 0
    async with factory() as s:
        tx = await s.begin()
        await s.execute(text('UPDATE candidate.fen SET score=99 WHERE fen_id=1'))
        assert await s.scalar(text('SELECT count(*) FROM candidate.fen_summary_refresh')) > 0
        await tx.rollback()
    assert await connection.fetchval('SELECT count(*) FROM candidate.fen_summary_refresh') == 0
    await connection.execute('UPDATE candidate.fen SET score=score,analysis_source=analysis_source')
    assert await connection.fetchval('SELECT count(*) FROM candidate.fen_summary_refresh') == 0
    await connection.execute("INSERT INTO public.cloud_fen_claim VALUES('synthetic-1')")
    await connection.execute('UPDATE candidate.fen SET score=score+1 WHERE fen_id=1')
    assert await connection.fetchval('SELECT count(*) FROM candidate.fen_summary_refresh') == 0
    await connection.execute('DELETE FROM public.cloud_fen_claim')
    await connection.execute('UPDATE candidate.fen SET score=score+1 WHERE fen_id=1')
    pending = await connection.fetchval('SELECT count(DISTINCT game_link) FROM candidate.game_fen_association WHERE fen_id=1')
    assert await connection.fetchval('SELECT count(*) FROM candidate.fen_summary_refresh') == pending
    # This transaction survived independently of a failed downstream calculation.
    failure = AsyncMock(side_effect=RuntimeError('synthetic summary failure'))
    with patch.object(recovery.LOG, 'exception'):
        failed = await recovery.recover_batch(factory=factory, refresh=failure)
    assert failed['processed'] == 0 and failed['pending'] == pending and failed['error']
    assert await connection.fetchval('SELECT bool_and(attempts=1 AND error IS NOT NULL) FROM candidate.fen_summary_refresh')
    assert (await recovery.recover_batch(factory=factory, refresh=failure))['processed'] == 0
    assert failure.await_count == 1  # Backoff is durable, not a busy retry loop.
    with patch.dict('os.environ', {'CHESSISM_CANONICAL_BLUNDERS': '1'}):
        async with factory() as s:
            assert (await readers.saved(s, 'alice'))['stale']
            assert (await readers.game_flags(s, 1))['pending']
    # Pending input refresh must not be selected from stale coverage.
    await connection.execute('UPDATE candidate.game SET white_elo=white_elo WHERE link=1')
    async with factory() as s:
        await source.prepare(s)
        data = await source.load(s)
        assert all(item['game_link'] != 1 for item in data[2])
    await connection.execute("UPDATE candidate.fen_summary_refresh SET retry_after=now()-interval '1 second'")
    from chessism_api.operations.relational_coverage import refresh_coverage
    async def real_coverage(links, *, strict):
        assert strict
        async with factory() as s, s.begin():
            await refresh_coverage(s, links)
    async def racing_refresh(links, *, strict):
        await real_coverage(links, strict=strict)
        await connection.execute('UPDATE candidate.fen SET score=score+1 WHERE fen_id=1')
    raced = await recovery.recover_batch(factory=factory, refresh=racing_refresh)
    assert raced['pending'] == pending  # Never acknowledge a newer source revision.
    done = await recovery.recover_batch(factory=factory, refresh=real_coverage)
    assert done['pending'] == 0 and done['processed'] == pending
    assert await connection.fetchval('SELECT count(*) FROM candidate.canonical_blunder_dirty') >= pending
    assert (await recovery.recover_batch(factory=factory, refresh=real_coverage))['processed'] == 0
    await connection.execute("UPDATE candidate.fen SET analysis_source='stockfish' WHERE fen_id=4")
    assert await connection.fetchval('SELECT count(*) FROM candidate.fen_summary_refresh') == 1
    no_cpu = AsyncMock(side_effect=AssertionError('Source label must not refresh CPU statistics'))
    source_refresh = await recovery.recover_batch(factory=factory, refresh=no_cpu)
    assert source_refresh['source_only'] == 1 and source_refresh['pending'] == 0
    no_cpu.assert_not_awaited()
    await connection.execute('UPDATE candidate.fen SET score=score+1 WHERE fen_id=4')
    # The production refresh can commit coverage and then fail its CPU summary;
    # recovery must retain the request until both stages have succeeded.
    from chessism_api.database import ask_db
    from chessism_api.operations import player_game_scores
    cpu_summary = AsyncMock(side_effect=RuntimeError('synthetic engine-summary failure'))
    with patch.object(ask_db, 'AsyncDBSession', factory), \
            patch.object(player_game_scores, 'refresh_game_player_engine_summaries', cpu_summary), \
            patch.object(recovery.LOG, 'exception'):
        assert (await recovery.recover_batch(factory=factory))['pending'] == 1
        cpu_summary.assert_awaited_once_with((9,))
        await connection.execute("UPDATE candidate.fen_summary_refresh SET retry_after=now()-interval '1 second'")
        cpu_summary.side_effect = None
        assert (await recovery.recover_batch(factory=factory))['pending'] == 0
        assert cpu_summary.await_count == 2
    print('PASS coverage/recovery: odd/even sides, partial zero exclusion, chart/popup parity, immutable inference, transactional queue, cloud claims, failed refresh/backoff, source-race retention, real coverage retries, provenance changes')

    # A new game using exclusively pre-scored FENs still needs durable input
    # refresh. Simulate an ingestion interruption before its normal finalizer.
    await connection.execute('''INSERT INTO candidate.game
        SELECT (jsonb_populate_record(NULL::candidate.game,to_jsonb(g)||'{"link":1001}')).*
        FROM candidate.game g WHERE link=2;
        INSERT INTO candidate.game_fen_association
            SELECT 1001,n_move,is_white,fen_id FROM candidate.game_fen_association WHERE game_link=2;
        INSERT INTO candidate.game_player(game_link,player_id,is_white) VALUES(1001,1,true);''')
    assert await connection.fetchval('SELECT count(*) FROM candidate.fen_summary_refresh WHERE game_link=1001') == 1
    assert await connection.fetchval('SELECT count(*) FROM candidate.game_analysis_coverage WHERE link=1001') == 0
    await connection.execute('UPDATE candidate.game_player SET player_id=player_id WHERE game_link=1001')
    assert await connection.fetchval('SELECT count(*) FROM candidate.fen_summary_refresh WHERE game_link=1001') == 1
    await recovery.recover_batch(factory=factory, refresh=real_coverage)
    assert await connection.fetchval('SELECT is_fully_analyzed FROM candidate.game_analysis_coverage WHERE link=1001')

    # A FEN change after CUDA extraction can precede the coverage refresh that
    # bumps the canonical revision. Pending inputs must block publication NOW.
    from chessism_api.operations.canonical_blunders import compute
    import numpy as np
    async with factory() as s:
        await source.prepare(s)
        mid, config, items, (eligible, unsupported), values = await source.load(s)
    records, info = compute.infer(np, values, eligible, config, require_cuda=False)
    records.extend(unsupported)
    assert any(item['game_link'] == 1001 for item in items)
    await connection.execute('UPDATE candidate.fen SET score=score+1 WHERE fen_id=1')
    async with factory() as s, s.begin():
        published = await publication.publish(s, mid, items, records, info)
    assert await connection.fetchval('SELECT count(*) FROM candidate.canonical_blunder_game WHERE game_link=1001') == 0
    assert await connection.fetchval('SELECT count(*) FROM candidate.canonical_blunder_dirty WHERE game_link=1001') == 1
    assert published['remaining'] > 0
    await recovery.recover_batch(factory=factory, refresh=real_coverage)
    async with factory() as s:
        await source.prepare(s)
        mid, config, items, (eligible, unsupported), values = await source.load(s)
    records, info = compute.infer(np, values, eligible, config, require_cuda=False)
    records.extend(unsupported)
    async with factory() as s, s.begin():
        await publication.publish(s, mid, items, records, info)
    assert await connection.fetchval('SELECT count(*) FROM candidate.canonical_blunder_game WHERE game_link=1001') == 1
    before = await connection.fetchval('SELECT jsonb_agg(to_jsonb(f) ORDER BY fen_id) FROM candidate.fen f')
    for _ in range(2):
        async with factory() as s, s.begin():
            upgraded = await recovery.upgrade(s)
            assert upgraded['scores_changed'] == upgraded['games_queued'] == 0
    assert before == await connection.fetchval('SELECT jsonb_agg(to_jsonb(f) ORDER BY fen_id) FROM candidate.fen f')
    await connection.execute("INSERT INTO cloud_analysis_job(status) VALUES('running')")
    try:
        async with factory() as s, s.begin():
            await recovery.upgrade(s)
    except ValueError as error:
        assert 'active cloud_analysis_job' in str(error)
    else:
        raise AssertionError('Input pipeline migration accepted an active cloud job')
    await connection.execute("DELETE FROM cloud_analysis_job WHERE status='running'")
    print('PASS input pipeline: pre-scored reuse, durable association/ownership updates, coalesced refresh, FEN-change-during-inference rejection, refreshed publication')


if __name__ == '__main__':
    base.check = check
    asyncio.run(base.main())
