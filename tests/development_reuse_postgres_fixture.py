"""Real private PostgreSQL: reuse, role drift, checkpoints, stale histories."""
import io
import json
from datetime import datetime, timedelta
from unittest.mock import AsyncMock


async def verify(connection, engine, historic, metadata):
    from sqlalchemy.ext.asyncio import async_sessionmaker
    from chessism_api.database.relational_storage import RelationalStorage
    from chessism_api.operations.research_algorithms import blunder_development_reuse as reuse
    from chessism_api.operations.research_algorithms import blunder_refinement, blunder_v2_source as source
    from chessism_api.operations.research_algorithms.blunder_v2_job import configuration
    await connection.execute('ALTER TABLE algorithm_run ADD COLUMN cancel_requested boolean DEFAULT false')
    async with async_sessionmaker(engine)() as session:
        policy = await reuse.reference_plan(session)
    assert policy['references'] == [historic]
    config = configuration(1000, policy, blunder_refinement.policy())
    old_train = {g['link'] for g in metadata if g['split'] == 0}
    old_val = {g['link'] for g in metadata if g['split'] == 1}
    old_test = {g['link'] for g in metadata if g['split'] not in (0, 1)}
    results, frozen_layouts = [], []
    for layout in (None, RelationalStorage('candidate', 'archive', 'reads', 'relational_v3')):
        stream = io.BytesIO()
        selected, arrays, frozen = await source.freeze(config, AsyncMock(), stream,
            session_factory=async_sessionmaker(engine, info={'relational_storage_v2': layout}))
        chosen = {g['link'] for g in selected}
        assert chosen & old_train and chosen & old_val, 'Development data was not reused'
        assert not chosen & old_test
        assert not {g['link'] for g in selected if g['split'] in (2, 3)} & {g['link'] for g in metadata}
        assert all(g['split'] == 0 for g in selected if g['link'] in old_train)
        assert all(g['split'] == 1 for g in selected if g['link'] in old_val)
        assert '_reuse_context' not in frozen
        reuse.verified_manifest(frozen)
        json.dumps(frozen, allow_nan=False)
        results.append(stream.getvalue())
        frozen_layouts.append(frozen)
    assert results[0] == results[1], 'Native/legacy role-reuse inputs differ'
    from tests.final_evaluation_postgres_fixture import verify as verify_final
    await verify_final(connection, engine, config, frozen_layouts, results[0])
    # Move an old training game into validation and an old validation game into
    # training. SQL must reject both before the limited pool is ranked.
    train_id, val_id = min(old_train), min(old_val)
    boundary = datetime.fromisoformat(policy['training_before'])
    await connection.execute('UPDATE game SET played_at=CASE WHEN link=$1 THEN $3::timestamptz ELSE $4::timestamptz END WHERE link IN ($1,$2)',
        train_id, val_id, boundary, boundary-timedelta(days=1))
    # Use the manifest instead of legacy replay to isolate role drift filtering.
    roles = reuse.manifest(old_train, old_val, old_test, {g['link'] for g in metadata})
    async with async_sessionmaker(engine)() as session:
        from sqlalchemy import text
        rows = (await session.execute(text(reuse.candidate_sql(session)), {**policy,
            'excluded': roles['protected'], 'prior_training': roles['training'], 'prior_validation': roles['validation'],
            'seen': [g['link'] for g in metadata], 'training_before': datetime.fromisoformat(policy['training_before']),
            'test_from': datetime.fromisoformat(policy['test_from'])})).mappings().all()
        # The chosen validation timestamp is explicitly between the boundaries.
        assert not {train_id, val_id} & {r['link'] for r in rows}
    # Source identities are durable before fitting and survive failure.
    reserved_id = '00000000-0000-4000-8000-000000000088'
    await connection.execute("INSERT INTO algorithm_run(id,status,config,created_at) VALUES($1,'running',$2::jsonb,now()+interval '2 seconds')", reserved_id, json.dumps(config))
    reporter = AsyncMock(); reporter.run_id = reserved_id
    await reuse.checkpoint(frozen, reporter, session_factory=async_sessionmaker(engine))
    await connection.execute("UPDATE algorithm_run SET status='failed' WHERE id=$1", reserved_id)
    async with async_sessionmaker(engine)() as session:
        newer = await reuse.reference_plan(session)
        assert newer['references'][0]['id'] == reserved_id
        roles2, known, proof = await reuse.history(session, newer, AsyncMock())
        assert roles2 == frozen['development_roles']
        assert proof[0]['method'] == 'verified_transitive_role_manifest'
    try:
        await source.freeze(config, AsyncMock(), io.BytesIO(), session_factory=async_sessionmaker(engine))
    except ValueError as error: assert 'history changed' in str(error)
    else: raise AssertionError('Stale queued reuse request was accepted')
    # A cancelled request cannot cross the reservation boundary into fitting.
    await connection.execute("UPDATE algorithm_run SET status='running',cancel_requested=true WHERE id=$1", reserved_id)
    from chessism_api.operations.research_algorithms.repository import RunCancelled
    try: await reuse.checkpoint(frozen, reporter, session_factory=async_sessionmaker(engine))
    except RunCancelled: pass
    else: raise AssertionError('Cancelled request reserved a source')
    print('PASS: development reuse, native/legacy parity, fresh tests, SQL role filtering, durable failed/cancelled reservations and stale-history rejection')
