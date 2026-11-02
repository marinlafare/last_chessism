"""One bounded, retryable graph consumer; no Stockfish, CUDA or external API calls."""
import asyncio
from contextlib import suppress
import json
import logging
import os
import signal
import time
from sqlalchemy import text
from chessism_api.database.postgresql.runtime import bind_runtime_engine, configured_storage
from chessism_api.database.postgresql.engine import AsyncDBSession
from .config import GraphConfig
from .runtime import connect
from . import sync_schema as schema, sync_source as source, sync_graph as graph
from . import sync_budget

LOG = logging.getLogger(__name__)
SINGLETON = 731946300


async def heartbeat(detail):
    """Visible liveness while a larger transaction has not committed its offset."""
    while True:
        try:
            async with AsyncDBSession() as session, session.begin():
                await session.execute(text("SET LOCAL lock_timeout='500ms'"))
                await session.execute(text("SET LOCAL statement_timeout='2s'"))
                await session.execute(text('''UPDATE graph_sync.control SET heartbeat=clock_timestamp(),
                    phase='syncing',detail=:detail WHERE id'''), {'detail': detail})
        except Exception:
            LOG.warning('Could not publish graph batch heartbeat', exc_info=True)
        await asyncio.sleep(5)


async def stop_heartbeat(pulse):
    """Stop a reporting task before publishing a terminal/retry state."""
    if pulse is not None:
        pulse.cancel()
        with suppress(asyncio.CancelledError):
            await pulse
    return None


async def tick(driver, config, policy=None):
    event_id = None
    pulse = None
    cost = 0
    metrics = {}
    started = time.monotonic()
    try:
        async with AsyncDBSession() as session, session.begin():
            await session.execute(text("SET LOCAL statement_timeout='30s'"))
            if not await session.scalar(text('SELECT enabled FROM graph_sync.control WHERE id')):
                return False
            # Backup takes this exclusively BEFORE obtaining its PostgreSQL snapshot.
            if not await session.scalar(text('SELECT pg_try_advisory_xact_lock_shared(:key)'), {'key':schema.LOCK}):
                await session.execute(text('''UPDATE graph_sync.control SET heartbeat=clock_timestamp(),
                    phase='waiting',detail='Waiting for the database backup snapshot to finish.' WHERE id'''))
                return False
            event = (await session.execute(text('''SELECT * FROM graph_sync.event
                WHERE completed_at IS NULL AND (retry_at IS NULL OR retry_at<=clock_timestamp())
                ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED'''))).mappings().first()
            if not event:
                pending = await session.scalar(text('SELECT EXISTS(SELECT 1 FROM graph_sync.event WHERE completed_at IS NULL)'))
                await session.execute(text("UPDATE graph_sync.control SET heartbeat=clock_timestamp(),phase=:phase,detail=:detail WHERE id"),
                    {'phase':'retrying' if pending else 'current', 'detail':'Waiting to retry a failed change; see pending events.' if pending else 'Graph is synchronized with captured changes.'})
                return False
            event_id = event['id']
            ids = event['ids'][event['offset_items']:]
            updates = [{'id': event_id, 'offset': len(event['ids']), 'done': True}]
            async with driver.session(database=config.database) as gs:
                if event['kind'] == 'game':
                    policy = policy or await sync_budget.from_server(driver, config.database)
                    # Combine adjacent game events without crossing a score or
                    # player event, or stealing a delayed retry. IDs/offsets stay
                    # durable and are acknowledged together only after publication.
                    following = (await session.execute(text('''SELECT *,retry_at IS NULL OR
                        retry_at<=clock_timestamp() AS ready FROM graph_sync.event
                        WHERE completed_at IS NULL AND id>:id ORDER BY id LIMIT :limit
                        FOR UPDATE SKIP LOCKED'''), {'id': event_id, 'limit': sync_budget.MAX_EVENTS-1})).mappings().all()
                    events = [event]
                    for candidate in following:
                        if candidate['kind'] != 'game' or not candidate['ready']:
                            break
                        events.append(candidate)
                    candidates = sorted({id for item in events for id in item['ids'][item['offset_items']:]})
                    lengths = dict((await session.execute(text(f'''SELECT link,n_moves FROM
                        {configured_storage().table('game')} WHERE link=ANY(CAST(:ids AS bigint[]))'''),
                        {'ids': candidates})).all())
                    # Deletions and shortened/corrected games still load their
                    # previous graph paths for exact counter subtraction.
                    previous_lengths = await (await gs.run('''MATCH (g:Game) WHERE g.id IN $ids
                        RETURN g.id AS id,g.n_moves AS n_moves''', ids=candidates)).data()
                    for previous in previous_lengths:
                        lengths[previous['id']] = max(int(lengths.get(previous['id']) or 0),
                                                     int(previous['n_moves'] or 0))
                    capacity = policy.capacity()
                    ids, updates, cost = sync_budget.select_prefix(events, lengths,
                        policy.limit(attempts=event['attempts']), capacity)
                    pulse = asyncio.create_task(heartbeat(f"Publishing {len(ids):,} games in a memory-bounded batch; saved offsets advance after commit."))
                    before = time.monotonic()
                    games, positions, deleted = await source.read_games(session, ids, configured_storage())
                    metrics.update(games=len(ids), plies=sum(len(g['steps']) for g in games),
                                   positions=len(positions), source_seconds=time.monotonic()-before,
                                   capacity_plies=capacity, selected_cost=cost, events=len(updates))
                    before = time.monotonic()
                    publication = await gs.execute_write(graph.publish, games, positions, deleted)
                    metrics.update(publication, graph_seconds=time.monotonic()-before)
                elif event['kind'] == 'score':
                    # Ignore positions not present in the graph; future game imports
                    # read their latest evaluations. No million-ID application joins.
                    if event['matched_ids'] is None:
                        matched = await (await gs.run('MATCH (p:Position) WHERE p.fen_id IN $ids RETURN p.fen_id AS id', ids=ids)).data()
                        # Publish the denominator before applying evaluations so
                        # retries show eligible graph positions, not every FEN.
                        await session.execute(text('''UPDATE graph_sync.event SET matched_ids=:ids,
                            started_at=COALESCE(started_at,clock_timestamp()) WHERE id=:id'''),
                            {'ids':[r['id'] for r in matched],'id':event_id})
                        if matched:
                            return True  # Commit classification; next tick applies it.
                        event = {**event, 'matched_ids': []}  # Nothing to publish; acknowledge now.
                    rows = await source.read_scores(session, event['matched_ids'], configured_storage())
                    await (await gs.run('''UNWIND $rows AS row MATCH (p:Position {fen_id:row.fen_id}) SET p += row''', rows=rows)).consume()
                else:
                    rows = [dict(r) for r in (await session.execute(text(f'''SELECT id,player_name AS name,
                        joined>0 AND deleted_at IS NULL AS active FROM {configured_storage().table('player')}
                        WHERE id=ANY(CAST(:ids AS bigint[]))'''), {'ids':ids})).mappings()]
                    await (await gs.run('UNWIND $rows AS row MATCH (p:Player {id:row.id}) SET p += row', rows=rows)).consume()
                    absent = sorted(set(ids)-{r['id'] for r in rows})
                    # Keep historical identity if any games still reference it.
                    await (await gs.run('MATCH (p:Player) WHERE p.id IN $ids SET p.active=false', ids=absent)).consume()
                totals = await graph.counts(gs)
            pulse = await stop_heartbeat(pulse)
            await session.execute(text('''UPDATE graph_sync.event SET offset_items=:offset,error=NULL,retry_at=NULL,
                completed_at=CASE WHEN :done THEN clock_timestamp() ELSE NULL END WHERE id=:id'''),
                updates)
            await session.execute(text('''UPDATE graph_sync.control SET heartbeat=clock_timestamp(),phase='syncing',
                detail=:detail,games=:games,positions=:positions,occurrences=:occurrences,moves=:moves,
                processed_events=processed_events+:done,last_batch_seconds=:elapsed,updated_at=clock_timestamp() WHERE id'''),
                {**totals,'done':sum(int(update['done']) for update in updates),'elapsed':time.monotonic()-started,
                 'detail':f"Synchronized {len(ids):,} {event['kind']} items across {len(updates)} event(s)."})
        if cost:
            policy.observe(cost, time.monotonic()-started)
            LOG.info('Graph batch %s', json.dumps({**metrics, 'total_seconds':time.monotonic()-started}))
        return True
    except Exception as error:
        pulse = await stop_heartbeat(pulse)
        if policy and cost:
            policy.shrink(cost)
        LOG.exception('Graph publication failed; PostgreSQL ingestion is unaffected')
        async with AsyncDBSession() as session, session.begin():
            if event_id:
                await session.execute(text('''UPDATE graph_sync.event SET attempts=attempts+1,error=:error,
                    retry_at=clock_timestamp()+make_interval(secs=>LEAST(300,5*power(2,LEAST(attempts,6)))::integer)
                    WHERE id=:id AND completed_at IS NULL'''), {'id':event_id,'error':str(error)[:1500]})
            await session.execute(text("UPDATE graph_sync.control SET heartbeat=clock_timestamp(),phase='retrying',detail=:error WHERE id"),
                {'error':str(error)[:1500]})
        return False
    finally:
        await stop_heartbeat(pulse)


async def main():
    engine = await bind_runtime_engine(os.environ['DATABASE_URL'])
    try:
        async with engine.connect() as singleton:
            if not await singleton.scalar(text('SELECT pg_try_advisory_lock(:key)'), {'key':SINGLETON}):
                raise RuntimeError('Another Neo4j synchronization worker is running')
            # Session advisory locks survive commit; avoid a permanently idle
            # transaction holding back normal PostgreSQL maintenance.
            await singleton.commit()
            config = GraphConfig.from_env()
            stopping = asyncio.Event()
            loop = asyncio.get_running_loop()
            for signum in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(signum, stopping.set)
            while not stopping.is_set():
                try:
                    async with connect(config) as driver:
                        policy = await sync_budget.from_server(driver, config.database)
                        LOG.info('Graph batch budget: %s bytes, %s estimated plies; adaptive target %.1fs',
                                 policy.graph_bytes, policy.capacity(), policy.target_seconds)
                        while not stopping.is_set():
                            busy = await tick(driver, config, policy)
                            if not busy:
                                with suppress(asyncio.TimeoutError):
                                    await asyncio.wait_for(stopping.wait(), 2)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    LOG.warning('Neo4j unavailable; retrying: %s', error)
                    async with AsyncDBSession() as session, session.begin():
                        await session.execute(text("UPDATE graph_sync.control SET heartbeat=clock_timestamp(),phase='retrying',detail=:error WHERE id"),
                            {'error':str(error)[:1500]})
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(stopping.wait(), 5)
    finally:
        await engine.dispose()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('neo4j.notifications').setLevel(logging.WARNING)
    asyncio.run(main())
