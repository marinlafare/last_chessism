"""Read-only ingestion progress from captured changes, not global graph counts."""
from datetime import datetime, timezone
from sqlalchemy import text
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.runtime import configured_storage
from .sync_schema import available


def progress_row(run, counts, control, now=None):
    now = now or datetime.now(timezone.utc)
    total = sum(int(row['total']) for row in counts)
    done = sum(int(row['processed']) for row in counts)
    errors = [row['error'] for row in counts if row.get('error')]
    started = min((row['started_at'] for row in counts if row['started_at']), default=None)
    finished = max((row['finished_at'] for row in counts if row['finished_at']), default=None)
    upstream_running = run.status == 'running'
    status = ('retrying' if errors else 'working') if done < total else (
        'waiting' if upstream_running else 'complete')
    detail = ' · '.join(f"{row['processed']:,} / {row['total']:,} { {'game':'games','score':'evaluations','player':'profiles'}[row['kind']]}"
                        for row in counts)
    if not total:
        detail = 'Waiting for committed game paths.' if upstream_running else 'No graph changes were needed for this update.'
    elif upstream_running and done == total:
        detail += ' · Waiting for the remaining ingestion steps.'
    if errors:
        detail += ' · Retrying: ' + errors[0]
    elif done < total and control.get('phase') == 'waiting':
        status = 'waiting'
        detail += ' · ' + (control.get('detail') or 'Waiting for graph publication.')
    end = finished if done == total and finished else now
    return {'step':'neo4j_ingestion', 'label':'Neo4j ingestion', 'group':'Graph',
            'status':status, 'processed':done, 'total':total or None, 'unit':'updates',
            'fixed_total':not upstream_running, 'elapsed_seconds':max(0,(end-started).total_seconds()) if started else 0,
            'elapsed_at':now.timestamp(),
            'heartbeat_at':control['heartbeat'].timestamp() if control.get('heartbeat') else now.timestamp(),
            'detail':detail,
            'scope':'Player graph changes captured during this ingestion',
            'started_at':started.isoformat() if started else None}


def progress_query(layout):
    # Resolve score membership ONCE. The former correlated EXISTS walked the
    # materialized game_items again for every score occurrence (quadratic), and
    # its OR also made game/profile events evaluate that expensive subplan.
    return f'''
            WITH events AS MATERIALIZED (
                SELECT * FROM graph_sync.event WHERE created_at>=:started
                AND (CAST(:finished AS timestamptz) IS NULL OR created_at<=CAST(:finished AS timestamptz)) AND batch_id IS NULL
            ), player AS MATERIALIZED (
                SELECT id FROM {layout.table('player')} WHERE player_name=:player
            ), game_items AS MATERIALIZED (
                SELECT e.kind,item.entity_id,item.ordinality,e.offset_items,e.completed_at,e.error,e.created_at FROM events e
                CROSS JOIN LATERAL unnest(e.ids) WITH ORDINALITY item(entity_id,ordinality)
                WHERE e.kind='game' AND EXISTS (
                    SELECT 1 FROM {layout.table('game_player')} gp,player p
                    WHERE gp.game_link=item.entity_id AND gp.player_id=p.id)
            ), game_keys AS MATERIALIZED (
                SELECT DISTINCT entity_id FROM game_items
            ), score_items AS MATERIALIZED (
                SELECT e.kind,item.entity_id,item.ordinality,e.offset_items,e.completed_at,e.error,e.created_at
                FROM events e CROSS JOIN LATERAL unnest(e.ids) WITH ORDINALITY item(entity_id,ordinality)
                WHERE e.kind='score'
            ), score_keys AS MATERIALIZED (
                SELECT DISTINCT entity_id FROM score_items
            ), matching_scores AS MATERIALIZED (
                SELECT DISTINCT a.fen_id FROM score_keys k
                JOIN {layout.table('game_fen_association')} a ON a.fen_id=k.entity_id
                JOIN game_keys g ON g.entity_id=a.game_link
            ), items AS (
                SELECT * FROM game_items
                UNION ALL
                SELECT s.* FROM score_items s JOIN matching_scores m ON m.fen_id=s.entity_id
                UNION ALL
                SELECT e.kind,item.entity_id,item.ordinality,e.offset_items,e.completed_at,e.error,e.created_at
                FROM events e CROSS JOIN LATERAL unnest(e.ids) WITH ORDINALITY item(entity_id,ordinality)
                WHERE e.kind='player' AND item.entity_id IN (SELECT id FROM player)
            ), entities AS (
                SELECT kind,entity_id,bool_and(completed_at IS NOT NULL OR ordinality<=offset_items) AS done,
                max(error) FILTER(WHERE completed_at IS NULL AND ordinality>offset_items) AS error,
                min(created_at) AS started_at,max(completed_at) AS finished_at
                FROM items GROUP BY kind,entity_id
            ) SELECT kind,count(*)::int AS total,count(*) FILTER(WHERE done)::int AS processed,
                max(error) AS error,min(started_at) AS started_at,max(finished_at) AS finished_at
                FROM entities GROUP BY kind ORDER BY kind
        '''


async def ingestion_progress(run):
    layout = configured_storage()
    if not layout or not run.player_name:
        return None
    async with AsyncDBSession() as session:
        await session.execute(text("SET LOCAL statement_timeout='3s'"))
        if not await available(session):
            return None
        control = (await session.execute(text('SELECT enabled,heartbeat,phase,detail FROM graph_sync.control WHERE id'))).mappings().one()
        if not control['enabled']:
            return None
        first = await session.scalar(text('SELECT min(created_at) FROM graph_sync.event'))
        if first and run.finished_at and run.finished_at < first:
            return None  # Do not invent graph history for pre-installation runs.
        rows = (await session.execute(text(progress_query(layout)),
            {'started':run.started_at,'finished':run.finished_at,'player':run.player_name.lower()})).mappings().all()
        return progress_row(run, rows, control)
