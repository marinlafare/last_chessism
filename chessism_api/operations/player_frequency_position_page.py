"""Rank only saved repeated-FEN counts. Never join raw game occurrences."""
import base64
import hashlib
import json
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from chessism_api.database.player_analytics_schema import analytics_table
from chessism_api.database.stockfish_summary_schema import summary_table
from .cuda_summaries import frequency_store as store
from .cuda_summaries.frequency_compute import VERSION
from .player_score_position_page import scope_key, PAGE_SIZE


class FrequencySnapshotChanged(ValueError):
    pass


def cursor_value(scope, snapshot, row):
    return base64.urlsafe_b64encode(json.dumps([4,scope,snapshot,row['appearances'],
        row['fen_id'],row['player_color']=='white']).encode()).decode().rstrip('=')


def decode_cursor(cursor, scope, snapshot):
    if not cursor: return None, None, None
    try:
        if len(cursor)>512: raise ValueError()
        version,expected,revision,count,fen,white = json.loads(base64.b64decode(
            cursor+'='*(-len(cursor)%4),altchars=b'-_',validate=True))
        if version!=4 or expected!=scope or revision!=snapshot:
            raise FrequencySnapshotChanged('Position frequencies or scores changed. Update the position list.')
        if type(count) is not int or not 1<count<2**63 or type(white) is not bool:
            raise ValueError()
        if not isinstance(fen,str) or not fen.isascii() or not fen.isdecimal() or not 0<int(fen)<2**63:
            raise ValueError()
        return count,int(fen),white
    except FrequencySnapshotChanged:
        raise
    except (ValueError,TypeError,UnicodeError):
        raise ValueError('Invalid ranked position cursor. Reopen the list.') from None


def ranked_sql(layout, interval):
    score = 'CASE WHEN n.is_white THEN f.score ELSE -f.score END'
    conditions = []
    if interval['lower'] is not None:
        conditions.append(f'({score}) {">=" if interval["lower_inclusive"] else ">"} :lower')
    if interval['upper'] is not None:
        conditions.append(f'({score}) {"<=" if interval["upper_inclusive"] else "<"} :upper')
    return text(f'''WITH ranked AS (
        SELECT n.fen_id,n.is_white,SUM(n.appearances)::bigint AS appearances,({score}) AS score
        FROM {store.TABLE} n JOIN {layout.table('game_mode')} m ON m.id=n.mode_id
        JOIN {layout.table('fen')} f ON f.fen_id=n.fen_id
        WHERE n.player_id=:owner AND m.name=ANY(CAST(:modes AS text[]))
          AND (CAST(:color AS text) IS NULL OR n.is_white=(:color='white'))
          AND f.score IS NOT NULL AND ABS(f.score)<9000
          AND f.analysis_source IS DISTINCT FROM 'tablebase' AND {' AND '.join(conditions)}
        GROUP BY n.fen_id,n.is_white,f.score HAVING SUM(n.appearances)>1)
        SELECT * FROM ranked WHERE CAST(:after_count AS bigint) IS NULL
          OR appearances<:after_count OR (appearances=:after_count AND fen_id>:after_fen)
          OR (appearances=:after_count AND fen_id=:after_fen AND is_white<:after_white)
        ORDER BY appearances DESC,fen_id,is_white DESC LIMIT :limit''')


async def read_ranked_page(session, player, modes, interval, cursor=None, *, color=None):
    if not await store.enabled(session): return None
    layout = storage_for(session)
    meta = (await session.execute(text(f'''SELECT p.id,s.frequency_generation,s.frequency_version,
        s.frequency_revision,s.revision,s.frequency_behavior_revision,a.revision AS behavior_revision,
        c.revision AS score_revision FROM {layout.table('player')} p
        LEFT JOIN {store.STATE} s ON s.player_id=p.id
        LEFT JOIN {analytics_table(layout,'state')} a USING(player_name)
        LEFT JOIN {summary_table(layout,'state')} c USING(player_name)
        WHERE p.player_name=:player AND p.joined>0 AND p.deleted_at IS NULL'''),{'player':player})).mappings().first()
    if not meta: raise LookupError(f'Player {player!r} was not found.')
    if meta['frequency_version']!=VERSION or not meta['frequency_generation']:
        return {'ranking_pending':True}
    scope=scope_key(player,modes,interval['id'],color)
    snapshot=hashlib.sha256(json.dumps([str(meta['frequency_generation']),meta['score_revision']]).encode()).hexdigest()
    count,fen,white=decode_cursor(cursor,scope,snapshot)
    params=dict(owner=meta['id'],modes=list(modes),color=color,lower=interval['lower'],upper=interval['upper'],
        after_count=count,after_fen=fen,after_white=white,limit=PAGE_SIZE+1)
    found=(await session.execute(ranked_sql(layout,interval),params)).mappings().all()
    rows=[dict(fen_id=str(r['fen_id']),player_color='white' if r['is_white'] else 'black',
        appearances=r['appearances'],score_min=r['score'],score_max=r['score']) for r in found[:PAGE_SIZE]]
    if rows:
        boards=dict((await session.execute(text(f'SELECT fen_id,fen FROM {layout.table("fen")} WHERE fen_id=ANY(CAST(:ids AS bigint[]))'),
            {'ids':[int(r['fen_id']) for r in rows]})).all())
        for row in rows: row['fen']=boards[int(row['fen_id'])]
    return dict(rows=rows,next_cursor=cursor_value(scope,snapshot,rows[-1]) if len(found)>PAGE_SIZE else None,
        snapshot=snapshot,bin_id=interval['id'],modes=list(modes),color=color,score_perspective='player',
        sort='appearances_desc',summary_stale=meta['frequency_revision']!=meta['revision']
            or meta['frequency_behavior_revision']!=(meta['behavior_revision'] or 0))
