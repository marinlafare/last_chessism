"""Saved daily bands. No FEN reads, accuracy evaluation or implicit GPU jobs."""
from sqlalchemy import text
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import storage_for
from chessism_api.database.stockfish_summary_schema import summary_table
from ..player_hero_analytics import _analytics_scope
from ..player_hero_efficiency import normalize_efficiency_modes
from ..player_timezone import player_local_timestamp_sql
from .accuracy_compute import LABELS, VERSION
from .accuracy_store import STATE, GAME, DAILY


def bands():
    return [{'id': i, 'label': label, 'lower': (0,50,60,70,80,90)[i],
        'upper': (50,60,70,80,90,100)[i], 'lower_inclusive': i == 0, 'upper_inclusive': True}
        for i,label in enumerate(LABELS)]


async def custom_timezone(session, scope, modes):
    """Rare timezone overrides group saved game scalars, never move/FEN data.

Normal hero requests use the CUDA-produced daily cache directly. An explicit
different timezone must not silently receive days grouped in the default zone.
"""
    layout = storage_for(session)
    cases = 'CASE ' + ' '.join(f'WHEN a.accuracy<={value} THEN {i}' for i,value in enumerate((50,60,70,80,90))) + ' ELSE 5 END'
    rows = (await session.execute(text(f'''WITH selected AS (
        SELECT ({player_local_timestamp_sql('a')})::date AS day,{cases} AS band,s.salience
        FROM {GAME} a JOIN {layout.table('player')} p ON p.player_name=a.player_name
        LEFT JOIN {layout.table('game_player')} gp ON gp.game_link=a.game_link AND gp.player_id=p.id
        LEFT JOIN {layout.table('game_player_salience')} s ON s.game_player_id=gp.id
        WHERE a.player_name=:player AND a.mode=ANY(CAST(:modes AS text[]))
        AND a.moves>10 AND a.accuracy BETWEEN 0 AND 100 AND a.played_at IS NOT NULL
        AND (CAST(:date_from_utc AS timestamptz) IS NULL OR a.played_at>=CAST(:date_from_utc AS timestamptz))
        AND (CAST(:date_to_utc AS timestamptz) IS NULL OR a.played_at<CAST(:date_to_utc AS timestamptz))
    ) SELECT day,ARRAY[{','.join(f'count(*) FILTER(WHERE band={i})' for i in range(6))}] AS counts,
        count(*) AS games,CASE WHEN count(salience)=count(*) THEN sum(salience) END AS daily_salience
        FROM selected GROUP BY day ORDER BY day'''), {**scope.params(), 'modes': list(modes)})).mappings().all()
    return [{'day':r['day'].isoformat(),'counts':list(r['counts']),'games':r['games'],
             'daily_salience':r['daily_salience'], 'modal_bands':[i for i,c in enumerate(r['counts']) if c == max(r['counts'])]} for r in rows]


async def daily_chart(player, mode='all', date_from=None, date_to=None, timezone_name=None):
    modes = normalize_efficiency_modes(mode); mask = sum({'bullet':1,'blitz':2,'rapid':4}[m] for m in modes)
    async with AsyncDBSession() as session:
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        scope = await _analytics_scope(session, player, 'all', date_from, date_to, timezone_name)
        base = scope.response_base()
        base['filters'].update(mode=','.join(modes),modes=list(modes),minimum_analyzed_player_moves_exclusive=10)
        base.update(metric='modal_accuracy_band',bands=bands(),columns=['date','modal_bands','counts','daily_salience','games'])
        from .salience_reader import saved as saved_salience
        salience_bands = await saved_salience(session,scope,modes)
        if salience_bands is not None:base['salience']=salience_bands
        if not await session.scalar(text('SELECT to_regclass(:table) IS NOT NULL'), {'table': STATE}):
            return {**base,'summary_pending':True,'points':[]}
        state = (await session.execute(text(f'''SELECT a.*,s.revision AS current_revision FROM {STATE} a
            LEFT JOIN {summary_table(storage_for(session),'state')} s USING(player_name)
            WHERE a.player_name=:player'''), {'player':scope.player})).mappings().first()
        if not state:
            return {**base,'summary_pending':True,'points':[]}
        same_timezone = state['timezone']['component_timezones'] == list(scope.timezone.zones)
        if same_timezone:
            rows = (await session.execute(text(f'''SELECT payload FROM {DAILY}
                WHERE player_name=:player AND mode_mask=:mask
                AND (CAST(:first AS date) IS NULL OR day>=CAST(:first AS date))
                AND (CAST(:last AS date) IS NULL OR day<=CAST(:last AS date)) ORDER BY day'''),
                {'player':scope.player,'mask':mask,'first':date_from,'last':date_to})).scalars().all()
        else:
            rows = await custom_timezone(session, scope, modes)
        from .salience_store import published_version
        salience_version = await published_version(session,scope.player)
    return {**base,'summary_pending':False,
        'stale':state['revision'] != state['current_revision'] or state['version'] != VERSION,
        'generated_at':state['generated_at'].isoformat(),'backend':'saved_cuda_daily' if same_timezone else 'saved_game_timezone_adapter',
        'salience':salience_bands if salience_bands is not None else {'status':'ready' if any(r['daily_salience'] is not None for r in rows) else 'unavailable',
                    'version':salience_version or 'legacy_full_game_depth_weighted'},
        'points':[[r['day'],r['modal_bands'],r['counts'],r['daily_salience'],r['games']] for r in rows]}
