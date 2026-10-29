"""Read independent saved daily salience; never compute or enqueue on GET."""
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from . import salience_store as store,salience_daily as daily


async def saved(session, scope, modes):
    if not await store.enabled(session):return None
    base={'metric':'modal_salience_band','bands':daily.bands(),'points':[],
        'columns':['date','modal_bands','counts','games'],'cohort':'all_games_with_salience',
        'scale':'absolute_0_100','version':daily.VERSION}
    if not await session.scalar(text('SELECT to_regclass(:t) IS NOT NULL'),{'t':store.DAILY}):
        return {**base,'status':'pending'}
    layout=storage_for(session)
    state=(await session.execute(text(f'''SELECT s.* FROM {store.STATE} s
        JOIN {layout.table('player')} p ON p.id=s.player_id WHERE p.player_name=:player'''),
        {'player':scope.player})).mappings().first()
    if not state or state['daily_version']!=daily.VERSION:
        return {**base,'status':'pending'}
    if state['timezone'].get('component_timezones')!=list(scope.timezone.zones):
        # Never silently return another timezone's bins or recalculate on CPU.
        return {**base,'status':'unavailable_timezone','timezone':state['timezone']}
    mask=sum({'bullet':1,'blitz':2,'rapid':4}[m] for m in modes)
    rows=(await session.execute(text(f'''SELECT payload FROM {store.DAILY} WHERE player_id=:owner AND mode_mask=:mask
        AND (CAST(:first AS date) IS NULL OR day>=CAST(:first AS date))
        AND (CAST(:last AS date) IS NULL OR day<=CAST(:last AS date)) ORDER BY day'''),
        {'owner':state['player_id'],'mask':mask,'first':scope.date_from,'last':scope.date_to})).scalars().all()
    return {**base,'status':'ready','stale':state['revision']!=state['published_revision'],
        'generated_at':state['generated_at'].isoformat(),'backend':'saved_cuda_salience_daily',
        'points':[[r['day'],r['modal_bands'],r['counts'],r['games']] for r in rows]}
