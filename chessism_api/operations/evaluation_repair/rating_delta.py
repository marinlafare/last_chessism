"""Atomic rating-chart bucket deltas for already-scored repair positions.

Score-only replacement cannot change full-game membership or rating boundaries.
Count actual occurrences (not distinct FENs), just like the full projection.
"""
import copy
import json
from sqlalchemy import text
from .aggregates import contribution, BINS


def bucket(row):
    values = contribution(row)
    return next(name for name in BINS if values[name + '_positions'])


def patch_groups(groups, changes):
    result = copy.deepcopy(groups)
    indexed = {group['key']: group for group in result}
    if len(indexed) != len(result):
        raise ValueError('Duplicate rating group')
    for change in changes:
        group = indexed[change['group_key']]
        buckets = {item['key']: item for item in group['buckets']}
        if set(buckets) != set(BINS):
            raise ValueError('Unexpected rating buckets')
        for name, sign in ((change['old_bucket'], -1), (change['new_bucket'], 1)):
            buckets[name]['positions'] += sign * int(change['occurrences'])
        if any(item['positions'] < 0 for item in buckets.values()):
            raise ValueError('Rating delta would make a bucket negative')
        if sum(item['positions'] for item in buckets.values()) != group['positions']:
            raise ValueError('Rating bucket totals disagree')
    return result


async def apply(session, layout, candidates):
    transitions = [{'fen_id': c['before']['fen_id'], 'old_bucket': bucket(c['before']),
                    'new_bucket': bucket({**c['replacement'], 'n_games': c['before']['n_games']})}
                   for c in candidates if bucket(c['before']) != bucket(c['replacement'])]
    if not transitions:
        return None
    groups = await session.scalar(text('SELECT groups_payload FROM public.scored_rating_summary WHERE id=1 FOR UPDATE NOWAIT'))
    if groups is None:
        raise ValueError('Rating summary is missing')
    if isinstance(groups, str):
        groups = json.loads(groups)
    changes = [dict(r) for r in (await session.execute(text(f'''
        WITH changed AS (SELECT * FROM jsonb_to_recordset(CAST(:changes AS jsonb))
            AS x(fen_id bigint,old_bucket text,new_bucket text)),
        groups AS (SELECT * FROM jsonb_to_recordset(CAST(:groups AS jsonb))
            AS x(key text,min_rating int,max_rating int))
        SELECT rg.key AS group_key,c.old_bucket,c.new_bucket,count(*) AS occurrences
        FROM changed c JOIN {layout.table('game_fen_association')} a USING(fen_id)
        JOIN {layout.table('game')} g ON g.link=a.game_link
        JOIN {layout.table('game_analysis_coverage')} gas ON gas.link=g.link
        JOIN groups rg ON round(g.avg_elo::numeric)::int BETWEEN rg.min_rating AND rg.max_rating
        WHERE gas.is_fully_analyzed AND gas.total_positions>0 AND g.avg_elo>0
        GROUP BY rg.key,c.old_bucket,c.new_bucket ORDER BY rg.key,c.old_bucket,c.new_bucket
        '''), {'changes': json.dumps(transitions), 'groups': json.dumps(groups)})).mappings()]
    updated = patch_groups(groups, changes)
    if changes:
        await session.execute(text('''UPDATE public.scored_rating_summary
            SET groups_payload=CAST(:groups AS jsonb),refreshed_at=clock_timestamp() WHERE id=1'''),
            {'groups': json.dumps(updated)})
    return {'before': groups, 'after': updated, 'changes': changes}
