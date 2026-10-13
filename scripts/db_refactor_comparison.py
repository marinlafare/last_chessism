"""Fixed added-player selection and explicit legacy-to-numeric read adapters."""
from copy import deepcopy
import re


async def add_verified_medium(conn, inputs):
    result = deepcopy(inputs)
    exclude = [p['player_name'] for p in inputs['players']]
    player = await conn.fetchrow('''
        SELECT gp.player_name,count(*)::bigint AS games,p.joined
        FROM public.game_player gp JOIN public.player p ON p.player_name=gp.player_name
        WHERE p.joined IS NOT NULL AND p.joined<>0 AND NOT (gp.player_name=ANY($1::text[]))
        GROUP BY gp.player_name,p.joined HAVING count(*)>=1000
        ORDER BY abs(count(*)-10000),gp.player_name LIMIT 1
    ''',exclude)
    if player is None:
        raise RuntimeError('No additional joined!=0 player has at least 1,000 games')
    seed = await conn.fetchrow('''
        SELECT gp.link,gp.mode,g.year FROM public.game_player gp JOIN public.game g ON g.link=gp.link
        WHERE gp.player_name=$1 AND g.rules='chess' AND g.fens_done
        ORDER BY gp.played_at DESC NULLS LAST,gp.link DESC LIMIT 1
    ''',player['player_name'])
    if seed is None:
        raise RuntimeError('Added player has no extracted standard game')
    result['players'].append(dict(player,**dict(seed),cohort='medium_verified'))
    return result


def candidate_case(case):
    adapted = deepcopy(case)
    sql = case['sql']
    if case['name'].endswith('/position_frequencies'):
        sql = '''SELECT p.player_color,f.fen AS fen_fen,p.games_with_position,p.total_occurrences,f.score
            FROM player_position_frequency p JOIN fen f ON f.fen_id=p.fen_id
            WHERE p.player_name=$1
            ORDER BY p.games_with_position DESC,p.player_color,f.fen COLLATE "C" LIMIT 100'''
    else:
        # Only the known SQL join shape is rewritten, not arbitrary identifiers,
        # string literals, catalog definitions or production source files.
        sql = re.sub(r'\bf\.fen\s*=\s*(a|gfa)\.fen_fen\b',r'f.fen_id=\1.fen_id',sql)
        if case['name'].endswith('/fen_selection'):
            sql = sql.replace('a.fen_fen COLLATE "C" AS fen','f.fen COLLATE "C" AS fen')
    if '.fen_fen' in sql:
        raise ValueError('Unmapped text-FEN reference in candidate workload')
    adapted.update(sql=sql,source='candidate read projection + numeric FEN joins')
    return adapted


def compare_results(before,after):
    if {r['name'] for r in before}!={r['name'] for r in after}:
        raise ValueError('Before/after cases differ')
    original = {r['name']:r for r in before}
    comparisons = []
    for new in after:
        old = original[new['name']]
        row = {'name':new['name'],'before_status':old['status'],'after_status':new['status']}
        if old['status']=='succeeded' and new['status']=='succeeded':
            baseline,current = old['warm_median_ms'],new['warm_median_ms']
            row.update(results_match=old['result_sha256']==new['result_sha256'],
                       before_ms=baseline,after_ms=current,
                       ratio=round(current/max(baseline,.001),3),
                       delta_ms=round(current-baseline,3),
                       # A triage flag, never a blanket "20% regression allowed"
                       # acceptance rule. Small/noisy changes still need review.
                       investigate_slowdown=current>baseline*1.2 and current-baseline>5)
        else:
            row.update(results_match=False,investigate_slowdown=True)
        comparisons.append(row)
    return comparisons
