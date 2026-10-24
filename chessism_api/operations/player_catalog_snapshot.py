"""Versioned catalog ranking over precomputed counts, never over game history."""
from collections import defaultdict
import hashlib
import json

from redis.exceptions import RedisError
from sqlalchemy import text

from chessism_api.database.player_analytics_schema import analytics_table
from chessism_api.database.relational_storage import storage_for
from .player_analytics_summary import enabled, read_snapshot, counts


def rank_catalog(rows, mode, color):
    grouped = defaultdict(counts)
    for row in rows:
        if (mode != 'all' and row['mode'] != mode) or (color != 'all' and row['color'] != color):
            continue
        for field in grouped[row['key']]:
            grouped[row['key']][field] += row[field]
    return [{'key': key, 'rank': index + 1, **value,
             'unknown': value['games'] - value['wins'] - value['draws'] - value['losses']}
            for index, (key, value) in enumerate(sorted(grouped.items(), key=lambda item: (-item[1]['games'], item[0])))]


async def catalog_snapshot_page(session, redis, scope, *, kind, mode, color, search, page, limit):
    if not enabled():
        return None
    meta = (await session.execute(text(f'''SELECT revision,generated_at FROM
        {analytics_table(storage_for(session), 'snapshot')} WHERE player_name=:player AND section=:kind'''),
        {'player': scope.player, 'kind': kind})).mappings().first()
    if not meta:
        return None
    identity = [scope.player, kind, mode, color, meta['revision']]
    key = 'chessism:catalog-ranking:v1:' + hashlib.sha256(json.dumps(identity).encode()).hexdigest()
    ranked = None
    try:
        raw = await redis.get(key)
        ranked = json.loads(raw) if raw else None
    except (RedisError, TypeError, ValueError):
        pass
    if not isinstance(ranked, list):
        saved = await read_snapshot(session, scope.player, kind)
        if not saved:
            return None
        ranked = rank_catalog(saved['rows'], mode, color)
        # A refresh between reads must not cache a new ranking under an old key.
        if saved['summary_revision'] == meta['revision']:
            try:
                await redis.set(key, json.dumps(ranked), ex=3600)
            except RedisError:
                pass
        meta = {**meta, 'revision': saved['summary_revision']}
    def matches(row):
        label = (row['key'].replace('-', ' ') or 'Unknown opening') if kind == 'openings' else (row['key'] or 'Unknown opponent')
        return not search or search in row['key'].lower() or search in label.lower()
    matching = [row for row in ranked if matches(row)]
    rows = [dict(row) for row in matching[(page - 1) * limit:page * limit]]
    if kind == 'opponents':
        from .player_game_catalog import _latest_opponent_ratings, RATING_MODES
        ratings = await _latest_opponent_ratings(session, [row['key'] for row in rows if row['key']], mode)
        for row in rows:
            row['latest_ratings'] = ratings.get(row['key'], dict.fromkeys(RATING_MODES if mode == 'all' else [mode]))
    total = len(matching)
    return {'version': 1, **scope.response_base(), 'kind': kind, 'color': color, 'search': search,
            'rows': rows, 'total_games': sum(row['games'] for row in matching), 'summary_revision': meta['revision'],
            'max_age_seconds': 0, 'pagination': {'page': page, 'limit': limit, 'total_groups': total,
                'total_pages': (total + limit - 1) // limit, 'has_more': page * limit < total}}
