"""Versioned catalog ranking over precomputed counts, never over game history."""
from collections import defaultdict
from fractions import Fraction
import hashlib
import json

from redis.exceptions import RedisError
from sqlalchemy import text

from chessism_api.database.player_analytics_schema import analytics_table
from chessism_api.database.relational_storage import storage_for
from .player_analytics_summary import enabled, read_snapshot, counts

CATALOG_SORTS = ('games', 'wins', 'losses', 'draws')
CATALOG_MIN_GAMES = 2


def catalog_min_games(kind, search):
    """Explicit opponent searches include single encounters; browsing does not."""
    return 1 if kind == 'opponents' and search.strip() else CATALOG_MIN_GAMES


def catalog_ratio_bin(sort_by, ratio_bin):
    if sort_by not in CATALOG_SORTS:
        raise ValueError('Choose games, wins, losses or draws.')
    if sort_by == 'games':
        if ratio_bin is not None:
            raise ValueError('Percentage bins require wins, losses or draws.')
        return None
    if ratio_bin is None:
        return 9
    if type(ratio_bin) is not int or not 0 <= ratio_bin <= 9:
        raise ValueError('Percentage bin must be an integer from 0 to 9.')
    return ratio_bin


def filter_catalog_bin(ranked, sort_by='games', ratio_bin=None):
    ratio_bin = catalog_ratio_bin(sort_by, ratio_bin)
    if ratio_bin is None:
        return ranked
    # Integer arithmetic keeps exact boundaries unambiguous: [0,10), ...,
    # [80,90), [90,100]. Re-rank the filtered list before search/pagination.
    matching = (row for row in ranked if min(9, 10 * row[sort_by] // row['games']) == ratio_bin)
    ordered = sorted(matching, key=lambda row: (-Fraction(row[sort_by], row['games']), -row['games'], row['key']))
    return [{**row, 'rank': index + 1} for index, row in enumerate(ordered)]


def rank_catalog(rows, mode, color, sort_by='games', *, min_games=1, ratio_bin=None):
    ratio_bin = catalog_ratio_bin(sort_by, ratio_bin)
    grouped = defaultdict(counts)
    for row in rows:
        if (mode != 'all' and row['mode'] != mode) or (color != 'all' and row['color'] != color):
            continue
        for field in grouped[row['key']]:
            grouped[row['key']][field] += row[field]
    # Apply the minimum after pooling the selected modes/colors, but before
    # assigning ranks, searching, or paginating.
    eligible = ((key, value) for key, value in grouped.items() if value['games'] >= min_games)
    ranked = [{'key': key, 'rank': index + 1, **value,
             'unknown': value['games'] - value['wins'] - value['draws'] - value['losses']}
            for index, (key, value) in enumerate(sorted(eligible, key=lambda item: (-item[1]['games'], item[0])))]
    return filter_catalog_bin(ranked, sort_by, ratio_bin)


async def catalog_snapshot_page(session, redis, scope, *, kind, mode, color, search, page, limit, sort_by='games', ratio_bin=None):
    ratio_bin = catalog_ratio_bin(sort_by, ratio_bin)
    if not enabled():
        return None
    meta = (await session.execute(text(f'''SELECT revision,generated_at FROM
        {analytics_table(storage_for(session), 'snapshot')} WHERE player_name=:player AND section=:kind'''),
        {'player': scope.player, 'kind': kind})).mappings().first()
    if not meta:
        from .cuda_summaries.metadata_store import enabled as cuda_metadata
        if not await cuda_metadata(session):return None
        meta={'revision':0,'generated_at':None}
    # Reuse the saved counts across outcomes; filter and rank before paging.
    minimum = catalog_min_games(kind, search)
    # Separate the searchable ranking from the repeated-only browsing ranking,
    # but reuse it across search strings rather than rebuilding per keystroke.
    identity = [scope.player, kind, mode, color, meta['revision'], minimum]
    key = 'chessism:catalog-ranking:v7:' + hashlib.sha256(json.dumps(identity).encode()).hexdigest()
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
        ranked = rank_catalog(saved['rows'], mode, color, min_games=minimum)
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
    matching = [row for row in filter_catalog_bin(ranked, sort_by, ratio_bin) if matches(row)]
    rows = [dict(row) for row in matching[(page - 1) * limit:page * limit]]
    if kind == 'opponents':
        from .player_game_catalog import _latest_opponent_ratings, RATING_MODES
        ratings = await _latest_opponent_ratings(session, [row['key'] for row in rows if row['key']], mode)
        for row in rows:
            row['latest_ratings'] = ratings.get(row['key'], dict.fromkeys(RATING_MODES if mode == 'all' else [mode]))
    total = len(matching)
    return {'version': 1, **scope.response_base(), 'kind': kind, 'color': color, 'search': search, 'sort_by': sort_by, 'ratio_bin': ratio_bin,
            'rows': rows, 'total_games': sum(row['games'] for row in matching), 'summary_revision': meta['revision'],
            'max_age_seconds': 0, 'pagination': {'page': page, 'limit': limit, 'total_groups': total,
                'total_pages': (total + limit - 1) // limit, 'has_more': page * limit < total}}
