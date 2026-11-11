"""Analysis coverage views and frozen player game previews."""
from typing import Any, Dict, List, Tuple

from sqlalchemy import text

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql import game_analysis_reads as reads
from chessism_api.database.postgresql.transactions import write_transaction


async def get_scored_game_analysis_overview(*, factory=None):
    async with write_transaction(factory or AsyncDBSession) as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        return await reads.overview(session)


async def get_scored_game_analysis_page(status='fully', page=1, page_size=20, *, factory=None):
    status = (status or 'fully').strip().lower()
    if status not in {'fully', 'incomplete', 'all'}:
        status = 'fully'
    page_size = max(1, min(int(page_size or 20), 50))
    page = max(1, int(page or 1))
    async with write_transaction(factory or AsyncDBSession) as session:
        # Count and visible rows share one snapshot, even during concurrent refresh.
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
        await session.execute(text("SET LOCAL TIME ZONE 'UTC'"))
        result = await reads.coverage_page(session, status, page, page_size)
    return {'status': status, 'page_size': page_size, **result, 'rows': [
        {
            'link': int(row['link']),
            'total_positions': int(row['total_positions']),
            'analyzed_positions': int(row['analyzed_positions']),
            'unscored_positions': int(row['unscored_positions']),
            'is_fully_analyzed': bool(row['is_fully_analyzed']),
            'avg_score': row['avg_score'], 'avg_abs_score': row['avg_abs_score'],
            'max_abs_score': row['max_abs_score'],
            'white': row['white'], 'black': row['black'], 'mode': row['mode'] or '',
            'played_at': row['played_at'].isoformat(),
            'white_result': float(row['white_result']), 'black_result': float(row['black_result']),
        } for row in result['rows']]}


async def get_player_game_analysis_scope(player_name, date_from=None, date_to_exclusive=None, *, factory=None):
    async with write_transaction(factory or AsyncDBSession) as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        await session.execute(text("SET LOCAL TIME ZONE 'UTC'"))
        result = await reads.player_scope(session, player_name, date_from, date_to_exclusive)
    return {'player_name': player_name, **result,
            'earliest_game': result['earliest_game'].isoformat() if result['earliest_game'] else None,
            'latest_game': result['latest_game'].isoformat() if result['latest_game'] else None}


async def preview_player_games_for_analysis(player_name, *, order, game_limit,
        date_from=None, date_to_exclusive=None, prepared_plan_id=None, factory=None):
    """Freeze games and their workload; preparation commits only as one owned unit."""
    order = str(order or 'latest').lower()
    fair_range = order == 'fair_range'
    limit = max(1, int(game_limit)) if game_limit is not None and not fair_range else None
    async with write_transaction(factory or AsyncDBSession) as session:
        mode = '' if prepared_plan_id else ' READ ONLY'
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ'+mode))
        await session.execute(text("SET LOCAL TIME ZONE 'UTC'"))
        selected = await reads.incomplete_games(session, player_name,
            descending=order != 'oldest' and not fair_range, limit=limit,
            date_from=date_from, date_to_exclusive=date_to_exclusive)
        sampling = {'available_periods': 0, 'sampled_periods': 0}
        if fair_range and game_limit is not None:
            selected, sampling = fair_sample_player_games_by_month(selected, game_limit)
        links = [int(row['link']) for row in selected]
        if not links:
            return {'player_name': player_name, 'game_links': [], 'selected_games': 0,
                    'position_occurrences': 0, 'analyzed_occurrences': 0, 'unscored_occurrences': 0,
                    'unique_fens': 0, 'analyzed_unique_fens': 0, 'fens_to_analyze': 0,
                    'tablebase_fens': 0, **sampling}
        if prepared_plan_id:
            from chessism_api.operations.cloud_analysis.prepared_selection import build
            stats = await build(session, prepared_plan_id, links)
            return {'player_name': player_name, 'game_links': links,
                    'selected_games': len(links), **stats, **sampling}
        stats = await reads.position_counts(session, links)
    return {'player_name': player_name, 'game_links': links, 'selected_games': len(links),
            'first_game_at': selected[0]['played_at'].isoformat(),
            'last_game_at': selected[-1]['played_at'].isoformat(), **stats, **sampling}


def _evenly_spaced_indices(size: int, count: int) -> List[int]:
    """Return deterministic indices spanning the complete input range."""
    safe_size = max(0, int(size))
    safe_count = min(safe_size, max(0, int(count)))
    if safe_count <= 0:
        return []
    if safe_count == 1:
        return [(safe_size - 1) // 2]
    denominator = safe_count - 1
    return [
        ((index * (safe_size - 1)) + (denominator // 2)) // denominator
        for index in range(safe_count)
    ]


def fair_sample_player_games_by_month(
    chronological_rows: List[Any],
    game_limit: int,
) -> Tuple[List[Any], Dict[str, int]]:
    """Balance a game sample across every available calendar month.

    Every represented month receives one game before any month receives a
    second. If the requested sample is smaller than the number of months,
    evenly spaced months are selected across the full history. Games within
    each month are also selected at evenly spaced chronological positions.
    """
    rows = list(chronological_rows or [])
    target = min(len(rows), max(1, int(game_limit))) if rows else 0
    if target <= 0:
        return [], {"available_periods": 0, "sampled_periods": 0}

    period_groups: Dict[Tuple[Any, ...], List[Tuple[int, Any]]] = {}
    for chronological_index, row in enumerate(rows):
        played_at = row.get("played_at")
        period = (
            (int(played_at.year), int(played_at.month))
            if played_at is not None
            else ("unknown",)
        )
        period_groups.setdefault(period, []).append((chronological_index, row))

    groups = list(period_groups.values())
    allocations = [0] * len(groups)
    if target < len(groups):
        for group_index in _evenly_spaced_indices(len(groups), target):
            allocations[group_index] = 1
    else:
        allocations = [1] * len(groups)
        remaining = target - len(groups)
        while remaining > 0:
            eligible = [
                index
                for index, group in enumerate(groups)
                if allocations[index] < len(group)
            ]
            if not eligible:
                break
            if remaining < len(eligible):
                chosen = [
                    eligible[index]
                    for index in _evenly_spaced_indices(len(eligible), remaining)
                ]
            else:
                chosen = eligible
            for group_index in chosen:
                allocations[group_index] += 1
                remaining -= 1

    selected_with_indices: List[Tuple[int, Any]] = []
    for group, allocation in zip(groups, allocations):
        for row_index in _evenly_spaced_indices(len(group), allocation):
            selected_with_indices.append(group[row_index])
    selected_with_indices.sort(key=lambda item: item[0])

    return [row for _, row in selected_with_indices], {
        "available_periods": len(groups),
        "sampled_periods": sum(1 for allocation in allocations if allocation > 0),
    }


