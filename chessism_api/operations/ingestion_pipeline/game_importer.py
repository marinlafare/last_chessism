"""Parse downloaded games and commit their complete canonical import unit."""
from __future__ import annotations

from datetime import timezone
import time
from typing import Any

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql import game_imports
from chessism_api.database.postgresql.game_import_summaries import refresh_import_summaries
from chessism_api.database.postgresql.transactions import driver_connection, write_transaction
from chessism_api.operations.ingestion_pipeline.pgn import (
    create_game_dict, create_game_opening_rows, format_one_game_moves,
)
from chessism_api.operations.models import GameCreateData
from .steps import measured

GameArchive = dict[str, dict[str, list[dict[str, Any]]]]


async def format_games(games: GameArchive, player_name: str, reporter=None) -> list[dict[str, Any]]:
    """Parse without database writes; persisted identity checks occur at commit.

    Existing identities must still be parsed: filtering them out before comparing
    content would silently acknowledge conflicting payloads on a retry.
    """
    started = time.monotonic()
    raw_games = {}
    async with measured(reporter, 'filter_games', None, 'games'):
        for months in games.values():
            for rows in months.values():
                for game in rows:
                    try:
                        link = int(str(game.get('url') or '').rsplit('/', 1)[-1])
                    except ValueError:
                        print('Skipping downloaded game with an invalid URL.', flush=True)
                        continue
                    if link in raw_games and raw_games[link] != game:
                        raise ValueError('Conflicting downloaded game identities')
                    raw_games[link] = game
        if reporter:
            await reporter.advance(sum(len(rows) for months in games.values() for rows in months.values()))
    valid = []
    async with measured(reporter, 'parse_games', len(raw_games), 'games'):
        for index, game in enumerate(raw_games.values(), 1):
            try:
                parsed = create_game_dict(game)
                if isinstance(parsed, dict):
                    valid.append(parsed)
            except Exception as error:
                print(f"Skipping malformed Chess.com game {game.get('url', 'N/A')}: {error}", flush=True)
            if reporter and index % 1000 == 0:
                await reporter.advance(1000)
    print(f'Parsed {len(valid)}/{len(raw_games)} games for {player_name} '
          f'in {time.monotonic() - started:.2f}s.', flush=True)
    return valid


def _prepare_game_bundle(formatted_games: list[dict[str, Any]]) -> dict[str, Any]:
    bundle = {'games': [], 'moves': [], 'game_openings': [],
              'affected_months': set(), 'affected_players': set()}
    for formatted_game in formatted_games:
        moves_data = formatted_game.get('moves_data')
        try:
            game = GameCreateData(**{k: v for k, v in formatted_game.items() if k != 'moves_data'}).model_dump()
            played = game['played_at']
            if played is None or played.utcoffset() is None:
                raise ValueError('A UTC game timestamp is required')
            played = played.astimezone(timezone.utc)
        except Exception as error:
            print(f"Skipping malformed formatted game {formatted_game.get('link')}: {error}", flush=True)
            continue
        game_moves = format_one_game_moves(moves_data) if moves_data else []
        if game['n_moves'] > 0 and not game_moves:
            print(f"Skipping game {game['link']}: no valid move rows.", flush=True)
            continue
        # Zero-move games retain exactly the same metadata as every other game.
        bundle['games'].append(game)
        bundle['moves'].extend(game_moves)
        bundle['game_openings'].extend(create_game_opening_rows(game, moves_data))
        bundle['affected_players'].update(str(game[side]).strip().lower() for side in ('white','black'))
        bundle['affected_months'].add((played.year, played.month))
    return bundle


async def sync_player_months(player_name: str, months: set[tuple[int, int]]) -> None:
    """Application transaction for an empty or already imported archive response."""
    if not months:
        return
    async with write_transaction(AsyncDBSession) as session:
        connection = await driver_connection(session)
        await game_imports.sync_month_counts(connection, player_name, months)
        await refresh_import_summaries(connection, [player_name], [])


async def insert_games_months_moves_and_players(
    formatted_games: list[dict[str, Any]], player_name: str, *, fetched_months=None,
) -> dict[str, Any]:
    """Commit games, month ledger and projection invalidation as one operation."""
    started = time.monotonic()
    bundle = _prepare_game_bundle(formatted_games)
    months = bundle['affected_months'] | set(fetched_months or ())
    if not bundle['games'] and not months:
        return {'committed_games': 0, 'zero_move_games': 0, 'message': f'No new data to insert for {player_name}.'}
    async with write_transaction(AsyncDBSession) as session:
        connection = await driver_connection(session)
        result = await game_imports.ingest_games(connection, bundle['games'], bundle['moves'], bundle['game_openings'])
        await game_imports.sync_month_counts(connection, player_name, months)
        await refresh_import_summaries(connection, bundle['affected_players'], result['analyzable_links'])
        zero_moves = await connection.fetchval('''SELECT count(*) FROM public.game
            WHERE link=ANY($1::bigint[]) AND is_standard AND n_moves=0''', result['inserted_links'])
    inserted_games = len(result['inserted_links']) - zero_moves
    print(f'Persisted {inserted_games} games and {zero_moves} zero-move games '
          f'in {time.monotonic() - started:.2f}s.', flush=True)
    if bundle['games'] and not result['inserted_links']:
        return {'committed_games': 0, 'zero_move_games': 0, 'message': 'All games already at DB'}
    if not bundle['games']:
        return {'committed_games': 0, 'zero_move_games': 0, 'message': f'No new data to insert for {player_name}.'}
    return {'committed_games': len(result['inserted_links']), 'zero_move_games': zero_moves,
            'message': (f'Successfully processed and inserted {inserted_games} games '
                        f'and recorded {zero_moves} zero-move games for {player_name}.')}
