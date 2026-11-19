import asyncio
from typing import Optional, Dict, Any, List
import logging

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql import player_profiles as persistence
from chessism_api.database.postgresql.transactions import read_transaction, write_transaction
from chessism_api.operations.dashboard_views import (
    get_main_character_time_control_counts,
    get_top_main_characters_by_time_control,
)
from chessism_api.operations.models import PlayerCreateData, PlayerStatsCreateData
from chessism_api.operations.ingestion_pipeline.chesscom import get_profile, get_player_stats

LOG = logging.getLogger(__name__)


async def read_player(player_name: str, *, session_factory=None) -> Optional[Dict[str, Any]]:
    async with read_transaction(session_factory or AsyncDBSession) as session:
        return await persistence.read_profile(session, player_name.strip().lower())


async def insert_player(data: dict, *, session_factory=None) -> PlayerCreateData:
    """Fetch outside a transaction, then atomically save or preserve a local shell."""
    player = data['player_name'].strip().lower()
    fetched = await get_profile(player)
    async with write_transaction(session_factory or AsyncDBSession) as session:
        if fetched is None:
            saved = await persistence.ensure_shell(session, player)
        else:
            saved = await persistence.save_profile(session, {**fetched.model_dump(), 'player_name': player})
    return PlayerCreateData(**saved)


async def get_current_players_with_games_in_db(*, session_factory=None) -> List[Dict[str, Any]]:
    """Return full, nondeleted profiles, including profiles with no games yet."""
    async with read_transaction(session_factory or AsyncDBSession) as session:
        return await persistence.primary_profiles(session)


async def read_player_stats(player_name: str, *, session_factory=None) -> Optional[Dict[str, Any]]:
    async with read_transaction(session_factory or AsyncDBSession) as session:
        return await persistence.read_stats(session, player_name.strip().lower())


def _parse_stats_category(raw_stats: Dict[str, Any], category: str) -> Dict[str, Any]:
    """Helper to safely parse one stats category (e.g., 'chess_rapid')."""
    data = raw_stats.get(category, {})
    
    # Handle cases where data is None
    if data is None:
        data = {}

    last = data.get('last', {}) or {}
    best = data.get('best', {}) or {}
    record = data.get('record', {}) or {}
    
    return {
        f"{category}_last_rating": last.get('rating'),
        f"{category}_best_rating": best.get('rating'),
        f"{category}_games": record.get('game_count'), # Custom mapping
        f"{category}_wins": record.get('win'),
        f"{category}_losses": record.get('loss'),
        f"{category}_draws": record.get('draw'),
        # --- UPDATED: Check for percentile OR percentage_rank ---
        f"{category}_last_percentile": last.get('percentile') or last.get('percentage_rank')
    }

async def create_and_store_player_stats(player_name: str, *, session_factory=None) -> Optional[PlayerStatsCreateData]:
    """Publish a fetched statistics snapshot; absence/errors never erase old stats."""
    player = player_name.strip().lower()
    if await read_player(player, session_factory=session_factory) is None:
        await insert_player({'player_name': player}, session_factory=session_factory)
    raw = await get_player_stats(player)
    if not raw:
        return None
    parsed = {'player_name': player}
    for category in ('chess_rapid', 'chess_blitz', 'chess_bullet'):
        parsed.update(_parse_stats_category(raw, category))
    parsed.update(fide=raw.get('fide'),
        puzzle_rush_best_score=raw.get('puzzle_rush', {}).get('best', {}).get('score'),
        tactics_highest_rating=raw.get('tactics', {}).get('highest', {}).get('rating'),
        tactics_lowest_rating=raw.get('tactics', {}).get('lowest', {}).get('rating'))
    try:
        stats = PlayerStatsCreateData(**parsed)
    except ValueError:
        LOG.warning('Invalid remote statistics for %s; previous snapshot retained', player)
        return None
    async with write_transaction(session_factory or AsyncDBSession) as session:
        saved = await persistence.save_stats(session, stats.model_dump())
    return PlayerStatsCreateData(**saved)

# --- NEW FUNCTION ---
async def update_stats_for_all_primary_players(api_delay: float = 1.0) -> Dict[str, List[str]]:
    """
    Fetches stats for all primary players (joined != 0).
    Includes a delay to be respectful to the Chess.com API.
    
    Returns:
        A dictionary categorizing successful and failed player updates.
    """
    print("Starting batch job: Update stats for all primary players...")
    primary_players = await get_current_players_with_games_in_db()
    
    if not primary_players:
        print("No primary players found in the database.")
        return {"success": [], "failed": []}
        
    print(f"Found {len(primary_players)} primary players to update.")
    
    success_list = []
    failed_list = []
    
    for i, player in enumerate(primary_players):
        player_name = player.get('player_name')
        if not player_name:
            continue
            
        print(f"Updating stats for: {player_name} ({i+1}/{len(primary_players)})...")
        try:
            stats = await create_and_store_player_stats(player_name)
            if stats:
                success_list.append(player_name)
            else:
                print(f"Failed to update stats for {player_name} (API returned None).")
                failed_list.append(player_name)
        except Exception as e:
            print(f"An exception occurred while updating stats for {player_name}: {repr(e)}")
            failed_list.append(player_name)
            
        # --- API Delay ---
        # Wait before the next request, even if this one failed
        await asyncio.sleep(api_delay)
        
    print("--- Batch Stats Update Complete ---")
    print(f"Successfully updated: {len(success_list)}")
    print(f"Failed to update: {len(failed_list)}")
    if failed_list:
        print(f"Failed players: {', '.join(failed_list)}")
        
    return {"success": success_list, "failed": failed_list}


async def get_main_character_time_control_counts_payload() -> Dict[str, int]:
    """
    Operations-layer wrapper for main-character mode counts.
    """
    return await get_main_character_time_control_counts()


async def get_top_main_characters_by_time_control_payload(
    time_control: str,
    limit: int = 5,
    title_status: str = "all",
) -> Dict[str, Any]:
    """
    Operations-layer wrapper for top main characters by time control.
    """
    return await get_top_main_characters_by_time_control(
        time_control=time_control,
        limit=limit,
        title_status=title_status,
    )
