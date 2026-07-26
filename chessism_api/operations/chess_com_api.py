import json
import time
import asyncio
from typing import Any, Awaitable, Callable, Dict, List, Optional

import httpx

import constants
from chessism_api.operations.models import PlayerCreateData

async def get_profile(player_name: str) -> Optional[PlayerCreateData]:
    """
    Fetches a player's profile from the Chess.com API.
    """
    player_url = constants.PLAYER.replace("{player}", player_name)
    
    # --- FIX: Force HTTP/1.1 ---
    async with httpx.AsyncClient(timeout=5, http2=False) as client:
        try:
            response = await client.get(
                player_url,
                headers={"User-Agent": constants.USER_AGENT}
            )
            response.raise_for_status()
            
            raw_data = response.json()

            # --- Transformation to match PlayerCreateData Pydantic model ---
            processed_data = {} 
            processed_data['player_name'] = player_name.lower()

            processed_data['name'] = raw_data.get('name')
            processed_data['url'] = raw_data.get('url')
            processed_data['title'] = raw_data.get('title')
            processed_data['avatar'] = raw_data.get('avatar')
            processed_data['followers'] = raw_data.get('followers')
            
            country_url = raw_data.get('country')
            if country_url:
                processed_data['country'] = country_url.split('/')[-1]
            else:
                processed_data['country'] = None

            processed_data['location'] = raw_data.get('location')
            
            joined_ts = raw_data.get('joined')
            if joined_ts is not None:
                try:
                    processed_data['joined'] = int(joined_ts)
                except (ValueError, TypeError):
                    print(f"Warning: Could not convert 'joined' ({joined_ts}) to int for {player_name}. Setting to 0.")
                    processed_data['joined'] = 0
            else:
                processed_data['joined'] = 0 # Default to 0 if joined is None
            
            processed_data['status'] = raw_data.get('status')
            processed_data['is_streamer'] = raw_data.get('is_streamer')
            processed_data['twitch_url'] = raw_data.get('twitch_url')
            processed_data['verified'] = raw_data.get('verified')
            processed_data['league'] = raw_data.get('league')
            
            # Use Pydantic to validate and create the data object
            player_data = PlayerCreateData(**processed_data)
            return player_data # Return the Pydantic model instance

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                print(f"Player '{player_name}' not found on Chess.com (404).")
                return None
            print(f"HTTP error for profile {player_name}: {e.response.status_code} - {e.response.text}")
            return None
        except httpx.RequestError as e:
            # --- UPDATED: Use repr(e) for better error details ---
            print(f"Request error for profile {player_name}: {repr(e)}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred getting profile {player_name}: {e}")
            return None


# --- NEW FUNCTION: get_player_stats ---
async def get_player_stats(player_name: str) -> Optional[Dict[str, Any]]:
    """
    Fetches a player's stats from the Chess.com API.
    Returns the raw JSON dictionary.
    """
    stats_url = constants.STATS.replace("{username}", player_name)
    
    async with httpx.AsyncClient(timeout=5, http2=False) as client:
        try:
            response = await client.get(
                stats_url,
                headers={"User-Agent": constants.USER_AGENT}
            )
            response.raise_for_status()
            raw_data = response.json()
            return raw_data

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                print(f"Stats for '{player_name}' not found on Chess.com (404).")
                return None
            print(f"HTTP error for stats {player_name}: {e.response.status_code} - {e.response.text}")
            return None
        except httpx.RequestError as e:
            print(f"Request error for stats {player_name}: {repr(e)}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred getting stats {player_name}: {e}")
            return None


async def download_month(
    player_name: str,
    year: int,
    month: int,
    client: httpx.AsyncClient,
) -> Optional[httpx.Response]:
    """Fetch one monthly archive, retrying once on an empty response."""
    month_str = f"{month:02d}"
    download_url = (
        constants.DOWNLOAD_MONTH
        .replace("{player}", player_name)
        .replace("{year}", str(year))
        .replace("{month}", month_str)
    )

    try:
        games_response = await client.get(
            download_url,
            follow_redirects=True,
            timeout=5,
            headers={"User-Agent": constants.USER_AGENT}
        )
        
        if not games_response.content:
            await asyncio.sleep(1)
            games_response = await client.get(
                download_url,
                follow_redirects=True,
                timeout=10,
                headers={"User-Agent": constants.USER_AGENT}
            )
        
        if not games_response.content:
            print(f"No content after two attempts for {player_name} in {year}-{month_str}.")
            return None

        games_response.raise_for_status()
        return games_response

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            print(f"No games found for {player_name} in {year}-{month_str} (404).")
            return None
        print(f"HTTP error downloading month {year}-{month_str}: {e.response.status_code} - {e.response.text}")
        return None
    except httpx.RequestError as e:
        print(f"Request error downloading month {year}-{month_str}: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred downloading month {year}-{month_str}: {e}")
        return None


async def _month_of_games(
    player_name: str,
    year: int,
    month: int,
    client: httpx.AsyncClient,
) -> Optional[Dict[str, Any]]:
    """
    Downloads a month of games and returns as parsed JSON dictionary.
    """
    pgn_response = await download_month(player_name, year, month, client)
    
    if pgn_response is None:
        return None

    try:
        text_games = pgn_response.text
        text_games = text_games.replace(' \\"Let"s Play!','lets_play') 
        
        parsed_json = json.loads(text_games)
        return parsed_json
    except json.JSONDecodeError as e:
        print(f'JSON decoding failed for year: {year}, month: {month}: {e}')
        return None
    except Exception as e:
        print(f"Error filtering raw PGN for {year}-{month}: {e}")
        return None


async def download_months(
    player_name: str,
    valid_dates: List[str],
    min_delay_between_requests: float = 0.5,
    progress_callback: Callable[[int, str], Awaitable[None]] | None = None,
) -> Dict[int, Dict[int, List[Dict[str, Any]]]]:
    """
    Downloads games for a player's month strings one by one
    to comply with Chess.com policies.
    """
    all_games_by_month: Dict[int, Dict[int, List[Dict[str, Any]]]] = {}
    
    async with httpx.AsyncClient(timeout=15, http2=False) as shared_client:
        print(f"Starting serial download of {len(valid_dates)} months...")
        start_time = time.time()

        for index, month_str in enumerate(valid_dates, start=1):
            if index > 1:
                await asyncio.sleep(min_delay_between_requests)

            year_str, month_str_val = month_str.split('-')
            year = int(year_str)
            month = int(month_str_val)

            try:
                result = await _month_of_games(player_name, year, month, shared_client)

                if result is None:
                    print(f"No data for {year}-{month}.")
                elif result.get('games') is not None:
                    all_games_by_month.setdefault(year, {})[month] = result['games']
                else:
                    print(f"No games or invalid data for {year}-{month} (missing/empty 'games' key in parsed JSON).")

            except Exception as e:
                print(f"An error occurred processing {month_str}: {e}")

            finally:
                if progress_callback:
                    await progress_callback(index, month_str)
        
        end_time = time.time()
        print(f"Finished downloading {len(valid_dates)} months in {end_time - start_time:.2f} seconds.")

    return all_games_by_month
