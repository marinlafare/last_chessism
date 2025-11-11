import httpx
import asyncio
import os
from pprint import pprint

# All API calls go to the chessism-api service
API_BASE_URL = "http://localhost:8000"

async def test_api_create_games(player_name: str):
    """
    Calls the POST /games endpoint to trigger the full
    download and ingestion process for a player.
    """
    print("\n--- [API TEST] ---")
    print(f"Starting 'Create Games' job for player: {player_name}")
    print("This may take a very long time, as it is downloading")
    print("all game archives one-by-one...")
    
    url = f"{API_BASE_URL}/games" # Correct path
    payload = {"player_name": player_name}
    
    try:
        # --- FIX: Disable HTTP/2, force HTTP/1.1 ---
        async with httpx.AsyncClient(http2=False) as client:
            # Set timeout=None because this can take minutes
            response = await client.post(url, json=payload, timeout=None)
        
        response.raise_for_status() # Raise an error for 4xx or 5xx status
        
        print("\n--- SUCCESS (Job Response) ---")
        pprint(response.json())
        
    except httpx.HTTPStatusError as e:
        print(f"\n--- ERROR (HTTP {e.response.status_code}) ---")
        try:
            pprint(e.response.json())
        except:
            print(e.response.text)
    except httpx.RequestError as e:
        print(f"\n--- REQUEST ERROR (Connection Failed) ---")
        print(repr(e))
    except Exception as e:
        print(f"\n--- UNEXPECTED ERROR ---")
        print(repr(e))


async def test_api_get_game(link: int):
    """
    Calls the GET /games/{link} endpoint to fetch a single game.
    """
    print(f"\n--- [API TEST] ---")
    print(f"Fetching game link: {link}")
    
    url = f"{API_BASE_URL}/games/{link}"
    
    try:
        # --- FIX: Disable HTTP/2, force HTTP/1.1 ---
        async with httpx.AsyncClient(http2=False) as client:
            response = await client.get(url)
        
        response.raise_for_status()
        
        print("\n--- SUCCESS (Game Data) ---")
        pprint(response.json())
        
    except httpx.HTTPStatusError as e:
        print(f"\n--- ERROR (HTTP {e.response.status_code}) ---")
        try:
            pprint(e.response.json())
        except:
            print(e.response.text)
    except Exception as e:
        print(f"\n--- UNEXPECTED ERROR ---")
        print(repr(e))


async def test_api_get_players():
    """
    Calls the GET /players/current_players endpoint.
    """
    print(f"\n--- [API TEST] ---")
    print("Fetching all 'real' players (joined != 0)...")
    
    url = f"{API_BASE_URL}/players/current_players"
    
    try:
        # --- FIX: Disable HTTP/2, force HTTP/1.1 ---
        async with httpx.AsyncClient(http2=False) as client:
            response = await client.get(url)
        
        response.raise_for_status()
        
        print("\n--- SUCCESS (Player List) ---")
        pprint(response.json())
        
    except httpx.HTTPStatusError as e:
        print(f"\n--- ERROR (HTTP {e.response.status_code}) ---")
        try:
            pprint(e.response.json())
        except:
            print(e.response.text)
    except Exception as e:
        print(f"\n--- UNEXPECTED ERROR ---")
        print(repr(e))

async def test_api_get_player_profile(player_name: str):
    """
    Calls the GET /players/{player_name} endpoint.
    This will fetch from DB or from Chess.com if not found.
    """
    print(f"\n--- [API TEST] ---")
    print(f"Fetching API profile for: {player_name}")
    
    url = f"{API_BASE_URL}/players/{player_name}"
    
    try:
        # --- FIX: Disable HTTP/2, force HTTP/1.1 ---
        async with httpx.AsyncClient(http2=False) as client:
            response = await client.get(url)
        
        response.raise_for_status()
        
        print("\n--- SUCCESS (Profile Data from API) ---")
        pprint(response.json())
        
    except httpx.HTTPStatusError as e:
        print(f"\n--- ERROR (HTTP {e.response.status_code}) ---")
        try:
            pprint(e.response.json())
        except:
            print(e.response.text)
    except Exception as e:
        print(f"\n--- UNEXPECTED ERROR ---")
        print(repr(e))

#
# --- FUNCTION REMOVED ---
# test_local_get_profile() has been deleted as requested.
#