import httpx
import asyncio
import os
from pprint import pprint

# The base URL for your API, as defined in docker-compose.yml
API_BASE_URL = "http://localhost:8000"

async def test_api_create_games(player_name: str):
    """
    This is the function you requested.
    It calls the POST /games endpoint to trigger the full
    download and ingestion process for a player.
    """
    print(f"--- [API TEST] ---")
    print(f"Starting 'Create Games' job for player: {player_name}")
    print("This may take a very long time, as it is downloading")
    print("all game archives one-by-one...")
    
    url = f"{API_BASE_URL}/games"
    payload = {"player_name": player_name}

    try:
        async with httpx.AsyncClient() as client:
            # Set timeout=None because this can take minutes
            response = await client.post(url, json=payload, timeout=None)
        
        response.raise_for_status() # Raise an error for 4xx or 5xx status
        
        print("\n--- JOB COMPLETE ---")
        pprint(response.json())
        
    except httpx.HTTPStatusError as e:
        print(f"\n--- ERROR (HTTP {e.response.status_code}) ---")
        try:
            pprint(e.response.json())
        except:
            print(e.response.text)
    except httpx.RequestError as e:
        print(f"\n--- REQUEST FAILED ---")
        print(f"Could not connect to {url}.")
        print(f"Error: {repr(e)}")
    except Exception as e:
        print(f"\n--- UNEXPECTED ERROR ---")
        print(repr(e))

async def test_api_get_game(link: int):
    """
    Helper function to test the GET /games/{link} endpoint.
    """
    print(f"\n--- [API TEST] ---")
    print(f"Fetching game by link: {link}")
    
    url = f"{API_BASE_URL}/games/{link}"
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
        
        response.raise_for_status()
        
        print(f"--- GAME {link} FOUND ---")
        pprint(response.json())

    except httpx.HTTPStatusError as e:
        print(f"\n--- ERROR (HTTP {e.response.status_code}) ---")
        pprint(e.response.json())
    except httpx.RequestError as e:
        print(f"\n--- REQUEST FAILED ---")
        print(f"Could not connect to {url}. Error: {repr(e)}")
    except Exception as e:
        print(f"\n--- UNEXPECTED ERROR ---")
        print(repr(e))

async def test_api_get_players():
    """
    Helper function to test the GET /players/current_players endpoint.
    """
    print(f"\n--- [API TEST] ---")
    print("Fetching all 'real' players (joined != 0)...")
    
    url = f"{API_BASE_URL}/players/current_players"
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
        
        response.raise_for_status()
        
        players = response.json()
        print(f"--- FOUND {len(players)} PLAYERS ---")
        pprint(players)

    except httpx.HTTPStatusError as e:
        print(f"\n--- ERROR (HTTP {e.response.status_code}) ---")
        pprint(e.response.json())
    except httpx.RequestError as e:
        print(f"\n--- REQUEST FAILED ---")
        print(f"Could not connect to {url}. Error: {repr(e)}")
    except Exception as e:
        print(f"\n--- UNEXPECTED ERROR ---")
        print(repr(e))

async def test_local_get_profile(player_name: str):
    """
    Tests the *local* get_profile function by importing it.
    This does NOT call the API.
    """
    print(f"\n--- [LOCAL TEST] ---")
    print(f"Testing local import of get_profile for: {player_name}")
    
    # Set a dummy env var just in case constants.py needs it
    if "DATABASE_URL" not in os.environ:
        os.environ["DATABASE_URL"] = "dummy_value"
        
    try:
        # Import the function locally
        from chessism_api.operations.chess_com_api import get_profile
        
        profile = await get_profile(player_name)
        
        if profile:
            print("--- PROFILE FOUND ---")
            # .model_dump() converts the Pydantic model to a dict for printing
            pprint(profile.model_dump())
        else:
            print("--- PROFILE NOT FOUND (Check console for 404 or other errors) ---")
            
    except ImportError as e:
        print(f"\n--- IMPORT ERROR ---")
        print(f"Could not import: {e}")
        print("Make sure your notebook is running from the project's root directory.")
    except Exception as e:
        print(f"\n--- UNEXPECTED ERROR ---")
        print(repr(e))
