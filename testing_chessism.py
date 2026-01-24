import httpx
import asyncio
import os
from pprint import pprint

# All API calls go to the chessism-api service
API_BASE_URL = "http://localhost:8003"

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
async def test_api_get_player_stats(player_name: str):
    """
    Calls the GET /players/{player_name}/stats endpoint.
    This will fetch fresh stats from Chess.com and "upsert" them.
    """
    print(f"\n--- [API TEST] ---")
    print(f"Fetching API stats for: {player_name}")
    
    url = f"{API_BASE_URL}/players/{player_name}/stats"
    
    try:
        # --- FIX: Disable HTTP/2, force HTTP/1.1 ---
        async with httpx.AsyncClient(http2=False) as client:
            response = await client.get(url)
        
        response.raise_for_status()
        
        print("\n--- SUCCESS (Stats Data from API) ---")
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

async def test_api_update_all_stats():
    """
    Calls the POST /players/update-all-stats endpoint.
    This triggers a background task on the server.
    """
    print(f"\n--- [API TEST] ---")
    print("Triggering batch job to update stats for ALL primary players...")
    
    url = f"{API_BASE_URL}/players/update-all-stats"
    
    try:
        # --- FIX: Disable HTTP/2, force HTTP/1.1 ---
        async with httpx.AsyncClient(http2=False) as client:
            # This request should be very fast, so a short timeout is fine
            response = await client.post(url, timeout=30) 
        
        response.raise_for_status()
        
        print("\n--- SUCCESS (Job Started) ---")
        print(f"Status Code: {response.status_code}") # Should be 202
        pprint(response.json())
        print("\nCheck your docker-compose logs to see the job progress.")
        
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
async def test_api_generate_fens(total_games: int, batch_size: int = 10):
    """
    Calls the POST /fens/generate endpoint.
    This triggers a background task on the server.
    
    Args:
        total_games (int): The max number of games to process.
        batch_size (int): How many games to process per loop.
    """
    print(f"\n--- [API TEST] ---")
    print(f"Triggering batch job to generate FENs for {total_games} games...")
    
    url = f"{API_BASE_URL}/fens/generate"
    
    payload = {
        "total_games_to_process": total_games,
        "batch_size": batch_size
    }
    
    try:
        # --- FIX: Disable HTTP/2, force HTTP/1.1 ---
        async with httpx.AsyncClient(http2=False) as client:
            response = await client.post(url, json=payload, timeout=30) 
        
        response.raise_for_status()
        
        print("\n--- SUCCESS (Job Started) ---")
        print(f"Status Code: {response.status_code}") # Should be 202
        pprint(response.json())
        print("\nCheck your docker-compose logs to see the job progress.")
        
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
async def test_api_get_top_fens(limit: int = 20):
    """
    Calls the GET /fens/top endpoint with a query parameter.
    """
    print(f"\n--- [API TEST] ---")
    print(f"Fetching top {limit} FENs...")
    
    # --- FIXED URL: Changed from /fens/top/{limit} to /fens/top?limit={limit} ---
    url = f"{API_BASE_URL}/fens/top?limit={limit}"
    
    try:
        # --- FIX: Disable HTTP/2, force HTTP/1.1 ---
        async with httpx.AsyncClient(http2=False) as client:
            response = await client.get(url, timeout=30) 
        
        response.raise_for_status()
        
        data = response.json()
        
        print("\n--- SUCCESS (Top FENs) ---")
        # Print the results string directly for clear output
        if 'results' in data:
            print(data['results'])
        else:
            pprint(data)
        
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
async def test_api_get_sum_n_games(threshold: int = 10):
    """
    Calls the GET /fens/sum_n_games endpoint.
    """
    print(f"\n--- [API TEST] ---")
    print(f"Fetching SUM(n_games) for FENs where n_games > {threshold}...")
    
    url = f"{API_BASE_URL}/fens/sum_n_games?threshold={threshold}"
    
    try:
        # --- FIX: Disable HTTP/2, force HTTP/1.1 ---
        async with httpx.AsyncClient(http2=False) as client:
            response = await client.get(url, timeout=60) # Give it a reasonable timeout
        
        response.raise_for_status()
        
        data = response.json()
        
        print("\n--- SUCCESS (Sum Data) ---")
        pprint(data)
        
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
        
async def test_api_run_analysis_job(
    total_fens: int,
    batch_size: int = 1000,
    nodes: int = 1000000
):
    """
    Calls the POST /analysis/run_job endpoint.
    This triggers a background task on the server.
    """
    print(f"\n--- [API TEST] ---")
    print(f"Triggering analysis job for {total_fens} FENs...")
    
    url = f"{API_BASE_URL}/analysis/run_job"
    
    payload = {
        "total_fens_to_process": total_fens,
        "batch_size": batch_size,
        "nodes_limit": nodes
    }
    
    try:
        async with httpx.AsyncClient(http2=False) as client:
            response = await client.post(url, json=payload, timeout=30) 
        
        response.raise_for_status()
        
        print("\n--- SUCCESS (Job Started) ---")
        print(f"Status Code: {response.status_code}") # Should be 202
        pprint(response.json())
        print("\nCheck your docker-compose logs to see the job progress.")
        
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

async def test_api_run_player_analysis_job(
    player_name: str,
    total_fens: int,
    batch_size: int = 1000,
    nodes: int = 1000000
):
    """
    Calls the POST /analysis/run_player_job endpoint.
    This triggers a background task on the server.
    """
    print(f"\n--- [API TEST] ---")
    print(f"Triggering PLAYER job for '{player_name}'...")
    
    url = f"{API_BASE_URL}/analysis/run_player_job"
    
    payload = {
        "player_name": player_name,
        "total_fens_to_process": total_fens,
        "batch_size": batch_size,
        "nodes_limit": nodes
    }
    
    try:
        async with httpx.AsyncClient(http2=False) as client:
            response = await client.post(url, json=payload, timeout=30) 
        
        response.raise_for_status()
        
        print("\n--- SUCCESS (Player Job Started) ---")
        print(f"Status Code: {response.status_code}") # Should be 202
        pprint(response.json())
        print("\nCheck your docker-compose logs to see the job progress.")
        
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


async def test_api_run_analysis_job_night(
    total_fens: int,
    batch_size: int = 1000,
    nodes: int = 1000000,
    workers_count: int = 6
):
    """
    Calls the POST /analysis/run_job_night endpoint.
    """
    print(f"\n--- [API TEST] ---")
    print(f"Triggering NIGHT analysis job for {total_fens} FENs with {workers_count} workers...")
    
    url = f"{API_BASE_URL}/analysis/run_job_night"
    
    payload = {
        "total_fens_to_process": total_fens,
        "batch_size": batch_size,
        "nodes_limit": nodes,
        "workers_count": workers_count
    }
    
    try:
        async with httpx.AsyncClient(http2=False) as client:
            response = await client.post(url, json=payload, timeout=30) 
        
        response.raise_for_status()
        
        print("\n--- SUCCESS (Job Started) ---")
        print(f"Status Code: {response.status_code}") # Should be 202
        pprint(response.json())
        print("\nCheck your docker-compose logs to see the job progress.")
        
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


async def test_api_run_player_analysis_job_night(
    player_name: str,
    total_fens: int,
    batch_size: int = 1000,
    nodes: int = 1000000,
    workers_count: int = 6
):
    """
    Calls the POST /analysis/run_player_job_night endpoint.
    """
    print(f"\n--- [API TEST] ---")
    print(f"Triggering NIGHT PLAYER job for '{player_name}' with {workers_count} workers...")
    
    url = f"{API_BASE_URL}/analysis/run_player_job_night"
    
    payload = {
        "player_name": player_name,
        "total_fens_to_process": total_fens,
        "batch_size": batch_size,
        "nodes_limit": nodes,
        "workers_count": workers_count
    }
    
    try:
        async with httpx.AsyncClient(http2=False) as client:
            response = await client.post(url, json=payload, timeout=30) 
        
        response.raise_for_status()
        
        print("\n--- SUCCESS (Job Started) ---")
        print(f"Status Code: {response.status_code}") # Should be 202
        pprint(response.json())
        print("\nCheck your docker-compose logs to see the job progress.")
        
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
async def test_api_get_top_fens_unscored(limit: int = 20):
    """
    Calls the GET /fens/top_unscored endpoint with a query parameter.
    """
    print(f"\n--- [API TEST] ---")
    print(f"Fetching top {limit} UNSCORED FENs...")
    
    url = f"{API_BASE_URL}/fens/top_unscored?limit={limit}"
    
    try:
        # --- FIX: Disable HTTP/2, force HTTP/1.1 ---
        async with httpx.AsyncClient(http2=False) as client:
            response = await client.get(url, timeout=30) 
        
        response.raise_for_status()
        
        data = response.json()
        
        print("\n--- SUCCESS (Top Unscored FENs) ---")
        if 'results' in data:
            print(data['results'])
        else:
            pprint(data)
        
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
async def test_api_get_fen_score_counts(player_name: str):
    """
    Calls the GET /players/{player_name}/fen_counts endpoint.
    Returns the number of FENs where score is 0 vs not 0.
    """
    print(f"\n--- [API TEST] ---")
    print(f"Fetching FEN score counts for: {player_name}")
    
    url = f"{API_BASE_URL}/players/{player_name}/fen_counts"
    
    try:
        async with httpx.AsyncClient(http2=False) as client:
            response = await client.get(url, timeout=30)
        
        response.raise_for_status()
        data = response.json()
        
        score_zero = data.get('score_zero', 0)
        # "score is not 0" technically includes existing scores != 0. 
        # Depending on your definition, you might want to include or exclude NULLs.
        # The DB query separates NULLs, so 'score_not_zero' implies analyzed positions that aren't draws.
        score_not_zero = data.get('score_not_zero', 0)
        score_null = data.get('score_null', 0)

        print("\n--- SUCCESS (FEN Score Counts) ---")
        print(f"Player: {player_name}")
        print(f"├── Score == 0 (Draw/Equal):  {score_zero:,}")
        print(f"├── Score != 0 (Decisive):    {score_not_zero:,}")
        print(f"└── Score is NULL (Unscored): {score_null:,}")
        print(f"Total FENs associated: {score_zero + score_not_zero + score_null:,}")
        
        return score_zero, score_not_zero

    except httpx.HTTPStatusError as e:
        print(f"\n--- ERROR (HTTP {e.response.status_code}) ---")
        print(e.response.text)
        return 0, 0
    except Exception as e:
        print(f"\n--- UNEXPECTED ERROR ---")
        print(repr(e))
        return 0, 0
async def test_api_get_player_game_count(player_name: str):
    """
    Calls the GET /players/{player_name}/game_count endpoint
    to find the total number of games for a player in the DB.
    """
    print(f"\n--- [API TEST] ---")
    print(f"Fetching game count for: {player_name}")
    
    url = f"{API_BASE_URL}/players/{player_name}/game_count"
    
    try:
        async with httpx.AsyncClient(http2=False) as client:
            response = await client.get(url, timeout=30)
        
        response.raise_for_status()
        data = response.json()
        
        print("\n--- SUCCESS (Game Count) ---")
        pprint(data)
        
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
async def test_api_get_current_player_count():
    """
    Calls the GET /players/current_players endpoint and
    returns the total number of players found.
    """
    print(f"\n--- [API TEST] ---")
    print("Fetching count of 'real' players (joined != 0)...")
    
    url = f"{API_BASE_URL}/players/current_players"
    
    try:
        async with httpx.AsyncClient(http2=False) as client:
            response = await client.get(url)
        
        response.raise_for_status()
        
        player_list = response.json()
        player_count = len(player_list)
        
        print("\n--- SUCCESS (Player Count) ---")
        print(f"Total players with games in DB: {player_count}")
        
    except httpx.HTTPStatusError as e:
        print(f"\n--- ERROR (HTTP {e.response.status_code}) ---")
        try:
            pprint(e.response.json())
        except:
            print(e.response.text)
    except Exception as e:
        print(f"\n--- UNEXPECTED ERROR ---")
        print(repr(e))


async def test_pipeline_create_games(player_name: str):
    """
    End-to-end test: create games for a player.
    """
    return await test_api_create_games(player_name)


async def test_pipeline_update_games(player_name: str):
    """
    End-to-end test: update games for a player.
    """
    return await test_api_update_all_stats()  # Keep stats fresh


async def test_pipeline_generate_fens(total_games: int, batch_size: int = 1000):
    """
    End-to-end test: run FEN generation pipeline.
    """
    return await test_api_generate_fens(total_games, batch_size)


async def test_pipeline_analyze_fens(
    total_fens: int,
    batch_size: int = 1000,
    nodes: int = 1000000
):
    """
    End-to-end test: run analysis pipeline.
    """
    return await test_api_run_analysis_job(total_fens, batch_size, nodes)


async def test_pipeline_analyze_fens_night(
    total_fens: int,
    batch_size: int = 1000,
    nodes: int = 1000000,
    workers_count: int = 6
):
    """
    End-to-end test: run night analysis pipeline.
    """
    return await test_api_run_analysis_job_night(total_fens, batch_size, nodes, workers_count)
