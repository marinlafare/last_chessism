import httpx
import asyncio
import os
import time
from pprint import pprint
from arq import create_pool
from chessism_api.redis_client import redis_settings

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
    batch_size: int = 500,
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
        result = response.json()
        pprint(result)
        print("\nCheck your docker-compose logs to see the job progress.")
        
        return result
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
    batch_size: int = 500,
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
        result = response.json()
        pprint(result)
        print("\nCheck your docker-compose logs to see the job progress.")
        
        return result
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
    batch_size: int = 500,
    nodes: int = 1000000
):
    """
    End-to-end test: run analysis pipeline.
    """
    return await test_api_run_analysis_job(total_fens, batch_size, nodes)



async def _count_keys(redis, pattern: str) -> int:
    count = 0
    cursor = b"0"
    while cursor:
        cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=1000)
        count += len(keys)
        if cursor == b"0":
            break
    return count


async def _get_queue_metrics(redis, queue_name: str) -> tuple[int, int]:
    queue_key = f"arq:queue:{queue_name}"
    queued = await redis.llen(queue_key)
    # ARQ stores in-progress jobs as per-job keys (arq:in-progress:<job_id>).
    in_progress = await _count_keys(redis, "arq:in-progress:*")
    return int(queued or 0), int(in_progress or 0)


async def _job_done(redis, job_id: str) -> bool:
    job_key = f"arq:job:{job_id}"
    in_progress_key = f"arq:in-progress:{job_id}"
    retry_key = f"arq:retry:{job_id}"
    if await redis.exists(job_key) or await redis.exists(in_progress_key) or await redis.exists(retry_key):
        return False
    return True


async def wait_for_jobs_done(
    job_ids: list[str],
    poll_seconds: int = 5,
    timeout_seconds: int | None = None
) -> float:
    """
    Waits until all job_ids have result keys.
    Returns elapsed seconds.
    """
    start = time.perf_counter()
    redis = await create_pool(redis_settings)
    try:
        while True:
            done = 0
            for job_id in job_ids:
                if await _job_done(redis, job_id):
                    done += 1
            if done == len(job_ids):
                return time.perf_counter() - start
            if timeout_seconds is not None and (time.perf_counter() - start) > timeout_seconds:
                return time.perf_counter() - start
            await asyncio.sleep(poll_seconds)
    finally:
        await redis.close()


async def test_pipeline_analyze_fens_timed(
    total_fens: int,
    batch_size: int = 500,
    nodes: int = 1000000,
    poll_seconds: int = 5
) -> float:
    """
    Enqueues analysis and waits for the normal queue to drain.
    Returns elapsed seconds.
    """
    result = await test_api_run_analysis_job(total_fens, batch_size, nodes)
    job_id = result.get("job_id") if isinstance(result, dict) else None
    if not job_id:
        return 0.0
    return await wait_for_jobs_done([job_id], poll_seconds=poll_seconds)

