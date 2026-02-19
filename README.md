# Chessism API

**Chessism API** is a high-performance, asynchronous Python suite for downloading, processing, and analyzing chess data from Chess.com.

It is built on a scalable, containerized architecture that leverages a FastAPI frontend, an ARQ task queue, and a dedicated analysis microservice for chess engine evaluation.

## Core Architecture

This project is not a single application, but a collection of services managed by `docker-compose.yml`:

* **`chessism-api`**: The main FastAPI service that provides all REST API endpoints. It handles user requests and enqueues background jobs.
* **`db`**: A PostgreSQL database that stores all data (players, games, FENs, etc.).
* **`redis`**: A Redis instance that acts as the message broker for the task queue.
* **`pipeline-worker`**: The "boss" worker. It listens for high-level API calls (like "generate FENs") and orchestrates the multi-stage pipeline.
* **`fen-worker-1, 2, 3`**: "Child" workers that perform the heavy CPU (generation) and I/O (insertion) tasks in parallel.
* **`worker-analysis`**: Dedicated analysis worker that listens on the analysis queue and processes FENs.
* **`stockfish-service`**: Dedicated FastAPI service that wraps Stockfish, accepting FENs and returning analysis.

## Key Features

* **Data Ingestion**: Download player profiles, stats, and complete game histories from Chess.com.
* **Incremental Updates**: The API can update a player's games, only downloading archives from the last recorded month.
* **Parallel FEN Pipeline**: A robust, multi-stage MapReduce pipeline for FEN extraction:
  1. **(Map)**: 3 `fen-worker`s run in parallel to parse PGNs and generate raw FEN data (CPU-bound).
  2. **(Reduce)**: The `pipeline-worker` (boss) collects and aggregates millions of data points into a unique set of FENs and associations (Memory-bound).
  3. **(Write)**: The boss splits the aggregated data and enqueues 3 parallel insertion jobs, which the `fen-worker`s execute to write the data to the database (I/O-bound).
* **CPU Analysis**: Enqueue analysis jobs to the analysis queue. The analysis worker handles the database logic and calls Stockfish for engine evaluation.
* **Database Utilities**: Includes shell scripts for easy database backup (`backup.sh`) and restore (`restore_db_from_backup.sh`).

## API Endpoints (Core)

* `POST /games`: Downloads all game archives for a player.
* `POST /games/update`: Downloads only the newest game archives for a player.
* `GET /players/{player_name}`: Gets a player's profile (from DB or Chess.com).
* `GET /players/{player_name}/stats`: Gets fresh player stats from Chess.com.
* `GET /players/{player_name}/game_count`: Returns the total number of games for a player in the DB.
* `POST /fens/generate`: Triggers the high-speed FEN extraction pipeline.
* `POST /analysis/run_job`: Enqueues a general analysis job.
* `POST /analysis/run_player_job`: Enqueues an analysis job for a specific player's FENs.

## Project Analysis

### Strong Parts

1. **Scalable, Asynchronous Architecture**: The use of FastAPI, ARQ, and Redis is a modern, high-performance design. It allows the API to be non-blocking, instantly accepting jobs and offloading all heavy work to background workers.
2. **Robust FEN Pipeline (MapReduce)**: The final pipeline architecture (Map -> Reduce -> Parallel FEN Write -> Parallel Assoc Write) is excellent. It correctly parallelizes both CPU-bound and I/O-bound tasks and, by waiting for FEN insertion to finish, guarantees data integrity by preventing `ForeignKeyViolationError`.
3. **Decoupled Analysis Service**: Isolating the engine in its own `stockfish-service` microservice is a very strong design choice. It keeps the main API and workers lightweight and allows the analysis service to be managed and scaled independently.
4. **Analysis Worker Separation**: The `docker-compose.yml` and ARQ configuration isolate analysis work in a dedicated worker and queue, keeping the API responsive.
5. **Database Resiliency**: The database startup logic in `chessism_api/database/engine.py` includes a retry-loop, which makes the worker services resilient to database startup delays, preventing "connection refused" race conditions.

### Possible Areas of Upgrade

1. **Endgame Tablebase Integration**: This is the single most valuable performance upgrade. Integrating Syzygy tablebases (work in progress) will make endgame analysis instantaneous, dramatically reducing engine load and job time.
2. **Statistical Analysis**: Statistical endpoints are still minimal. Building out SQL/report payloads that consume `PlayerStats` and `Fen` data to generate a `PlayerStatsReport` is the main "next feature" to implement.
3. **API Security**: The API is currently open. Adding a simple API key check (e.g., as a FastAPI dependency checking `x-api-key` in the request header) would prevent unauthorized access.
4. **Configuration Management**: Centralize all configuration (like `CONN_STRING`, `REDIS_HOST`, etc.) into environment variables, perhaps using Pydantic's `BaseSettings` for validation and loading.
5. **Failed Job Management**: The FEN generation job writes failures to `logs/illegall_fen.txt`. This is good, but a more robust solution would be to write these failures to a `failed_games` table in the database. This would make it possible to build an API endpoint to retry failed jobs.
