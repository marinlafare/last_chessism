# Chessism: Advanced Chess Analysis Suite

**Chessism** is a high-performance, dual-GPU capable chess analysis suite. It automates the extraction of player data from Chess.com, transforms raw PGNs into structural data (Moves, Reaction Times, FENs), and performs large-scale chess engine analysis using Leela Chess Zero (Lc0).

## 🏗 Architecture

The system is containerized using Docker and composed of three primary service types:

1.  **`chessism-api` (FastAPI):** The central orchestrator. It handles:
    * ETL from Chess.com (respecting rate limits).
    * PGN parsing and "Reaction Time" calculation.
    * Database management (PostgreSQL).
    * Job dispatching for FEN generation and Analysis.
2.  **`leela-service` (GPU Workers):** Two distinct services (`gpu0` and `gpu1`) running a custom-compiled Lc0 engine with the **T1-256x10-distilled** network.
3.  **`db` (PostgreSQL):** Stores relational data (Players, Games, Moves, FENs) and analysis results.

## 🚀 Prerequisites

* **Docker** & **Docker Compose**
* **NVIDIA GPU(s)** (The system is configured for a dual-GPU setup, but can run on one with modification).
* **NVIDIA Container Toolkit** (Required for Docker to access the GPUs).

## 🛠 Installation & Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd chessism
    ```

2.  **Configuration:**
    The `docker-compose.yml` comes pre-configured with internal networking and environment variables.
    * **Database:** `chessism_db` (User: `chessism_user`)
    * **Ports:**
        * API: `8000`
        * DB: `5433` (Host) -> `5432` (Container)
        * Leela GPU0: `9999`
        * Leela GPU1: `9998`

3.  **Build and Run:**
    ```bash
    docker-compose up --build -d
    ```
    *Note: The initial build of `leela-service` compiles Lc0 from source and may take several minutes.*

## 🔌 API Endpoints

The API is accessible at `http://localhost:8000`. Interactive documentation is available at `/docs`.

### 1. Player & Game Ingestion (ETL)
* **`POST /games`**: Triggers a full download history for a specific player.
    * *Logic:* Checks joined date -> Generates months range -> Downloads archives -> Parses PGNs -> Inserts Games & Moves.
    * *Optimization:* Uses temporary tables to filter duplicates efficiently.
* **`POST /games/update`**: Updates a player's history from the last recorded month in the DB to the present.
* **`GET /players/{player_name}/stats`**: Fetches and upserts fresh stats (Rapid, Blitz, Bullet, Puzzle Rush) from Chess.com.

### 2. FEN Generation
* **`POST /fens/generate`**: Background job to deconstruct games into unique FEN strings.
    * *Logic:* Deconstructs moves into board states -> Canonicalizes FENs -> Bulk upserts to DB.
    * *Concurrency:* Uses atomic batch transactions to manage huge datasets without locking the DB.

### 3. Analysis (Leela)
* **`POST /analysis/run_job`**: Dispatches a general analysis job to a specific GPU.
    * *Payload:* `{"gpu_index": 0, "total_fens_to_process": 1000, "nodes_limit": 50000}`.
    * *Concurrency:* Uses `FOR UPDATE SKIP LOCKED` to allow `gpu0` and `gpu1` to fetch unique work batches simultaneously.
* **`POST /analysis/run_player_job`**: Same as above, but prioritizes FENs belonging to a specific player's games.

### 4. Data Retrieval
* **`GET /fens/top`**: Returns the most frequent positions (FENs) stored in the database.
* **`GET /fens/top_unscored`**: Returns the most frequent positions that have not yet been analyzed by Leela.

## 🔧 Utilities

Located in the root directory, these scripts assist with maintenance and monitoring.

* **`backup.sh`**
    * Creates a compressed (`.dump`) backup of the PostgreSQL database.
    * *Behavior:* Deletes the previous backup in `./backups` before creating a new one to save space.
    * *Usage:* `./backup.sh`

* **`restore_db_from_backup.sh`**
    * Restores the database from the most recent file in `./backups`.
    * *Warning:* This runs with `--clean`, meaning it **drops** existing tables before restoring.
    * *Usage:* `./restore_db_from_backup.sh`

* **`start_temp_monitor.sh`**
    * Starts a background process logging GPU temperatures, utilization, and clock speeds to `gpu_temp_log.csv`.
    * *Usage:* `./start_temp_monitor.sh`

## 📂 Project Structure

```text
.
├── chessism_api/           # Core API Application
│   ├── database/           # DB Models and Interface (SQLAlchemy/AsyncPG)
│   ├── operations/         # ETL Logic, PGN Parsing, API Clients
│   └── routers/            # FastAPI Route Definitions
├── leela-service/          # GPU Worker Service
│   ├── Dockerfile          # Multi-stage build for Lc0
│   └── main.py             # API Wrapper for Lc0 Engine
├── main.py                 # API Entry Point
├── docker-compose.yml      # Container Orchestration
├── backup.sh               # DB Backup Utility
├── restore_db_from_backup.sh # DB Restore Utility
└── start_temp_monitor.sh   # GPU Monitoring Utility