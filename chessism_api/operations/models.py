# chessism_api/operations/models.py

from typing import Optional, List, Any
from pydantic import BaseModel, Field

# --- Pydantic model for creating a Player (used in operations/players.py) ---
class PlayerCreateData(BaseModel):
    player_name: str
    name: Optional[str] = None
    url: Optional[str] = None
    title: Optional[str] = None
    avatar: Optional[str] = None
    followers: Optional[int] = None
    country: Optional[str] = None
    location: Optional[str] = None
    joined: Optional[int] = 0 # Default to 0 for 'shell' players
    status: Optional[str] = None
    is_streamer: Optional[bool] = False
    twitch_url: Optional[str] = None
    verified: Optional[bool] = False
    league: Optional[str] = None
    
    class Config:
        # Allows Pydantic to read from ORM models (e.g., Player)
        from_attributes = True 

# --- Pydantic model for creating a Game (used in operations/format_games.py) ---
class GameCreateData(BaseModel):
    link: int
    white: str
    black: str
    year: int
    month: int
    day: int
    hour: int
    minute: int
    second: int
    white_elo: int
    black_elo: int
    white_result: float
    black_result: float
    white_str_result: str
    black_str_result: str
    time_control: str
    eco: str
    time_elapsed: float # Matches the Float in models.py
    n_moves: int
    fens_done: bool

# --- Pydantic model for creating a Move (used in operations/format_games.py) ---
class MoveCreateData(BaseModel):
    link: int
    n_move: int
    white_move: str
    black_move: str
    white_reaction_time: float
    black_reaction_time: float
    white_time_left: float
    black_time_left: float

# --- Pydantic model for creating a Month (used in operations/format_games.py) ---
class MonthCreateData(BaseModel):
    player_name: str
    year: int
    month: int
    n_games: int

# --- Pydantic model for returning a Month (used in operations/months.py) ---
class MonthResult(MonthCreateData):
    id: int # Include the 'id' from the database
    
    class Config:
        from_attributes = True # Allow Pydantic to read from ORM models

# --- Pydantic models for FEN operations (used in db_interface.py) ---
class FenCreateData(BaseModel):
    fen: str
    n_games: int
    moves_counter: str
    next_moves: Optional[str] = None
    score: Optional[float] = None

class FenGameAssociateData(BaseModel):
    fen_fen: str  # The FEN string to associate
    game_link: int

# --- Pydantic model for creating AnalysisTimes (used in ask_db.py) ---
class AnalysisTimesCreateData(BaseModel):
    batch_index: int
    n_batches:int
    card: int
    model: str
    n_fens: int
    time_elapsed: float
    fens_per_second:float
    analyse_time_limit:float
    nodes_limit:int

# --- NEW: Pydantic model for creating PlayerStats ---
# This defines the data we expect to parse from the API
# and save to the database.

class StatsRecord(BaseModel):
    last: Optional[Dict[str, Any]] = None
    best: Optional[Dict[str, Any]] = None
    record: Optional[Dict[str, Any]] = None

class PuzzleRushStats(BaseModel):
    best: Optional[Dict[str, int]] = None

class TacticsStats(BaseModel):
    highest: Optional[Dict[str, int]] = None
    lowest: Optional[Dict[str, int]] = None

# This is the Pydantic model for creating/updating the DB
class PlayerStatsCreateData(BaseModel):
    player_name: str
    chess_rapid_last_rating: Optional[int] = None
    chess_rapid_best_rating: Optional[int] = None
    chess_rapid_games: Optional[int] = None
    chess_rapid_wins: Optional[int] = None
    chess_rapid_losses: Optional[int] = None
    chess_rapid_draws: Optional[int] = None

    chess_blitz_last_rating: Optional[int] = None
    chess_blitz_best_rating: Optional[int] = None
    chess_blitz_games: Optional[int] = None
    chess_blitz_wins: Optional[int] = None
    chess_blitz_losses: Optional[int] = None
    chess_blitz_draws: Optional[int] = None

    chess_bullet_last_rating: Optional[int] = None
    chess_bullet_best_rating: Optional[int] = None
    chess_bullet_games: Optional[int] = None
    chess_bullet_wins: Optional[int] = None
    chess_bullet_losses: Optional[int] = None
    chess_bullet_draws: Optional[int] = None

    fide: Optional[int] = None
    puzzle_rush_best_score: Optional[int] = None
    tactics_highest_rating: Optional[int] = None
    tactics_lowest_rating: Optional[int] = None
    
    class Config:
        from_attributes = True