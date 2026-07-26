from typing import Optional
from datetime import datetime

from pydantic import BaseModel

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
    mode: Optional[str] = None
    played_at: Optional[datetime] = None
    avg_elo: Optional[float] = None
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

# This is the Pydantic model for creating/updating the DB
class PlayerStatsCreateData(BaseModel):
    player_name: str
    chess_rapid_last_rating: Optional[int] = None
    chess_rapid_best_rating: Optional[int] = None
    chess_rapid_games: Optional[int] = None
    chess_rapid_wins: Optional[int] = None
    chess_rapid_losses: Optional[int] = None
    chess_rapid_draws: Optional[int] = None
    # --- NEW ---
    chess_rapid_last_percentile: Optional[float] = None

    chess_blitz_last_rating: Optional[int] = None
    chess_blitz_best_rating: Optional[int] = None
    chess_blitz_games: Optional[int] = None
    chess_blitz_wins: Optional[int] = None
    chess_blitz_losses: Optional[int] = None
    chess_blitz_draws: Optional[int] = None
    # --- NEW ---
    chess_blitz_last_percentile: Optional[float] = None

    chess_bullet_last_rating: Optional[int] = None
    chess_bullet_best_rating: Optional[int] = None
    chess_bullet_games: Optional[int] = None
    chess_bullet_wins: Optional[int] = None
    chess_bullet_losses: Optional[int] = None
    chess_bullet_draws: Optional[int] = None
    # --- NEW ---
    chess_bullet_last_percentile: Optional[float] = None

    fide: Optional[int] = None
    puzzle_rush_best_score: Optional[int] = None
    tactics_highest_rating: Optional[int] = None
    tactics_lowest_rating: Optional[int] = None
    
    class Config:
        from_attributes = True # Allow reading from the DB model
