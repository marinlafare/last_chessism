# chessism_api/operations/models.py

from typing import Optional, List
from pydantic import BaseModel

class PlayerCreateData(BaseModel):
    player_name: str
    name: Optional[str] = None
    url: Optional[str] = None
    title: Optional[str] = None
    avatar: Optional[str] = None
    followers: Optional[int] = None
    country: Optional[str] = None
    location: Optional[str] = None
    joined: int
    status: Optional[str] = None
    is_streamer: Optional[bool] = None
    twitch_url: Optional[str] = None
    verified: Optional[bool] = None
    league: Optional[str] = None

class FenCreateData(BaseModel):
    fen: str
    n_games: int
    moves_counter: str
    next_moves: Optional[str] = None
    score: Optional[float] = None

class FenGameAssociateData(BaseModel):
    fen_fen: str  # The FEN string to associate
    game_link: int
    
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
    time_elapsed: float 
    n_moves: int
    fens_done: bool

class MoveCreateData(BaseModel):
    link: int
    n_move: int
    white_move: str
    black_move: str
    white_reaction_time: float
    black_reaction_time: float
    white_time_left: float
    black_time_left: float

class MonthCreateData(BaseModel):
    player_name: str
    year: int
    month: int
    n_games: int

# --- NEW: Added MonthResult model ---
class MonthResult(MonthCreateData):
    id: int