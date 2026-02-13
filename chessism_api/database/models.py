# chessism_api/database/models.py
from typing import Any, Dict
from sqlalchemy import (
    Column, ForeignKey, Integer, String, Float, BigInteger, Table,
    DateTime, func, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.types import Boolean

Base = declarative_base()


def to_dict(obj: Base) -> Dict[str, Any]:
    """
    Serializes a SQLAlchemy ORM object into a dictionary.
    """
    return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}


class Player(Base):
    __tablename__ = "player"
    player_name = Column("player_name", String, primary_key=True, nullable=False, unique=True)
    name = Column('name', String, nullable=True)
    url = Column('url', String, nullable=True)
    title = Column('title', String, nullable=True)
    avatar = Column('avatar', String, nullable=True)
    followers = Column('followers', Integer,nullable=True)
    country = Column('country', String, nullable=True)
    location = Column('location', String, nullable=True)
    joined = Column('joined', BigInteger, nullable=True) # Use BigInteger for Unix timestamps
    status = Column('status', String, nullable=True)
    is_streamer = Column('is_streamer', Boolean, nullable=True)
    twitch_url = Column('twitch_url', String, nullable=True)
    verified = Column('verified', Boolean, nullable=True)
    league = Column('league', String, nullable=True)
    
    stats = relationship(
        "PlayerStats", 
        back_populates="player", 
        uselist=False, # Signifies a one-to-one relationship
        cascade="all, delete-orphan"
    )

class Game(Base):
    __tablename__ = 'game'
    link = Column('link',BigInteger, primary_key = True, unique = True)
    white = Column("white", String, ForeignKey("player.player_name"), nullable=False)
    black = Column("black", String, ForeignKey("player.player_name"), nullable=False)

    year = Column("year", Integer, nullable=False)
    month = Column("month", Integer, nullable=False)
    day = Column("day", Integer, nullable=False)
    hour = Column("hour", Integer, nullable=False)
    minute = Column("minute", Integer, nullable=False)
    second = Column("second", Integer, nullable=False)
        
    white_elo = Column("white_elo", Integer, nullable=False)
    black_elo = Column("black_elo", Integer, nullable=False)
    white_result = Column("white_result", Float, nullable=False)
    black_result = Column("black_result", Float, nullable=False)
    white_str_result = Column("white_str_result", String, nullable=False)
    black_str_result = Column("black_str_result", String, nullable=False)
    time_control = Column("time_control", String, nullable=False)
    eco = Column("eco", String, nullable=False)
    
    time_elapsed = Column("time_elapsed", Float, nullable=False)
    
    n_moves = Column("n_moves", Integer, nullable=False)
    fens_done = Column('fens_done', Boolean, nullable = False)
    
    white_player = relationship(Player, foreign_keys=[white])
    black_player = relationship(Player, foreign_keys=[black])
    
    # --- MODIFIED: Point to the new association class ---
    fens = relationship(
        'Fen',
        secondary='game_fen_association',
        back_populates='games',
        # --- FIX: Tell SQLAlchemy this overlaps with the 'fen_associations' relationship ---
        overlaps="fen_associations" 
    )
    
    # --- NEW: Add direct relationship to the association object ---
    fen_associations = relationship("GameFenAssociation", back_populates="game", overlaps="games,fens")


class Month(Base):
    __tablename__ = "months"
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    player_name = Column("player_name",
                         String,
                         ForeignKey("player.player_name"),
                         primary_key = False,
                         unique=False,
                         nullable=False)
    year = Column("year", Integer, nullable=False, unique=False)
    month = Column("month", Integer, nullable=False, unique=False)
    n_games = Column("n_games",Integer, nullable=False, unique=False)
    
    player = relationship(Player, foreign_keys=[player_name])
    
    __table_args__ = (
        UniqueConstraint('player_name', 'year', 'month', name='_player_year_month_uc'),
    )


class Move(Base):
    __tablename__ = "moves"
    id = Column(Integer, primary_key=True, autoincrement=True)
    link = Column("link",BigInteger,ForeignKey("game.link"),
                 nullable=False,unique=False)
    n_move = Column("n_move", Integer, nullable=False)
    white_move = Column("white_move", String, nullable=False)
    black_move = Column("black_move", String, nullable=False)
    white_reaction_time = Column("white_reaction_time", Float, nullable=False)
    black_reaction_time = Column("black_reaction_time", Float, nullable=False)
    white_time_left = Column("white_time_left", Float, nullable=False)
    black_time_left = Column("black_time_left", Float, nullable=False)
    
    game = relationship(Game, foreign_keys=[link])
    
    __table_args__ = (
        UniqueConstraint('link', 'n_move', name='_game_link_move_num_uc'),
    )


class Fen(Base):
    __tablename__ = "fen"
    fen = Column('fen',String, primary_key = True, index = True, unique = True)
    n_games = Column('n_games',BigInteger, nullable = False)
    moves_counter = Column('moves_counter',String, nullable = False)
    next_moves = Column('next_moves',String, nullable = True)
    score = Column('score', Float, nullable = True)
    wdl_win = Column('wdl_win', Float, nullable=True)
    wdl_draw = Column('wdl_draw', Float, nullable=True)
    wdl_loss = Column('wdl_loss', Float, nullable=True)
    
    # --- MODIFIED: Point to the new association class ---
    games = relationship(
        'Game',
        secondary='game_fen_association',
        back_populates='fens',
        # --- FIX: Tell SQLAlchemy this overlaps with the 'game_associations' relationship ---
        overlaps="game_associations"
    )
    
    # --- NEW: Add direct relationship to the association object ---
    game_associations = relationship("GameFenAssociation", back_populates="fen", overlaps="games,fens")

    continuations = relationship(
        "FenContinuation",
        back_populates="fen",
        cascade="all, delete-orphan"
    )


# --- NEW: The GameFenAssociation class ---
class GameFenAssociation(Base):
    __tablename__ = 'game_fen_association'
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    game_link = Column(BigInteger, ForeignKey('game.link'), nullable=False, index=True)
    fen_fen = Column(String, ForeignKey('fen.fen'), nullable=False, index=True)
    n_move = Column(Integer, nullable=False)
    move_color = Column(String(5), nullable=False) # 'white' or 'black'

    # --- FIX: Add overlaps parameter to both relationships ---
    game = relationship("Game", back_populates="fen_associations", overlaps="games,fens")
    fen = relationship("Fen", back_populates="game_associations", overlaps="games,fens")

    __table_args__ = (
        UniqueConstraint('game_link', 'fen_fen', 'n_move', 'move_color', name='_game_fen_move_color_uc'),
    )


class FenContinuation(Base):
    __tablename__ = "fen_continuation"
    id = Column(Integer, primary_key=True, autoincrement=True)
    fen_fen = Column(String, ForeignKey('fen.fen'), nullable=False, index=True)
    rank = Column(Integer, nullable=False) # PV rank: 2..4
    move = Column(String, nullable=False) # First move of the continuation
    score = Column(Float, nullable=False)

    fen = relationship("Fen", back_populates="continuations")

    __table_args__ = (
        UniqueConstraint('fen_fen', 'rank', name='_fen_rank_uc'),
    )


class PlayerStats(Base):
    __tablename__ = "player_stats"
    
    player_name = Column(
        String, 
        ForeignKey("player.player_name", ondelete="CASCADE"), 
        primary_key=True
    )
    
    last_updated = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    player = relationship("Player", back_populates="stats")

    # --- Rapid ---
    chess_rapid_last_rating = Column(Integer, nullable=True)
    chess_rapid_best_rating = Column(Integer, nullable=True)
    chess_rapid_games = Column(Integer, nullable=True)
    chess_rapid_wins = Column(Integer, nullable=True)
    chess_rapid_losses = Column(Integer, nullable=True)
    chess_rapid_draws = Column(Integer, nullable=True)
    # --- NEW: Percentile ---
    chess_rapid_last_percentile = Column(Float, nullable=True)

    # --- Blitz ---
    chess_blitz_last_rating = Column(Integer, nullable=True)
    chess_blitz_best_rating = Column(Integer, nullable=True)
    chess_blitz_games = Column(Integer, nullable=True)
    chess_blitz_wins = Column(Integer, nullable=True)
    chess_blitz_losses = Column(Integer, nullable=True)
    chess_blitz_draws = Column(Integer, nullable=True)
    # --- NEW: Percentile ---
    chess_blitz_last_percentile = Column(Float, nullable=True)

    # --- Bullet ---
    chess_bullet_last_rating = Column(Integer, nullable=True)
    chess_bullet_best_rating = Column(Integer, nullable=True)
    chess_bullet_games = Column(Integer, nullable=True)
    chess_bullet_wins = Column(Integer, nullable=True)
    chess_bullet_losses = Column(Integer, nullable=True)
    chess_bullet_draws = Column(Integer, nullable=True)
    # --- NEW: Percentile ---
    chess_bullet_last_percentile = Column(Float, nullable=True)

    # --- Other ---
    fide = Column(Integer, nullable=True)
    puzzle_rush_best_score = Column(Integer, nullable=True)
    tactics_highest_rating = Column(Integer, nullable=True)
    tactics_lowest_rating = Column(Integer, nullable=True)
