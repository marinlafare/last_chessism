# chessism_api/database/models.py
import enum
from typing import Any, Dict
from sqlalchemy import (
    Column, ForeignKey, Integer, String, Float, BigInteger, Table,
    DateTime, Enum, func, UniqueConstraint, Index, CheckConstraint, JSON
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.types import Boolean

Base = declarative_base()


class AccountRole(str, enum.Enum):
    admin = "admin"
    user = "user"


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
        uselist=False, # one-to-one relationship
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
    mode = Column("mode", String(16), nullable=True)
    played_at = Column("played_at", DateTime(timezone=True), nullable=True)
    avg_elo = Column("avg_elo", Float, nullable=True)
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

    __table_args__ = (
        Index("ix_game_mode", "mode"),
        Index("ix_game_played_at", "played_at"),
        Index("ix_game_mode_avg_elo", "mode", "avg_elo"),
        Index("ix_game_white_played_at", "white", "played_at"),
        Index("ix_game_black_played_at", "black", "played_at"),
    )


class GamePlayer(Base):
    __tablename__ = "game_player"

    link = Column(BigInteger, ForeignKey("game.link", ondelete="CASCADE"), primary_key=True)
    color = Column(String(5), primary_key=True, nullable=False)
    player_name = Column(String, ForeignKey("player.player_name"), nullable=False)
    opponent_name = Column(String, ForeignKey("player.player_name"), nullable=False)
    result = Column(Float, nullable=False)
    rating = Column(Integer, nullable=False)
    opponent_rating = Column(Integer, nullable=False)
    mode = Column(String(16), nullable=True)
    played_at = Column(DateTime(timezone=True), nullable=True)
    eco = Column(String, nullable=False)
    n_moves = Column(Integer, nullable=False)
    time_elapsed = Column(Float, nullable=False)
    avg_elo = Column(Float, nullable=True)

    game = relationship(Game, foreign_keys=[link])
    player = relationship(Player, foreign_keys=[player_name])
    opponent = relationship(Player, foreign_keys=[opponent_name])

    __table_args__ = (
        Index("ix_game_player_player_played_at", "player_name", "played_at"),
        Index("ix_game_player_player_mode_played_at", "player_name", "mode", "played_at"),
        Index("ix_game_player_mode_rating", "mode", "rating"),
        Index("ix_game_player_link", "link"),
    )


class GameOpening(Base):
    __tablename__ = "game_opening"

    link = Column(BigInteger, ForeignKey("game.link", ondelete="CASCADE"), primary_key=True)
    n_moves = Column(Integer, primary_key=True, nullable=False)
    opening = Column(String, nullable=False)
    mode = Column(String(16), nullable=True)
    avg_elo = Column(Float, nullable=True)
    played_at = Column(DateTime(timezone=True), nullable=True)

    game = relationship(Game, foreign_keys=[link])

    __table_args__ = (
        Index("ix_game_opening_mode_n_moves_opening", "mode", "n_moves", "opening"),
        Index("ix_game_opening_n_moves_mode_avg_elo", "n_moves", "mode", "avg_elo"),
    )


class GameAnalysisSummary(Base):
    __tablename__ = "game_analysis_summary"

    link = Column(BigInteger, ForeignKey("game.link", ondelete="CASCADE"), primary_key=True)
    total_positions = Column(Integer, nullable=False, default=0, server_default="0")
    analyzed_positions = Column(Integer, nullable=False, default=0, server_default="0")
    unscored_positions = Column(Integer, nullable=False, default=0, server_default="0")
    is_fully_analyzed = Column(Boolean, nullable=False, default=False, server_default="false")
    score_sum = Column(Float, nullable=False, default=0, server_default="0")
    abs_score_sum = Column(Float, nullable=False, default=0, server_default="0")
    avg_score = Column(Float, nullable=True)
    avg_abs_score = Column(Float, nullable=True)
    max_abs_score = Column(Float, nullable=True)
    equal_positions = Column(Integer, nullable=False, default=0, server_default="0")
    small_positions = Column(Integer, nullable=False, default=0, server_default="0")
    clear_positions = Column(Integer, nullable=False, default=0, server_default="0")
    decisive_positions = Column(Integer, nullable=False, default=0, server_default="0")
    mate_positions = Column(Integer, nullable=False, default=0, server_default="0")
    refreshed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    game = relationship(Game, foreign_keys=[link])

    __table_args__ = (
        Index(
            "ix_game_analysis_summary_fully_positions",
            total_positions.desc(),
            max_abs_score.desc().nulls_last(),
            link.desc(),
            postgresql_where=is_fully_analyzed.is_(True) & (total_positions > 0),
        ),
        Index(
            "ix_game_analysis_summary_incomplete",
            unscored_positions.desc(),
            total_positions.desc(),
            link.desc(),
            postgresql_where=is_fully_analyzed.is_(False) & (total_positions > 0),
        ),
        Index(
            "ix_game_analysis_summary_all_positions",
            is_fully_analyzed.desc(),
            total_positions.desc(),
            link.desc(),
            postgresql_where=total_positions > 0,
        ),
    )


class ScoredPositionSummary(Base):
    __tablename__ = "scored_position_summary"

    id = Column(Integer, primary_key=True, default=1, server_default="1")
    total_positions = Column(BigInteger, nullable=False, default=0, server_default="0")
    analyzed_fens = Column(BigInteger, nullable=False, default=0, server_default="0")
    scored_positions = Column(BigInteger, nullable=False, default=0, server_default="0")
    nonzero_scored_fens = Column(BigInteger, nullable=False, default=0, server_default="0")
    unscored_fens = Column(BigInteger, nullable=False, default=0, server_default="0")
    equal_positions = Column(BigInteger, nullable=False, default=0, server_default="0")
    small_positions = Column(BigInteger, nullable=False, default=0, server_default="0")
    clear_positions = Column(BigInteger, nullable=False, default=0, server_default="0")
    decisive_positions = Column(BigInteger, nullable=False, default=0, server_default="0")
    mate_positions = Column(BigInteger, nullable=False, default=0, server_default="0")
    equal_appearances = Column(BigInteger, nullable=False, default=0, server_default="0")
    small_appearances = Column(BigInteger, nullable=False, default=0, server_default="0")
    clear_appearances = Column(BigInteger, nullable=False, default=0, server_default="0")
    decisive_appearances = Column(BigInteger, nullable=False, default=0, server_default="0")
    mate_appearances = Column(BigInteger, nullable=False, default=0, server_default="0")
    equal_abs_score_sum = Column(Float, nullable=False, default=0, server_default="0")
    small_abs_score_sum = Column(Float, nullable=False, default=0, server_default="0")
    clear_abs_score_sum = Column(Float, nullable=False, default=0, server_default="0")
    decisive_abs_score_sum = Column(Float, nullable=False, default=0, server_default="0")
    mate_abs_score_sum = Column(Float, nullable=False, default=0, server_default="0")
    white_better = Column(BigInteger, nullable=False, default=0, server_default="0")
    black_better = Column(BigInteger, nullable=False, default=0, server_default="0")
    balanced = Column(BigInteger, nullable=False, default=0, server_default="0")
    score_sum = Column(Float, nullable=False, default=0, server_default="0")
    abs_score_sum = Column(Float, nullable=False, default=0, server_default="0")
    wdl_win_sum = Column(Float, nullable=False, default=0, server_default="0")
    wdl_draw_sum = Column(Float, nullable=False, default=0, server_default="0")
    wdl_loss_sum = Column(Float, nullable=False, default=0, server_default="0")
    wdl_positions = Column(BigInteger, nullable=False, default=0, server_default="0")
    refreshed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        CheckConstraint("id = 1", name="scored_position_summary_singleton"),
    )


class ScoredRatingSummary(Base):
    __tablename__ = "scored_rating_summary"

    id = Column(Integer, primary_key=True, default=1, server_default="1")
    rating_basis = Column(String(32), nullable=False, default="avg_elo")
    source_full_games = Column(BigInteger, nullable=False, default=0, server_default="0")
    source_distinct_ratings = Column(BigInteger, nullable=False, default=0, server_default="0")
    groups_payload = Column(JSON, nullable=False)
    ratings_payload = Column(JSON, nullable=False)
    refreshed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        CheckConstraint("id = 1", name="scored_rating_summary_singleton"),
    )


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
        overlaps="game_associations"
    )
    
    # --- NEW: Add direct relationship to the association object ---
    game_associations = relationship("GameFenAssociation", back_populates="fen", overlaps="games,fens")

    continuations = relationship(
        "FenContinuation",
        back_populates="fen",
        cascade="all, delete-orphan"
    )



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


class AnalysisTime(Base):
    __tablename__ = "analysis_time"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    source = Column(String(32), nullable=False, index=True)
    fen_hash = Column(String(64), nullable=False, index=True)
    n_pieces = Column(Integer, nullable=False, index=True)
    nodes_limit = Column(Integer, nullable=False)
    multipv = Column(Integer, nullable=False)
    elapsed_ms = Column(Float, nullable=False)


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


class MainCharacterModeSummary(Base):
    __tablename__ = "main_character_mode_summary"

    mode = Column(String(16), primary_key=True, nullable=False)
    player_name = Column(
        String,
        ForeignKey("player.player_name", ondelete="CASCADE"),
        primary_key=True,
        nullable=False
    )
    n_games = Column(Integer, nullable=False, default=0)
    rating = Column(Integer, nullable=True)
    avg_game_rating = Column(Integer, nullable=True)
    last_rating = Column(Integer, nullable=True)
    wins = Column(Integer, nullable=False, default=0)
    draws = Column(Integer, nullable=False, default=0)
    losses = Column(Integer, nullable=False, default=0)
    as_white = Column(Integer, nullable=False, default=0)
    as_black = Column(Integer, nullable=False, default=0)
    last_game_at = Column(DateTime(timezone=True), nullable=True)
    refreshed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    player = relationship(Player, foreign_keys=[player_name])

    __table_args__ = (
        Index("ix_main_character_mode_summary_mode_rating", "mode", "rating"),
        Index("ix_main_character_mode_summary_mode_games", "mode", "n_games"),
    )


class DatabaseSummary(Base):
    __tablename__ = "database_summary"

    id = Column(Integer, primary_key=True, default=1, server_default="1")
    n_games_in_db = Column(BigInteger, nullable=False, default=0, server_default="0")
    main_characters = Column(BigInteger, nullable=False, default=0, server_default="0")
    secondary_characters = Column(BigInteger, nullable=False, default=0, server_default="0")
    n_positions = Column(BigInteger, nullable=False, default=0, server_default="0")
    analyzed_fens = Column(BigInteger, nullable=False, default=0, server_default="0")
    unscored_fens = Column(BigInteger, nullable=False, default=0, server_default="0")
    scored_fens = Column(BigInteger, nullable=False, default=0, server_default="0")
    nonzero_scored_fens = Column(BigInteger, nullable=False, default=0, server_default="0")
    bullet_games = Column(BigInteger, nullable=False, default=0, server_default="0")
    blitz_games = Column(BigInteger, nullable=False, default=0, server_default="0")
    rapid_games = Column(BigInteger, nullable=False, default=0, server_default="0")
    refreshed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        CheckConstraint("id = 1", name="database_summary_singleton"),
    )


class Account(Base):
    __tablename__ = "account"

    id = Column(String(36), primary_key=True)
    name = Column(String, nullable=False)
    email = Column(String, nullable=False, unique=True, index=True)
    password_hash = Column(String, nullable=False)
    chess_com_nickname = Column(String, nullable=True, index=True)
    role = Column(Enum(AccountRole, name="account_role"), nullable=False, default=AccountRole.user)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    last_login_at = Column(DateTime(timezone=True), nullable=True)

    sessions = relationship("AuthSession", back_populates="account", cascade="all, delete-orphan")


class AuthSession(Base):
    __tablename__ = "auth_session"

    id = Column(String(36), primary_key=True)
    account_id = Column(String(36), ForeignKey("account.id", ondelete="CASCADE"), nullable=False, index=True)
    token_hash = Column(String(64), nullable=False, unique=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)
    revoked_at = Column(DateTime(timezone=True), nullable=True)
    ip_address = Column(String, nullable=True)
    user_agent = Column(String, nullable=True)

    account = relationship("Account", back_populates="sessions")
