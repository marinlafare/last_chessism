"""Canonical ORM mappings. Only the canonical initializer creates database objects."""
import enum
from typing import Any, Dict
from sqlalchemy import (
    Column, ForeignKey, Integer, String, Float, BigInteger,
    DateTime, Enum, func, UniqueConstraint, Index, CheckConstraint, JSON,
    SmallInteger, text, ForeignKeyConstraint, Text, Numeric, Date, Identity
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.types import Boolean
from sqlalchemy.dialects.postgresql import ARRAY

Base = declarative_base()
from .appearance_models import register as register_appearance
AppearancePreset, AppearanceSetting, AppearanceOverride = register_appearance(Base)
from .cloud_columns import register as register_cloud_columns
from .cloud_codec import Document, Receipts, install as install_cloud_documents
register_cloud_columns(Base.metadata)


class CloudAnalysisJob(Base):
    __tablename__ = "cloud_analysis_job"
    id = Column(String(32), primary_key=True)
    status = Column(String(32), nullable=False, default="queued", index=True)
    CLOUD_DOCUMENTS = ('selection',)
    _selection_ref = Column('selection_record_id', String(96))
    selection = Document('selection', 'selection', dict)
    target = Column(Integer, nullable=False)
    imported = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    error = Column(Text)


class CloudAnalysisRun(Base):
    __tablename__ = "cloud_analysis_run"
    id = Column(String(32), primary_key=True)
    job_id = Column(String(32), ForeignKey("cloud_analysis_job.id"), nullable=False, index=True)
    status = Column(String(32), nullable=False, default="preparing")
    CLOUD_DOCUMENTS = ('positions', 'launch', 'receipts', 'contract', 'cleanup_plan')
    _positions_ref = Column('positions_record_id', String(96))
    _launch_ref = Column('launch_record_id', String(96))
    _receipts_ref = Column('receipts_record_id', String(96))
    _contract_ref = Column('contract_record_id', String(96))
    _cleanup_plan_ref = Column('cleanup_plan_record_id', String(96))
    positions = Document('positions', 'positions', list)
    launch = Document('launch', 'launch', dict)
    receipts = Receipts('receipts', 'receipts', dict)
    contract = Document('contract', 'contract')
    cloud_state = Column(String(32))
    cleanup_plan = Document('cleanup_plan', 'cleanup')
    error = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    position_count = Column(Integer, nullable=False, default=0, server_default='0')
    imported_count = Column(Integer, nullable=False, default=0, server_default='0')
    details_pruned = Column(Boolean, nullable=False, default=False, server_default='false')
    results_verified_at = Column(DateTime(timezone=True))
    cleanup_completed_at = Column(DateTime(timezone=True))
    completion_manifest_sha256 = Column(String(64))


class CloudJobCancellation(Base):
    """Durable stop/cleanup intent; never a successful-analysis completion."""
    __tablename__ = 'cloud_job_cancellation'
    job_id = Column(String(32), ForeignKey('cloud_analysis_job.id'), primary_key=True)
    phase = Column(String(32), nullable=False, default='stopping')
    cursor = Column(Integer, nullable=False, default=0)
    requested_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    finished_at = Column(DateTime(timezone=True))
    CLOUD_DOCUMENTS = ('batch_plan', 'artifact_plan')
    _batch_plan_ref = Column('batch_plan_record_id', String(96))
    _artifact_plan_ref = Column('artifact_plan_record_id', String(96))
    batch_plan = Document('batch_plan', 'cleanup')
    artifact_plan = Document('artifact_plan', 'cleanup')

    @property
    def id(self):
        return self.job_id + ':cancellation'


class CloudGameDraft(Base):
    """A shared, durable selection builder; never authorizes work until submitted."""
    __tablename__ = 'cloud_game_draft'
    id = Column(String(32), primary_key=True)
    revision = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    submitted_job_id = Column(String(32), ForeignKey('cloud_analysis_job.id'), unique=True)
    preview_revision = Column(Integer)
    preview_fens = Column(Integer)
    preview_missing_fens = Column(Integer)
    preview_claimed_fens = Column(Integer)
    preview_games = Column(Integer)
    previewed_at = Column(DateTime(timezone=True))


class CloudGameDraftEntry(Base):
    __tablename__ = 'cloud_game_draft_entry'
    id = Column(String(32), primary_key=True)
    draft_id = Column(String(32), ForeignKey('cloud_game_draft.id', ondelete='CASCADE'), nullable=False, index=True)
    ordinal = Column(Integer, nullable=False)
    player_name = Column(String(200), nullable=False)
    selection_mode = Column(String(24), nullable=False)
    selection_order = Column(String(24), nullable=False)
    requested_games = Column(Integer)
    date_from = Column(Date)
    date_to = Column(Date)
    selected_games = Column(Integer, nullable=False)
    preview_fens = Column(Integer, nullable=False)
    __table_args__ = (UniqueConstraint('draft_id', 'ordinal'),)


class CloudGameDraftGame(Base):
    """Exact game identities, not copies of their positions or engine results."""
    __tablename__ = 'cloud_game_draft_game'
    entry_id = Column(String(32), ForeignKey('cloud_game_draft_entry.id', ondelete='CASCADE'), primary_key=True)
    game_link = Column(BigInteger, primary_key=True)
    ordinal = Column(Integer, nullable=False)


class CloudGameDraftCache(Base):
    """Disposable builder snapshot; never an authorization or FEN reservation."""
    __tablename__ = 'cloud_game_draft_cache'
    draft_id = Column(String(32), ForeignKey('cloud_game_draft.id', ondelete='CASCADE'), primary_key=True)
    revision = Column(Integer, nullable=False)
    missing = Column(BigInteger, nullable=False, default=0)
    claimed = Column(BigInteger, nullable=False, default=0)
    __table_args__ = (CheckConstraint('missing >= 0 AND claimed >= 0 AND claimed <= missing'),)


class CloudGamePreparedSelection(Base):
    """One-hour Calculate result, not a reservation or authorization to run."""
    __tablename__ = 'cloud_game_prepared_selection'
    id = Column(String(32), primary_key=True)
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)
    games_sha256 = Column(String(64), nullable=False)
    missing = Column(BigInteger, nullable=False)
    claimed = Column(BigInteger, nullable=False)


class CloudGamePreparedChunk(Base):
    """Bounded native arrays avoid millions of temporary indexed/FK rows."""
    __tablename__ = 'cloud_game_prepared_chunk'
    plan_id = Column(String(32), ForeignKey('cloud_game_prepared_selection.id', ondelete='CASCADE'), primary_key=True)
    ordinal = Column(Integer, primary_key=True)
    fen_ids = Column(ARRAY(BigInteger), nullable=False)
    claimed = Column(ARRAY(Boolean), nullable=False)
    __table_args__ = (CheckConstraint('cardinality(fen_ids) BETWEEN 1 AND 16384'),
                     CheckConstraint('cardinality(fen_ids) = cardinality(claimed)'),)


class CloudInputPreparation(Base):
    """Restartable local reservation; never authorizes a cloud launch."""
    __tablename__ = 'cloud_input_preparation'
    run_id = Column(String(32), ForeignKey('cloud_analysis_run.id'), primary_key=True)
    target = Column(Integer, nullable=False)
    phase = Column(String(24), nullable=False, default='reserving')
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class CloudInputChunk(Base):
    """Validated input in bounded native arrays, consumed when the run seals."""
    __tablename__ = 'cloud_input_chunk'
    run_id = Column(String(32), ForeignKey('cloud_input_preparation.run_id', ondelete='CASCADE'), primary_key=True)
    ordinal = Column(Integer, primary_key=True)
    ids = Column(ARRAY(Text), nullable=False)
    fens = Column(ARRAY(Text), nullable=False)
    __table_args__ = (CheckConstraint('cardinality(ids) BETWEEN 1 AND 500'),
                     CheckConstraint('cardinality(ids) = cardinality(fens)'),)


class CloudGameDraftEntryFen(Base):
    __tablename__ = 'cloud_game_draft_entry_fen'
    entry_id = Column(String(32), ForeignKey('cloud_game_draft_entry.id', ondelete='CASCADE'), primary_key=True)
    # No FEN FK: draft calculation must not key-share-lock millions of live FENs.
    fen_id = Column(BigInteger, primary_key=True)
    draft_id = Column(String(32), ForeignKey('cloud_game_draft_cache.draft_id', ondelete='CASCADE'), nullable=False, index=True)


class CloudGameDraftFen(Base):
    __tablename__ = 'cloud_game_draft_fen'
    draft_id = Column(String(32), ForeignKey('cloud_game_draft_cache.draft_id', ondelete='CASCADE'), primary_key=True)
    fen_id = Column(BigInteger, primary_key=True)
    n_entries = Column(Integer, nullable=False)
    is_missing = Column(Boolean, nullable=False)
    is_claimed = Column(Boolean, nullable=False)
    __table_args__ = (CheckConstraint('n_entries > 0'), CheckConstraint('NOT is_claimed OR is_missing'))


install_cloud_documents((CloudAnalysisJob, CloudAnalysisRun, CloudJobCancellation))


class CloudBatchUnit(Base):
    """Small parent/child index; FEN identities stay in bounded existing runs."""
    __tablename__ = 'cloud_batch_unit'
    root_run_id = Column(String(32), ForeignKey('cloud_analysis_run.id'), primary_key=True)
    unit_index = Column(Integer, primary_key=True)
    run_id = Column(String(32), ForeignKey('cloud_analysis_run.id'), nullable=False, unique=True)
    vm_index = Column(Integer, nullable=False)
    position_count = Column(Integer, nullable=False)


class CloudGameFenQueue(Base):
    """A job-local ordered candidate snapshot, not a cache of engine results."""
    __tablename__ = 'cloud_game_fen_queue'
    job_id = Column(String(32), ForeignKey('cloud_analysis_job.id'), primary_key=True)
    selection_sha256 = Column(String(64), nullable=False)
    built_at = Column(DateTime(timezone=True))
    candidate_count = Column(BigInteger, nullable=False, default=0)


class CloudGameFenCandidate(Base):
    __tablename__ = 'cloud_game_fen_candidate'
    job_id = Column(String(32), ForeignKey('cloud_game_fen_queue.job_id', ondelete='CASCADE'), primary_key=True)
    ordinal = Column(BigInteger, primary_key=True)
    # IDs are immutable; live eligibility is checked under the real FEN lock.
    # No FK here: queue maintenance must not take millions of FEN key-share locks.
    fen_id = Column(BigInteger, nullable=False)


class CloudBatchSequence(Base):
    """One frozen input set, consumed by sequential normal cloud lifecycles."""
    __tablename__ = 'cloud_batch_sequence'
    job_id = Column(String(32), ForeignKey('cloud_analysis_job.id'), primary_key=True)
    repeat_count = Column(Integer, nullable=False)
    requested_count = Column(Integer, nullable=False)
    reserved_count = Column(Integer, nullable=False, default=0)
    frozen_at = Column(DateTime(timezone=True))
    stop_requested = Column(Boolean, nullable=False, default=False)


class CloudBatchCycle(Base):
    __tablename__ = 'cloud_batch_cycle'
    job_id = Column(String(32), ForeignKey('cloud_batch_sequence.job_id'), primary_key=True)
    cycle_index = Column(Integer, primary_key=True)
    run_id = Column(String(32), ForeignKey('cloud_analysis_run.id'), nullable=False, unique=True)
    target_count = Column(Integer, nullable=False)


class CloudSequenceControl(Base):
    """Small native-column checkpoint for cloud-owned sequencing; no FEN payloads."""
    __tablename__ = 'cloud_sequence_control'
    job_id = Column(String(32), ForeignKey('cloud_batch_sequence.job_id'), primary_key=True)
    phase = Column(String(32), nullable=False, default='preparing')
    control_run_id = Column(String(32), nullable=False)
    session = Column(Integer, nullable=False, default=1)
    image_id = Column(String(80))
    image = Column(Text)
    plan_sha256 = Column(String(64))
    execution = Column(Text)
    accepted_at = Column(DateTime(timezone=True))
    remote_status = Column(String(32))
    completed_cycles = Column(Integer, nullable=False, default=0)
    import_cursor = Column(Integer, nullable=False, default=0)
    failure_import_cursor = Column(Integer)
    stop_acknowledged = Column(Boolean, nullable=False, default=False)


class CloudCleanupProgress(Base):
    """Observed progress only; never authorizes deletion or replaces inventory."""
    __tablename__ = 'cloud_cleanup_progress'
    job_id = Column(String(32), ForeignKey('cloud_analysis_job.id'), primary_key=True)
    plan_sha256 = Column(String(64), primary_key=True)
    phase = Column(String(24), nullable=False)
    total = Column(Integer, nullable=False)
    removed = Column(Integer)
    updated_at = Column(DateTime(timezone=True), nullable=False)
    __table_args__ = (CheckConstraint('total >= 0 AND (removed IS NULL OR (removed >= 0 AND removed <= total))'),)


class CloudPhaseTiming(Base):
    __tablename__ = 'cloud_phase_timing'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    job_id = Column(String(32), ForeignKey('cloud_analysis_job.id'), nullable=False, index=True)
    run_id = Column(String(32), ForeignKey('cloud_analysis_run.id'), index=True)
    phase = Column(String(40), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=False)
    finished_at = Column(DateTime(timezone=True))
    duration_seconds = Column(Float)
    outcome = Column(String(24), nullable=False, default='running')
    error = Column(Text)


class CloudRecoveryEvent(Base):
    """Append-only operational history; never removed by result compaction."""
    __tablename__ = 'cloud_recovery_event'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    job_id = Column(String(32), ForeignKey('cloud_analysis_job.id'), nullable=False, index=True)
    episode_id = Column(String(32), nullable=False)
    kind = Column(String(24), nullable=False)
    operation = Column(String(80), nullable=False)
    error_type = Column(String(120))
    message = Column(Text)
    attempt = Column(Integer)
    delay_seconds = Column(Float)
    elapsed_seconds = Column(Float)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class CloudResultBatch(Base):
    __tablename__ = 'cloud_result_batch'
    run_id = Column(String(32), ForeignKey('cloud_analysis_run.id'), primary_key=True)
    task_index = Column(Integer, primary_key=True)
    batch_index = Column(Integer, primary_key=True)
    object_name = Column(Text, nullable=False)
    sha256 = Column(String(64), nullable=False)
    receipt_sha256 = Column(String(64), nullable=False)
    position_count = Column(Integer, nullable=False)
    size_bytes = Column(BigInteger)
    download_seconds = Column(Float)
    validation_seconds = Column(Float)
    transaction_seconds = Column(Float)
    committed_at = Column(DateTime(timezone=True))
    detail_record_id = Column(String(96))


class CloudUsage(Base):
    __tablename__ = 'cloud_usage'
    run_id = Column(String(32), ForeignKey('cloud_analysis_run.id'), primary_key=True)
    task_index = Column(Integer, primary_key=True)
    backend = Column(String(24), nullable=False)
    cpu_count = Column(Integer)
    memory_mib = Column(Integer)
    worker_memory_mib = Column(Integer)
    machine_type = Column(String(80))
    worker_seconds = Column(Float)
    analyzed_fens = Column(Integer)
    peak_memory_bytes = Column(BigInteger)
    cpu_busy_percent = Column(Float)
    # Processing time is not billed time. Costs stay NULL until an explicit,
    # sourced estimate or billing reconciliation exists.
    estimated_cost = Column(Numeric(20, 8))
    actual_cost = Column(Numeric(20, 8))
    currency = Column(String(3))
    price_source = Column(Text)
    measured_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class CloudExecutionAttempt(Base):
    __tablename__ = 'cloud_execution_attempt'
    run_id = Column(String(32), ForeignKey('cloud_analysis_run.id'), primary_key=True)
    resource_uid = Column(String(160), primary_key=True)
    task_index = Column(Integer, nullable=False)
    backend = Column(String(24), nullable=False)
    resource_name = Column(Text, nullable=False)
    recovery_session = Column(Integer)
    state = Column(String(32))
    started_at = Column(DateTime(timezone=True))
    finished_at = Column(DateTime(timezone=True))
    reported_run_seconds = Column(Float)
    last_exit_code = Column(Integer)
    observed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class CloudFenClaim(Base):
    __tablename__ = "cloud_fen_claim"
    fen = Column(String, ForeignKey("fen.fen"), primary_key=True)
    run_id = Column(String(32), ForeignKey("cloud_analysis_run.id"), nullable=False, index=True)


class CloudControllerHeartbeat(Base):
    __tablename__ = "cloud_controller_heartbeat"
    id = Column(Integer, primary_key=True)
    seen_at = Column(DateTime(timezone=True), nullable=False)


class AccountRole(str, enum.Enum):
    admin = "admin"
    user = "user"


class Player(Base):
    __tablename__ = 'player'
    id = Column(BigInteger, Identity(always=True), primary_key=True)
    player_name = Column(Text, nullable=False, unique=True)
    name = Column(Text)
    url = Column(Text)
    title = Column(Text)
    avatar = Column(Text)
    followers = Column(Integer)
    country = Column(Text)
    location = Column(Text)
    timezone = Column(String(64))
    timezone_source = Column(String(32))
    joined = Column(BigInteger)
    status = Column(Text)
    is_streamer = Column(Boolean)
    twitch_url = Column(Text)
    verified = Column(Boolean)
    league = Column(Text)
    deleted_at = Column(DateTime(timezone=True))

class Game(Base):
    __tablename__ = 'game'
    link = Column(BigInteger, primary_key=True)
    white_player_id = Column(BigInteger, ForeignKey('player.id'), nullable=False)
    black_player_id = Column(BigInteger, ForeignKey('player.id'), nullable=False)
    mode_id = Column(SmallInteger)
    played_at = Column(DateTime(timezone=True), nullable=False)
    white_elo = Column(Integer, nullable=False)
    black_elo = Column(Integer, nullable=False)
    white_result = Column(Float, nullable=False)
    black_result = Column(Float, nullable=False)
    white_str_result = Column(Text, nullable=False)
    black_str_result = Column(Text, nullable=False)
    time_control = Column(Text, nullable=False)
    eco = Column(Text, nullable=False)
    time_elapsed = Column(Float, nullable=False)
    n_moves = Column(Integer, nullable=False)
    fens_done = Column(Boolean, nullable=False, server_default='false')
    fens_processing = Column(Boolean, nullable=False, server_default='false')
    rules = Column(String(32), nullable=False)
    initial_setup = Column(String(128))


class GameAnalysisCoverage(Base):
    __tablename__ = 'game_analysis_coverage'
    link = Column(BigInteger, ForeignKey('game.link', ondelete='CASCADE'), primary_key=True)
    total_positions = Column(Integer, nullable=False, default=0)
    analyzed_positions = Column(Integer, nullable=False, default=0)
    unscored_positions = Column(Integer, nullable=False, default=0)
    is_fully_analyzed = Column(Boolean, nullable=False, default=False)
    equal_positions = Column(Integer, nullable=False, default=0)
    small_positions = Column(Integer, nullable=False, default=0)
    clear_positions = Column(Integer, nullable=False, default=0)
    decisive_positions = Column(Integer, nullable=False, default=0)
    mate_positions = Column(Integer, nullable=False, default=0)
    refreshed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Fen(Base):
    __tablename__ = 'fen'
    fen_id = Column(BigInteger, Identity(always=True), primary_key=True)
    fen = Column(Text, nullable=False, unique=True)
    n_games = Column(BigInteger, nullable=False, server_default='0')
    moves_counter = Column(Text, nullable=False, server_default="''")
    next_moves = Column(Text)
    score = Column(Integer)
    wdl_win = Column(SmallInteger)
    wdl_draw = Column(SmallInteger)
    wdl_loss = Column(SmallInteger)
    piece_count = Column(SmallInteger)
    analysis_source = Column(String(32))
    tablebase_wdl = Column(SmallInteger)
    tablebase_dtz = Column(Integer)
    analyzed_at = Column(DateTime(timezone=True))


class GameFenAssociation(Base):
    __tablename__ = 'game_fen_association'
    game_link = Column(BigInteger, ForeignKey('game.link', ondelete='CASCADE'), primary_key=True)
    n_move = Column(Integer, primary_key=True)
    is_white = Column(Boolean, primary_key=True)
    fen_id = Column(BigInteger, ForeignKey('fen.fen_id'), nullable=False)


class FenContinuation(Base):
    __tablename__ = 'fen_continuation'
    fen_id = Column(BigInteger, ForeignKey('fen.fen_id', ondelete='CASCADE'), primary_key=True)
    rank = Column(SmallInteger, primary_key=True)
    move = Column(Text, nullable=False)
    score = Column(Integer, nullable=False)


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


class IngestionPipelineRun(Base):
    """One complete player/manual ingestion from parsing through tablebase."""

    __tablename__ = "ingestion_pipeline_run"

    run_id = Column(String(36), primary_key=True)
    player_name = Column(String, nullable=True, index=True)
    trigger = Column(String(32), nullable=False, index=True)
    status = Column(String(16), nullable=False, index=True)
    started_at = Column(DateTime(timezone=True), nullable=False, index=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    elapsed_ms = Column(Float, nullable=True)
    last_error = Column(String, nullable=True)


class IngestionPipelineStageTiming(Base):
    """Measured execution of one stage; stages may repeat in follow-up passes."""

    __tablename__ = "ingestion_pipeline_stage_timing"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(
        String(36),
        ForeignKey("ingestion_pipeline_run.run_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    stage = Column(String(32), nullable=False, index=True)
    status = Column(String(16), nullable=False, index=True)
    started_at = Column(DateTime(timezone=True), nullable=False)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    elapsed_ms = Column(Float, nullable=True)
    processed = Column(BigInteger, nullable=False, default=0, server_default="0")
    total = Column(BigInteger, nullable=False, default=0, server_default="0")
    detail = Column(String, nullable=True)

    __table_args__ = (
        Index(
            "ix_ingestion_stage_run_stage_started",
            "run_id",
            "stage",
            "started_at",
        ),
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


class CoefficientResearchExperiment(Base):
    """A reproducible, non-production fit of CP-to-outcome coefficients."""

    __tablename__ = "coefficient_research_experiment"

    id = Column(String(36), primary_key=True)
    status = Column(String(16), nullable=False, default="queued", index=True)
    job_id = Column(String(64), nullable=True, index=True)
    created_by = Column(
        String(36),
        ForeignKey("account.id", ondelete="SET NULL"),
        nullable=True,
    )
    config = Column(JSON, nullable=False)
    dataset_summary = Column(JSON, nullable=True)
    result = Column(JSON, nullable=True)
    error = Column(String, nullable=True)
    decision = Column(String(32), nullable=True)
    decision_config = Column(JSON, nullable=True)
    decision_notes = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    decided_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint(
            "status IN ('queued', 'running', 'complete', 'failed')",
            name="coefficient_research_experiment_status",
        ),
        CheckConstraint(
            "decision IS NULL OR decision IN ('keep_lichess', 'alongside', 'replace')",
            name="coefficient_research_experiment_decision",
        ),
    )


class MatrixDefinition(Base):
    """Small reusable instructions; contains no materialized rows or arrays."""

    __tablename__ = "matrix_definition"

    id = Column(String(36), primary_key=True)
    name = Column(String(100), nullable=False)
    row_type = Column(String(32), nullable=False)
    config = Column(JSON, nullable=False)
    created_by = Column(String(36), ForeignKey("account.id", ondelete="SET NULL"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (Index("ix_matrix_definition_created_at", created_at.desc()),)


class AlgorithmDefinition(Base):
    """Immutable calculation instructions, including a copy of the matrix recipe."""

    __tablename__ = "algorithm_definition"
    id = Column(String(36), primary_key=True)
    name = Column(String(100), nullable=False)
    matrix_definition_id = Column(String(36), ForeignKey("matrix_definition.id", ondelete="SET NULL"))
    config = Column(JSON, nullable=False)
    created_by = Column(String(36), ForeignKey("account.id", ondelete="SET NULL"))
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    __table_args__ = (Index("ix_algorithm_definition_created_at", created_at.desc()),)


class AlgorithmRun(Base):
    """Durable execution state; completed compact results are immutable."""

    __tablename__ = "algorithm_run"
    id = Column(String(36), primary_key=True)
    definition_id = Column(String(36), ForeignKey("algorithm_definition.id", ondelete="SET NULL"))
    name = Column(String(100), nullable=False)
    config = Column(JSON, nullable=False)
    status = Column(String(16), nullable=False, default="queued")
    cancel_requested = Column(Boolean, nullable=False, default=False)
    progress = Column(JSON, nullable=False, default=dict)
    result = Column(JSON)
    error = Column(String(2000))
    created_by = Column(String(36), ForeignKey("account.id", ondelete="SET NULL"))
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    started_at = Column(DateTime(timezone=True))
    finished_at = Column(DateTime(timezone=True))
    __table_args__ = (
        CheckConstraint("status IN ('queued','running','complete','failed','cancelled')", name="algorithm_run_status"),
        Index("ix_algorithm_run_created_at", created_at.desc()),
        Index("ix_algorithm_run_active", status, postgresql_where=text("status IN ('queued','running')")),
    )


class MatrixArtifact(Base):
    """Metadata for an immutable matrix snapshot stored outside PostgreSQL."""

    __tablename__ = "matrix_artifact"

    id = Column(String(36), primary_key=True)
    name = Column(String(100), nullable=False)
    row_type = Column(String(32), nullable=False, index=True)
    status = Column(String(16), nullable=False, default="queued", index=True)
    job_id = Column(String(96), nullable=True, index=True)
    created_by = Column(
        String(36),
        ForeignKey("account.id", ondelete="SET NULL"),
        nullable=True,
    )
    config = Column(JSON, nullable=False)
    estimate = Column(JSON, nullable=True)
    result = Column(JSON, nullable=True)
    artifact_path = Column(String, nullable=True)
    row_count = Column(BigInteger, nullable=True)
    feature_count = Column(Integer, nullable=True)
    label_count = Column(Integer, nullable=True)
    size_bytes = Column(BigInteger, nullable=True)
    error = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint(
            "status IN ('queued', 'running', 'complete', 'failed')",
            name="matrix_artifact_status",
        ),
        Index("ix_matrix_artifact_created_at", created_at.desc()),
    )
