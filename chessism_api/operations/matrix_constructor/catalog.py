"""Allowlisted matrix row types and source columns.

SQL expressions deliberately live on the server. The browser can combine the
published fields, but it can never submit identifiers or arbitrary SQL.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MatrixColumn:
    key: str
    label: str
    expression: str
    source: str
    data_type: str = "number"
    storage_dtype: str | None = None
    description: str = ""
    default: bool = False
    label_allowed: bool = True

    @property
    def numpy_dtype(self) -> str:
        """Return the lossless-enough on-disk dtype for research snapshots."""
        if self.storage_dtype:
            return self.storage_dtype
        if self.data_type == "category":
            return "int32"
        return "float32"

    def public_payload(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "source": self.source,
            "data_type": self.data_type,
            "encoding": "dictionary" if self.data_type == "category" else "numeric",
            "storage_dtype": self.numpy_dtype,
            "description": self.description,
            "default": self.default,
            "label_allowed": self.label_allowed,
        }


@dataclass(frozen=True)
class MatrixRowType:
    key: str
    label: str
    description: str
    sources: tuple[str, ...]
    row_key_expression: str
    from_sql: str
    order_sql: str
    columns: tuple[MatrixColumn, ...]
    group_by_sql: str = ""
    player_filter_sql: str | None = None
    mode_filter_sql: str | None = None
    date_filter_sql: str | None = None
    min_moves_filter_sql: str | None = None
    analyzed_filter_sql: str | None = None

    @property
    def columns_by_key(self) -> dict[str, MatrixColumn]:
        return {column.key: column for column in self.columns}

    def public_payload(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "description": self.description,
            "sources": list(self.sources),
            "columns": [column.public_payload() for column in self.columns],
            "filters": {
                "players": self.player_filter_sql is not None,
                "modes": self.mode_filter_sql is not None,
                "dates": self.date_filter_sql is not None,
                "minimum_moves": self.min_moves_filter_sql is not None,
                "analyzed_only": self.analyzed_filter_sql is not None,
            },
        }


GAME_COLUMNS = (
    MatrixColumn("white_player", "White player", "g.white", "game.white", data_type="category"),
    MatrixColumn("black_player", "Black player", "g.black", "game.black", data_type="category"),
    MatrixColumn("white_rating", "White rating", "g.white_elo", "game.white_elo", storage_dtype="int32", default=True),
    MatrixColumn("black_rating", "Black rating", "g.black_elo", "game.black_elo", storage_dtype="int32", default=True),
    MatrixColumn("average_rating", "Average rating", "g.avg_elo", "game.avg_elo", default=True),
    MatrixColumn("white_result", "White result", "g.white_result", "game.white_result", default=True),
    MatrixColumn("black_result", "Black result", "g.black_result", "game.black_result"),
    MatrixColumn("moves", "Full moves", "g.n_moves", "game.n_moves", storage_dtype="int32", default=True),
    MatrixColumn("elapsed_seconds", "Elapsed seconds", "g.time_elapsed", "game.time_elapsed", default=True),
    MatrixColumn("started_at", "Start timestamp", "EXTRACT(EPOCH FROM g.played_at)", "game.played_at", storage_dtype="int64", default=True),
    MatrixColumn("start_hour", "Start hour UTC", "g.hour", "game.hour", storage_dtype="int32"),
    MatrixColumn("start_weekday", "Start weekday UTC", "EXTRACT(ISODOW FROM g.played_at)", "game.played_at", storage_dtype="int32"),
    MatrixColumn("mode", "Mode", "g.mode", "game.mode", data_type="category"),
    MatrixColumn("eco", "ECO", "g.eco", "game.eco", data_type="category"),
    MatrixColumn("time_control", "Time control", "g.time_control", "game.time_control", data_type="category"),
    MatrixColumn("rules", "Rules", "g.rules", "game.rules", data_type="category"),
    MatrixColumn("analyzed_positions", "Analyzed positions", "gas.analyzed_positions", "game_analysis_summary.analyzed_positions", storage_dtype="int32"),
    MatrixColumn("total_positions", "Total positions", "gas.total_positions", "game_analysis_summary.total_positions", storage_dtype="int32"),
    MatrixColumn("fully_analyzed", "Fully analyzed", "CASE WHEN gas.is_fully_analyzed THEN 1 ELSE 0 END", "game_analysis_summary.is_fully_analyzed", storage_dtype="uint8"),
    MatrixColumn("average_abs_score", "Average absolute CP", "gas.avg_abs_score", "game_analysis_summary.avg_abs_score"),
)

GAME_PLAYER_COLUMNS = (
    MatrixColumn("player", "Player", "gp.player_name", "game_player.player_name", data_type="category"),
    MatrixColumn("opponent", "Opponent", "gp.opponent_name", "game_player.opponent_name", data_type="category"),
    MatrixColumn("player_color", "Player color", "gp.color", "game_player.color", data_type="category", default=True),
    MatrixColumn("result", "Player result", "gp.result", "game_player.result", default=True),
    MatrixColumn("rating", "Player rating", "gp.rating", "game_player.rating", storage_dtype="int32", default=True),
    MatrixColumn("opponent_rating", "Opponent rating", "gp.opponent_rating", "game_player.opponent_rating", storage_dtype="int32", default=True),
    MatrixColumn("average_rating", "Average rating", "gp.avg_elo", "game_player.avg_elo"),
    MatrixColumn("moves", "Full moves", "gp.n_moves", "game_player.n_moves", storage_dtype="int32", default=True),
    MatrixColumn("elapsed_seconds", "Elapsed seconds", "gp.time_elapsed", "game_player.time_elapsed", default=True),
    MatrixColumn("started_at", "Start timestamp", "EXTRACT(EPOCH FROM gp.played_at)", "game_player.played_at", storage_dtype="int64", default=True),
    MatrixColumn("mode", "Mode", "gp.mode", "game_player.mode", data_type="category"),
    MatrixColumn("eco", "ECO", "gp.eco", "game_player.eco", data_type="category"),
    MatrixColumn("analyzed_player_moves", "Analyzed player moves", "gps.analyzed_player_moves", "game_player_engine_summary.analyzed_player_moves", storage_dtype="int32"),
    MatrixColumn("cp_gain", "Own-move CP gain", "gps.own_move_cp_gain", "game_player_engine_summary.own_move_cp_gain"),
    MatrixColumn("cp_loss", "Own-move CP loss", "gps.own_move_cp_loss", "game_player_engine_summary.own_move_cp_loss"),
    MatrixColumn("accuracy", "Game accuracy", "gps.game_efficiency", "game_player_engine_summary.game_efficiency", default=True),
    MatrixColumn("mean_win_percent_loss", "Mean win-% loss", "gps.mean_win_percent_loss", "game_player_engine_summary.mean_win_percent_loss"),
    MatrixColumn("median_win_percent_loss", "Median win-% loss", "gps.median_win_percent_loss", "game_player_engine_summary.median_win_percent_loss"),
    MatrixColumn("blunders", "Blunders", "gps.blunder_count", "game_player_engine_summary.blunder_count", storage_dtype="int32"),
    MatrixColumn("final_player_cp", "Final player CP", "gps.final_player_cp", "game_player_engine_summary.final_player_cp"),
    MatrixColumn("end_by", "Ending reason", "gps.end_by", "game_player_engine_summary.end_by", data_type="category"),
    MatrixColumn("game_salience", "Game salience", "gsl.salience", "game_player_salience.salience", default=True),
    MatrixColumn("salience_numerator", "Salience weighted numerator", "gsl.weighted_numerator", "game_player_salience.weighted_numerator"),
    MatrixColumn("salience_depth_weight", "Salience depth-weight sum", "gsl.depth_weight_sum", "game_player_salience.depth_weight_sum"),
    MatrixColumn("position_occurrences", "Position occurrences", "gsl.position_occurrence_count", "game_player_salience.position_occurrence_count", storage_dtype="int32"),
    MatrixColumn("unique_positions", "Unique positions inside game", "gsl.unique_position_count", "game_player_salience.unique_position_count", storage_dtype="int32"),
    MatrixColumn("repeated_positions", "Repeated positions inside game", "gsl.repeated_position_count", "game_player_salience.repeated_position_count", storage_dtype="int32"),
)

MOVE_COLUMNS = (
    MatrixColumn("player", "Player", "gp.player_name", "game_player.player_name", data_type="category"),
    MatrixColumn("opponent", "Opponent", "gp.opponent_name", "game_player.opponent_name", data_type="category"),
    MatrixColumn("fullmove", "Fullmove number", "m.n_move", "moves.n_move", storage_dtype="int32", default=True),
    MatrixColumn("ply", "Ply", "((m.n_move - 1) * 2 + side.color_order)", "moves.n_move", storage_dtype="int32", default=True),
    MatrixColumn("move_color", "Move color", "side.move_color", "game_fen_association.move_color", data_type="category", default=True),
    MatrixColumn("san", "SAN move", "side.san", "moves.white_move/black_move", data_type="category"),
    MatrixColumn("reaction_seconds", "Reaction seconds", "side.reaction_time", "moves.white_reaction_time/black_reaction_time", default=True),
    MatrixColumn("time_left_seconds", "Time left seconds", "side.time_left", "moves.white_time_left/black_time_left", default=True),
    MatrixColumn("player_result", "Player result", "gp.result", "game_player.result", default=True),
    MatrixColumn("player_rating", "Player rating", "gp.rating", "game_player.rating", storage_dtype="int32", default=True),
    MatrixColumn("opponent_rating", "Opponent rating", "gp.opponent_rating", "game_player.opponent_rating", storage_dtype="int32"),
    MatrixColumn("mode", "Mode", "gp.mode", "game_player.mode", data_type="category"),
    MatrixColumn("started_at", "Game start timestamp", "EXTRACT(EPOCH FROM gp.played_at)", "game_player.played_at", storage_dtype="int64"),
    MatrixColumn("position_fen", "Resulting FEN", "f.fen", "fen.fen", data_type="category", description="High-cardinality position identity; select only when the matrix needs it."),
    MatrixColumn("position_cp", "Position CP", "f.score", "fen.score", default=True),
    MatrixColumn("wdl_win", "WDL win", "f.wdl_win", "fen.wdl_win"),
    MatrixColumn("wdl_draw", "WDL draw", "f.wdl_draw", "fen.wdl_draw"),
    MatrixColumn("wdl_loss", "WDL loss", "f.wdl_loss", "fen.wdl_loss"),
    MatrixColumn("piece_count", "Piece count", "f.piece_count", "fen.piece_count", storage_dtype="int32", default=True),
    MatrixColumn("tablebase_wdl", "Tablebase WDL", "f.tablebase_wdl", "fen.tablebase_wdl", storage_dtype="int32"),
    MatrixColumn("tablebase_dtz", "Tablebase DTZ", "f.tablebase_dtz", "fen.tablebase_dtz", storage_dtype="int32"),
    MatrixColumn("position_repetitions", "Position repetitions", "f.n_games", "fen.n_games", storage_dtype="int64"),
    MatrixColumn("game_salience", "Game salience", "gsl.salience", "game_player_salience.salience"),
    MatrixColumn(
        "corpus_games_with_position", "Player corpus games with position",
        "CASE WHEN pss.status = 'ready' AND gfa.fen_fen IS NOT NULL THEN COALESCE(ppf.games_with_position, 1) END",
        "player_position_frequency.games_with_position", storage_dtype="int64",
    ),
    MatrixColumn(
        "occurrence_index", "Occurrence inside game",
        "salience_occurrence.occurrence_index",
        "game_fen_association", storage_dtype="int32",
    ),
    MatrixColumn(
        "position_salience", "Position salience",
        "CASE WHEN pss.status = 'ready' AND gfa.fen_fen IS NOT NULL THEN 1.0 / COALESCE(ppf.games_with_position, 1) / salience_occurrence.occurrence_index END",
        "player_position_frequency",
    ),
    MatrixColumn(
        "depth_weight", "Salience depth weight",
        "CASE WHEN gfa.fen_fen IS NOT NULL THEN 0.25 + 0.75 * LEAST((((m.n_move - 1) * 2 + side.color_order)::double precision) / 16.0, 1.0) END",
        "game_fen_association",
    ),
    MatrixColumn(
        "weighted_move_salience", "Weighted move salience",
        "CASE WHEN pss.status = 'ready' AND gfa.fen_fen IS NOT NULL THEN (1.0 / COALESCE(ppf.games_with_position, 1) / salience_occurrence.occurrence_index) * (0.25 + 0.75 * LEAST((((m.n_move - 1) * 2 + side.color_order)::double precision) / 16.0, 1.0)) END",
        "player_position_frequency",
    ),
)

POSITION_COLUMNS = (
    MatrixColumn("game_repetitions", "Game repetitions", "f.n_games", "fen.n_games", storage_dtype="int64", default=True),
    MatrixColumn("score", "Centipawn score", "f.score", "fen.score", default=True),
    MatrixColumn("wdl_win", "WDL win", "f.wdl_win", "fen.wdl_win", default=True),
    MatrixColumn("wdl_draw", "WDL draw", "f.wdl_draw", "fen.wdl_draw", default=True),
    MatrixColumn("wdl_loss", "WDL loss", "f.wdl_loss", "fen.wdl_loss", default=True),
    MatrixColumn("piece_count", "Piece count", "f.piece_count", "fen.piece_count", storage_dtype="int32", default=True),
    MatrixColumn("tablebase_wdl", "Tablebase WDL", "f.tablebase_wdl", "fen.tablebase_wdl", storage_dtype="int32"),
    MatrixColumn("tablebase_dtz", "Tablebase DTZ", "f.tablebase_dtz", "fen.tablebase_dtz", storage_dtype="int32"),
    MatrixColumn("analysis_source", "Analysis source", "f.analysis_source", "fen.analysis_source", data_type="category"),
    MatrixColumn("analyzed", "Analyzed", "CASE WHEN f.score IS NOT NULL THEN 1 ELSE 0 END", "fen.score", storage_dtype="uint8"),
)

GAME_POSITION_COLUMNS = (
    MatrixColumn("player", "Mover", "gp.player_name", "game_player.player_name", data_type="category"),
    MatrixColumn("opponent", "Opponent", "gp.opponent_name", "game_player.opponent_name", data_type="category"),
    MatrixColumn("fullmove", "Fullmove number", "gfa.n_move", "game_fen_association.n_move", storage_dtype="int32", default=True),
    MatrixColumn("ply", "Ply", "((gfa.n_move - 1) * 2 + CASE WHEN gfa.move_color = 'white' THEN 1 ELSE 2 END)", "game_fen_association", storage_dtype="int32", default=True),
    MatrixColumn("move_color", "Move color", "gfa.move_color", "game_fen_association.move_color", data_type="category", default=True),
    MatrixColumn("game_result", "Mover result", "gp.result", "game_player.result", default=True),
    MatrixColumn("player_rating", "Mover rating", "gp.rating", "game_player.rating", storage_dtype="int32", default=True),
    MatrixColumn("mode", "Mode", "gp.mode", "game_player.mode", data_type="category"),
    MatrixColumn("started_at", "Game start timestamp", "EXTRACT(EPOCH FROM gp.played_at)", "game_player.played_at", storage_dtype="int64"),
    MatrixColumn("position_fen", "Resulting FEN", "f.fen", "fen.fen", data_type="category", description="High-cardinality position identity; select only when the matrix needs it."),
    MatrixColumn("position_cp", "Position CP", "f.score", "fen.score", default=True),
    MatrixColumn("piece_count", "Piece count", "f.piece_count", "fen.piece_count", storage_dtype="int32", default=True),
    MatrixColumn("position_repetitions", "Position repetitions", "f.n_games", "fen.n_games", storage_dtype="int64"),
    MatrixColumn("analysis_source", "Analysis source", "f.analysis_source", "fen.analysis_source", data_type="category"),
    MatrixColumn("game_salience", "Game salience", "gsl.salience", "game_player_salience.salience"),
    MatrixColumn(
        "corpus_games_with_position", "Player corpus games with position",
        "CASE WHEN pss.status = 'ready' THEN COALESCE(ppf.games_with_position, 1) END",
        "player_position_frequency.games_with_position", storage_dtype="int64",
    ),
    MatrixColumn(
        "occurrence_index", "Occurrence inside game",
        "salience_occurrence.occurrence_index",
        "game_fen_association", storage_dtype="int32",
    ),
    MatrixColumn(
        "position_salience", "Position salience",
        "CASE WHEN pss.status = 'ready' THEN 1.0 / COALESCE(ppf.games_with_position, 1) / salience_occurrence.occurrence_index END",
        "player_position_frequency",
    ),
    MatrixColumn(
        "depth_weight", "Salience depth weight",
        "0.25 + 0.75 * LEAST((((gfa.n_move - 1) * 2 + CASE WHEN gfa.move_color = 'white' THEN 1 ELSE 2 END)::double precision) / 16.0, 1.0)",
        "game_fen_association",
    ),
    MatrixColumn(
        "weighted_move_salience", "Weighted move salience",
        "CASE WHEN pss.status = 'ready' THEN (1.0 / COALESCE(ppf.games_with_position, 1) / salience_occurrence.occurrence_index) * (0.25 + 0.75 * LEAST((((gfa.n_move - 1) * 2 + CASE WHEN gfa.move_color = 'white' THEN 1 ELSE 2 END)::double precision) / 16.0, 1.0)) END",
        "player_position_frequency",
    ),
)

PLAYER_PERIOD_COLUMNS = (
    MatrixColumn("player", "Player", "gp.player_name", "game_player.player_name", data_type="category"),
    MatrixColumn("games", "Games", "COUNT(*)", "game_player", storage_dtype="int64", default=True),
    MatrixColumn("wins", "Wins", "COUNT(*) FILTER (WHERE gp.result = 1)", "game_player.result", storage_dtype="int64", default=True),
    MatrixColumn("draws", "Draws", "COUNT(*) FILTER (WHERE gp.result = 0.5)", "game_player.result", storage_dtype="int64", default=True),
    MatrixColumn("losses", "Losses", "COUNT(*) FILTER (WHERE gp.result = 0)", "game_player.result", storage_dtype="int64", default=True),
    MatrixColumn("average_rating", "Average rating", "AVG(gp.rating)", "game_player.rating", default=True),
    MatrixColumn("total_moves", "Total full moves", "SUM(gp.n_moves)", "game_player.n_moves", storage_dtype="int64"),
    MatrixColumn("total_elapsed_seconds", "Total elapsed seconds", "SUM(gp.time_elapsed)", "game_player.time_elapsed"),
    MatrixColumn("analyzed_games", "Analyzed games", "COUNT(gps.game_link)", "game_player_engine_summary", storage_dtype="int64", default=True),
    MatrixColumn("average_accuracy", "Average accuracy", "AVG(gps.game_efficiency)", "game_player_engine_summary.game_efficiency", default=True),
    MatrixColumn("total_cp_gain", "Total CP gain", "SUM(gps.own_move_cp_gain)", "game_player_engine_summary.own_move_cp_gain"),
    MatrixColumn("total_cp_loss", "Total CP loss", "SUM(gps.own_move_cp_loss)", "game_player_engine_summary.own_move_cp_loss"),
    MatrixColumn("blunders", "Blunders", "SUM(gps.blunder_count)", "game_player_engine_summary.blunder_count", storage_dtype="int64"),
    MatrixColumn("mode", "Mode", "gp.mode", "game_player.mode", data_type="category"),
    MatrixColumn("period_start", "Month start timestamp", "EXTRACT(EPOCH FROM DATE_TRUNC('month', gp.played_at))", "game_player.played_at", storage_dtype="int64"),
    MatrixColumn("effective_games", "Salience effective games", "SUM(gsl.salience)", "game_player_salience.salience"),
    MatrixColumn(
        "salience_weighted_accuracy", "Salience-weighted accuracy",
        "SUM(gps.game_efficiency * gsl.salience) / NULLIF(SUM(gsl.salience) FILTER (WHERE gps.game_efficiency IS NOT NULL), 0)",
        "game_player_engine_summary/game_player_salience",
    ),
)


ROW_TYPES: dict[str, MatrixRowType] = {
    "game": MatrixRowType(
        key="game", label="Games", description="One row per game.",
        sources=("game", "game_analysis_summary"), row_key_expression="g.link::text",
        from_sql="game g", order_sql="g.played_at NULLS LAST, g.link", columns=GAME_COLUMNS,
        player_filter_sql="(g.white = ANY(CAST(:players AS text[])) OR g.black = ANY(CAST(:players AS text[])))",
        mode_filter_sql="g.mode = ANY(CAST(:modes AS text[]))", date_filter_sql="g.played_at",
        min_moves_filter_sql="g.n_moves >= :min_moves", analyzed_filter_sql="gas.is_fully_analyzed",
    ),
    "game_player": MatrixRowType(
        key="game_player", label="Player-games", description="One row for each player color in a game.",
        sources=("game_player", "game_player_engine_summary", "game_player_salience"),
        row_key_expression="CONCAT(gp.link, ':', gp.color)", from_sql="game_player gp",
        order_sql="gp.played_at NULLS LAST, gp.link, gp.color", columns=GAME_PLAYER_COLUMNS,
        player_filter_sql="gp.player_name = ANY(CAST(:players AS text[]))",
        mode_filter_sql="gp.mode = ANY(CAST(:modes AS text[]))", date_filter_sql="gp.played_at",
        min_moves_filter_sql="gp.n_moves >= :min_moves", analyzed_filter_sql="gps.analyzed_player_moves > 0",
    ),
    "move": MatrixRowType(
        key="move", label="Moves",
        description="One row per played half-move. Player filters and salience refer to the mover.",
        sources=("moves", "game_player", "game_fen_association", "fen", "game_player_salience", "player_position_frequency"),
        row_key_expression="CONCAT(m.link, ':', m.n_move, ':', side.move_color)",
        from_sql="""game_player gp
            JOIN moves m ON m.link = gp.link
            CROSS JOIN LATERAL (VALUES
                ('white'::varchar, m.white_move, m.white_reaction_time, m.white_time_left, 1),
                ('black'::varchar, m.black_move, m.black_reaction_time, m.black_time_left, 2)
            ) side(move_color, san, reaction_time, time_left, color_order)""",
        order_sql="gp.played_at DESC NULLS LAST, m.link, m.n_move, side.color_order", columns=MOVE_COLUMNS,
        player_filter_sql="gp.player_name = ANY(CAST(:players AS text[]))",
        mode_filter_sql="gp.mode = ANY(CAST(:modes AS text[]))", date_filter_sql="gp.played_at",
        min_moves_filter_sql="gp.n_moves >= :min_moves", analyzed_filter_sql="f.score IS NOT NULL",
    ),
    "position": MatrixRowType(
        key="position", label="Positions", description="One row per canonical FEN; FEN is retained as the row key.",
        sources=("fen", "game_fen_association", "game", "game_player"),
        row_key_expression="f.fen", from_sql="fen f", order_sql="f.fen", columns=POSITION_COLUMNS,
        player_filter_sql="__POSITION_SCOPE__", mode_filter_sql="__POSITION_SCOPE__",
        date_filter_sql="__POSITION_SCOPE__", min_moves_filter_sql="__POSITION_SCOPE__",
        analyzed_filter_sql="f.score IS NOT NULL",
    ),
    "game_position": MatrixRowType(
        key="game_position", label="Game-position appearances",
        description="One row per resulting position appearance. Player filters and salience refer to the mover.",
        sources=("game_fen_association", "game_player", "fen", "game_player_salience", "player_position_frequency"),
        row_key_expression="CONCAT(gfa.game_link, ':', gfa.n_move, ':', gfa.move_color)",
        from_sql="""game_player gp
            JOIN game_fen_association gfa ON gfa.game_link = gp.link AND gfa.move_color = gp.color""",
        order_sql="gp.played_at DESC NULLS LAST, gfa.game_link, gfa.n_move, CASE WHEN gfa.move_color = 'white' THEN 1 ELSE 2 END",
        columns=GAME_POSITION_COLUMNS,
        player_filter_sql="gp.player_name = ANY(CAST(:players AS text[]))",
        mode_filter_sql="gp.mode = ANY(CAST(:modes AS text[]))", date_filter_sql="gp.played_at",
        min_moves_filter_sql="gp.n_moves >= :min_moves", analyzed_filter_sql="f.score IS NOT NULL",
    ),
    "player_period": MatrixRowType(
        key="player_period", label="Player-months", description="One row per player, mode, and calendar month.",
        sources=("game_player", "game_player_engine_summary", "game_player_salience"),
        row_key_expression="CONCAT(gp.player_name, ':', gp.mode, ':', TO_CHAR(DATE_TRUNC('month', gp.played_at), 'YYYY-MM'))",
        from_sql="game_player gp",
        order_sql="DATE_TRUNC('month', gp.played_at), gp.player_name, gp.mode", columns=PLAYER_PERIOD_COLUMNS,
        group_by_sql="gp.player_name, gp.mode, DATE_TRUNC('month', gp.played_at)",
        player_filter_sql="gp.player_name = ANY(CAST(:players AS text[]))",
        mode_filter_sql="gp.mode = ANY(CAST(:modes AS text[]))", date_filter_sql="gp.played_at",
        min_moves_filter_sql="gp.n_moves >= :min_moves", analyzed_filter_sql="gps.analyzed_player_moves > 0",
    ),
}


def matrix_catalog() -> dict[str, Any]:
    return {
        "version": 2,
        "formats": ["typed column-group npy", "jsonl.gz row keys", "manifest/dictionaries"],
        "row_types": [row_type.public_payload() for row_type in ROW_TYPES.values()],
        "limits": {"maximum_rows": 5_000_000, "maximum_columns": 48},
        "notes": [
            "Artifacts are immutable snapshots stored outside PostgreSQL.",
            "Categorical values are dictionary-encoded; missing values have a separate mask.",
            "Only allowlisted fields are accepted. Arbitrary SQL is never executed.",
            "Corpus-based salience fields are missing until the player's projection is ready.",
            "Row limits select an ordered prefix, not a random sample.",
        ],
    }
