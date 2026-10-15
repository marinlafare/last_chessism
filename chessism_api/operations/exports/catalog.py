"""Public dataset contract backed by compact-v3 tables, never caller SQL.

All joins are named, server-owned many-to-one relationships. Each source has a
stable key, including the two sides of a full move as separate half-moves.
"""
from dataclasses import dataclass, field

from chessism_api.database.relational_storage import storage_for


@dataclass(frozen=True)
class Column:
    expression: str
    dtype: str
    description: str = ''


@dataclass(frozen=True)
class Source:
    base: str
    keys: tuple[str, ...]
    meaning: str
    columns: dict[str, Column]
    joins: dict[str, tuple[str, tuple[str, ...]]] = field(default_factory=dict)
    predicate: str = 'TRUE'


GAME_COLUMNS = {
    'game_id': Column('g.link', 'int64'),
    'played_at': Column('g.played_at', 'timestamp[us, UTC]', 'Stored game start date/time in UTC; NULL when unavailable.'),
    'white_player_id': Column('g.white_player_id', 'int64'),
    'black_player_id': Column('g.black_player_id', 'int64'),
    'white_player': Column('wp.player_name', 'string'),
    'black_player': Column('bp.player_name', 'string'),
    'white_rating': Column('g.white_elo', 'int32'),
    'black_rating': Column('g.black_elo', 'int32'),
    'average_rating': Column('g.avg_elo', 'float64'),
    'white_result': Column('g.white_result', 'float64'),
    'black_result': Column('g.black_result', 'float64'),
    'white_result_name': Column('g.white_str_result', 'string'),
    'black_result_name': Column('g.black_str_result', 'string'),
    'mode': Column('gm.name', 'string'),
    'time_control': Column('g.time_control', 'string'),
    'opening': Column('g.eco', 'string', 'Stored opening suffix, not an opening classifier.'),
    'moves': Column('g.n_moves', 'int32'),
    'elapsed_seconds': Column('g.time_elapsed', 'int32'),
}
GAME_JOINS = {
    'wp': ('LEFT JOIN {player} wp ON wp.id = g.white_player_id', ('g',)),
    'bp': ('LEFT JOIN {player} bp ON bp.id = g.black_player_id', ('g',)),
    'gm': ('LEFT JOIN {game_mode} gm ON gm.id = g.mode_id', ('g',)),
}
POSITION_COLUMNS = {
    'fen_id': Column('f.fen_id', 'int64'),
    'fen': Column('f.fen', 'string', 'Canonical four-field FEN.'),
    'appearances': Column('f.n_games', 'int64', 'Stored corpus appearance count.'),
    'score_cp': Column('f.score', 'int32', 'Stored score, White perspective; NULL is unscored. No score conversion.'),
    'wdl_win': Column('f.wdl_win', 'int16', 'Stored Stockfish WDL, per mille.'),
    'wdl_draw': Column('f.wdl_draw', 'int16'),
    'wdl_loss': Column('f.wdl_loss', 'int16'),
    'piece_count': Column('f.piece_count', 'int16'),
    'analysis_source': Column('f.analysis_source', 'string'),
    'analyzed_at': Column('f.analyzed_at', 'timestamp[us, UTC]'),
    'tablebase_wdl': Column('f.tablebase_wdl', 'int16'),
    'tablebase_dtz': Column('f.tablebase_dtz', 'int32'),
}
SIDE = "CASE WHEN a.is_white THEN 'white' ELSE 'black' END"
ARRIVAL_COLUMNS = {
    **GAME_COLUMNS, **POSITION_COLUMNS,
    'game_id': Column('a.game_link', 'int64'),
    'fen_id': Column('a.fen_id', 'int64'),
    'move_number': Column('a.n_move', 'int32'),
    'move_color': Column(SIDE, 'string'),
    'is_white': Column('a.is_white', 'bool'),
    'player_id': Column('CASE WHEN a.is_white THEN g.white_player_id ELSE g.black_player_id END', 'int64', 'Mover, not either participant.'),
    'player': Column("CASE WHEN a.is_white THEN wp.player_name ELSE bp.player_name END", 'string', 'The player who made the arriving move.'),
    'player_rating': Column('CASE WHEN a.is_white THEN g.white_elo ELSE g.black_elo END', 'int32'),
    'san': Column('CASE WHEN a.is_white THEN m.white_move ELSE m.black_move END', 'string'),
}
ARRIVAL_JOINS = {
    'g': ('JOIN {game} g ON g.link = a.game_link', ()),
    'f': ('JOIN {fen} f ON f.fen_id = a.fen_id', ()),
    'm': ('LEFT JOIN {moves} m ON m.link = a.game_link AND m.n_move = a.n_move', ()),
    **GAME_JOINS,
}
PLAYER_GAME_COLUMNS = {
    **GAME_COLUMNS,
    'player_game_id': Column('gp.id', 'int64'),
    'game_id': Column('gp.game_link', 'int64'),
    'player_id': Column('gp.player_id', 'int64'),
    'player': Column('p.player_name', 'string'),
    'is_white': Column('gp.is_white', 'bool'),
    'player_rating': Column('CASE WHEN gp.is_white THEN g.white_elo ELSE g.black_elo END', 'int32'),
    'player_result': Column('CASE WHEN gp.is_white THEN g.white_result ELSE g.black_result END', 'float64'),
}
MOVE_COLUMNS = {
    **GAME_COLUMNS, **POSITION_COLUMNS,
    'game_id': Column('m.link', 'int64'),
    'move_number': Column('m.n_move', 'int32'),
    'is_white': Column('s.is_white', 'bool'),
    'move_color': Column("CASE WHEN s.is_white THEN 'white' ELSE 'black' END", 'string'),
    'san': Column('s.san', 'string'),
    'reaction_seconds': Column('s.reaction', 'float64'),
    'time_left_seconds': Column('s.time_left', 'float64'),
    'player_id': Column('CASE WHEN s.is_white THEN g.white_player_id ELSE g.black_player_id END', 'int64'),
    'player': Column('CASE WHEN s.is_white THEN wp.player_name ELSE bp.player_name END', 'string', 'Mover, not either participant.'),
    'player_rating': Column('CASE WHEN s.is_white THEN g.white_elo ELSE g.black_elo END', 'int32'),
}
SOURCES = {
    'games': Source('{game} g', ('game_id',), 'One game.', GAME_COLUMNS, GAME_JOINS),
    'players': Source('{player} p', ('player_id',), 'One player identity; profiles can be incomplete.', {
        'player_id': Column('p.id', 'int64'), 'player_name': Column('p.player_name', 'string'),
        'name': Column('p.name', 'string'), 'title': Column('p.title', 'string'),
        'joined': Column('p.joined', 'int32', 'Stored Unix seconds; zero means unknown.'),
        'country': Column('p.country', 'string'), 'verified': Column('p.verified', 'bool'),
        'deleted_at': Column('p.deleted_at', 'timestamp[us, UTC]'),
    }),
    'positions': Source('{fen} f', ('fen_id',), 'One distinct canonical FEN.', POSITION_COLUMNS),
    'game_positions': Source('{game_fen_association} a', ('game_id', 'move_number', 'is_white'),
        'One resulting FEN occurrence in a game. Repetitions are retained; player means mover.',
        ARRIVAL_COLUMNS, ARRIVAL_JOINS),
    'player_games': Source('{game_player} gp', ('player_game_id',), 'One recorded player participation in a game.',
        PLAYER_GAME_COLUMNS, {'g': ('JOIN {game} g ON g.link = gp.game_link', ()),
                             'p': ('JOIN {player} p ON p.id = gp.player_id', ()), **GAME_JOINS}),
    'moves': Source('''{moves} m CROSS JOIN LATERAL (VALUES
        (TRUE, m.white_move, m.white_reaction_time, m.white_time_left),
        (FALSE, m.black_move, m.black_reaction_time, m.black_time_left)
    ) s(is_white, san, reaction, time_left)''', ('game_id', 'move_number', 'is_white'),
        'One played half-move; excludes empty final half-moves. Player means mover.', MOVE_COLUMNS,
        {'g': ('JOIN {game} g ON g.link = m.link', ()), **GAME_JOINS,
         'a': ('LEFT JOIN {game_fen_association} a ON a.game_link = m.link AND a.n_move = m.n_move AND a.is_white = s.is_white', ()),
         'f': ('LEFT JOIN {fen} f ON f.fen_id = a.fen_id', ('a',))},
        "NULLIF(BTRIM(s.san), '') IS NOT NULL AND s.san <> '--'"),
}


def tables_for(session):
    layout = storage_for(session)
    if not layout or not layout.compact:
        raise RuntimeError('Dataset exports require the validated relational_v3 database profile.')
    return {name: layout.table(name) for name in ('game', 'player', 'game_mode', 'fen', 'moves', 'game_player', 'game_fen_association')}


def public_catalog(allowed, max_rows):
    return {'version': 1, 'format': 'parquet', 'max_rows': max_rows, 'max_columns': 48,
            'operators': ['eq', 'ne', 'in', 'not_in', 'gt', 'gte', 'lt', 'lte', 'is_null', 'is_not_null'],
            'sources': [{'name': name, 'row_meaning': s.meaning, 'keys': list(s.keys),
                'columns': [{'name': key, 'type': col.dtype, 'nullable': True, 'description': col.description}
                            for key, col in s.columns.items()]}
                for name, s in SOURCES.items() if name in allowed]}
