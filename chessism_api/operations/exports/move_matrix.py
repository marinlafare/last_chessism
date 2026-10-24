"""Filter game eligibility before limiting, then pivot the requested arriving moves."""
from .sql_policy import SCHEMA

SCORE_SEMANTICS = {
    'units': 'centipawns', 'stored_perspective': 'white', 'position': 'after the named player makes that full-move-number move',
    'missing': 'NULL: move absent, position unavailable, or score missing; zero is a valid score',
    'mate_encoding': 'Existing Stockfish scores use python-chess score(mate_score=10000). Mate values are encoded near +/-10000, not ordinary centipawn evaluations; the scalar alone does not preserve a separate mate flag.',
    'analysis': 'Uses stored scores, including their existing analysis provenance; does not run Stockfish or claim a uniform engine version.'}


def year_plan(spec):
    """Find the player's populated mode-years without scanning move scores."""
    return f'''WITH target_player AS MATERIALIZED (
        SELECT player_id FROM {SCHEMA}.players WHERE player_name=$1
    )
    SELECT DISTINCT EXTRACT(YEAR FROM played_at)::integer AS year
    FROM {SCHEMA}.games
    WHERE mode=$2 AND
        (white_player_id=(SELECT player_id FROM target_player) OR black_player_id=(SELECT player_id FROM target_player))
        AND played_at < date_trunc('year', CURRENT_TIMESTAMP) + INTERVAL '1 year'
    ORDER BY year''', [spec.player, spec.game_mode]


def plan(spec, *, start_year=None):
    # Only bounded, validated integers are formatted; all selection values bind.
    both = 'TRUE' if spec.players == 'both' else 'FALSE'
    # Ordering is fixed by the server contract, never caller-provided SQL.
    game_order = spec.game_order
    row_order = ', '.join(f'p.{term}' for term in game_order.split(', '))
    selection_order = game_order
    parameters = [spec.player, spec.game_mode, spec.max_games]
    eligibility = ''
    if spec.require_complete_scores:
        expected = (spec.last_move-spec.first_move+1) * (2 if spec.players == 'both' else 1)
        # Correlate through the approved view for an indexed lookup per game.
        # DISTINCT prevents duplicate move rows from masking holes in the range;
        # selected-game duplicates still fail in the separate preflight below.
        # This predicate is BELOW LIMIT: incomplete early games do not consume
        # the caller's max_games allowance. No fixed candidate pre-limit exists.
        eligibility = f''' AND (
            SELECT count(DISTINCT (eligible.is_white,eligible.move_number))
                FILTER (WHERE eligible.score_cp IS NOT NULL)
            FROM (
                SELECT is_white,move_number,score_cp FROM {SCHEMA}.moves
                WHERE game_id=g.game_id AND move_number BETWEEN {spec.first_move} AND {spec.last_move}
                  AND ({both} OR is_white=(g.white_player_id=(SELECT player_id FROM target_player)))
                OFFSET 0
            ) eligible
        ) = {expected}'''
        if spec.complete_score_scope == 'whole_game':
            # Read every played half-move, for BOTH sides and without a move
            # range restriction. Missing associations also produce NULL scores
            # through the approved moves view. No stale coverage flag can let
            # an unscored late/opponent move slip through. A nonexistent final
            # Black move is not a played move and does not disqualify the game.
            eligibility = f''' AND (
                SELECT bool_and(eligible.score_cp IS NOT NULL) AND
                    count(DISTINCT (eligible.is_white,eligible.move_number)) FILTER (
                        WHERE eligible.move_number BETWEEN {spec.first_move} AND {spec.last_move}
                        AND ({both} OR eligible.is_white=(g.white_player_id=(SELECT player_id FROM target_player)))
                    ) = {expected}
                FROM (
                    SELECT is_white,move_number,score_cp FROM {SCHEMA}.moves
                    WHERE game_id=g.game_id OFFSET 0
                ) eligible
            )'''
    candidates = f'''SELECT g.* FROM {SCHEMA}.games g WHERE mode=$2 AND
        (white_player_id=(SELECT player_id FROM target_player) OR black_player_id=(SELECT player_id FROM target_player))'''
    if spec.player_win != 'any':
        # These are recorded game points (1 / 0 / 0.5), not engine scores.
        # Require a consistent final result for both sides; NULL/unknown results
        # are never coerced into a loss or draw. Filter before eligibility/LIMIT.
        parameters.append({'win': 1.0, 'loss': 0.0, 'draw': 0.5}[spec.player_win])
        candidates += ''' AND (
            CASE WHEN g.white_player_id=(SELECT player_id FROM target_player)
                 THEN g.white_result ELSE g.black_result END
        ) = $4::double precision AND g.white_result + g.black_result = 1.0'''
    if spec.contract_version >= 3:
        parameters.append(start_year)
        anchor = f'${len(parameters)}::integer'
        year = 'EXTRACT(YEAR FROM played_at)::integer'
        # Fill the random start year, move right up to the current year, then
        # visit earlier years in reverse. Dates within each year stay ascending.
        # Missing dates are a final fallback, never a randomly chosen year.
        selection_order = f'''CASE WHEN played_at IS NULL THEN 2
            WHEN {year} >= {anchor} THEN 0 ELSE 1 END,
            CASE WHEN {year} >= {anchor} THEN {year} ELSE -({year}) END,
            {game_order}'''
        candidates += " AND (played_at IS NULL OR played_at < date_trunc('year', CURRENT_TIMESTAMP) + INTERVAL '1 year')"
    if spec.require_complete_scores:
        # Preserve the ordered candidate stream before evaluating the expensive
        # predicate. LIMIT can stop after enough eligible games; it must not
        # evaluate eligibility on the whole player's history before sorting.
        candidates = f'''SELECT g.* FROM (
            {candidates} ORDER BY {selection_order} OFFSET 0
        ) g WHERE TRUE {eligibility}'''
    common = f'''WITH target_player AS MATERIALIZED (
        SELECT player_id FROM {SCHEMA}.players WHERE player_name=$1
    ), selected_games AS MATERIALIZED (
        {candidates}
        ORDER BY {selection_order} LIMIT $3
    ), participants AS MATERIALIZED (
        SELECT game_id,played_at,white_player AS player_name,TRUE AS is_white FROM selected_games WHERE ({both} OR white_player=$1)
        UNION ALL
        SELECT game_id,played_at,black_player AS player_name,FALSE AS is_white FROM selected_games WHERE ({both} OR black_player=$1)
    ), selected_moves AS MATERIALIZED (
        SELECT p.game_id,p.is_white,m.move_number,m.score_cp FROM participants p
        CROSS JOIN LATERAL (
            SELECT move_number,score_cp FROM {SCHEMA}.moves
            WHERE game_id=p.game_id AND is_white=p.is_white
            AND move_number BETWEEN {spec.first_move} AND {spec.last_move} OFFSET 0
        ) m
    )'''
    checks = common + ''' SELECT (SELECT count(*) FROM selected_games) AS selected_games,
        (SELECT count(*) FROM participants) AS selected_participants,
        (SELECT count(*) FROM (SELECT game_id,is_white,move_number FROM selected_moves
          GROUP BY game_id,is_white,move_number HAVING count(*)>1) d) AS duplicates'''
    if spec.contract_version >= 3:
        checks += ''', ARRAY(SELECT DISTINCT EXTRACT(YEAR FROM played_at)::integer
            FROM selected_games WHERE played_at IS NOT NULL ORDER BY 1) AS selected_years,
            (SELECT count(*) FROM selected_games WHERE played_at IS NULL) AS undated_games'''
    score = 'm.score_cp' if spec.score_perspective == 'white' else 'CASE WHEN p.is_white THEN m.score_cp ELSE -m.score_cp END'
    values = ', '.join(f'MAX({score}) FILTER (WHERE m.move_number={number}) AS move_{number}'
                      for number in range(spec.first_move, spec.last_move+1))
    # Already present for ordering/grouping: exposing the stored start timestamp
    # needs no extra join. Older immutable jobs retain their original schema.
    date_column = 'p.played_at AS played_at,' if spec.contract_version >= 4 else ''
    query = common + f''' SELECT p.game_id,p.player_name,
        CASE WHEN p.is_white THEN 'white' ELSE 'black' END AS color,{date_column}{values}
        FROM participants p LEFT JOIN selected_moves m ON m.game_id=p.game_id AND m.is_white=p.is_white
        GROUP BY p.game_id,p.played_at,p.player_name,p.is_white ORDER BY {row_order},p.is_white DESC'''
    return query, parameters, checks
