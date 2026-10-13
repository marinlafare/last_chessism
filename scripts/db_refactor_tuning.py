"""Second-pass reads/indexes for the guarded, PRIVATE database candidate only.

No application imports or production migration. Keep legacy result contracts,
including float output and lexical FEN tie ordering; never relax comparisons.
"""
from scripts.db_refactor_candidate import C, R, V, guard
from scripts.db_refactor_comparison import candidate_case
from scripts.db_refactor_workload import row_digest


def game_page(limit, *, mode=False, calendar=False, salience=False, extracted=False):
    """Bound each side independently, then merge with the legacy total order.

    Apply ALL eligibility filters before LIMIT (including salience presence).
    UNION ALL deliberately preserves two perspectives of a self-play game.
    """
    branches = []
    for side, flag in (('black', 'false'), ('white', 'true')):
        filters = [f'g.{side}_player_id=(SELECT id FROM owner)',
                   'gp.player_id=(SELECT id FROM owner)']
        if mode:
            filters.append(f'g.mode_id=(SELECT id FROM {C}.game_mode WHERE name=$2)')
        if calendar:
            filters.append('g.year=$2')
        if extracted:
            filters.append('g.fens_done')
        extra = (f'JOIN {C}.game_player_salience s ON s.game_player_id=gp.id'
                 if salience else '')
        branches.append(f'''(SELECT g.*,gp.id AS game_player_id,gp.is_white
            FROM {C}.game g JOIN {C}.game_player gp
              ON gp.game_link=g.link AND gp.is_white={flag}
            {extra} WHERE {' AND '.join(filters)}
            ORDER BY g.played_at DESC NULLS LAST,g.link DESC LIMIT {limit})''')
    return f'''WITH owner AS MATERIALIZED (
        SELECT id FROM {C}.player WHERE player_name=$1
    ), side_pages AS ({' UNION ALL '.join(branches)}), page AS MATERIALIZED (
        SELECT * FROM side_pages
        ORDER BY played_at DESC NULLS LAST,link DESC,is_white LIMIT {limit}
    )'''


def tuned_candidate_case(case):
    adapted = candidate_case(case)
    name = case['name'].split('/')[-1]
    if name == 'recent_games':
        sql = game_page(50) + f'''
            SELECT g.link,CASE WHEN g.is_white THEN 'white' ELSE 'black' END::varchar(5) AS color,
                $1::varchar AS player_name,p.player_name AS opponent_name,
                CASE WHEN g.is_white THEN g.white_elo ELSE g.black_elo END AS rating,
                CASE WHEN g.is_white THEN g.black_elo ELSE g.white_elo END AS opponent_rating,
                CASE WHEN g.is_white THEN g.white_result ELSE g.black_result END AS result,
                m.name AS mode,g.played_at,
                CASE WHEN g.eco='no_eco' THEN g.eco ELSE 'https://www.chess.com/openings/'||g.eco END AS eco,
                g.n_moves,g.time_elapsed::double precision AS time_elapsed,g.avg_elo
            FROM page g JOIN {C}.player p
              ON p.id=CASE WHEN g.is_white THEN g.black_player_id ELSE g.white_player_id END
            LEFT JOIN {C}.game_mode m ON m.id=g.mode_id
            ORDER BY g.played_at DESC NULLS LAST,g.link DESC,g.is_white'''
    elif name == 'mode_games':
        sql = game_page(50, mode=True) + '''
            SELECT link,CASE WHEN is_white THEN 'white' ELSE 'black' END::varchar(5) AS color,
                CASE WHEN is_white THEN white_elo ELSE black_elo END AS rating,
                CASE WHEN is_white THEN white_result ELSE black_result END AS result,played_at
            FROM page ORDER BY played_at DESC NULLS LAST,link DESC,is_white'''
    elif name == 'calendar_filter':
        sql = game_page(100, calendar=True) + f'''
            SELECT g.link,g.year,g.month,g.day,g.avg_elo,m.name AS mode
            FROM page g LEFT JOIN {C}.game_mode m ON m.id=g.mode_id
            ORDER BY g.played_at DESC NULLS LAST,g.link DESC,g.is_white'''
    elif name == 'salience':
        sql = game_page(100, salience=True) + f'''
            SELECT g.link,CASE WHEN g.is_white THEN 'white' ELSE 'black' END::varchar(5) AS color,
                s.salience,s.weighted_numerator,s.depth_weight_sum,
                s.position_occurrence_count,s.unique_position_count,
                s.position_occurrence_count-s.unique_position_count AS repeated_position_count
            FROM page g JOIN {C}.game_player_salience s ON s.game_player_id=g.game_player_id
            ORDER BY g.played_at DESC NULLS LAST,g.link DESC,g.is_white'''
    elif name == 'fen_selection':
        sql = game_page(100, extracted=True) + f'''
            SELECT DISTINCT f.fen COLLATE "C" AS fen FROM page
            JOIN {C}.game_fen_association a ON a.game_link=page.link
            JOIN public.fen f ON f.fen_id=a.fen_id WHERE f.score IS NULL
            ORDER BY fen LIMIT 5000'''
    elif name == 'position_frequencies':
        # FEN text is needed to resolve ties but NOT to rank frequency groups.
        # Include the entire boundary group before joining FENs. LIMIT 100 alone
        # here would arbitrarily lose rows and change the original top 100.
        sql = f'''WITH ranked AS MATERIALIZED (
            SELECT p.* FROM {C}.player_position_frequency p
            WHERE p.player_id=(SELECT id FROM {C}.player WHERE player_name=$1)
            ORDER BY p.games_with_position DESC,p.is_white
            FETCH FIRST 100 ROWS WITH TIES
        ) SELECT CASE WHEN p.is_white THEN 'white' ELSE 'black' END::varchar(5) AS player_color,
            f.fen AS fen_fen,p.games_with_position,p.total_occurrences,f.score
            FROM ranked p JOIN public.fen f ON f.fen_id=p.fen_id
            ORDER BY p.games_with_position DESC,p.is_white,f.fen COLLATE "C" LIMIT 100'''
    elif case['name'] == 'game/rating_filter':
        sql = f'''WITH page AS MATERIALIZED (
            SELECT link,white_player_id,black_player_id,avg_elo FROM {C}.game
            WHERE mode_id=(SELECT id FROM {C}.game_mode WHERE name='blitz')
              AND avg_elo BETWEEN 2200 AND 2600
            ORDER BY avg_elo DESC,link DESC LIMIT 100
        ) SELECT g.link,w.player_name AS white,b.player_name AS black,g.avg_elo
            FROM page g JOIN {C}.player w ON w.id=g.white_player_id
            JOIN {C}.player b ON b.id=g.black_player_id
            ORDER BY g.avg_elo DESC,g.link DESC'''
    else:
        return adapted
    adapted.update(sql=sql.strip(), source='candidate v2: native numeric queries with bounded enrichment')
    return adapted


async def apply_tuning(conn, pgdata, publish):
    await guard(conn, pgdata)
    # Index-only changes: do not rebuild/copy/delete any stored application rows.
    async with conn.transaction():
        await conn.execute("SET LOCAL statement_timeout='20min'")
        await conn.execute("SET LOCAL maintenance_work_mem='256MB'")
        for side in ('white', 'black'):
            for suffix, prefix in (('recent', ''), ('mode', 'mode_id,'), ('year', 'year,')):
                name = f'tune_game_{side}_{suffix}'
                await publish('indexing_candidate_reads', current_index=name)
                await conn.execute(f'''CREATE INDEX IF NOT EXISTS {name} ON {C}.game
                    ({side}_player_id,{prefix}played_at DESC NULLS LAST,link DESC)''')
        await publish('indexing_candidate_reads', current_index='tune_frequency_rank')
        await conn.execute(f'''CREATE INDEX IF NOT EXISTS tune_frequency_rank
            ON {C}.player_position_frequency(player_id,games_with_position DESC,is_white)
            INCLUDE (fen_id,total_occurrences)''')
        await publish('indexing_candidate_reads', current_index='tune_game_rating')
        await conn.execute(f'''CREATE INDEX IF NOT EXISTS tune_game_rating
            ON {C}.game(mode_id,avg_elo DESC,link DESC) INCLUDE (white_player_id,black_player_id)''')


async def verify_original_mismatches(conn, prior):
    """Prove the *saved* v1 failures are exclusively int-versus-float output.

    Never use this normalization to accept the new queries: their hashes must
    match the original reference without any normalization.
    """
    from scripts.db_refactor_workload import digest
    reference = {c['name']: c for c in prior['manifest']['reference_cases']}
    candidates = {c['name']: c for c in prior['manifest']['candidate_cases']}
    expected_old = {r['name']: r for r in prior['reference']['results']}
    expected_new = {r['name']: r for r in prior['candidate']['results']}
    report = []
    for mismatch in prior['comparison']['comparisons']:
        if mismatch.get('results_match'):
            continue
        name = mismatch['name']
        if not name.endswith('/recent_games'):
            raise RuntimeError('Unexpected original mismatch needs separate diagnosis')
        outputs = []
        for schema, case, expected in ((R, reference[name], expected_old[name]),
                                       (V, candidates[name], expected_new[name])):
            await conn.execute('SELECT set_config($1,$2,false)', 'search_path', schema+',public')
            # The identical legacy SQL returns int in one schema and float in
            # the other. Do not reuse a prepared result descriptor across them.
            await conn.reload_schema_state()
            async with conn.transaction(readonly=True):
                rows = await conn.fetch(case['sql'], *case['args'])
            if row_digest(rows) != expected['result_sha256']:
                raise RuntimeError('Retained query results differ from the saved first rehearsal')
            outputs.append([dict(row) for row in rows])
        original, candidate = outputs
        type_changes = 0
        if len(original) != len(candidate):
            raise RuntimeError('Recent-game row counts differ')
        for left, right in zip(original, candidate):
            if (type(left['time_elapsed']) is float and type(right['time_elapsed']) is int
                    and left['time_elapsed'] == right['time_elapsed']):
                type_changes += 1
            right['time_elapsed'] = float(right['time_elapsed'])
        if not type_changes or digest(original) != digest(candidate):
            raise RuntimeError('Type conversion does not explain every original result difference')
        report.append({'name': name, 'rows': len(original), 'type_changes': type_changes,
                       'field': 'time_elapsed', 'old_type': 'float', 'candidate_v1_type': 'int',
                       'values_and_order_identical': True, 'unexplained_differences': 0})
    return report
