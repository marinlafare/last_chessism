"""Canonical global projections in the caller's existing PostgreSQL transaction."""

DATABASE_FIELDS = ('n_games_in_db', 'main_characters', 'secondary_characters',
    'n_positions', 'analyzed_fens', 'unscored_fens', 'scored_fens',
    'nonzero_scored_fens', 'bullet_games', 'blitz_games', 'rapid_games')


def _fen_aggregates():
    expressions = dict(total_positions='count(*)', analyzed_fens='count(score)',
        scored_positions='count(score)', nonzero_scored_fens='count(*) FILTER(WHERE score<>0)',
        unscored_fens='count(*) FILTER(WHERE score IS NULL)')
    magnitude = 'abs(score::bigint)'
    lower = 0
    for name, upper in (('equal',50),('small',150),('clear',300),('decisive',9000),('mate',None)):
        condition = f'{magnitude}>={lower}'
        if upper is not None:
            condition += f' AND {magnitude}<{upper}'
        expressions[name+'_positions'] = f'count(*) FILTER(WHERE {condition})'
        expressions[name+'_appearances'] = f'coalesce(sum(n_games) FILTER(WHERE {condition}),0)::bigint'
        expressions[name+'_abs_score_sum'] = f'coalesce(sum({magnitude}) FILTER(WHERE {condition}),0)::double precision'
        lower = upper
    expressions.update(white_better='count(*) FILTER(WHERE score>50)',
        black_better='count(*) FILTER(WHERE score< -50)',
        balanced=f'count(*) FILTER(WHERE {magnitude}<=50)',
        score_sum='coalesce(sum(score::bigint),0)::double precision',
        abs_score_sum=f'coalesce(sum({magnitude}),0)::double precision')
    wdl = 'score IS NOT NULL AND wdl_win IS NOT NULL AND wdl_draw IS NOT NULL AND wdl_loss IS NOT NULL'
    for name in ('win','draw','loss'):
        expressions['wdl_'+name+'_sum'] = f'coalesce(sum(wdl_{name}) FILTER(WHERE {wdl}),0)::double precision'
    expressions['wdl_positions'] = f'count(*) FILTER(WHERE {wdl})'
    return expressions


FEN_AGGREGATES = _fen_aggregates()
FEN_SELECT = 'SELECT '+','.join(f'{sql} AS {field}' for field,sql in FEN_AGGREGATES.items())+' FROM public.fen'


async def _lock(connection, *tables):
    if not connection.is_in_transaction():
        raise ValueError('Summary publication requires a caller-owned transaction')
    for table in tables:
        if await connection.fetchval(f'SELECT id FROM public.{table} WHERE id=1 FOR UPDATE') != 1:
            raise ValueError(f'{table} totals are missing; initialize before publishing')


async def refresh_game_counts(connection):
    await _lock(connection, 'database_summary')
    return dict(await connection.fetchrow('''
        UPDATE public.database_summary SET
            n_games_in_db=counts.games,bullet_games=counts.bullet,
            blitz_games=counts.blitz,rapid_games=counts.rapid,
            main_characters=players.main,secondary_characters=players.secondary,
            refreshed_at=clock_timestamp()
        FROM (SELECT count(*)::bigint AS games,
            count(*) FILTER(WHERE m.name='bullet')::bigint AS bullet,
            count(*) FILTER(WHERE m.name='blitz')::bigint AS blitz,
            count(*) FILTER(WHERE m.name='rapid')::bigint AS rapid
            FROM public.game g LEFT JOIN public.game_mode m ON m.id=g.mode_id
            WHERE g.is_standard AND g.n_moves>0) counts
        CROSS JOIN (SELECT count(*) FILTER(WHERE joined IS NOT NULL AND joined<>0)::bigint AS main,
            count(*) FILTER(WHERE joined IS NULL OR joined=0)::bigint AS secondary FROM public.player) players
        WHERE id=1 RETURNING '''+','.join(DATABASE_FIELDS)))


async def refresh_fen_summaries(connection):
    """Lock before reading, then publish both summaries from one source snapshot."""
    await _lock(connection, 'database_summary', 'scored_position_summary')
    counts = await connection.fetchrow(FEN_SELECT)
    database = await connection.fetchrow('''UPDATE public.database_summary SET
        n_positions=$1,analyzed_fens=$2,unscored_fens=$3,scored_fens=$4,
        nonzero_scored_fens=$4,refreshed_at=clock_timestamp() WHERE id=1
        RETURNING '''+','.join(DATABASE_FIELDS), counts['total_positions'],
        counts['analyzed_fens'], counts['unscored_fens'], counts['nonzero_scored_fens'])
    fields = list(FEN_AGGREGATES)
    scored = await connection.fetchrow('UPDATE public.scored_position_summary SET '+
        ','.join(f'{field}=${i}' for i,field in enumerate(fields,1))+
        ',refreshed_at=clock_timestamp() WHERE id=1 RETURNING '+','.join(fields),
        *(counts[field] for field in fields))
    return dict(database), dict(scored)


async def increment_scored_fens(connection, rows):
    """Only verified, locked unscored->scored transitions, in the score transaction.

    The caller has already written/locked every selected FEN. The same ordered
    singleton locks as refresh/ingestion ensure an absolute rebuild cannot count
    a transition before its later delta. Actual stored scores/occurrences are used.
    """
    await _lock(connection, 'database_summary', 'scored_position_summary')
    delta = await connection.fetchrow(FEN_SELECT+' WHERE fen=ANY($1::text[]) AND score IS NOT NULL',
        [row['fen'] for row in rows])
    if delta['analyzed_fens'] != len(rows):
        raise ValueError('Every new-score delta must identify one stored scored FEN')
    await connection.execute('''UPDATE public.database_summary SET
        analyzed_fens=analyzed_fens+$1,unscored_fens=GREATEST(unscored_fens-$1,0),
        scored_fens=scored_fens+$2,nonzero_scored_fens=nonzero_scored_fens+$2,
        refreshed_at=clock_timestamp() WHERE id=1''',delta['analyzed_fens'],delta['nonzero_scored_fens'])
    fields = [field for field in FEN_AGGREGATES if field not in ('total_positions','unscored_fens')]
    analyzed_parameter = fields.index('analyzed_fens')+1
    await connection.execute('UPDATE public.scored_position_summary SET '+
        ','.join(f'{field}={field}+${i}' for i,field in enumerate(fields,1))+
        f',unscored_fens=GREATEST(unscored_fens-${analyzed_parameter},0),refreshed_at=clock_timestamp() WHERE id=1',
        *(delta[field] for field in fields))
