"""Dashboard counters and rankings; reads never rebuild maintained projections."""
from chessism_api.database.postgresql import dashboard_reads as reads
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.transactions import read_transaction


async def get_games_database_generalities(*, factory=None):
    async with read_transaction(factory or AsyncDBSession) as session:
        row = await reads.database_totals(session)
    return {key:row[key] for key in ('n_games_in_db','main_characters','secondary_characters','n_positions','scored_fens')}


async def get_time_control_mode_counts(*, factory=None):
    async with read_transaction(factory or AsyncDBSession) as session:
        row = await reads.database_totals(session)
    return {mode:row[mode+'_games'] for mode in ('bullet','blitz','rapid')}


async def get_main_character_time_control_counts(*, factory=None):
    async with read_transaction(factory or AsyncDBSession) as session:
        rows = await reads.main_game_counts(session)
    return {'bullet':0,'blitz':0,'rapid':0, **{row['mode']:row['total'] for row in rows}}


async def get_fen_pipeline_overview(*, factory=None):
    async with read_transaction(factory or AsyncDBSession) as session:
        row = await reads.pipeline_totals(session)
    return {'total_games':row['parsed_games'], **row,
            'refreshed_at':row['refreshed_at'].isoformat() if row['refreshed_at'] else None}


async def get_remaining_extraction_count(*, factory=None):
    async with read_transaction(factory or AsyncDBSession) as session:
        return await reads.remaining_extraction(session)


async def get_top_main_characters_by_time_control(time_control, limit=5, title_status='all', *, factory=None):
    mode = (time_control or '').strip().lower()
    if mode not in {'bullet','blitz','rapid'}:
        return {'time_control':mode, 'players':[], 'limit':0}
    limit = max(1,min(int(limit),5000))
    if title_status not in {'all','titled','untitled'}: raise ValueError('Unknown player title filter')
    async with read_transaction(factory or AsyncDBSession) as session:
        rows = await reads.ranked_players(session,mode,limit,title_status)
    players = []
    for row in rows:
        wins,draws,losses = (int(row[key] or 0) for key in ('wins','draws','losses'))
        total = wins+draws+losses
        players.append({
            **{key:str(row[key] or '') for key in ('player_name','full_name','avatar','profile_url','last_game_date')},
            'title':str(row['title'] or '').strip().upper(),
            **{key:int(row[key] or 0) for key in ('rating','last_rating','avg_game_rating','n_games','wins','draws','losses','as_white','as_black')},
            'score_rate':round((wins+0.5*draws)/total,4) if total else 0.0})
    return {'time_control':mode,'limit':limit,'title_status':title_status,'players':players}
