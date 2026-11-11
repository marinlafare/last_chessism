"""Small, deterministic synthetic corpus; never reads application data."""
from datetime import datetime, timezone

START = 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -'
FENS = (
    START,
    'rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq -',
    'rnbqkbnr/pppppppp/8/8/3P4/8/PPP1PPPP/RNBQKBNR b KQkq -',
    '7k/6Q1/6K1/8/8/8/8/8 b - -',
    '8/8/8/8/8/4K3/8/6k1 w - -',
)
PLAYED = datetime(2026, 1, 1, tzinfo=timezone.utc)
GAME_SQL = '''INSERT INTO public.game
    (link,white_player_id,black_player_id,played_at,white_elo,black_elo,
     white_result,black_result,white_str_result,black_str_result,time_control,
     eco,time_elapsed,n_moves,rules,initial_setup)
    VALUES($1,$2,$3,$4,1500,1600,1,0,'win','resigned','600','no_eco',60,$5,$6,$7)'''


async def seed(connection):
    """Caller owns the transaction, including all games and accounting."""
    white = await connection.fetchval("INSERT INTO public.player(player_name) VALUES('synthetic_white') RETURNING id")
    black = await connection.fetchval("INSERT INTO public.player(player_name) VALUES('synthetic_black') RETURNING id")
    games = ((101,white,black,2,'chess',None), (102,white,black,2,'chess960',None),
             (103,white,black,2,'chess',FENS[4]), (104,white,black,0,'chess',None),
             (105,white,white,2,'chess',START+' 0 1'))
    for link,w,b,moves,rules,setup in games:
        await connection.execute(GAME_SQL,link,w,b,PLAYED,moves,rules,setup)
    ids = []
    for index, fen in enumerate(FENS):
        ids.append(await connection.fetchval('INSERT INTO public.fen(fen,n_games) VALUES($1,$2) RETURNING fen_id',fen,100-index))
    for game in (101,105):
        for move,fen_id in enumerate(ids[:3]):
            await connection.execute('INSERT INTO public.game_fen_association VALUES($1,$2,$3,$4)',game,fen_id,move,move%2==0)
    # Explicit completed empty month is different from no imported month.
    await connection.execute('INSERT INTO public.months VALUES($1,2026,1,5),($1,2026,2,0)',white)
    await connection.execute("""INSERT INTO public.account(id,name,email,password_hash,role,is_active)
        VALUES('account-a','Synthetic A','a@example.invalid','synthetic-test-only','admin',true),
              ('account-b','Synthetic B','b@example.invalid','synthetic-test-only','user',false)""")
    await connection.execute("""INSERT INTO public.cloud_analysis_job(id,status,target,imported)
        VALUES('job-a','running',5,0),('job-b','running',5,0);
        INSERT INTO public.cloud_analysis_run(id,job_id,status) VALUES
        ('run-a','job-a','running'),('run-b','job-b','running');""")
    return {'white':white,'black':black,'fens':ids}
