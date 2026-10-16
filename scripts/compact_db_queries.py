"""Bounded read-only workloads shared by v2 baseline and isolated v3 checks."""
import hashlib
import json
import math


SETTINGS = "SET LOCAL statement_timeout='20s'; SET LOCAL lock_timeout='1s'; SET LOCAL jit=off; SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL work_mem='64MB';"
DISCOVER = """WITH ranked AS (
    SELECT player_name,sum(n_games) AS games,row_number() OVER(ORDER BY sum(n_games) DESC,player_name) AS rn,
           count(*) OVER() AS total FROM public.main_character_mode_summary GROUP BY player_name)
    SELECT r.player_name,r.games,p.id AS player_id,
           (SELECT max(game_link) FROM public.game_player WHERE player_id=p.id) AS link
    FROM ranked r JOIN public.player p USING(player_name)
    WHERE rn IN (1,GREATEST(1,total/2),total) ORDER BY rn"""


def literal(value):
    if value is None:
        return 'NULL'
    if type(value) is int:
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise ValueError('Unsupported SQL literal')


def canonical(value):
    # Integer-valued float -> integer is precisely the approved score/WDL change.
    # Fractional values, NULLs, strings and array ordering are never discarded.
    if isinstance(value, float) and math.isfinite(value) and value.is_integer():
        return int(value)
    if isinstance(value, dict):
        return {key:canonical(item) for key,item in value.items()}
    if isinstance(value, (list,tuple)):
        return [canonical(item) for item in value]
    return value


def digest(value):
    return hashlib.sha256(json.dumps(canonical(value),sort_keys=True,default=str,separators=(',',':')).encode()).hexdigest()


def cases(inputs, *, compact):
    reads = 'chessism_reads' if compact else 'public'
    result=[]
    def add(name, sql):
        result.append({'name':name,'sql':sql.strip()})
    for player in inputs:
        name,link=literal(player['player_name']),literal(player['link'])
        prefix=player['player_name']+'/'
        add(prefix+'moves',f'''SELECT n_move,white_move,black_move,white_time_left,black_time_left,
            white_reaction_time,black_reaction_time FROM {reads}.moves WHERE link={link} ORDER BY n_move''')
        add(prefix+'positions',f'''SELECT a.n_move,a.move_color,a.fen_id,f.fen,f.score,f.wdl_win,f.wdl_draw,f.wdl_loss
            FROM {reads}.game_fen_association a JOIN public.fen f USING(fen_id)
            WHERE a.game_link={link} ORDER BY a.n_move,a.move_color DESC''')
        add(prefix+'months',f'''SELECT player_name,year,month,n_games FROM {reads}.months
            WHERE player_name={name} ORDER BY year,month''')
        add(prefix+'stats',f'''SELECT player_name,chess_blitz_last_rating,chess_rapid_last_rating,chess_bullet_last_rating
            FROM {reads}.player_stats WHERE player_name={name}''')
        add(prefix+'mode_summary',f'''SELECT mode,player_name,n_games,rating,wins,draws,losses,as_white,as_black
            FROM {reads}.main_character_mode_summary WHERE player_name={name} ORDER BY mode''')
    add('home/titled_players',f'SELECT * FROM {reads}.home_titled_player_summary ORDER BY player_name')
    for sort,order in (
        ('impact','(n_games * ABS(score)) DESC,n_games DESC,fen ASC'),
        ('evaluation','ABS(score) DESC,n_games DESC,fen ASC'),
    ):
        add('scored/'+sort,f'''SELECT fen_id,fen,n_games,score,wdl_win,wdl_draw,wdl_loss
            FROM public.fen WHERE score IS NOT NULL AND ABS(score)>=0 ORDER BY {order} LIMIT 20''')
    # This measures the first export page; it does NOT claim both tie-breaker
    # orders produce the same page. Exact boundary/replay content is tested by
    # the separate FEN archive integration test.
    tie='fen_id' if compact else 'fen'
    add('backup/first_page_count',f'''SELECT count(*) AS rows FROM (
        SELECT fen_id FROM public.fen WHERE score IS NOT NULL AND analyzed_at>='2020-01-01'::timestamptz
        ORDER BY analyzed_at,{tie} LIMIT 1000) page''')
    return result


def decode_stream(raw):
    decoder=json.JSONDecoder()
    values=[]
    while raw.strip():
        raw=raw.lstrip()
        item,end=decoder.raw_decode(raw)
        values.append(item)
        raw=raw[end:]
    return values
