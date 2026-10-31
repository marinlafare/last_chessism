"""All page reads come from the experiment. No live SQL or recalculation."""
import json
import chess
from .artifacts import connect,annotations
from .compute import occurrence_details

SORTS={'date':'played_at','current':'current','equal':'equal','opening':'opening',
       'difference':'difference','delta_equal':'delta_equal','delta_opening':'delta_opening'}


def games(identifier, sort='difference', direction='desc', mode='all', offset=0, limit=25):
    if sort not in SORTS or direction not in ('asc','desc') or mode not in ('all','bullet','blitz','rapid'):
        raise ValueError('Invalid comparison filter')
    if not 1<=limit<=50 or offset<0:
        raise ValueError('Invalid page')
    db=connect(identifier)
    try:
        params=[] if mode=='all' else [mode]
        where='' if mode=='all' else 'WHERE mode=?'
        total=db.execute(f'SELECT count(*) FROM games {where}',params).fetchone()[0]
        rows=db.execute(f'''SELECT game_link,is_white,played_at,mode,opponent,result,positions,
            current,equal,opening,delta_equal,delta_opening,difference FROM games {where}
            ORDER BY {SORTS[sort]} {direction},played_at DESC,game_link DESC LIMIT ? OFFSET ?''',
            [*params,limit,offset]).fetchall()
        notes=annotations(identifier)
        return {'games':[{**dict(r),'review':notes.get(r['game_link'])} for r in rows],
                'total':total,'offset':offset,'limit':limit}
    finally:
        db.close()


def game_detail(identifier,game_id):
    db=connect(identifier)
    try:
        row=db.execute('SELECT * FROM games WHERE game_link=?',(game_id,)).fetchone()
        if not row:
            raise FileNotFoundError('Game not in this snapshot')
        meta=json.loads(row['metadata'])
        recorded=json.loads(row['moves'])
        details=occurrence_details(db,game_id)
        board=chess.Board(meta.get('initial_fen') or chess.STARTING_FEN)
        moves=[]
        for move_number,white,black in recorded:
            for color,san in (('white',white),('black',black)):
                if not san or san=='--':
                    continue
                move=board.parse_san(san)
                before=board.fen()
                board.push(move)
                moves.append(dict(ply=len(moves)+1,move_number=move_number,move_color=color,san=san,uci=move.uci(),
                    fen_before=before,fen_after=board.fen(),evaluation_after=None,classification=None))
        if len(moves)!=len(details):
            raise ValueError('Saved moves do not match the experiment positions')
        return {'game':meta,'moves':moves,'player_summaries':[],
            'comparison':{'scores':{k:row[k] for k in ('current','equal','opening')},
                'positions':details,'review':annotations(identifier).get(game_id)}}
    finally:
        db.close()
