"""Explicit one-shot snapshot builder. PostgreSQL access is READ ONLY throughout.

python -m chessism_api.operations.salience_comparison.build --player lafareto
The reference formulas run locally in the experimental SQLite artifact, not in
official summary tables. No background task is launched by any GET endpoint.
"""
import argparse
import asyncio
from collections import Counter
from datetime import datetime,timezone
import hashlib
import json
import os
import shutil
import sqlite3
from uuid import uuid4

import chess
from sqlalchemy import text
from chessism_api.database.runtime import bind_runtime_engine
from chessism_api.database.engine import AsyncDBSession
from .artifacts import root
from .compute import VERSION,initialize,metadata,compute


async def build(player):
    player=player.strip().lower()
    folder=root();folder.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(folder).free<5_000_000_000:
        raise RuntimeError('At least 5 GB free research storage is required')
    identifier=str(uuid4());temporary=folder/f'{identifier}.building'
    db=sqlite3.connect(temporary)
    db.execute('PRAGMA cache_size=-131072')
    db.execute('PRAGMA temp_store=FILE')
    initialize(db)
    digest=hashlib.sha256()
    engine=await bind_runtime_engine(os.environ['DATABASE_URL'])
    try:
        async with AsyncDBSession() as session:
            await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
            await session.execute(text("SET LOCAL statement_timeout='180s'"))
            tracked=await session.scalar(text('''SELECT EXISTS(SELECT 1 FROM player
                WHERE player_name=:player AND joined>0 AND deleted_at IS NULL)'''),{'player':player})
            if not tracked:
                raise ValueError('Tracked player not found')
            snapshot=await session.scalar(text('SELECT pg_current_snapshot()::text'))
            source_state=await session.scalar(text('SELECT status FROM player_salience_summary WHERE player_name=:player'),{'player':player})
            query=text('''SELECT gp.link,gp.color,gp.opponent_name,gp.result,g.mode,g.played_at,
                g.white,g.black,g.white_elo,g.black_elo,g.white_result,g.black_result,
                g.white_str_result,g.black_str_result,g.time_control,g.initial_setup,s.salience
                FROM game_player gp JOIN game g ON g.link=gp.link
                LEFT JOIN game_player_salience s ON s.game_link=gp.link AND s.player_name=gp.player_name AND s.player_color=gp.color
                WHERE gp.player_name=:player AND g.fens_done AND g.n_moves>0 ORDER BY gp.link''')
            rows=(await session.execute(query,{'player':player})).mappings().all()
            if not rows:
                raise ValueError('No extracted games available')
            game_ids=set()
            for r in rows:
                played=r['played_at'].isoformat() if r['played_at'] else None
                meta=dict(link=r['link'],played_at_utc=played,mode=r['mode'],time_control=r['time_control'],
                    initial_fen=r['initial_setup'] or chess.STARTING_FEN,
                    white={'name':r['white'],'rating':r['white_elo']},black={'name':r['black'],'rating':r['black_elo']},
                    result={'white':r['white_result'],'black':r['black_result'],
                        'white_reason':r['white_str_result'],'black_reason':r['black_str_result']})
                encoded=json.dumps(meta,sort_keys=True)
                digest.update(encoded.encode())
                db.execute('''INSERT INTO games(game_link,is_white,played_at,mode,opponent,result,metadata,official)
                    VALUES(?,?,?,?,?,?,?,?)''',(r['link'],int(r['color']=='white'),played,r['mode'],r['opponent_name'],
                    r['result'],encoded,r['salience'] if source_state=='ready' else None))
                game_ids.add(r['link'])
            print(f'Snapshot: {len(rows):,} games; reading positions',flush=True)
            # Indexed per-game association reads; no global FEN grouping on the live DB.
            stream=await session.stream(text('''SELECT gp.link,a.fen_id,a.n_move,a.move_color
                FROM game_player gp JOIN game g ON g.link=gp.link
                CROSS JOIN LATERAL (SELECT fen_id,n_move,move_color FROM game_fen_association
                    WHERE game_link=gp.link OFFSET 0) a
                WHERE gp.player_name=:player AND g.fens_done AND g.n_moves>0
                ORDER BY gp.link,a.n_move,CASE WHEN a.move_color='white' THEN 0 ELSE 1 END'''),{'player':player})
            previous=None;repetitions=Counter();count=0
            async for batch in stream.partitions(10000):
                records=[]
                for link,fen_id,n_move,color in batch:
                    if link!=previous:
                        previous=link;repetitions.clear()
                    repetitions[fen_id]+=1
                    records.append((link,n_move*2-(color=='white'),fen_id,repetitions[fen_id]))
                db.executemany('INSERT INTO positions VALUES(?,?,?,?)',records)
                digest.update(json.dumps(records,separators=(',',':')).encode())
                count+=len(records)
                if count%250000==0:
                    print(f'Snapshot: {count:,} position occurrences read',flush=True)
            stream=await session.stream(text('''SELECT gp.link,m.n_move,m.white_move,m.black_move
                FROM game_player gp JOIN game g ON g.link=gp.link
                CROSS JOIN LATERAL (SELECT n_move,white_move,black_move FROM moves WHERE link=gp.link OFFSET 0) m
                WHERE gp.player_name=:player AND g.fens_done AND g.n_moves>0 ORDER BY gp.link,m.n_move'''),{'player':player})
            previous=None;recorded=[]
            def save_moves(link,moves):
                if link is not None:
                    encoded=json.dumps(moves,separators=(',',':'))
                    digest.update(f'{link}:{encoded}'.encode())
                    db.execute('UPDATE games SET moves=? WHERE game_link=?',(encoded,link))
            async for link,n_move,white,black in stream:
                if link!=previous:
                    save_moves(previous,recorded);previous=link;recorded=[]
                recorded.append((n_move,white,black))
            save_moves(previous,recorded)
        # Close the database snapshot before running the local reference formulas.
        await engine.dispose()
        if db.execute('SELECT count(*) FROM games WHERE moves IS NULL').fetchone()[0]:
            raise ValueError('Missing snapshot move records')
        for key,value in dict(id=identifier,player=player,version=VERSION,
            created_at=datetime.now(timezone.utc).isoformat(),source_snapshot=snapshot,
            input_sha256=digest.hexdigest(),games=len(game_ids),positions=count,
            source_salience_status=source_state,window_plies=30,
            scope='All extracted games; separate White/Black histories; modes pooled',
            storage='Experimental snapshot only; official summaries unchanged').items():
            metadata(db,key,value)
        db.commit()
        print('Calculating the three reference variants from the saved snapshot',flush=True)
        compute(db)
        if db.execute('PRAGMA integrity_check').fetchone()[0]!='ok':
            raise RuntimeError('Snapshot integrity check failed')
        db.close()
        temporary.rename(folder/f'{identifier}.sqlite')
        print(json.dumps({'status':'complete','id':identifier,'player':player,'games':len(game_ids),'positions':count}),flush=True)
        return identifier
    except BaseException:
        db.close()
        # Preserve incomplete inputs for diagnosis, but readers never list them.
        raise
    finally:
        await engine.dispose()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--player',default='lafareto')
    asyncio.run(build(parser.parse_args().player))
