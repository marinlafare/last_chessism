"""Isolated PostgreSQL: FEN games, deduplication, filters and newest-first paging."""
import asyncio
import os
import tempfile
from contextlib import suppress
from pathlib import Path
from unittest.mock import patch
os.environ.setdefault('DATABASE_URL','postgresql+asyncpg://test@127.0.0.1:1/test')
import asyncpg
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker
from chessism_api.database.relational_storage import RelationalStorage,SESSION_KEY
from chessism_api.operations import player_position_games as pages
from tests.check_opening_book_postgres import command

FIXTURE = '''
CREATE TABLE player(id bigint PRIMARY KEY,player_name text,joined bigint,deleted_at timestamptz,
 country text,location text,timezone text,timezone_source text);
INSERT INTO player VALUES(1,'alice',1,NULL,NULL,NULL,'UTC','manual'),(2,'bob',1,NULL,NULL,NULL,'UTC','manual');
CREATE TABLE game_mode(id int PRIMARY KEY,name text);
INSERT INTO game_mode VALUES(1,'blitz'),(2,'bullet'),(3,'rapid');
CREATE TABLE fen(fen_id bigint PRIMARY KEY,n_games bigint);
INSERT INTO fen VALUES(100,80),(200,1);
CREATE TABLE game(link bigint PRIMARY KEY,played_at timestamptz,mode_id int,white_player_id bigint,
 black_player_id bigint,white_elo int,black_elo int,white_result float8,black_result float8);
INSERT INTO game SELECT n,CASE WHEN n>36 THEN NULL ELSE '2026-01-01'::timestamptz+(n%9)*interval '1 day' END,
 1+n%3,CASE WHEN n%4=0 THEN 2 ELSE 1 END,CASE WHEN n%4=0 THEN 1 ELSE 2 END,1800,1900,1,0 FROM generate_series(1,40) n;
CREATE TABLE game_player(game_link bigint,is_white bool,player_id bigint,PRIMARY KEY(game_link,is_white));
INSERT INTO game_player SELECT link,true,white_player_id FROM game UNION ALL SELECT link,false,black_player_id FROM game;
DELETE FROM game_player WHERE game_link=10 AND player_id=1;
CREATE TABLE game_fen_association(game_link bigint,fen_id bigint,n_move int,is_white bool);
INSERT INTO game_fen_association SELECT link,100,1,true FROM game UNION ALL SELECT link,100,2,false FROM game;
INSERT INTO game_fen_association VALUES(1,200,3,true);
CREATE INDEX ON game_fen_association(fen_id);
CREATE INDEX ON game_fen_association(game_link,n_move,is_white);
CREATE INDEX ON game(white_player_id,mode_id,played_at DESC NULLS LAST,link DESC);
CREATE INDEX ON game(black_player_id,mode_id,played_at DESC NULLS LAST,link DESC);
'''


async def main():
 root=Path(tempfile.mkdtemp(prefix='position-games-',dir='/restore-test'));socket=root/'socket';socket.mkdir()
 pgctl='/usr/lib/postgresql/15/bin/pg_ctl';engine=connection=None
 try:
  await command('/usr/lib/postgresql/15/bin/initdb','-D',str(root/'db'),'-U','test','--auth-local=trust','--auth-host=reject')
  await command(pgctl,'-D',str(root/'db'),'-l',str(root/'log'),'-w','-o',f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'",'start')
  connection=await asyncpg.connect(host=str(socket),port=55432,user='test',database='postgres');await connection.execute(FIXTURE)
  engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',database='postgres',port=55432,query={'host':str(socket)}))
  layout=RelationalStorage('public','archive','reads','relational_v3')
  factory=async_sessionmaker(engine,info={SESSION_KEY:layout})
  with patch.object(pages,'AsyncDBSession',factory):
   for hot in (False,True):
    await connection.execute('UPDATE fen SET n_games=$1 WHERE fen_id=100',25000 if hot else 80)
    for color in ('white','black'):
     found=[];cursor=None
     for _ in range(20):
      page=await pages.explore_position_games('alice',fen_id=100,color=color,modes=['blitz','bullet'],limit=3,cursor=cursor)
      assert all(g['player_color']==color and g['mode'] in ('blitz','bullet') for g in page['games'])
      found.extend(g['game_id'] for g in page['games']);cursor=page['pagination']['next_cursor']
      if not cursor:break
     else:raise AssertionError('Pagination never completed')
     expected=await connection.fetch(f'''SELECT g.link FROM game g JOIN game_player gp ON gp.game_link=g.link
       WHERE gp.player_id=1 AND gp.is_white=$1 AND g.mode_id IN (1,2)
       ORDER BY g.played_at DESC NULLS LAST,g.link DESC''',color=='white')
     assert found==[r['link'] for r in expected],(hot,color,found,expected)
   first=await pages.explore_position_games('alice',fen_id=100,color='white',modes=['blitz','bullet'],limit=3)
   try:await pages.explore_position_games('alice',fen_id=100,color='black',modes=['blitz','bullet'],limit=3,cursor=first['pagination']['next_cursor'])
   except ValueError:pass
   else:raise AssertionError('Cross-filter cursor accepted')
   empty=await pages.explore_position_games('alice',fen_id=200,color='black',modes=['blitz'])
   assert not empty['games'] and not empty['pagination']['has_more']
  print('PASS: rare/common FEN paths, complete date-descending pages including null dates and ties, duplicate occurrences, ownership/color/mode filters, empty results and cursor binding.')
 finally:
  if engine:await engine.dispose()
  if connection:await connection.close()
  with suppress(Exception):await command(pgctl,'-D',str(root/'db'),'-m','fast','-w','stop')


if __name__=='__main__':asyncio.run(main())
