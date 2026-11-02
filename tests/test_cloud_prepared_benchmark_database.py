"""Opt-in synthetic Calculate + Add timings; never live player data."""
import ast
import inspect
import json
import time
import unittest
from uuid import uuid4
from sqlalchemy import text
import test_cloud_analysis_database as existing
from test_cloud_game_drafts_database import DraftDatabaseTests
from test_cloud_game_drafts import plan
from unittest.mock import AsyncMock
from chessism_api.database.postgresql.ask_db import preview_player_games_for_analysis
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.operations.cloud_analysis import prepared_selection
from chessism_api.routers.cloud_game_drafts import save_entry,remove_entry,AddSelection,Revision


@unittest.skipUnless(existing.TEST_URL,'requires isolated PostgreSQL')
class PreparedTiming(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = DraftDatabaseTests.asyncSetUp
    asyncTearDown = DraftDatabaseTests.asyncTearDown

    async def test_total_calculate_and_add(self):
        async with AsyncDBSession() as s,s.begin():
            await s.execute(text("""INSERT INTO fen(fen,n_games,moves_counter,score)
                SELECT 'prepared-benchmark-'||i,1,'{}',CASE WHEN i%4=0 THEN 15 END
                FROM generate_series(1,100000) i"""))
            await s.execute(text("""INSERT INTO game_fen_association(game_link,fen_id,n_move,move_color)
                SELECT 101,fen_id,repeat,'white' FROM fen CROSS JOIN generate_series(1,10) repeat
                WHERE fen LIKE 'prepared-benchmark-%'"""))
            await s.execute(text('ANALYZE fen'));await s.execute(text('ANALYZE game_fen_association'))
        tree=ast.parse(inspect.getsource(preview_player_games_for_analysis))
        old_sql=next(n.value for n in ast.walk(tree) if isinstance(n,ast.Constant) and isinstance(n.value,str)
            and 'COUNT(DISTINCT gfa.fen_id)' in n.value)
        results=[]
        for version in ('prepared','old','old','prepared'):
            ident=uuid4().hex;start=time.monotonic()
            async with AsyncDBSession() as s,s.begin():
                if version=='prepared':stats=await prepared_selection.build(s,ident,[101])
                else:stats=dict((await s.execute(text(old_sql),{'game_links':[101]})).mappings().one())
            calculate=time.monotonic()-start
            redis=AsyncMock();redis.get.return_value=json.dumps({**plan((101,)),**stats})
            start=time.monotonic()
            self.draft=await save_entry(self.draft['id'],AddSelection(revision=self.draft['revision'],plan_id=ident),redis)
            add=time.monotonic()-start
            self.assertEqual(self.draft['preview']['fens_to_analyze'],75002)
            results.append({'version':version,'calculate_seconds':round(calculate,4),'add_seconds':round(add,4),
                'total_seconds':round(calculate+add,4),'identical_count':True})
            self.draft=await remove_entry(self.draft['id'],self.draft['entries'][0]['id'],Revision(revision=self.draft['revision']))
        print(json.dumps({'synthetic_occurrences':1000002,'unique_missing_fens':75002,'runs':results}),flush=True)


if __name__=='__main__':unittest.main()
