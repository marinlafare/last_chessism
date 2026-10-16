import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from sqlalchemy import Integer, SmallInteger, Boolean, update, bindparam
from sqlalchemy.dialects.postgresql import dialect

from chessism_api.database.compact_storage import exact_integer, integer_result, compact_for, compact_table
from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY
from chessism_api.database.runtime import configured_storage, PROFILE_ENV
from chessism_api.database.models import Fen, FenContinuation, GameFenAssociation, Move, PlayerStats
from scripts.compact_db_preflight import require_private


class CompactTypesTests(unittest.TestCase):
    def test_explicit_profile_only(self):
        self.assertFalse(configured_storage({PROFILE_ENV:'relational_v2'}).compact)
        self.assertTrue(configured_storage({PROFILE_ENV:'relational_v3'}).compact)
        self.assertIsNone(compact_for(AsyncMock()))
        self.assertIsNone(compact_for(SimpleNamespace(info={})))
        layout = RelationalStorage('public','chessism_archive','chessism_reads','relational_v3')
        self.assertIs(compact_for(SimpleNamespace(info={SESSION_KEY:layout})), layout)

    def test_lossless_integers_and_nulls(self):
        for value in (None,0,1,1.0,-19997,20000,-2147483648,2147483647):
            self.assertEqual(exact_integer(value,'score'),value)
        for value in (True,False,'10',1.5,-0.01,float('nan'),float('inf'),float('-inf'),2147483648):
            with self.subTest(value=value), self.assertRaises(ValueError):
                exact_integer(value,'score')
        row={'score':-10000.0,'wdl_win':0.0,'wdl_draw':1000.0,'wdl_loss':None,'next_moves':'e2e4'}
        converted=integer_result(row)
        self.assertIs(type(converted['score']),int)
        self.assertIs(type(converted['wdl_draw']),int)
        self.assertIs(type(row['score']),float)
        self.assertIsNone(converted['wdl_loss'])
        for value in (-1,1001,0.5,float('nan')):
            with self.assertRaises(ValueError): integer_result({'wdl_win':value})

    def test_compact_metadata_does_not_mutate_legacy_models(self):
        fen=compact_table(Fen,'public')
        self.assertIsInstance(fen.c.score.type,Integer)
        self.assertIsInstance(fen.c.wdl_draw.type,SmallInteger)
        self.assertEqual(str(Fen.score.type),'FLOAT')
        self.assertIn('id',compact_table(FenContinuation,'public').c)
        self.assertEqual(list(compact_table(Move,'public').primary_key.columns.keys()),['link','n_move'])
        self.assertNotIn('id',compact_table(Move,'public').c)
        self.assertIsInstance(compact_table(GameFenAssociation,'public').c.is_white.type,Boolean)
        self.assertIn('move_color',GameFenAssociation.__table__.c)
        self.assertIn('player_id',compact_table(PlayerStats,'public').c)
        sql=str(update(fen).where(fen.c.fen==bindparam('key')).values(score=bindparam('score')).compile(dialect=dialect()))
        self.assertNotIn(' FROM ',sql)


class PrivateGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_locations_are_rejected(self):
        connection=AsyncMock()
        for path in ('/var/lib/postgresql/data','/restore-test/pgdata','/tmp/compact-db-test/pgdata'):
            with self.assertRaises(RuntimeError): await require_private(connection,path)
        connection.fetchrow.assert_not_awaited()
        connection.fetchrow.return_value={'directory':'/var/lib/postgresql/data','port':'5432','address':'127.0.0.1'}
        with self.assertRaises(RuntimeError):
            await require_private(connection,'/restore-test/compact-db-test/pgdata')
