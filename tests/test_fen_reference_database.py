"""Application paths against numeric references in an isolated test schema."""
import unittest
from sqlalchemy import select,text,update
from sqlalchemy.orm import selectinload

import test_cloud_analysis_database as base
from test_cloud_game_drafts_database import DraftDatabaseTests
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Fen,Game,GameFenAssociation,GameAnalysisSummary
from chessism_api.database.db_interface import DBInterface
from chessism_api.database.ask_db import count_game_set_fens_for_analysis,refresh_game_analysis_summary,increment_game_analysis_summary_for_scored_fens
from chessism_api.operations.ingestion_pipeline.fen_repository import refresh_fen_occurrence_counts
from chessism_api.operations.cloud_analysis.importer import refresh_projections
from chessism_api.operations.fen_explorer import position_games,describe_examples
from chessism_api.operations.common_positions import MODAL_MOVE_SQL
from chessism_api.operations.matrix_constructor.catalog import ROW_TYPES
from chessism_api.operations.matrix_constructor.config import normalize_matrix_config
from chessism_api.operations.matrix_constructor.queries import matrix_sql


@unittest.skipUnless(base.TEST_URL,'requires isolated PostgreSQL database')
class ReferenceDatabaseTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = DraftDatabaseTests.asyncSetUp
    asyncTearDown = base.CloudDatabaseTests.asyncTearDown

    async def test_ingestion_is_idempotent_and_orm_still_returns_full_positions(self):
        payload=[{'game_link':101,'fen_fen':self.fens[0],'n_move':20,'move_color':'white'}]
        async with AsyncDBSession() as session,session.begin():
            interface=DBInterface(GameFenAssociation)
            await interface.create_all_with_session(session,payload)
            await interface.create_all_with_session(session,payload)
        await refresh_fen_occurrence_counts([self.fens[0]])
        async with AsyncDBSession() as session:
            self.assertEqual((await session.get(Fen,self.fens[0])).n_games,3)
            game=(await session.scalars(select(Game).where(Game.link==101).options(selectinload(Game.fens)))).one()
            self.assertEqual({f.fen for f in game.fens},{self.fens[0],self.fens[1]})
        self.assertEqual(await count_game_set_fens_for_analysis([101,102]),3)

    async def test_cloud_and_local_summary_refreshes_find_numeric_associations(self):
        await refresh_game_analysis_summary([101,102,103],strict=True)
        async with AsyncDBSession() as session,session.begin():
            await session.execute(update(Fen).where(Fen.fen==self.fens[0]).values(score=25))
        await increment_game_analysis_summary_for_scored_fens([{'fen':self.fens[0],'score':25}])
        async with AsyncDBSession() as session:
            self.assertEqual((await session.get(GameAnalysisSummary,101)).analyzed_positions,1)
        await refresh_projections([{'fen':self.fens[0]}])
        async with AsyncDBSession() as session:
            self.assertEqual((await session.get(GameAnalysisSummary,102)).analyzed_positions,1)

    async def test_explorer_and_move_labels_still_accept_portable_fen_text(self):
        result=await position_games(self.fens[0])
        self.assertEqual({r['link'] for r in result['rows']},{101,102})
        examples=await describe_examples([{'fen':self.fens[0],'appearances':2}])
        self.assertEqual(examples[0]['fen'],self.fens[0])
        async with AsyncDBSession() as session:
            await session.execute(text(MODAL_MOVE_SQL),{'fen':self.fens[0]})

    async def test_every_matrix_catalog_column_compiles_on_numeric_schema(self):
        async with AsyncDBSession() as session:
            for kind,row_type in ROW_TYPES.items():
                for column in row_type.columns:
                    config=normalize_matrix_config({'row_type':kind,'feature_columns':[column.key],
                        'filters':{'players':['hikaru'],'max_rows':10}})
                    count,query,params=matrix_sql(config)
                    await session.execute(text(count),params)
                    await session.execute(text(query),params)
