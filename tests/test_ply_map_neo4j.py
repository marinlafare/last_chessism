"""Actual Cypher tests against the dedicated synthetic graph only."""
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from neo4j import AsyncGraphDatabase
from chessism_api.database.neo4j import ply_map_source as source
from tests.test_ply_map import fixture


@unittest.skipUnless(os.environ.get('PLY_MAP_TEST_NEO4J_URI'), 'Disposable Neo4j required')
class SourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_keyset_paths_and_expansion_hash_guard(self):
        uri=os.environ['PLY_MAP_TEST_NEO4J_URI']
        self.assertEqual(uri,'bolt://chessism-ply-map-test-graph:7687')
        config=SimpleNamespace(uri=uri,database='neo4j',credentials=lambda:None)
        driver=AsyncGraphDatabase.driver(uri,auth=None,telemetry_disabled=True)
        try:
            async with driver.session() as session:
                # Only the brand-new, explicitly named disposable graph.
                for game in fixture():
                    result=await session.run('''CREATE (g:Game) SET g=$game
                        WITH g UNWIND $path AS row MERGE (p:Position {key:'chess:'+toString(row.fen_id)})
                        SET p.fen_id=row.fen_id,p.games=row.games,p.fen='test board'
                        CREATE (g)-[:AT_PLY {ply:row.ply,san:row.san}]->(p)''',
                        game={k:v for k,v in game.items() if k!='path'},path=game['path'])
                    await result.consume()
            with patch.object(source.GraphConfig,'from_env',return_value=config):
                first=await source.read(-1,2);second=await source.read(first[-1]['id'],2)
                self.assertEqual(first+second,fixture())
                self.assertEqual(await source.read(3,2),[])
                games=[dict(game_id=1,source_hash='hash-1',start_ply=2,end_ply=2)]
                rows=await source.expand(games)
                self.assertEqual(rows[0]['path'][0]['fen_key'],'chess:11')
                with self.assertRaises(ValueError):
                    await source.expand([dict(game_id=1,source_hash='wrong',start_ply=1,end_ply=3)])
        finally:await driver.close()
