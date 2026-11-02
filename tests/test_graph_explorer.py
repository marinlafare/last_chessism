"""Synthetic graph acceptance: no PostgreSQL, cloud, or live graph credentials."""
import os
import unittest
from unittest.mock import patch
from chessism_api.database.neo4j import explorer
from chessism_api.database.neo4j import opening_map
from neo4j import AsyncGraphDatabase
from chessism_api.database.neo4j.explorer_styles import register


class StyleTests(unittest.TestCase):
    def test_independent_targets_and_controls(self):
        registry = {'scopes': {'scored_positions': {'targets': {}}}}
        register(registry)
        targets = registry['scopes']['scored_positions']['targets']
        self.assertGreater(len(targets), 60)
        for key, target in targets.items():
            self.assertTrue(key.startswith('commonGraph.'))
            if key != 'commonGraph.edge':
                self.assertTrue({'background','hoverBackground','radius','padding','textAlign'}.issubset(target['properties']))
        self.assertEqual(targets['commonGraph.barRow']['hoverState'], '[data-active="true"]')


@unittest.skipUnless(os.getenv('GRAPH_EXPLORER_TEST_URI'), 'requires disposable Neo4j')
class ExplorerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.driver = AsyncGraphDatabase.driver(os.environ['GRAPH_EXPLORER_TEST_URI'], auth=None,
                                               notifications_min_severity='OFF')
        async with self.driver.session() as s:
            await (await s.run('MATCH (n) DETACH DELETE n')).consume()
            await (await s.run('''CREATE (a:Player {id:1,name:'lafareto'}),(b:Player {id:2,name:'other'}),
              (c:Player {id:3,name:'outsider'}),
              (p:Position {key:'chess:1',fen_id:1,fen:'A',occurrences:4,games:3,score_kind:'cp',score_cp:35}),
              (q:Position {key:'chess:2',fen_id:2,fen:'B',occurrences:2,games:2}),
              (z:Position {key:'chess:3',fen_id:3,fen:'C',occurrences:1,games:1}),
              (root:Position {key:'chess:initial',fen:'ROOT',occurrences:0,games:0}),
              (unused:Position {key:'chess:4',fen:'unused',occurrences:0,games:0}),
              (root)-[:MOVE {key:'entry',san:'e4',occurrences:3,games:3}]->(p),
              (p)-[:MOVE {key:'next',san:'e5',occurrences:2,games:2}]->(q),
              (q)-[:MOVE {key:'repeat',san:'Nf3',occurrences:1,games:1}]->(p),
              (p)-[:MOVE {key:'other',san:'d5',occurrences:1,games:1}]->(z),
              (g1:Game {id:1,white_name:'lafareto',black_name:'other',played_at:'2026-10-30 12:00:00+00',mode:'blitz',white_result:1,black_result:0}),
              (g2:Game {id:2,white_name:'other',black_name:'lafareto',played_at:'2026-10-29 12:00:00+00',mode:'bullet'}),
              (g3:Game {id:3,white_name:'other',black_name:'outsider',played_at:'2026-10-31 12:00:00+00',mode:'rapid'}),
              (a)-[:PLAYED {color:'white'}]->(g1),(a)-[:PLAYED {color:'black'}]->(g2),
              (b)-[:PLAYED {color:'black'}]->(g1),(b)-[:PLAYED {color:'white'}]->(g2),
              (b)-[:PLAYED {color:'white'}]->(g3),(c)-[:PLAYED {color:'black'}]->(g3),
              (a)-[:REACHED {color:'white',occurrences:2,games:1}]->(p),
              (a)-[:REACHED {color:'black',occurrences:1,games:1}]->(p),
              (a)-[:REACHED {color:'white',occurrences:1,games:1}]->(q),
              (a)-[:REACHED {color:'black',occurrences:1,games:1}]->(z),
              (g1)-[:AT_PLY {ply:1,from_key:'chess:initial',move_key:'entry'}]->(p),
              (g1)-[:AT_PLY {ply:2,from_key:'chess:1',move_key:'next'}]->(q),
              (g1)-[:AT_PLY {ply:3,from_key:'chess:2',move_key:'repeat'}]->(p),
              (g2)-[:AT_PLY {ply:1,from_key:'chess:initial',move_key:'entry'}]->(p),
              (g2)-[:AT_PLY {ply:2,from_key:'chess:1',move_key:'other'}]->(z),
              (g3)-[:AT_PLY {ply:1,from_key:'chess:initial',move_key:'entry'}]->(p),
              (g3)-[:AT_PLY {ply:2,from_key:'chess:1',move_key:'next'}]->(q)''')).consume()

    async def asyncTearDown(self):
        await self.driver.close()

    async def read(self, operation, *args):
        return await explorer.read(self.driver, 'neo4j', operation, *args)

    async def test_global_player_color_and_zero_nodes(self):
        global_ = await self.read(explorer.ranking, '', 'all')
        self.assertEqual(global_['coverage']['games'], 3)
        self.assertEqual(len(global_['rows']), 3)
        self.assertEqual(global_['rows'][0]['appearances'], 4)
        both = await self.read(explorer.ranking, 'lafareto', 'all')
        self.assertEqual(both['coverage']['games'], 2)
        self.assertEqual((both['rows'][0]['appearances'], both['rows'][0]['games']), (3, 2))
        white = await self.read(explorer.ranking, 'lafareto', 'white')
        self.assertEqual((white['rows'][0]['appearances'], white['rows'][0]['games']), (2, 1))
        black = await self.read(explorer.ranking, 'lafareto', 'black')
        self.assertEqual(len(black['rows']), 2)
        missing = await self.read(explorer.ranking, 'missing', 'all')
        self.assertEqual(missing['rows'], [])

    async def test_actual_move_connections_filter_without_cross_game_paths(self):
        white = await self.read(explorer.neighborhood, 'chess:1', 'lafareto', 'white', 0, 0)
        self.assertEqual(white['appearances'], 2)
        self.assertEqual({r['move_key'] for r in white['incoming']['rows']}, {'entry','repeat'})
        self.assertEqual([(r['move_key'], r['appearances']) for r in white['outgoing']['rows']], [('next',1)])
        black = await self.read(explorer.neighborhood, 'chess:1', 'lafareto', 'black', 0, 0)
        self.assertEqual([r['move_key'] for r in black['outgoing']['rows']], ['other'])
        root = await self.read(explorer.neighborhood, 'chess:initial', 'lafareto', 'all', 0, 0)
        self.assertEqual(root['appearances'], 0)
        self.assertEqual(root['outgoing']['rows'][0]['appearances'], 2)
        self.assertIsNone(await self.read(explorer.neighborhood, 'chess:999', '', 'all', 0, 0))

    async def test_games_are_distinct_newest_first_and_match_the_edge(self):
        all_ = await self.read(explorer.games, 'chess:1', '', 'all', '', 0)
        self.assertEqual([r['game_id'] for r in all_['rows']], [3,1,2])
        self.assertEqual(all_['rows'][0]['played_at'], '2026-10-31T12:00:00+00:00')
        player = await self.read(explorer.games, 'chess:1', 'lafareto', 'all', '', 0)
        self.assertEqual([r['game_id'] for r in player['rows']], [1,2])
        edge = await self.read(explorer.games, 'chess:1', 'lafareto', 'all', 'next', 0)
        self.assertEqual([r['game_id'] for r in edge['rows']], [1])
        self.assertEqual((edge['rows'][0]['ply'], edge['rows'][0]['fen']), (2, 'B'))
        unrelated = await self.read(explorer.games, 'chess:3', 'lafareto', 'all', 'next', 0)
        self.assertEqual(unrelated['rows'], [])
        missing = await self.read(explorer.games, 'chess:1', 'nobody', 'all', '', 0)
        self.assertEqual(missing['rows'], [])

    async def test_search_is_bounded_and_parameters_are_not_cypher(self):
        result = await self.read(explorer.players, 'LAF')
        self.assertEqual(result, [{'id':1,'name':'lafareto'}])
        self.assertEqual(await self.read(explorer.players, "' MATCH (n) RETURN n"), [])
        self.assertEqual(await self.read(explorer.games, 'chess:1', '', 'all', '', 100),
                         {'rows':[], 'has_more':False, 'offset':100, 'coverage':{'player':None,'player_id':None,'color':'all','games':3}})

    async def test_games_pagination_has_no_duplicate_rows_from_repetitions(self):
        async with self.driver.session() as s:
            await (await s.run('''MATCH (p:Position {key:'chess:1'}) UNWIND range(10,30) AS id
                CREATE (g:Game {id:id,white_name:'extra',black_name:'other',played_at:'2026-10-31 12:00:00+00'})
                CREATE (g)-[:AT_PLY {ply:1}]->(p),(g)-[:AT_PLY {ply:3}]->(p)''')).consume()
        first = await self.read(explorer.games, 'chess:1', '', 'all', '', 0)
        second = await self.read(explorer.games, 'chess:1', '', 'all', '', 15)
        self.assertEqual(len(first['rows']), 15)
        self.assertTrue(first['has_more'])
        self.assertFalse(second['has_more'])
        ids = [r['game_id'] for r in first['rows'] + second['rows']]
        self.assertEqual(len(ids), 24)
        self.assertEqual(len(set(ids)), len(ids))

    async def test_opening_map_uses_actual_plies_and_deduplicates_shared_games(self):
        async with self.driver.session() as s:
            await (await s.run('''MATCH (g:Game {id:1}),(p:Position {key:'chess:4'})
                CREATE (g)-[:AT_PLY {ply:31,from_key:'chess:1',move_key:'late',san:'late'}]->(p)''')).consume()
        result = await self.read(opening_map.snapshot, 'lafareto', 'other')
        self.assertEqual(result['coverage']['games'], [3,2,3,3,1,1,2,1,3,2])
        self.assertEqual(result['coverage']['max_ply'], 30)
        nodes = {n['key']: n for n in result['nodes']}
        self.assertNotIn('chess:4', nodes)
        self.assertEqual(nodes['chess:1']['appearances'][:4], [4,3,4,4])
        self.assertEqual(nodes['chess:1']['games'][:4], [3,2,3,3])
        self.assertEqual(nodes['chess:initial']['appearances'], [0] * 10)
        edges = {e['key']: e for e in result['edges']}
        self.assertNotIn('late', edges)
        self.assertEqual(edges['next']['games'][:4], [2,1,2,2])
        self.assertEqual(edges['other']['games'][4:8], [0,1,1,0])
        repeated = await self.read(opening_map.snapshot, 'lafareto', 'other')
        self.assertEqual(result['revision'], repeated['revision'])

    async def test_map_limits_are_explicit_and_edges_have_visible_endpoints(self):
        with patch.object(opening_map, 'GAME_LIMIT', 2), patch.object(opening_map, 'NODE_LIMIT', 1):
            result = await self.read(opening_map.snapshot, 'lafareto', 'other')
        self.assertTrue(result['limits']['games'])
        self.assertTrue(result['limits']['nodes'])
        self.assertEqual(result['coverage']['sample_games'], 2)
        keys = {n['key'] for n in result['nodes']}
        self.assertTrue(all(e['source'] in keys and e['target'] in keys for e in result['edges']))

    async def test_union_drilldown_keeps_color_ply_and_actual_games(self):
        union = await self.read(explorer.games, 'chess:1', 'lafareto', 'all', '', 0, 'other', 30)
        self.assertEqual([r['game_id'] for r in union['rows']], [3,1,2])
        white = await self.read(explorer.games, 'chess:1', 'lafareto', 'white', '', 0, 'other', 30)
        self.assertEqual([r['game_id'] for r in white['rows']], [3,1,2])
        restricted = await self.read(explorer.neighborhood, 'chess:1', 'lafareto', 'all', 0, 0, 'other', 1)
        self.assertEqual(restricted['appearances'], 3)
        self.assertEqual(restricted['outgoing']['rows'], [])
        global_ = await self.read(explorer.neighborhood, 'chess:1', '', 'all', 0, 0, '', 30)
        self.assertEqual(global_['appearances'], 4)
