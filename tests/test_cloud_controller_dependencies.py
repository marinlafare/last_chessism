"""Backend and durable transition modules must not depend on their coordinator."""
import ast
from pathlib import Path
import unittest


class CloudControllerDependencyTests(unittest.TestCase):
    def test_backends_and_durable_helpers_have_no_path_to_coordinator(self):
        root = Path(__file__).resolve().parents[1] / 'chessism_api/operations/cloud_analysis'
        graph = {}
        prefix = 'chessism_api.operations.cloud_analysis.'
        for path in root.glob('*.py'):
            edges = set()
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.ImportFrom):
                    if node.level == 1:
                        edges.update([node.module.split('.')[0]] if node.module else [a.name for a in node.names])
                    elif node.module and node.module.startswith(prefix):
                        edges.add(node.module[len(prefix):].split('.')[0])
                    elif node.module == prefix[:-1]:
                        edges.update(a.name for a in node.names)
                elif isinstance(node, ast.Import):
                    edges.update(a.name[len(prefix):].split('.')[0] for a in node.names if a.name.startswith(prefix))
            graph[path.stem] = edges
        for start in ('batch_spot_controller','cloud_run_controller','work_units_controller',
                      'transitions','sequence','auto_sequence','cancellation'):
            with self.subTest(module=start):
                pending, visited = [start], set()
                while pending:
                    name = pending.pop()
                    if name in visited:
                        continue
                    visited.add(name)
                    pending.extend(graph.get(name, ()))
                self.assertNotIn('controller', visited)
