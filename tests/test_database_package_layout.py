"""Guard the backend split without loading data, touching schemas, or networking."""
import ast
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


class DatabasePackageLayoutTests(unittest.TestCase):
    def test_only_backend_packages_remain_at_database_root(self):
        root = ROOT / 'chessism_api/database'
        self.assertEqual({path.name for path in root.glob('*.py')}, {'__init__.py'})
        self.assertTrue((root / 'postgresql/models.py').is_file())
        self.assertTrue((root / 'neo4j/runtime.py').is_file())

    def test_application_imports_use_explicit_backend(self):
        paths = list((ROOT / 'chessism_api').rglob('*.py')) + [ROOT / 'main.py', ROOT / 'worker.py']
        prefix = 'chessism_api.database.'
        for path in paths:
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                names = ([node.module or ''] if isinstance(node, ast.ImportFrom) else
                         [alias.name for alias in node.names] if isinstance(node, ast.Import) else [])
                for name in names:
                    if name.startswith(prefix):
                        self.assertIn(name[len(prefix):].split('.')[0], ('postgresql', 'neo4j'), str(path))
