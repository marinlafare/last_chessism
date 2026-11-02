"""Explicit, secret-file based configuration for the optional graph service."""
from dataclasses import dataclass
import os
from pathlib import Path
from urllib.parse import urlsplit


@dataclass(frozen=True)
class GraphConfig:
    uri: str
    database: str
    auth_file: Path

    @classmethod
    def from_env(cls, environ=None):
        values = os.environ if environ is None else environ
        uri = values.get('CHESSISM_NEO4J_URI', '').strip()
        parsed = urlsplit(uri)
        if (parsed.scheme not in {'bolt', 'bolt+s', 'bolt+ssc', 'neo4j', 'neo4j+s', 'neo4j+ssc'}
                or not parsed.hostname or parsed.username or parsed.password
                or parsed.query or parsed.fragment or parsed.path not in {'', '/'}):
            raise ValueError('Set CHESSISM_NEO4J_URI to a graph URI without embedded credentials.')
        database = values.get('CHESSISM_NEO4J_DATABASE', 'neo4j').strip()
        if not database:
            raise ValueError('CHESSISM_NEO4J_DATABASE must not be empty.')
        filename = values.get('CHESSISM_NEO4J_AUTH_FILE', '').strip()
        if not filename:
            raise ValueError('CHESSISM_NEO4J_AUTH_FILE is required; anonymous access is not supported.')
        return cls(uri, database, Path(filename))

    def credentials(self):
        try:
            content = self.auth_file.read_text(encoding='utf-8').strip()
        except OSError:
            raise ValueError('The Neo4j authentication secret is missing or unreadable.') from None
        user, separator, password = content.partition('/')
        if not separator or not user or len(password) < 8 or '\n' in content or '\r' in content:
            raise ValueError('The Neo4j secret must contain username/password with a password of at least 8 characters.')
        return user, password
