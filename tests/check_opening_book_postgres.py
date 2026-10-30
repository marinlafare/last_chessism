"""Full catalogue import and lookup on private PostgreSQL; no network or live DB."""
import asyncio
import json
import tempfile
import time
from contextlib import suppress
from dataclasses import replace
from pathlib import Path

import asyncpg
import chess
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from chessism_api.operations.opening_book.catalog import load_catalog, position_key
from chessism_api.operations.opening_book.store import publish, lookup, TABLE


async def command(*args):
    process = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT)
    output, _ = await process.communicate()
    if process.returncode:
        raise RuntimeError(output.decode())


async def main():
    root = Path(tempfile.mkdtemp(prefix='opening-book-', dir='/restore-test'))
    socket = root / 'socket'
    socket.mkdir()
    pgctl = '/usr/lib/postgresql/15/bin/pg_ctl'
    engine = connection = None
    try:
        await command('/usr/lib/postgresql/15/bin/initdb', '-D', str(root/'db'), '-U', 'test',
            '--auth-local=trust', '--auth-host=reject')
        await command(pgctl, '-D', str(root/'db'), '-l', str(root/'log'), '-w', '-o',
            f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'", 'start')
        connection = await asyncpg.connect(host=str(socket), port=55432, user='test', database='postgres')
        engine = create_async_engine(URL.create('postgresql+asyncpg', username='test',
            database='postgres', port=55432, query={'host': str(socket)}))
        sessions = async_sessionmaker(engine)
        catalog = load_catalog()
        async with sessions() as session, session.begin():
            assert await publish(session, catalog)
        async with sessions() as session, session.begin():
            assert not await publish(session, catalog)
        rows = await connection.fetch(f'SELECT * FROM {TABLE} ORDER BY position_key,move_uci')
        actual = [(r['position_key'], r['move_uci'], r['result_position_key'],
            json.loads(r['opening_labels']), r['source_revision'], r['importer_version']) for r in rows]
        expected = [(a,b,c,json.loads(d),e,f) for a,b,c,d,e,f in catalog.records]
        # PostgreSQL locale ordering need not match Python's Unicode ordering.
        assert {r[:2]: r[2:] for r in actual} == {r[:2]: r[2:] for r in expected}, 'Stored catalogue differs from the validated source'
        # Different data must not silently replace the first import.
        changed = replace(catalog, records=catalog.records[:-1])
        try:
            async with sessions() as session, session.begin():
                await publish(session, changed)
        except ValueError:
            pass
        else:
            raise AssertionError('A conflicting catalogue was not rejected')
        assert await connection.fetchval(f'SELECT count(*) FROM {TABLE}') == len(catalog.records)
        # Exact position + UCI lookup; no book classification for a random key.
        board = chess.Board()
        keys = []
        for uci in ['e2e4','c7c5','g1f3','d7d6']:
            keys.append((position_key(board), uci))
            board.push_uci(uci)
        async with sessions() as session:
            started = time.monotonic()
            result = await lookup(session, keys + [keys[0], ('not a position', 'a2a3')])
            elapsed = time.monotonic() - started
            assert len(result) == 4
            assert any('Sicilian' in label['name'] for label in result[keys[1]]['opening_labels'])
            assert await lookup(session, []) == {}
            try:
                await lookup(session, [(str(i),'e2e4') for i in range(513)])
            except ValueError:
                pass
            else:
                raise AssertionError('Unbounded lookup accepted')
        assert await connection.fetchval("SELECT to_regclass('public.fen')") is None
        print(json.dumps({'result':'PASS','opening_lines':catalog.lines,'stored_moves':len(rows),
            'lookup_seconds':round(elapsed,4),'checks':'full source comparison; idempotence; conflict rollback; bounded lookup; no game/FEN dependencies'}))
    finally:
        if engine:
            await engine.dispose()
        if connection:
            await connection.close()
        with suppress(Exception):
            await command(pgctl, '-D', str(root/'db'), '-m', 'fast', '-w', 'stop')


if __name__ == '__main__':
    asyncio.run(main())
