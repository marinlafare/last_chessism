"""Bounded binary extraction of early positions, without scores/FEN text."""
import numpy as np
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for, driver_connection
from ..canonical_blunders.binary_source import HEADER
from ..matrix_constructor.artifact_files import file_operation
from .salience_store import SCOPE

WIRE = np.dtype([('fields','>i2'), *[(n,t) for i in range(4) for n,t in ((f'l{i}','>i4'),(f'v{i}','>i8'))]])


def decode(data, expected):
    if data[:19] != HEADER or data[-2:] != b'\xff\xff' or len(data) != 21+expected*WIRE.itemsize:
        raise ValueError('Invalid salience binary input.')
    wire = np.frombuffer(data, WIRE, count=expected, offset=19)
    if np.any(wire['fields'] != 4) or any(np.any(wire[f'l{i}'] != 8) for i in range(4)):
        raise ValueError('Salience binary column contract changed.')
    raw = np.column_stack([wire[f'v{i}'] for i in range(4)]).astype(np.int64)
    if len(raw) and (np.any(raw[:,:3] < 0) or np.any(raw[:,1] >= 2**53)
                    or np.any((raw[:,3] < 1) | (raw[:,3] > 30))):
        raise ValueError('Salience identities or plies exceed the numeric contract.')
    if len(raw)>1 and np.any((raw[1:,1] == raw[:-1,1]) & (raw[1:,3] <= raw[:-1,3])):
        raise ValueError('Duplicate or unordered salience plies.')
    return raw


async def read(session, run_id, after, limit=32768):
    await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
    await session.execute(text("SET LOCAL statement_timeout='60s'"))
    t = storage_for(session).table
    ids = (await session.execute(text(f'''SELECT gp.id FROM {t('game_player')} gp
        JOIN {SCOPE} s ON s.run_id=:id AND s.player_id=gp.player_id
        JOIN {t('game')} g ON g.link=gp.game_link
        WHERE gp.id>:after AND g.fens_done AND s.status='pending'
        ORDER BY gp.id LIMIT :limit'''), {'id':run_id,'after':after,'limit':limit})).scalars().all()
    if not ids:
        return None
    data = bytearray()
    async def write(chunk):
        if len(data)+len(chunk) > 21+len(ids)*30*WIRE.itemsize:
            raise ValueError('Salience input exceeds 30 positions per game.')
        data.extend(chunk)
    driver = await driver_connection(session)
    status = await driver.copy_from_query(f'''SELECT (gp.player_id*2+gp.is_white::int)::bigint,
        gp.id::bigint,a.fen_id::bigint,(a.n_move*2-a.is_white::int)::bigint
        FROM {t('game_player')} gp JOIN LATERAL (
            SELECT fen_id,n_move,is_white FROM {t('game_fen_association')}
            WHERE game_link=gp.game_link AND n_move BETWEEN 1 AND 15 OFFSET 0
        ) a ON true WHERE gp.id=ANY($1::bigint[]) ORDER BY gp.id,a.n_move,a.is_white DESC''',
        ids,output=write,format='binary',timeout=60)
    raw = await file_operation(decode,data,int(status.split()[-1]))
    return raw,ids[-1],len(ids)
