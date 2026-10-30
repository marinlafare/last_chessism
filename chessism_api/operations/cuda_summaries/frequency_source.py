"""Bounded numeric COPY: all plies and scores, no evaluation join or CPU count."""
import numpy as np
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for, driver_connection
from ..canonical_blunders.binary_source import HEADER
from ..matrix_constructor.artifact_files import file_operation

GAME_LIMIT = 8192
ROW_LIMIT = 1048576
WIRE = np.dtype([('fields','>i2'), *[(n,t) for i in range(6) for n,t in ((f'l{i}','>i4'),(f'v{i}','>i8'))]])


def decode(data, count):
    if data[:19] != HEADER or data[-2:] != b'\xff\xff' or len(data) != 21 + count * WIRE.itemsize:
        raise ValueError('Invalid frequency binary input.')
    wire = np.frombuffer(data, WIRE, count=count, offset=19)
    if np.any(wire['fields'] != 6) or any(np.any(wire[f'l{i}'] != 8) for i in range(6)):
        raise ValueError('Frequency input columns changed.')
    values = np.column_stack([wire[f'v{i}'] for i in range(6)]).astype(np.int64)
    if len(values) and (np.any(values[:, :3] <= 0) or np.any(~np.isin(values[:, 3], (0, 1)))
                        or np.any(values[:, 4:] <= 0)):
        raise ValueError('Invalid frequency identities.')
    if len(values)>1 and np.any((values[1:,0]<values[:-1,0]) |
            ((values[1:,0]==values[:-1,0]) & (values[1:,1]<=values[:-1,1]))):
        raise ValueError('Frequency source is not strictly ordered.')
    return values


async def read(session, players, cursor):
    await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
    await session.execute(text("SET LOCAL statement_timeout='30s'"))
    t = storage_for(session).table
    ids = (await session.execute(text(f'''SELECT gp.id FROM {t('game_player')} gp
        JOIN {t('game')} g ON g.link=gp.game_link
        WHERE gp.player_id=ANY(CAST(:players AS bigint[])) AND gp.id>=:after AND g.fens_done
        ORDER BY gp.id LIMIT :limit'''), {'players':players,'after':cursor[0],'limit':GAME_LIMIT+1})).scalars().all()
    if not ids: return None
    chosen = ids[:GAME_LIMIT]
    data = bytearray()
    async def write(chunk):
        if len(data)+len(chunk)>21+(ROW_LIMIT+1)*WIRE.itemsize:
            raise ValueError('Frequency input exceeds checkpoint budget.')
        data.extend(chunk)
    driver = await driver_connection(session)
    status = await driver.copy_from_query(f'''SELECT gp.id::bigint,a.id::bigint,gp.player_id::bigint,
        gp.is_white::int::bigint,a.fen_id::bigint,g.mode_id::bigint
        FROM {t('game_player')} gp JOIN {t('game')} g ON g.link=gp.game_link
        CROSS JOIN LATERAL (SELECT id,fen_id FROM {t('game_fen_association')}
            WHERE game_link=gp.game_link AND (gp.id>$2::bigint OR id>$3::bigint)
            ORDER BY id LIMIT $4::bigint) a
        WHERE gp.id=ANY($1::bigint[]) ORDER BY gp.id,a.id LIMIT $4::bigint''',
        chosen,cursor[0],cursor[1],ROW_LIMIT+1,output=write,format='binary',timeout=30)
    values = await file_operation(decode,data,int(status.split()[-1]))
    if len(values)>ROW_LIMIT:
        values=values[:ROW_LIMIT]; after=[int(values[-1,0]),int(values[-1,1])]
    else:
        # Advance past empty games too. IDs are positive bigint identities.
        after=[int(chosen[-1])+1,0]
    return values[:,2:].copy(),after
