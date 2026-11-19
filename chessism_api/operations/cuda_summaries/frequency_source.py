"""Bounded numeric COPY over native game-side/occurrence keys; no evaluation join."""
import numpy as np
from sqlalchemy import text
from chessism_api.database.postgresql.transactions import driver_connection
from ..canonical_blunders.binary_source import HEADER
from ..matrix_constructor.artifact_files import file_operation

GAME_LIMIT = 8192
ROW_LIMIT = 1048576
WIRE = np.dtype([('fields','>i2'), *[(n,t) for i in range(7) for n,t in ((f'l{i}','>i4'),(f'v{i}','>i8'))]])


def decode(data, count):
    if data[:19] != HEADER or data[-2:] != b'\xff\xff' or len(data) != 21 + count * WIRE.itemsize:
        raise ValueError('Invalid frequency binary input.')
    wire = np.frombuffer(data, WIRE, count=count, offset=19)
    if np.any(wire['fields'] != 7) or any(np.any(wire[f'l{i}'] != 8) for i in range(7)):
        raise ValueError('Frequency input columns changed.')
    values = np.column_stack([wire[f'v{i}'] for i in range(7)]).astype(np.int64)
    if len(values) and (np.any(values[:, [0,4,5,6]] <= 0) or np.any(values[:,2] < 0)
                        or np.any(~np.isin(values[:, [1,3]], (0, 1)))):
        raise ValueError('Invalid frequency identities.')
    # Transport ordering only; frequency counts still run exclusively in the existing kernel.
    if len(values)>1:
        same = np.ones(len(values)-1, dtype=np.bool_)
        ordered = np.zeros(len(values)-1, dtype=np.bool_)
        for key in range(4):
            ordered |= same & (values[1:,key]>values[:-1,key])
            same &= values[1:,key]==values[:-1,key]
        if not np.all(ordered):
            raise ValueError('Frequency source is not strictly ordered.')
    return values


async def read(session, players, cursor):
    await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
    await session.execute(text("SET LOCAL statement_timeout='30s'"))
    keys = (await session.execute(text('''SELECT gp.game_link,gp.is_white FROM public.game_player gp
        JOIN public.game g ON g.link=gp.game_link
        WHERE gp.player_id=ANY(CAST(:players AS bigint[])) AND g.is_standard AND g.n_moves>0 AND g.fens_done
            AND ((gp.game_link,gp.is_white)>(:link,:side)
                 OR ((gp.game_link,gp.is_white)=(:link,:side) AND :move<2147483647))
        ORDER BY gp.game_link,gp.is_white LIMIT :limit'''),
        {'players':players,'link':cursor[0],'side':bool(cursor[1]),'move':cursor[2],'limit':GAME_LIMIT})).all()
    if not keys:
        return None
    data = bytearray()
    async def write(chunk):
        if len(data)+len(chunk)>21+(ROW_LIMIT+1)*WIRE.itemsize:
            raise ValueError('Frequency input exceeds checkpoint budget.')
        data.extend(chunk)
    driver = await driver_connection(session)
    status = await driver.copy_from_query('''SELECT gp.game_link::bigint,gp.is_white::int::bigint,
        a.n_move::bigint,a.is_white::int::bigint,gp.player_id::bigint,a.fen_id::bigint,g.mode_id::bigint
        FROM unnest($1::bigint[],$2::boolean[]) selected(link,side)
        JOIN public.game_player gp ON gp.game_link=selected.link AND gp.is_white=selected.side
        JOIN public.game g ON g.link=gp.game_link
        CROSS JOIN LATERAL (SELECT n_move,is_white,fen_id FROM public.game_fen_association a
            WHERE a.game_link=gp.game_link
                AND (gp.game_link,gp.is_white,a.n_move,a.is_white)>($3::bigint,$4::boolean,$5::integer,$6::boolean)
            ORDER BY n_move,is_white LIMIT $7::bigint) a
        ORDER BY gp.game_link,gp.is_white,a.n_move,a.is_white LIMIT $7::bigint''',
        [k[0] for k in keys],[k[1] for k in keys],cursor[0],bool(cursor[1]),cursor[2],bool(cursor[3]),
        ROW_LIMIT+1,output=write,format='binary',timeout=30)
    values = await file_operation(decode,data,int(status.split()[-1]))
    if len(values)>ROW_LIMIT:
        values=values[:ROW_LIMIT]
        after=[int(v) for v in values[-1,:4]]
    else:
        # Skip empty sides too, without incrementing or packing an unrestricted bigint identity.
        after=[keys[-1][0],int(keys[-1][1]),2147483647,1]
    return values[:,[4,1,5,6]].copy(),after
