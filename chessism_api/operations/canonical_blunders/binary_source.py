"""Bounded COPY TO STDOUT and vectorized transition packing; never writes SQL.

Fixed PostgreSQL binary projection: int8 link, int4 ply, float8 score, int2 kind.
Each non-null row occupies 40 bytes. Lengths, ordering, coverage and EOF are
verified before its values are trusted. Source transaction belongs to caller.
https://www.postgresql.org/docs/current/sql-copy.html#SQL-COPY-BINARY
"""
import time
import numpy as np
from chessism_api.database.relational_storage import driver_connection,storage_for
from ..matrix_constructor.artifact_files import file_operation
from . import compute

VERSION='binary_v1'
QUERY_GAMES=512
QUERY_ROWS=65536
GAME_ROWS=32768  # Same per-game bound as the existing reader.
HEADER=b'PGCOPY\n\xff\r\n\x00'+b'\x00'*8
WIRE=np.dtype([('fields','>i2'),('len_link','>i4'),('link','>i8'),
    ('len_ply','>i4'),('ply','>i4'),('len_score','>i4'),('score','>f8'),
    ('len_kind','>i4'),('kind','>i2')])


def query(session):
    from chessism_api.evaluations import kind_sql
    layout=storage_for(session)
    if layout is None:raise ValueError('Binary canonical source requires explicit normalized storage.')
    white='a.is_white' if layout.compact else "a.move_color='white'"
    return f'''SELECT a.game_link::bigint,
        ((a.n_move-1)*2+CASE WHEN {white} THEN 1 ELSE 2 END)::integer AS ply,
        COALESCE(f.score::float8,'NaN'::float8),
        ({kind_sql('f.score', 'f.analysis_source', codes=True)})::smallint
        FROM {layout.table('game_fen_association')} a LEFT JOIN {layout.table('fen')} f ON f.fen_id=a.fen_id
        WHERE a.game_link=ANY($1::bigint[]) ORDER BY a.game_link,ply LIMIT $2::bigint'''


def blocks(games, *, max_games=QUERY_GAMES, max_rows=QUERY_ROWS):
    block=[]; rows=0
    for game in games:
        count=game['total_positions']
        if type(count) is not int or not 1<=count<=GAME_ROWS:raise ValueError('Game exceeds canonical position bound.')
        if block and (len(block)==max_games or rows+count>max_rows):
            yield block; block=[]; rows=0
        block.append(game); rows+=count
    if block:yield block


class Sink:
    def __init__(self,expected_rows):
        self.limit=len(HEADER)+(expected_rows+1)*WIRE.itemsize+2
        self.data=bytearray()

    async def write(self,chunk):
        if len(self.data)+len(chunk)>self.limit:raise ValueError('Binary source exceeded bounded transfer size.')
        self.data.extend(chunk)


def decode(data,expected):
    if len(data)<21 or data[:19]!=HEADER or data[-2:]!=b'\xff\xff':
        raise ValueError('Unsupported or truncated binary source header/trailer.')
    if len(data)!=21+expected*WIRE.itemsize:raise ValueError('Coverage disagrees with binary source positions.')
    rows=np.frombuffer(data,dtype=WIRE,count=expected,offset=19)
    for name,value in (('fields',4),('len_link',8),('len_ply',4),('len_score',8),('len_kind',2)):
        if np.any(rows[name]!=value):raise ValueError('Binary source column type/length changed.')
    return rows


def pack(data,games,index):
    counts=np.array([g['total_positions'] for g in games],dtype=np.int64)
    expected=int(counts.sum()); wire=decode(data,expected)
    if not expected:return np.empty(0,compute.DTYPE)
    links=np.array([g['game_link'] for g in games],dtype=np.int64)
    if len(np.unique(links))!=len(links):raise ValueError('Duplicate source game.')
    # Validate SQL order before changing it to the mixed-player batch order.
    if np.any(np.diff(wire['link'])<0):raise ValueError('Unordered source games.')
    order=np.argsort(links); positions=np.searchsorted(links[order],wire['link'])
    if np.any(positions>=len(links)) or np.any(links[order][positions]!=wire['link']):
        raise ValueError('Unexpected source game.')
    ids=order[positions]
    if not np.array_equal(np.bincount(ids,minlength=len(games)),counts):
        raise ValueError('Per-game coverage disagrees with binary source.')
    permutation=np.argsort(ids,kind='stable'); starts=np.r_[0,np.cumsum(counts)[:-1]]
    ply=wire['ply'][permutation]
    expected_ply=np.arange(expected)-np.repeat(starts,counts)+1
    if not np.array_equal(ply,expected_ply):raise ValueError('Missing, duplicate or unordered predecessor; no ply may be skipped.')
    score=wire['score'][permutation]; kind=wire['kind'][permutation]
    if np.any(~np.isin(kind,(-1,0,1,2))) or np.any((kind>=0)&~np.isfinite(score)):
        raise ValueError('Nonfinite scored position or invalid score kind.')
    result=np.empty(expected,dtype=compute.DTYPE)
    result['game']=ids[permutation]+index; result['ply']=ply
    result['after']=score; result['ak']=kind
    result['before'][1:]=score[:-1]; result['bk'][1:]=kind[:-1]
    result['before'][starts]=15.; result['bk'][starts]=0
    return result


async def load(session,games,*,timings=None,check=None):
    def add(key,start):
        if timings is not None:timings[key]=timings.get(key,0.)+time.perf_counter()-start
    parts=[]; index=0
    if not games:return np.empty(0,compute.DTYPE)
    start=time.perf_counter(); driver=await driver_connection(session); sql=query(session)
    add('source_queries_seconds',start)
    for block in blocks(games):
        if check:await check()
        expected=sum(g['total_positions'] for g in block); sink=Sink(expected)
        start=time.perf_counter()
        status=await driver.copy_from_query(sql,[g['game_link'] for g in block],expected+1,
            output=sink.write,format='binary',timeout=30)
        add('source_queries_seconds',start)
        if status!=f'COPY {expected}':raise ValueError('Binary source row count disagrees with coverage.')
        start=time.perf_counter()
        parts.append(await file_operation(pack,sink.data,block,index))
        add('source_pack_seconds',start); index+=len(block)
    start=time.perf_counter(); result=np.concatenate(parts)
    add('source_pack_seconds',start)
    return result
