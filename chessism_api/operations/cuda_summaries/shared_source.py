"""One bounded binary position read for score histograms and canonical blunders."""
import numpy as np
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for, driver_connection
from ..canonical_blunders import source, binary_source as binary, compute
from ..canonical_blunders.schema import table
from ..canonical_blunders.eligibility import moving_owners
from ..matrix_constructor.artifact_files import file_operation
from .score_runs import OWNER, pending_items

VERSION = 'shared_scores_v1'
RAW = np.dtype([('game', '<i4'), ('ply', '<i4'), ('score', '<f8'), ('source', 'i1')])


def query(layout):
    white = 'a.is_white' if layout.compact else "a.move_color='white'"
    return f'''SELECT a.game_link::bigint,
        ((a.n_move-1)*2+CASE WHEN {white} THEN 1 ELSE 2 END)::integer AS ply,
        COALESCE(f.score::float8,'NaN'::float8),
        (CASE WHEN f.score IS NULL THEN 3 WHEN f.analysis_source='tablebase' THEN 2
            WHEN f.analysis_source LIKE 'stockfish%' THEN 1 ELSE 0 END)::smallint
        FROM {layout.table('game_fen_association')} a LEFT JOIN {layout.table('fen')} f ON f.fen_id=a.fen_id
        WHERE a.game_link=ANY($1::bigint[]) ORDER BY a.game_link,ply LIMIT $2::bigint'''


def unpack(data, games, offset):
    counts = np.array([g['total_positions'] for g in games], dtype=np.int64)
    wire = binary.decode(data, int(counts.sum()))
    links = np.array([g['game_link'] for g in games], dtype=np.int64)
    ids = np.searchsorted(links, wire['link'])
    if np.any(ids >= len(links)) or np.any(links[ids] != wire['link']):
        raise ValueError('Unexpected shared-input game.')
    if not np.array_equal(np.bincount(ids, minlength=len(games)), counts):
        raise ValueError('Shared-input coverage count changed.')
    if np.any(np.diff(ids) < 0) or np.any((np.diff(ids) == 0) & (np.diff(wire['ply']) <= 0)):
        raise ValueError('Duplicate or unordered shared-input occurrence.')
    if np.any(~np.isin(wire['kind'], (0, 1, 2, 3))) or np.any((wire['kind'] != 3) & ~np.isfinite(wire['score'])):
        raise ValueError('Invalid evaluation in shared input.')
    result = np.empty(len(wire), RAW)
    result['game'] = ids + offset; result['ply'] = wire['ply']
    result['score'] = wire['score']; result['source'] = wire['kind']
    return result


def blunder_inputs(raw, games, items):
    by_link = {g['game_link']: (i, g) for i, g in enumerate(games)}
    accepted, unsupported, indexes = [], [], []
    for item in items:
        if not item['blunders']:
            continue
        found = by_link.get(item['game_link']); g = moving_owners(found[1]) if found else None
        eligible = bool(g and g['analyzed'] and g['total_positions'] > 0 and g['mode'] in ('bullet', 'blitz', 'rapid')
            and (g['white_owner'] or g['black_owner'])
            and all(isinstance(g[k], int) and 1 <= g[k] <= 4000 for k in ('white_elo', 'black_elo')))
        if eligible:
            indexes.append(found[0]); accepted.append({**g, **item, 'link': g['game_link']})
        else:
            unsupported.append({**(g or {}), **item, 'status': 'unsupported_or_ineligible',
                'white_count': None, 'black_count': None, 'white_assessed': 0, 'black_assessed': 0,
                'flags': {'blunders': [], 'unassessed': []}, 'source_sha256': 'ineligible'})
    remap = np.full(len(games), -1, dtype=np.int32)
    remap[indexes] = np.arange(len(indexes))
    selected = raw[remap[raw['game']] >= 0]
    counts = np.array([g['total_positions'] for g in accepted], dtype=np.int64)
    rows = np.empty(len(selected), compute.DTYPE)
    if not len(selected):
        return accepted, unsupported, rows
    starts = np.r_[0, np.cumsum(counts)[:-1]]
    if not np.array_equal(selected['ply'], np.arange(len(selected)) - np.repeat(starts, counts) + 1):
        raise ValueError('Blunder input requires contiguous predecessor positions.')
    rows['game'] = remap[selected['game']]; rows['ply'] = selected['ply']
    rows['after'] = selected['score']
    rows['ak'] = np.where(selected['source'] == 2, 2,
        np.where(selected['source'] == 1, np.where(np.abs(selected['score']) >= 9000, 1, 0), -1))
    rows['before'][1:] = rows['after'][:-1]; rows['bk'][1:] = rows['ak'][:-1]
    rows['before'][starts] = 15.; rows['bk'][starts] = 0
    return accepted, unsupported, rows


async def metadata(session, layout, links):
    return [dict(r) for r in (await session.execute(text(f'''SELECT g.link AS game_link,m.name AS mode,
        g.played_at,g.white_elo,g.black_elo,g.white_result,g.black_result,g.white_str_result,g.black_str_result,
        COALESCE(c.total_positions,(SELECT count(*) FROM {layout.table('game_fen_association')} a WHERE a.game_link=g.link))::int AS total_positions,
        COALESCE(c.is_fully_analyzed,false) AS analyzed,
        CASE WHEN w.joined>0 AND w.deleted_at IS NULL AND EXISTS(SELECT 1 FROM {layout.table('game_player')} gp
            WHERE gp.game_link=g.link AND gp.player_id=w.id AND gp.is_white) THEN w.player_name END AS white_owner,
        CASE WHEN b.joined>0 AND b.deleted_at IS NULL AND EXISTS(SELECT 1 FROM {layout.table('game_player')} gp
            WHERE gp.game_link=g.link AND gp.player_id=b.id AND NOT gp.is_white) THEN b.player_name END AS black_owner,
        last_move.is_white AS last_white
        FROM {layout.table('game')} g JOIN {layout.table('game_mode')} m ON m.id=g.mode_id
        JOIN {layout.table('player')} w ON w.id=g.white_player_id JOIN {layout.table('player')} b ON b.id=g.black_player_id
        LEFT JOIN {layout.table('game_analysis_coverage')} c ON c.link=g.link
        LEFT JOIN LATERAL (SELECT CASE WHEN lower(btrim(m.white_move)) NOT IN ('','--','none','null') THEN
            CASE WHEN lower(btrim(m.black_move)) NOT IN ('','--','none','null') THEN false
                 WHEN btrim(m.black_move)='--' THEN true END END AS is_white
            FROM {layout.table('moves')} m WHERE m.link=g.link AND m.n_move=g.n_moves AND g.n_moves>0
            AND (lower(g.white_str_result)='stalemate' OR lower(g.black_str_result)='stalemate') LIMIT 1
        ) last_move ON true WHERE g.link=ANY(CAST(:links AS bigint[])) ORDER BY g.link'''), {'links': links})).mappings()]


async def load(session, run):
    layout = storage_for(session)
    config = await session.scalar(text(f'SELECT config FROM {table(layout,"model")} WHERE id=:model'), {'model': run['model_id']})
    budget = run.get('batch_budget')
    page = run['items']; taken = []; games = []; owners = []; rows = 0
    while page:
        data = await metadata(session, layout, [i['game_link'] for i in page])
        by_link = {g['game_link']: g for g in data}; accepted = []
        for item in page:
            game = by_link.get(item['game_link']); count = game['total_positions'] if game else 0
            if not 0 <= count <= binary.GAME_ROWS:
                raise ValueError('Game exceeds bounded shared-input capacity.')
            fits = budget.fits(rows+count, len(taken)+1) if budget else rows+count <= source.MAX_ROWS
            if not fits:
                if not taken:
                    raise RuntimeError('One game exceeds the available CUDA input memory budget.')
                break
            taken.append(item); accepted.append(item['game_link']); rows += count
            if game:
                games.append(game)
        owners.extend(dict(r) for r in (await session.execute(text(f'''SELECT game_link,player_name,day::text AS day,is_white
            FROM {OWNER} WHERE run_id=:id AND game_link=ANY(CAST(:links AS bigint[])) ORDER BY game_link,player_name'''),
            {'id': run['id'], 'links': accepted})).mappings())
        if len(accepted) != len(page) or not budget:
            break
        page = await pending_items(session, run, after=taken[-1]['game_link'])
    # Include zero-position games in metadata, but not binary.blocks' positive-count contract.
    score_links = {o['game_link'] for o in owners}
    blunder_links = {i['game_link'] for i in taken if i['blunders']}
    positive = [g for g in games if g['total_positions'] and (g['game_link'] in score_links or (
        g['game_link'] in blunder_links and g['analyzed'] and (g['white_owner'] or (g['black_owner'] and g['total_positions'] > 1))
        and g['mode'] in ('bullet','blitz','rapid')
        and all(isinstance(g[k], int) and 1 <= g[k] <= 4000 for k in ('white_elo','black_elo'))))]
    indexes = {g['game_link']: i for i, g in enumerate(games)}
    # One allocation instead of retaining every COPY chunk plus a concatenated copy.
    raw = np.empty(sum(g['total_positions'] for g in positive), RAW); offset = 0
    driver = await driver_connection(session)
    for block in binary.blocks(positive, max_games=8192, max_rows=1048576):
        expected = sum(g['total_positions'] for g in block); sink = binary.Sink(expected)
        status = await driver.copy_from_query(query(layout), [g['game_link'] for g in block], expected + 1,
            output=sink.write, format='binary', timeout=30)
        if status != f'COPY {expected}':
            raise ValueError('Shared-input row count disagrees with coverage.')
        part = await file_operation(unpack, sink.data, block, 0)
        part['game'] = np.array([indexes[g['game_link']] for g in block])[part['game']]
        raw[offset:offset+len(part)] = part; offset += len(part)
        del part, sink
    eligible, unsupported, transitions = await file_operation(blunder_inputs, raw, games, taken)
    return config, taken, games, owners, raw, eligible, unsupported, transitions
