"""Bounded source reads of committed game paths; PostgreSQL remains authoritative."""
from collections import defaultdict
import hashlib
import json
import chess
from sqlalchemy import text
from chessism_api.evaluations import score_kind

START = chess.Board().fen().rsplit(" ", 2)[0]
ROOT = "chess:initial"


def position_key(fen_id, fen):
    return ROOT if fen == START else f"chess:{fen_id}"


def evaluation(row):
    score = row.get("score")
    kind = score_kind(score, row.get("analysis_source"))
    # Matches the canonical, signed 10000-distance Stockfish mate encoding.
    distance = int(10000 - abs(score)) if kind == "mate" and abs(score) <= 10000 else None
    wdl, dtz = row.get('tablebase_wdl'), row.get('tablebase_dtz')
    if kind == 'tablebase' and (wdl is not None or dtz is not None):
        # PostgreSQL stores raw Syzygy values from the side-to-move perspective.
        # Graph evaluation fields consistently use White's perspective.
        sign = 1 if row['fen'].split()[1] == 'w' else -1
        wdl = wdl * sign if wdl is not None else None
        dtz = dtz * sign if dtz is not None else None
    return {"score_kind": kind, "score_cp": score if kind == "cp" else None,
            "mate_in": distance, "mate_for_white": (score > 0) if kind == "mate" else None,
            "tablebase_wdl": wdl if kind == "tablebase" else None,
            "tablebase_dtz": dtz if kind == "tablebase" else None}


def position(row):
    return {"key": position_key(row["fen_id"], row["fen"]), "fen_id": row["fen_id"],
            "fen": row["fen"], "variant": "chess", **evaluation(row)}


def prepare_game(row, associations, moves):
    """Validate against saved associations while deriving UCI once, not by query."""
    board = chess.Board()
    steps = []
    source = ROOT
    for move in moves:
        for white, field in ((True, "white_move"), (False, "black_move")):
            san = move[field]
            if not san or san == "--":
                continue
            ply = (int(move["n_move"]) - 1) * 2 + (1 if white else 2)
            if ply != len(steps) + 1 or board.turn != white:
                raise ValueError(f"Nonconsecutive moves in game {row['id']}")
            parsed = board.parse_san(san)
            uci = board.uci(parsed)
            san = board.san_and_push(parsed)
            saved = associations.get(ply)
            if saved is None or saved["fen"] != board.fen().rsplit(" ", 2)[0]:
                raise ValueError(f"Saved FEN differs from move {ply} in game {row['id']}")
            target = position_key(saved["fen_id"], saved["fen"])
            steps.append({"ply": ply, "key": target, "from_key": source, "uci": uci, "san": san,
                          "move_key": hashlib.sha256(f"{source}|{uci}|{target}".encode()).hexdigest()})
            source = target
    if not steps or len(steps) != len(associations):
        raise ValueError(f"Incomplete FEN path in game {row['id']}")
    result = {**row, "steps": steps}
    result["source_hash"] = hashlib.sha256(json.dumps(result, sort_keys=True, default=str).encode()).hexdigest()
    return result


async def read_games(session, ids, layout):
    t = layout.table
    rows = (await session.execute(text(f'''SELECT g.link AS id,g.white_player_id AS white_id,
        g.black_player_id AS black_id,g.mode_id,m.name AS mode,g.played_at::text AS played_at,
        g.white_result,g.black_result,g.white_str_result,g.black_str_result,g.fens_done,g.n_moves,
        w.player_name AS white_name,b.player_name AS black_name
        FROM {t('game')} g JOIN {t('player')} w ON w.id=g.white_player_id
        JOIN {t('player')} b ON b.id=g.black_player_id LEFT JOIN {t('game_mode')} m ON m.id=g.mode_id
        WHERE g.link=ANY(CAST(:ids AS bigint[])) ORDER BY g.link'''), {"ids": ids})).mappings().all()
    ready = [dict(r) for r in rows if r['fens_done'] and r['n_moves'] > 0]
    selected = [r['id'] for r in ready]
    associations = (await session.execute(text(f'''SELECT a.game_link,a.n_move,a.is_white,
        f.fen_id,f.fen,f.score,f.analysis_source,f.tablebase_wdl,f.tablebase_dtz
        FROM {t('game_fen_association')} a JOIN {t('fen')} f USING(fen_id)
        WHERE a.game_link=ANY(CAST(:ids AS bigint[])) ORDER BY a.game_link,a.n_move,a.is_white DESC'''),
        {"ids": selected})).mappings().all()
    moves = (await session.execute(text(f'''SELECT link,n_move,white_move,black_move FROM {t('moves')}
        WHERE link=ANY(CAST(:ids AS bigint[])) ORDER BY link,n_move'''), {"ids": selected})).mappings().all()
    per_game, game_moves, positions = defaultdict(dict), defaultdict(list), {}
    for a in associations:
        ply = (a['n_move'] - 1) * 2 + (1 if a['is_white'] else 2)
        if ply in per_game[a['game_link']]:
            raise ValueError("Duplicate game/ply association")
        per_game[a['game_link']][ply] = dict(a)
        key = position_key(a['fen_id'], a['fen'])
        if key not in positions:
            positions[key] = position(a)
    for move in moves:
        game_moves[move['link']].append(dict(move))
    games = [prepare_game(r, per_game[r['id']], game_moves[r['id']]) for r in ready]
    deleted = sorted(set(ids) - {r['id'] for r in rows if r['n_moves'] > 0})
    return games, list(positions.values()), deleted


async def read_scores(session, ids, layout):
    return [position(r) for r in (await session.execute(text(f'''SELECT fen_id,fen,score,analysis_source,
        tablebase_wdl,tablebase_dtz FROM {layout.table('fen')} WHERE fen_id=ANY(CAST(:ids AS bigint[]))'''),
        {"ids": ids})).mappings()]
