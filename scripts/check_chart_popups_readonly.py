"""Bounded live SELECT diagnostics. No DDL, updates, queue or cloud operations.

Run inside the API container with updated query modules staged beside this
script. Every connection is read-only with a short statement timeout.
"""
import asyncio
import importlib.util
import os
from pathlib import Path
import sys
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.runtime import configured_storage
from chessism_api.database.relational_storage import SESSION_KEY


async def main():
    folder = Path(__file__).parent
    for name in ('player_score_position_page','player_blunder_page','player_accuracy_distribution'):
        path = folder / (name + '.py')
        if path.exists():
            key = 'chessism_api.operations.' + name
            spec = importlib.util.spec_from_file_location(key, path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[key] = module
            spec.loader.exec_module(module)
    from chessism_api.operations.player_score_position_page import read_position_page
    from chessism_api.operations.player_score_distribution import bins
    from chessism_api.operations.player_accuracy_distribution import explore_summary_games
    layout = configured_storage()
    engine = create_async_engine(os.environ['DATABASE_URL'], connect_args={'server_settings': {
        'default_transaction_read_only':'on','statement_timeout':'3000',
        'max_parallel_workers_per_gather':'0','jit':'off',
        'search_path':layout.search_path}})
    AsyncDBSession.configure(bind=engine,info={SESSION_KEY:layout})
    try:
        for player in sys.argv[1:] or ['lafareto']:
            for bin_id in (0,6,12):
                start = time.monotonic()
                async with AsyncDBSession() as s:
                    page = await read_position_page(s,player,('bullet','blitz','rapid'),bins()[bin_id])
                print(f'{player} positions bin={bin_id}: {time.monotonic()-start:.3f}s, '
                      f'{len(page["rows"])} rows, scanned={page["scanned_positions"]}, more={bool(page["next_cursor"])}',flush=True)
            for bin_id in (0,3,6):
                start=time.monotonic()
                first=await explore_summary_games(player,metric='blunders',modes=['bullet','blitz','rapid'],
                    bin_id=bin_id,timezone_name=None,limit=15,cursor=None)
                duration=time.monotonic()-start
                print(f'{player} blunders bin={bin_id}: {duration:.3f}s, {len(first["games"])} games',flush=True)
                if first['pagination']['next_cursor']:
                    second=await explore_summary_games(player,metric='blunders',modes=['bullet','blitz','rapid'],
                        bin_id=bin_id,timezone_name=None,limit=15,cursor=first['pagination']['next_cursor'])
                    rows=first['games']+second['games']
                    identities=[(r['game_id'],r['player_color']) for r in rows]
                    assert len(identities)==len(set(identities))
                    dates=[r['played_at_utc'] or '' for r in rows]
                    assert dates==sorted(dates,reverse=True)
                    assert all(r['blunder_count']==bin_id if bin_id<6 else r['blunder_count']>=6 for r in rows)
        print('PASS: bounded read-only popup queries, blunder bins and two-page date ordering',flush=True)
    finally:
        await engine.dispose()


if __name__=='__main__':
    asyncio.run(main())
