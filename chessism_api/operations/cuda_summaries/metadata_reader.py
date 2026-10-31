"""Read-only saved metadata. No rebuild, FEN scan or GPU work on page load."""
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from chessism_api.database.stockfish_summary_schema import summary_table
from .metadata_store import STATE,enabled
from .ending_compute import blank,VERSION


async def saved_endings(session,player):
    if not await enabled(session):return None
    from ..player_unexpected_endings import ending_payload
    row=(await session.execute(text(f'''SELECT c.*,s.revision AS current_revision FROM {STATE} c
        JOIN {summary_table(storage_for(session),'state')} s USING(player_name)
        WHERE c.player_name=:p'''),{'p':player})).mappings().first()
    if not row:
        return {**ending_payload(player,blank()['by_mode']),'summary_pending':True,'stale':True,'backend':'pending_cuda'}
    return {**ending_payload(player,row['payload']['by_mode']),'summary_pending':False,
        'stale':row['score_revision']!=row['current_revision'] or row['ending_version']!=VERSION,
        'generated_at':row['generated_at'].isoformat(),'backend':'saved_cuda_metadata','max_age_seconds':0}
