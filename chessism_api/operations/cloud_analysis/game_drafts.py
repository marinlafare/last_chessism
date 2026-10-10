"""Durable game selections and SQL-only FEN union counts. No Google calls."""
from datetime import date, datetime, timezone
from fastapi import HTTPException
from sqlalchemy import select, text
from chessism_api.database.models import CloudGameDraft, CloudGameDraftEntry, CloudGameDraftGame
from .schemas import MAX_BATCH_TOTAL_FENS, MAX_BATCH_FENS_PER_VM

MAX_DRAFT_ENTRIES = 100
DRAFT_LOCK = 731946224


async def locked_draft(session, ident, revision=None, *, editable=True):
    draft = await session.get(CloudGameDraft, ident, with_for_update=True)
    if draft is None:
        raise HTTPException(404, 'Combined selection draft not found')
    if editable and draft.submitted_job_id:
        raise HTTPException(409, 'This draft has already been submitted; start a new draft')
    if revision is not None and draft.revision != revision:
        raise HTTPException(409, 'Draft changed in another tab; reload the saved draft')
    return draft


def changed(draft):
    draft.revision += 1
    draft.updated_at = datetime.now(timezone.utc)
    draft.preview_revision = None
    draft.preview_fens = draft.preview_games = None
    draft.preview_missing_fens = draft.preview_claimed_fens = None
    draft.previewed_at = None


def preview_entry(plan):
    """Trust only the server's saved game preview, never client-supplied game IDs."""
    links = list(dict.fromkeys(plan.get('game_links', [])))
    if not links or any(type(link) is not int or link < 1 for link in links):
        raise HTTPException(409, 'Preview contains no valid games; preview again')
    mode, order = plan.get('selection_mode'), plan.get('selection_order')
    player = str(plan.get('player_name', '')).strip().lower()
    count = plan.get('fens_to_analyze')
    requested = plan.get('requested_games')
    if (not player or len(player) > 200 or mode not in {'latest', 'oldest', 'range', 'fair_range'}
            or order not in {'latest', 'oldest', 'fair_range'}
            or type(count) is not int or count < 0
            or (requested is not None and (type(requested) is not int or requested < 1))):
        raise HTTPException(409, 'Invalid game preview; preview again')
    try:
        values = {key: date.fromisoformat(plan[key]) if plan.get(key) else None
                  for key in ('date_from', 'date_to')}
    except (TypeError, ValueError) as exc:
        raise HTTPException(409, 'Invalid game preview dates; preview again') from exc
    return {**values, 'player_name': player, 'selection_mode': mode, 'selection_order': order,
            'requested_games': requested, 'selected_games': len(links), 'preview_fens': count}, links


async def count_union(session, draft_id):
    # Deduplicate games first, then FEN keys across all games/players. Never load
    # millions of FEN strings into the API process just to compute a preview.
    row = (await session.execute(text('''
        WITH games AS MATERIALIZED (
            SELECT DISTINCT dg.game_link
            FROM cloud_game_draft_game dg
            JOIN cloud_game_draft_entry e ON e.id = dg.entry_id
            WHERE e.draft_id = :draft_id
        ), positions AS (
            SELECT DISTINCT gfa.fen_fen
            FROM games JOIN game_fen_association gfa ON gfa.game_link = games.game_link
        )
        SELECT (SELECT count(*) FROM games) AS games,
               count(*) FILTER (WHERE f.score IS NULL) AS missing,
               count(*) FILTER (WHERE f.score IS NULL AND c.fen IS NULL) AS eligible,
               count(*) FILTER (WHERE f.score IS NULL AND c.fen IS NOT NULL) AS claimed
        FROM positions p JOIN fen f ON f.fen = p.fen_fen
        LEFT JOIN cloud_fen_claim c ON c.fen = f.fen
    '''), {'draft_id': draft_id})).mappings().one()
    return {key: int(value) for key, value in row.items()}


async def game_links(session, draft_id):
    # Preserve entry order, and the original game order within an entry. A game
    # appearing in multiple selections is carried into the normal job only once.
    rows = await session.scalars(select(CloudGameDraftGame.game_link)
        .join(CloudGameDraftEntry, CloudGameDraftEntry.id == CloudGameDraftGame.entry_id)
        .where(CloudGameDraftEntry.draft_id == draft_id)
        .order_by(CloudGameDraftEntry.ordinal, CloudGameDraftGame.ordinal))
    return list(dict.fromkeys(rows))


async def view(session, draft):
    entries = (await session.scalars(select(CloudGameDraftEntry)
        .where(CloudGameDraftEntry.draft_id == draft.id).order_by(CloudGameDraftEntry.ordinal))).all()
    preview = None
    if draft.preview_revision == draft.revision and draft.preview_fens is not None:
        preview = {'revision': draft.revision, 'unique_games': draft.preview_games,
                   'unique_missing_fens': draft.preview_missing_fens,
                   'claimed_fens': draft.preview_claimed_fens, 'fens_to_analyze': draft.preview_fens,
                   # The VM count is chosen at submission, not part of the saved
                   # game/FEN selection. Clients calculate cycles from this cap.
                   'max_fens_per_vm': MAX_BATCH_FENS_PER_VM,
                   'within_limit': 0 < draft.preview_fens <= MAX_BATCH_TOTAL_FENS,
                   'previewed_at': draft.previewed_at}
    return {'id': draft.id, 'revision': draft.revision, 'submitted_job_id': draft.submitted_job_id,
            'updated_at': draft.updated_at, 'preview': preview,
            'max_fens': MAX_BATCH_TOTAL_FENS, 'max_entries': MAX_DRAFT_ENTRIES,
            'entries': [{column.name: getattr(entry, column.name) for column in entry.__table__.columns
                         if column.name != 'draft_id'} for entry in entries]}
