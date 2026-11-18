"""Canonical rating projection SQL; the application owns the transaction."""
import json

from sqlalchemy import text


def require_transaction(session):
    if not session.in_transaction():
        raise ValueError('Scored-rating projection requires a caller-owned transaction')


async def lock_summary(session):
    require_transaction(session)
    result = await session.execute(text(
        'SELECT id FROM public.scored_rating_summary WHERE id=1 FOR UPDATE'))
    result.scalar_one()


async def source_games(session):
    """One statement snapshot, streamed without materializing wide rows twice."""
    require_transaction(session)
    result = await session.stream(text('''
        SELECT g.link,round((g.white_elo::numeric+g.black_elo)/2)::int AS rating,
            c.total_positions,c.equal_positions,c.small_positions,c.clear_positions,
            c.decisive_positions,c.mate_positions
        FROM public.game g JOIN public.game_analysis_coverage c ON c.link=g.link
        WHERE g.is_standard AND g.n_moves>0 AND c.is_fully_analyzed
            AND c.total_positions>0 AND (g.white_elo::numeric+g.black_elo)>0
        ORDER BY rating,g.link
    ''').execution_options(yield_per=1000))
    # The operation's guarded session cleanup also closes this cursor on failure.
    async for row in result.mappings():
        yield row


async def publish(session, payload):
    require_transaction(session)
    result = await session.execute(text('''
        UPDATE public.scored_rating_summary SET rating_basis=:rating_basis,
            source_full_games=:games,source_distinct_ratings=:distinct_ratings,
            groups_payload=CAST(:groups AS jsonb),ratings_payload=CAST(:ratings AS jsonb),
            refreshed_at=clock_timestamp()
        WHERE id=1 RETURNING id
    '''), dict(rating_basis=payload['rating_basis'], games=len(payload['ratings']),
              distinct_ratings=len({point['rating'] for point in payload['ratings']}),
              groups=json.dumps(payload['groups']), ratings=json.dumps(payload['ratings'])))
    result.scalar_one()


async def read_summary(session, *, include_ratings):
    require_transaction(session)
    # The overview must not select/decode the potentially large per-game chart.
    ratings = ',ratings_payload' if include_ratings else ''
    result = await session.execute(text(f'''
        SELECT rating_basis,groups_payload{ratings}
        FROM public.scored_rating_summary WHERE id=1
    '''))
    return result.mappings().one()
