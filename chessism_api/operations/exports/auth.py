"""Machine credentials are separate from browser cookies and admin accounts."""
import hashlib
from datetime import datetime, timezone

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select

from chessism_api.database.engine import AsyncDBSession
from .models import ExportClient

bearer = HTTPBearer(auto_error=False, scheme_name='ExportToken')


def token_hash(token):
    return hashlib.sha256(token.encode()).hexdigest()


async def require_client(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)):
    if not credentials or not credentials.credentials.startswith('chx_') or len(credentials.credentials) > 128:
        raise HTTPException(401, 'An export integration token is required.', headers={'WWW-Authenticate': 'Bearer'})
    async with AsyncDBSession() as session:
        client = await session.scalar(select(ExportClient).where(
            ExportClient.token_hash == token_hash(credentials.credentials),
            ExportClient.revoked_at.is_(None), ExportClient.expires_at > datetime.now(timezone.utc)))
    if client is None:
        raise HTTPException(401, 'Invalid or expired export token.', headers={'WWW-Authenticate': 'Bearer'})
    return client


def authorize_spec(client, spec):
    from .analytical_spec import required_sources
    if spec.source in ('sql', 'move_matrix') and not getattr(client, 'allow_analytics', False):
        raise HTTPException(403, 'This token is not enabled for analytical queries.')
    if not required_sources(spec) <= set(client.allowed_sources):
        raise HTTPException(403, 'This token cannot export the requested dataset.')
    if spec.limit > client.max_rows:
        raise HTTPException(422, f'This token permits at most {client.max_rows} rows per export.')
