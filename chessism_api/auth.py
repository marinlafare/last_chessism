import os
import secrets
import uuid
from datetime import datetime, timedelta, timezone

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError, VerificationError
from dotenv import load_dotenv
from fastapi import Depends, HTTPException, Request, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Account, AccountRole, AuthSession

load_dotenv()


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def required_bool_env(name: str) -> bool:
    value = required_env(name).lower()
    if value in ("true", "1", "yes"):
        return True
    if value in ("false", "0", "no"):
        return False
    raise RuntimeError(f"Environment variable {name} must be true or false.")


AUTH_COOKIE_NAME = required_env("AUTH_COOKIE_NAME")
AUTH_GATE_COOKIE_NAME = required_env("AUTH_GATE_COOKIE_NAME")
SESSION_TTL_DAYS = int(required_env("AUTH_SESSION_TTL_DAYS"))
GATE_TTL_MINUTES = int(required_env("AUTH_GATE_TTL_MINUTES"))
AUTH_COOKIE_SECURE = required_bool_env("AUTH_COOKIE_SECURE")
SUPERADMIN_GATE_CODE = required_env("SUPERADMIN_GATE_CODE")
ADMIN_ROLE = AccountRole.admin
USER_ROLE = AccountRole.user
_password_hasher = PasswordHasher()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def hash_secret(secret: str) -> str:
    return _password_hasher.hash(secret)


def verify_secret(secret: str, stored_hash: str) -> bool:
    try:
        return _password_hasher.verify(stored_hash, secret)
    except (VerifyMismatchError, VerificationError, ValueError):
        return False


def hash_session_token(token: str) -> str:
    import hashlib
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


async def get_db_session():
    async with AsyncDBSession() as session:
        yield session


async def create_auth_session(
    session: AsyncSession,
    account: Account,
    *,
    request: Request | None = None
) -> str:
    token = secrets.token_urlsafe(32)
    auth_session = AuthSession(
        id=str(uuid.uuid4()),
        account_id=account.id,
        token_hash=hash_session_token(token),
        expires_at=_utc_now() + timedelta(days=SESSION_TTL_DAYS),
        ip_address=request.client.host if request and request.client else None,
        user_agent=request.headers.get("user-agent") if request else None,
    )
    session.add(auth_session)
    account.last_login_at = _utc_now()
    await session.commit()
    await session.refresh(account)
    return token


def set_gate_cookie(response: Response) -> None:
    token = secrets.token_urlsafe(32)
    response.set_cookie(
        AUTH_GATE_COOKIE_NAME,
        token,
        max_age=GATE_TTL_MINUTES * 60,
        httponly=True,
        samesite="lax",
        secure=AUTH_COOKIE_SECURE,
        path="/",
    )


def clear_gate_cookie(response: Response) -> None:
    response.delete_cookie(AUTH_GATE_COOKIE_NAME, path="/")


def require_gate(request: Request) -> None:
    token = request.cookies.get(AUTH_GATE_COOKIE_NAME)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Gate access required.")


def set_auth_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        AUTH_COOKIE_NAME,
        token,
        max_age=SESSION_TTL_DAYS * 24 * 60 * 60,
        httponly=True,
        samesite="lax",
        secure=AUTH_COOKIE_SECURE,
        path="/",
    )


def clear_auth_cookie(response: Response) -> None:
    response.delete_cookie(AUTH_COOKIE_NAME, path="/")


async def get_current_account(
    request: Request,
    session: AsyncSession = Depends(get_db_session)
) -> Account:
    token = request.cookies.get(AUTH_COOKIE_NAME)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")

    token_hash = hash_session_token(token)
    result = await session.execute(
        select(Account)
        .join(AuthSession, AuthSession.account_id == Account.id)
        .where(
            AuthSession.token_hash == token_hash,
            AuthSession.revoked_at.is_(None),
            AuthSession.expires_at > _utc_now(),
            Account.is_active.is_(True),
        )
    )
    account = result.scalar_one_or_none()
    if not account:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")
    return account


async def require_admin(account: Account = Depends(get_current_account)) -> Account:
    if account.role != ADMIN_ROLE:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required.")
    return account


require_superuser = require_admin
