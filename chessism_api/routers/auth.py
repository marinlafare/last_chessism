import uuid

from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from chessism_api.auth import (
    AUTH_COOKIE_NAME,
    SUPERADMIN_GATE_CODE,
    ADMIN_ROLE,
    clear_auth_cookie,
    clear_gate_cookie,
    create_auth_session,
    get_current_account,
    get_db_session,
    hash_secret,
    hash_session_token,
    require_gate,
    set_auth_cookie,
    set_gate_cookie,
    verify_secret,
)
from chessism_api.database.models import Account, AuthSession

router = APIRouter()


class SuperadminGateRequest(BaseModel):
    code: str


class AdminSignupRequest(BaseModel):
    name: str
    email: str
    password: str
    repeat_password: str
    chess_com_nickname: str | None = None


class AdminLoginRequest(BaseModel):
    email: str
    password: str


def account_payload(account: Account) -> dict:
    return {
        "id": account.id,
        "name": account.name,
        "email": account.email,
        "chess_com_nickname": account.chess_com_nickname,
        "role": account.role.value if hasattr(account.role, "value") else str(account.role),
        "is_active": account.is_active,
        "created_at": account.created_at.isoformat() if account.created_at else None,
        "updated_at": account.updated_at.isoformat() if account.updated_at else None,
        "last_login_at": account.last_login_at.isoformat() if account.last_login_at else None,
    }


@router.post("/gate")
async def open_admin_gate(
    data: SuperadminGateRequest,
    response: Response,
) -> dict:
    if data.code != SUPERADMIN_GATE_CODE:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid access code.")

    set_gate_cookie(response)
    return {"ok": True, "redirect_to": "/admins"}


@router.get("/admins")
async def list_admins(
    request: Request,
    session: AsyncSession = Depends(get_db_session)
) -> dict:
    require_gate(request)
    result = await session.execute(
        select(Account)
        .where(Account.role == ADMIN_ROLE, Account.is_active.is_(True))
        .order_by(Account.created_at.asc())
    )
    return {"admins": [account_payload(account) for account in result.scalars().all()]}


@router.post("/admins/signup")
async def signup_admin(
    data: AdminSignupRequest,
    request: Request,
    response: Response,
    session: AsyncSession = Depends(get_db_session)
) -> dict:
    require_gate(request)
    email = data.email.strip().lower()
    name = data.name.strip()
    password = data.password
    chess_com_nickname = data.chess_com_nickname.strip().lower() if data.chess_com_nickname else None
    if not email or not name or not password:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name, email, and password are required.")
    if password != data.repeat_password:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Passwords do not match.")

    existing = await session.execute(select(Account).where(Account.email == email))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="An account with this email already exists.")

    account = Account(
        id=str(uuid.uuid4()),
        name=name,
        email=email,
        password_hash=hash_secret(password),
        chess_com_nickname=chess_com_nickname,
        role=ADMIN_ROLE,
        is_active=True,
    )
    session.add(account)
    await session.commit()
    await session.refresh(account)

    token = await create_auth_session(session, account, request=request)
    clear_gate_cookie(response)
    set_auth_cookie(response, token)
    return {"account": account_payload(account)}


@router.post("/admins/login")
async def login_admin(
    data: AdminLoginRequest,
    request: Request,
    response: Response,
    session: AsyncSession = Depends(get_db_session)
) -> dict:
    require_gate(request)
    email = data.email.strip().lower()
    result = await session.execute(
        select(Account).where(
            Account.email == email,
            Account.role == ADMIN_ROLE,
            Account.is_active.is_(True),
        )
    )
    account = result.scalar_one_or_none()
    if not account or not verify_secret(data.password, account.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials.")

    token = await create_auth_session(session, account, request=request)
    clear_gate_cookie(response)
    set_auth_cookie(response, token)
    return {"account": account_payload(account)}


@router.get("/me")
async def get_me(account: Account = Depends(get_current_account)) -> dict:
    return {"account": account_payload(account)}


@router.post("/logout")
async def logout(
    request: Request,
    response: Response,
    session: AsyncSession = Depends(get_db_session)
) -> dict:
    token = request.cookies.get(AUTH_COOKIE_NAME)
    if token:
        result = await session.execute(
            AuthSession.__table__.update()
            .where(AuthSession.token_hash == hash_session_token(token))
            .where(AuthSession.revoked_at.is_(None))
            .values(revoked_at=func.now())
        )
        if result.rowcount:
            await session.commit()

    clear_auth_cookie(response)
    clear_gate_cookie(response)
    return {"ok": True}
