"""Session-authorized appearance API, with CSRF and strict property validation."""
import os
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field
from chessism_api.auth import get_current_account, require_admin
from chessism_api.operations.appearance import read_appearance, change_appearance, AppearanceConflict

router = APIRouter()


class PresetRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    revision: int = Field(ge=0)
    preset_id: str | None = Field(None, max_length=36)
    name: str | None = Field(None, max_length=60)
    overrides: list[dict] = Field(default_factory=list, max_length=500)


def require_style_write(request: Request):
    # Custom header cannot be submitted by a cross-origin HTML form. No CORS
    # permission is granted for this endpoint; also enforce browser provenance.
    allowed = {os.getenv('APP_PUBLIC_ORIGIN', 'https://chessism.blackcrowlabs.com').rstrip('/'),
               'http://localhost:6789', 'http://127.0.0.1:6789', str(request.base_url).rstrip('/')}
    if request.headers.get('x-chessism-appearance') != '1' or request.headers.get('sec-fetch-site') == 'cross-site':
        raise HTTPException(403, 'Same-origin appearance request required.')
    if request.headers.get('origin') and request.headers['origin'] not in allowed:
        raise HTTPException(403, 'Untrusted appearance origin.')


@router.get('')
async def read(account=Depends(get_current_account)):
    try: return await read_appearance()
    except ValueError as error: raise HTTPException(409, str(error)) from error


@router.post('/{action}', dependencies=[Depends(require_style_write)])
async def write(action: str, payload: PresetRequest, account=Depends(require_admin)):
    if action not in ('save', 'activate', 'rename'): raise HTTPException(404, 'Unknown appearance action.')
    if action == 'rename' and not payload.preset_id: raise HTTPException(422, 'Default cannot be renamed.')
    try: return await change_appearance(action, payload.model_dump(), account.id)
    except AppearanceConflict as error: raise HTTPException(409, str(error)) from error
    except ValueError as error: raise HTTPException(422, str(error)) from error
