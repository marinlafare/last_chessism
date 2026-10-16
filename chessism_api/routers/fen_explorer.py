from fastapi import APIRouter, Depends, HTTPException, Query
from chessism_api.auth import require_admin
from chessism_api.redis_client import get_redis_pool
from chessism_api.operations.fen_explorer import explore_positions, position_games

router = APIRouter()


@router.get('')
async def explore(scope: str = Query(...,max_length=10), token: str | None = Query(None,max_length=32),
                  page: int = Query(0,ge=0,le=9), account=Depends(require_admin), redis=Depends(get_redis_pool)):
    try: return await explore_positions(redis,account.id,scope,token,page)
    except LookupError as error: raise HTTPException(410,str(error)) from error
    except ValueError as error: raise HTTPException(422,str(error)) from error


@router.get('/games')
async def games(fen: str = Query(...,max_length=120), after: int = Query(0,ge=0), account=Depends(require_admin)):
    try: return await position_games(fen,after)
    except ValueError as error: raise HTTPException(422,str(error)) from error
