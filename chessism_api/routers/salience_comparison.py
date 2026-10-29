"""Explicit superuser boundary; reads never queue or calculate experiments."""
from typing import Literal
from fastapi import APIRouter,Depends,HTTPException,Query
from pydantic import BaseModel,Field
from starlette.concurrency import run_in_threadpool
from chessism_api.auth import require_superuser,get_current_account
from chessism_api.database.models import Account
from chessism_api.operations.salience_comparison import artifacts,readers

router=APIRouter(dependencies=[Depends(require_superuser)])


async def call(fn,*args):
    try:
        return await run_in_threadpool(fn,*args)
    except FileNotFoundError as error:
        raise HTTPException(404,str(error)) from error
    except ValueError as error:
        raise HTTPException(422,str(error)) from error


@router.get('')
async def comparisons():
    return await call(artifacts.listing)


@router.get('/{identifier}/games')
async def games(identifier:str,sort:str='difference',direction:str='desc',mode:str='all',
                offset:int=Query(0,ge=0),limit:int=Query(25,ge=1,le=50)):
    return await call(readers.games,identifier,sort,direction,mode,offset,limit)


@router.get('/{identifier}/games/{game_id}')
async def game(identifier:str,game_id:int):
    return await call(readers.game_detail,identifier,game_id)


class Review(BaseModel):
    judgement:Literal['repetitive','distinctive','uncertain']
    note:str=Field('',max_length=2000)


@router.post('/{identifier}/games/{game_id}/review')
async def review(identifier:str,game_id:int,body:Review,account:Account=Depends(get_current_account)):
    return await call(artifacts.save_review,identifier,game_id,body.judgement,body.note,str(account.id))
