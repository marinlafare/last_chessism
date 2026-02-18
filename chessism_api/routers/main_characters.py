from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from chessism_api.operations.main_characters import (
    get_main_character_time_control_counts_payload,
    get_top_main_characters_by_time_control_payload
)

router = APIRouter()


@router.get("/time_controls")
async def api_get_main_character_time_controls() -> JSONResponse:
    """
    Returns normalized game counts for bullet, blitz and rapid considering
    games with at least one main character.
    """
    result = await get_main_character_time_control_counts_payload()
    return JSONResponse(content=result)


@router.get("/top")
async def api_get_top_main_characters(
    time_control: str = Query(..., pattern="^(bullet|blitz|rapid)$"),
    limit: int = Query(5, ge=1, le=200)
) -> JSONResponse:
    """
    Returns top main characters for one time control.
    """
    result = await get_top_main_characters_by_time_control_payload(
        time_control=time_control,
        limit=limit
    )
    return JSONResponse(content=result)
