from fastapi import APIRouter, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from chessism_api.operations.analysis_times import get_analysis_time_summary

router = APIRouter()


@router.get("/summary")
async def api_get_analysis_time_summary(limit: int = Query(10, ge=1, le=10)) -> JSONResponse:
    summary = await get_analysis_time_summary(limit=limit)
    return JSONResponse(content=jsonable_encoder(summary))
