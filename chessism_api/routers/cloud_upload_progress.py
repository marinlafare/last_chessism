"""Protected by the parent cloud router; upload observation is read-only."""
from fastapi import APIRouter, Response
from chessism_api.operations.cloud_analysis.upload_progress import read_progress

router = APIRouter()


@router.get('/{job_id}/upload-progress')
async def upload_progress(job_id: str, response: Response):
    response.headers['Cache-Control'] = 'no-store'
    return await read_progress(job_id)
