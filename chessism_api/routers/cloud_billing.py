"""Authenticated billing proxy: Google credentials never enter the web/API container."""
from fastapi import APIRouter, HTTPException, Response
import httpx

router = APIRouter()


@router.get('')
async def billing(response: Response):
    response.headers['Cache-Control'] = 'no-store'
    try:
        async with httpx.AsyncClient(timeout=90, follow_redirects=False, trust_env=False) as client:
            result = await client.get('http://cloud-billing:8080/billing')
        if result.status_code == 503:
            # The isolated reader returns deliberately sanitized errors only.
            raise HTTPException(503, result.json().get('detail', 'Billing export temporarily unavailable.'))
        result.raise_for_status()
        return result.json()
    except (httpx.HTTPError, ValueError):
        raise HTTPException(503, 'Billing reader unavailable. Check the cloud-billing Compose service.') from None
