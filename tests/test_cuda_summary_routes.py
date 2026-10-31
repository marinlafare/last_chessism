import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
import httpx
from fastapi import FastAPI, HTTPException
from chessism_api.auth import require_superuser
from chessism_api.redis_client import get_redis_pool
from chessism_api.routers import cuda_summaries as routes


class CudaSummaryRoutes(unittest.IsolatedAsyncioTestCase):
    def app(self, allowed):
        app = FastAPI()
        app.include_router(routes.router, prefix='/analysis/cuda-summaries')
        async def account():
            if not allowed: raise HTTPException(status_code=403, detail='Superuser required')
            return SimpleNamespace(id='admin')
        app.dependency_overrides[require_superuser] = account
        app.dependency_overrides[get_redis_pool] = lambda: AsyncMock()
        return app

    async def test_get_and_mutations_require_superuser(self):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app(False)), base_url='http://test') as client:
            for method, path in [('GET',''), ('POST','/run'), ('POST','/00000000-0000-0000-0000-000000000001/retry')]:
                self.assertEqual((await client.request(method, '/analysis/cuda-summaries'+path)).status_code, 403)

    async def test_status_never_enqueues_and_empty_work_is_a_conflict(self):
        session = AsyncMock()
        factory = MagicMock()
        factory.return_value.__aenter__.return_value = session
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app(True)), base_url='http://test') as client:
            with patch.object(routes,'AsyncDBSession',factory), patch.object(routes.runs,'status',AsyncMock(return_value={'manual':True})), patch.object(routes,'schedule',AsyncMock()) as wake:
                self.assertEqual((await client.get('/analysis/cuda-summaries')).json(), {'manual':True})
                wake.assert_not_awaited()
                self.assertEqual(str(session.execute.call_args_list[0].args[0]), 'SET TRANSACTION READ ONLY')
            session.begin = MagicMock()
            session.begin.return_value.__aenter__ = AsyncMock()
            session.begin.return_value.__aexit__ = AsyncMock(return_value=False)
            with patch.object(routes,'AsyncDBSession',factory), patch.object(routes.runs,'request_run',AsyncMock(side_effect=ValueError('No pending work'))), patch.object(routes,'schedule',AsyncMock()) as wake:
                self.assertEqual((await client.post('/analysis/cuda-summaries/run')).status_code,409)
                wake.assert_not_awaited()
