import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

for key, value in {'AUTH_COOKIE_NAME': 'test_session', 'AUTH_GATE_COOKIE_NAME': 'test_gate',
    'AUTH_SESSION_TTL_DAYS': '1', 'AUTH_GATE_TTL_MINUTES': '1', 'AUTH_COOKIE_SECURE': 'false',
    'SUPERADMIN_GATE_CODE': 'test-only', 'DATABASE_URL': 'postgresql+asyncpg://test:test@localhost/test'}.items():
    os.environ.setdefault(key, value)

from fastapi import Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient
from chessism_api.auth import get_current_account, require_superuser
from chessism_api.database.models import AccountRole
from chessism_api.routers import backups


class BackupVolumeApiTests(unittest.TestCase):
    def client(self, role='admin'):
        app = FastAPI()
        app.include_router(backups.router, prefix='/backups', dependencies=[Depends(require_superuser)])
        async def account():
            if role is None:
                raise HTTPException(401, 'Authentication required')
            return SimpleNamespace(id='test', role=AccountRole(role))
        app.dependency_overrides[get_current_account] = account
        return TestClient(app)

    def test_read_only_endpoint_is_superuser_only(self):
        with patch.object(backups.database_backups, 'storage_snapshot', return_value={
                'available': True, 'total_bytes': 1000}) as storage, \
             patch.object(backups, 'volume_usage', return_value={'refreshing': True}) as measure:
            self.assertEqual(self.client(None).get('/backups/database/volume').status_code, 401)
            self.assertEqual(self.client('user').get('/backups/database/volume').status_code, 403)
            measure.assert_not_called()
            response = self.client().get('/backups/database/volume')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()['total_bytes'], 1000)
            self.assertTrue(response.json()['refreshing'])
            storage.assert_called_once_with(include_app_usage=False)
            self.assertEqual(self.client().post('/backups/database/volume').status_code, 405)

    def test_missing_volume_does_not_scan_the_system_disk(self):
        with patch.object(backups.database_backups, 'storage_snapshot', return_value={
                'available': False, 'error': 'Backup disk not mounted'}), \
             patch.object(backups, 'volume_usage') as measure:
            response = self.client().get('/backups/database/volume')
            self.assertFalse(response.json()['available'])
            self.assertFalse(response.json()['refreshing'])
            measure.assert_not_called()


if __name__ == '__main__':
    unittest.main()
