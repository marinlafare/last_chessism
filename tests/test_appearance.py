import os
import unittest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

for key,value in {'AUTH_COOKIE_NAME':'test_session','AUTH_GATE_COOKIE_NAME':'test_gate',
    'AUTH_SESSION_TTL_DAYS':'1','AUTH_GATE_TTL_MINUTES':'1','AUTH_COOKIE_SECURE':'false',
    'SUPERADMIN_GATE_CODE':'test-only','DATABASE_URL':'postgresql+asyncpg://test:test@localhost/test'}.items():
    os.environ.setdefault(key,value)

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from chessism_api.routers import appearance as router
from chessism_api.operations.appearance import validate_overrides,preset_name,AppearanceConflict
from chessism_api.auth import get_current_account
from chessism_api.database.models import AccountRole


def override(**changes):
    return {'scope':'scored_positions','target':'unique.slice','property':'fill','value':'#ABCDEF80',**changes}


class AppearanceTests(unittest.TestCase):
    def test_registry_rejects_code_unknown_targets_invalid_values_and_duplicates(self):
        self.assertEqual(validate_overrides([override()])[0]['color_value'],'#abcdef80')
        for row in (override(target='body'),override(scope='another_page'),override(property='position'),
                    override(value='url(https://example.invalid)'),override(value='#fff'),
                    override(value={'red':1}),override(extra=True)):
            with self.subTest(row=row), self.assertRaises(ValueError): validate_overrides([row])
        for value in (True,float('nan'),float('inf'),-1,100):
            with self.assertRaises(ValueError):validate_overrides([override(target='unique.outline',property='strokeWidth',value=value)])
        with self.assertRaises(ValueError):validate_overrides([override(),override()])
        with self.assertRaises(ValueError):validate_overrides([override()]*501)

    def test_default_names_are_immutable(self):
        for name in ('Default',' default ','DEFAULT','',None,'a'*61,'hello\nworld'):
            with self.assertRaises(ValueError):preset_name(name)
        self.assertEqual(preset_name(' My theme '),'My theme')

    def client(self,role='admin'):
        app=FastAPI();app.include_router(router.router,prefix='/appearance')
        async def account():
            if role is None:raise HTTPException(401,'Authentication required')
            return SimpleNamespace(id='test',role=AccountRole(role))
        app.dependency_overrides[get_current_account]=account
        return TestClient(app)

    def test_write_requires_admin_origin_custom_header_and_strict_shape(self):
        payload={'revision':0,'name':'Test','overrides':[override()]}
        headers={'X-Chessism-Appearance':'1','Origin':'https://chessism.blackcrowlabs.com'}
        with patch.object(router,'change_appearance',AsyncMock(return_value={'revision':1})) as save:
            self.assertEqual(self.client(None).post('/appearance/save',json=payload,headers=headers).status_code,401)
            self.assertEqual(self.client('user').post('/appearance/save',json=payload,headers=headers).status_code,403)
            self.assertEqual(self.client().post('/appearance/save',json=payload).status_code,403)
            self.assertEqual(self.client().post('/appearance/save',json=payload,headers={**headers,'Origin':'https://bad.invalid'}).status_code,403)
            self.assertEqual(self.client().post('/appearance/save',json={**payload,'css':'evil'},headers=headers).status_code,422)
            save.assert_not_awaited()
            self.assertEqual(self.client().post('/appearance/save',json=payload,headers=headers).status_code,200)
            save.assert_awaited_once()
            save.side_effect=AppearanceConflict('stale revision')
            self.assertEqual(self.client().post('/appearance/save',json=payload,headers=headers).status_code,409)
            self.assertEqual(self.client().post('/appearance/rename',json={'revision':1},headers=headers).status_code,422)


if __name__=='__main__':unittest.main()
