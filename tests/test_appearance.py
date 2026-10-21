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
from chessism_api.operations.appearance import validate_overrides,preset_name,AppearanceConflict,REGISTRY,MAX_OVERRIDES
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
        with self.assertRaises(ValueError):validate_overrides([override()]*(MAX_OVERRIDES+1))

    def test_all_page_scopes_and_original_presets_are_supported(self):
        self.assertEqual(REGISTRY['version'],1)
        for scope,spec in REGISTRY['scopes'].items():
            rows=[override(scope=scope,target='page',property='background',value='#112233')]
            self.assertEqual(validate_overrides(rows)[0]['scope'],scope)
            for target in spec['targets']:
                self.assertLessEqual(len(target),64)
        self.assertEqual(validate_overrides([override()])[0]['color_value'],'#abcdef80')
        with self.assertRaises(ValueError):validate_overrides([override(scope='positions')])
        with self.assertRaises(ValueError):validate_overrides([override(scope='made-up')])

    def test_default_names_are_immutable(self):
        for name in ('Default',' default ','DEFAULT','',None,'a'*61,'hello\nworld'):
            with self.assertRaises(ValueError):preset_name(name)
        self.assertEqual(preset_name(' My theme '),'My theme')

    def test_home_margin_is_bounded_numeric_and_page_scoped(self):
        row = override(scope='home', target='overview.layout', property='homeTopMargin', value=3.5)
        self.assertEqual(validate_overrides([row])[0]['number_value'], 3.5)
        self.assertEqual(REGISTRY['properties']['homeTopMargin']['unit'], 'rem')
        for value in (-1, 13, True, '3rem', float('inf')):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_overrides([{**row, 'value': value}])
        with self.assertRaises(ValueError):
            validate_overrides([{**row, 'scope': 'scored_positions'}])

    def test_chart_palette_validation(self):
        rows=[override(scope='home',target='chart.accuracy',property='chartColor1',value='#11223380')]
        self.assertEqual(validate_overrides(rows)[0]['color_value'],'#11223380')
        with self.assertRaises(ValueError):validate_overrides([{**rows[0],'property':'chartColor99'}])
        with self.assertRaises(ValueError):validate_overrides([{**rows[0],'value':'url(https://invalid.test)'}])

    def test_independent_home_buttons_support_legacy_seed_and_scoped_writes(self):
        targets = [name for name, spec in REGISTRY['scopes']['home']['targets'].items()
                   if spec.get('initialStyleFrom') == 'button']
        self.assertEqual(len(targets), 20)
        rows = [override(scope='home', target=target, property='background', value='#123456') for target in targets]
        self.assertEqual(len(validate_overrides(rows)), len(targets))
        with self.assertRaises(ValueError): validate_overrides([{**rows[0], 'scope':'games'}])

    def test_database_scope_and_medal_pressed_color(self):
        self.assertEqual(REGISTRY['scopes']['database']['routes'], ['/database'])
        medal = REGISTRY['scopes']['home']['targets']['characters.medal']
        self.assertTrue(medal['pressedUsesHoverColor'])
        self.assertTrue(medal['pressedUsesHoverBackground'])
        row = override(scope='home', target='characters.medal', property='hoverColor', value='#123456')
        self.assertEqual(validate_overrides([row])[0]['color_value'], '#123456')
        self.assertEqual(validate_overrides([{**row, 'property':'hoverBackground'}])[0]['color_value'], '#123456')

    def test_player_list_ratings_are_independent_of_profile_values(self):
        target = REGISTRY['scopes']['home']['targets']['characters.rating']
        self.assertEqual(target['initialStyleFrom'], 'value')
        row = override(scope='home', target='characters.rating', property='color', value='#123456')
        self.assertEqual(validate_overrides([row])[0]['color_value'], '#123456')
        with self.assertRaises(ValueError): validate_overrides([{**row, 'scope':'games'}])

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
