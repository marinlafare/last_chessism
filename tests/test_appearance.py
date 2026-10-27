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
    def test_catalog_percentage_bins_have_independent_saveable_styles(self):
        for kind in ('openings', 'opponents'):
            for bin_index in range(10):
                target = f'player.{kind}.bin.{bin_index}'
                for prop, value in [('background', '#12345680'), ('hoverBackground', '#abcdef'),
                                    ('color', '#fedcba'), ('hoverColor', '#ffffff'), ('borderWidth', 2),
                                    ('hoverBorderColor', '#654321'), ('radius', 4), ('padding', 2)]:
                    validate_overrides([override(scope='home', target=target, property=prop, value=value)])
                spec = REGISTRY['scopes']['home']['targets'][target]
                self.assertEqual(spec['editorParent'], f'player.{kind}.bins')
                self.assertIn('aria-pressed', spec['hoverState'])

    def test_catalog_sort_buttons_have_independent_saveable_styles(self):
        for kind in ('openings', 'opponents'):
            for sort in ('games', 'wins', 'losses', 'draws'):
                target = f'player.{kind}.sort.{sort}'
                for prop, value in [('background', '#12345680'), ('hoverBackground', '#abcdef'),
                                    ('hoverBorderColor', '#654321'), ('radius', 8)]:
                    validate_overrides([override(scope='home', target=target, property=prop, value=value)])
                self.assertEqual(REGISTRY['scopes']['home']['targets'][target]['editorParent'], f'player.{kind}.sort')

    def test_days_palette_keeps_user_hue_but_not_user_opacity(self):
        for color in ('chartColor1', 'chartColor2', 'chartColor3'):
            row = override(scope='home', target='player.days.palette', property=color, value='#12345620')
            self.assertEqual(validate_overrides([row])[0]['color_value'], '#123456')
        background = override(scope='home', target='player.days.palette', property='background', value='#12345620')
        self.assertEqual(validate_overrides([background])[0]['color_value'], '#12345620')
        with self.assertRaises(ValueError):
            validate_overrides([override(scope='home', target='player.days.bin', property='opacity', value=0.5)])

    def test_all_game_review_targets_and_states_are_saveable_and_page_local(self):
        for scope in ('home', 'games', 'scored_positions'):
            targets = {key: value for key, value in REGISTRY['scopes'][scope]['targets'].items()
                       if key.startswith('gameReview.')}
            self.assertGreater(len(targets), 80)
            for target, definition in targets.items():
                for prop in definition['properties']:
                    spec = REGISTRY['properties'][prop]
                    value = '#12345680' if spec['kind'] == 'color' else spec['min'] if spec['kind'] == 'number' else spec['options'][0]
                    validate_overrides([override(scope=scope, target=target, property=prop, value=value)])
            with self.assertRaises(ValueError):
                validate_overrides([override(scope=scope, target='button', property='reviewSelectedBackground')])

    def test_game_review_flip_and_names_are_saveable_in_each_viewer_scope(self):
        for scope in ('home', 'games', 'scored_positions'):
            for target in ('gameReview.flip', 'gameReview.players', 'gameReview.player.white', 'gameReview.player.black'):
                for prop, value in [('background', '#123456'), ('hoverBackground', '#abcdef'),
                                    ('borderColor', '#112233'), ('hoverBorderColor', '#445566'), ('radius', 8)]:
                    validate_overrides([override(scope=scope, target=target, property=prop, value=value)])

    def test_game_explorer_result_filters_have_independent_saveable_styles(self):
        for result in ('win', 'draw', 'loss'):
            target = f'gameExplorer.results.{result}'
            for prop, value in [('modeActiveBackground', '#123456'), ('modeInactiveBackground', '#abcdef'),
                                ('hoverBackground', '#fedcba'), ('hoverBorderColor', '#112233'), ('radius', 8)]:
                validate_overrides([override(scope='home', target=target, property=prop, value=value)])
            self.assertEqual(REGISTRY['scopes']['home']['targets'][target]['editorParent'], 'gameExplorer.results')

    def test_behavioral_container_hover_background_is_saveable(self):
        row = override(scope='home', target='player.analysis', property='hoverBackground', value='#12345680')
        self.assertEqual(validate_overrides([row])[0]['color_value'], '#12345680')
        with self.assertRaises(ValueError):
            validate_overrides([{**row, 'scope': 'positions'}])

    def test_every_editable_background_exposes_a_saveable_hover_color(self):
        checked = 0
        for scope, spec in REGISTRY['scopes'].items():
            for target, definition in spec['targets'].items():
                if 'background' not in definition['properties']:
                    continue
                checked += 1
                self.assertIn('hoverBackground', definition['properties'], f'{scope}/{target}')
                self.assertNotIn('hoverBackground', definition.get('hiddenProperties', []), f'{scope}/{target}')
                row = override(scope=scope, target=target, property='hoverBackground', value='#12345680')
                self.assertEqual(validate_overrides([row])[0]['color_value'], '#12345680')
        self.assertGreater(checked, 500)

    def test_new_hover_controls_do_not_seed_shared_colors_into_existing_presets(self):
        target = REGISTRY['scopes']['home']['targets']['player.days.count']
        self.assertIn('hoverBackground', target['properties'])
        self.assertIn('hoverBackground', target['initialStyleExcludedProperties'])
        # Existing intentional inheritance remains unchanged.
        button = REGISTRY['scopes']['home']['targets']['player.analysis.toggle']
        self.assertNotIn('hoverBackground', button.get('initialStyleExcludedProperties', []))

    def test_hero_photo_and_container_hover_styles_are_saveable(self):
        for target in ('player.hero', 'player.navigation', 'player.profile', 'player.photo', 'player.photo.pinned',
                       'player.profilePopup', 'player.profilePopup.field'):
            for prop, value in [('background', '#123456'), ('hoverBackground', '#abcdef'),
                                ('borderColor', '#112233'), ('hoverBorderColor', '#445566'),
                                ('radius', 12), ('padding', 6)]:
                validate_overrides([override(scope='home', target=target, property=prop, value=value)])
                with self.assertRaises(ValueError):
                    validate_overrides([override(scope='positions', target=target, property=prop, value=value)])

    def test_player_metadata_backgrounds_are_independent_and_saveable(self):
        targets = REGISTRY['scopes']['home']['targets']
        for field in ('player', 'country', 'name', 'joined', 'last-play', 'dates'):
            target = f'player.metadata.{field}'
            for prop, value in [('background', '#12345680'), ('hoverBackground', '#abcdef'),
                                ('borderColor', '#112233'), ('hoverBorderColor', '#445566'),
                                ('radius', 12), ('padding', 6)]:
                validate_overrides([override(scope='home', target=target, property=prop, value=value)])
                with self.assertRaises(ValueError):
                    validate_overrides([override(scope='positions', target=target, property=prop, value=value)])
            if field != 'dates':
                for part in ('label', 'value'):
                    self.assertEqual(targets[f'{target}.{part}']['editorParent'], target)
                    self.assertEqual(targets[f'{target}.{part}']['initialStyleFrom'], f'player.metadata.{part}')

    def test_independent_analysis_headings_and_controls(self):
        targets = REGISTRY['scopes']['positions']['targets']
        for target in ('cloud.heading', 'cloud.job.title', 'cloud.performance.title',
                       'cloud.run.single.total.input', 'cloud.batch.single.total.input',
                       'cloud.run.combined.editor.calculate', 'cloud.batch.combined.editor.calculate'):
            for prop, value in [('radius', 18), ('hoverBorderColor', '#123456'),
                                ('hoverBackground', '#abcdef'), ('textAlign', 'center')]:
                validate_overrides([override(scope='positions', target=target, property=prop, value=value)])
                with self.assertRaises(ValueError):
                    validate_overrides([override(scope='home', target=target, property=prop, value=value)])
        self.assertEqual(targets['cloud.toggle']['editorParent'], 'cloud.heading')
        self.assertEqual(targets['cloud.heading']['initialStyleFrom'], 'title')
        self.assertEqual(targets['cloud.job.title']['initialStyleFrom'], 'title')
        # Splitting old shared controls must not make a valid saved preset too
        # large to save. Exercise real unique registry keys above the old limit.
        rows = []
        for target, spec in targets.items():
            for prop in ('background', 'color', 'borderColor', 'hoverBackground', 'hoverBorderColor'):
                if prop in spec['properties']:
                    rows.append(override(scope='positions', target=target, property=prop, value='#123456'))
        self.assertGreater(len(rows), 2000)
        self.assertEqual(len(validate_overrides(rows)), len(rows))

    def test_universal_spacing_and_new_container_controls(self):
        for scope, spec in REGISTRY['scopes'].items():
            for target in spec['targets']:
                for prop in REGISTRY['universalProperties']:
                    row = override(scope=scope, target=target, property=prop, value=12)
                    self.assertEqual(validate_overrides([row])[0]['number_value'], 12)
        for prop, value in [('margin', -16), ('marginLeft', 24), ('researchIntroWidth', 65)]:
            row = override(scope='research', target='intro.panel', property=prop, value=value)
            self.assertEqual(validate_overrides([row])[0]['number_value'], value)
            for invalid in (True, '12px', float('inf'), 10000):
                with self.assertRaises(ValueError): validate_overrides([{**row, 'value': invalid}])
        for target in ('cloud.job.vms', 'cloud.vm.toggle', 'cloud.vm.details', 'local.toggle', 'local.heading'):
            validate_overrides([override(scope='positions', target=target, property='hoverBorderColor', value='#abcdef')])

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
        targets = [target for target in targets if not target.startswith('gameReview.')]
        self.assertEqual(len(targets), 22)
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

    def test_salience_styles_are_scoped_and_dimensions_are_bounded(self):
        prefix = 'player.accuracy.salience.'
        targets = {name: spec for name, spec in REGISTRY['scopes']['home']['targets'].items() if name.startswith(prefix)}
        self.assertEqual(len(targets), 10)
        for target, spec in targets.items():
            for prop in spec['properties']:
                field = REGISTRY['properties'][prop]
                value = '#123456' if field['kind'] == 'color' else field['min'] if field['kind'] == 'number' else field['options'][0]
                row = override(scope='home', target=target, property=prop, value=value)
                self.assertEqual(len(validate_overrides([row])), 1)
                with self.assertRaises(ValueError): validate_overrides([{**row, 'scope': 'positions'}])
        for part, prop in [('track', 'salienceTrackHeight'), ('rail', 'salienceRailWidth'), ('minimum.knob', 'salienceKnobSize')]:
            for value in (-1, True, '20px', 10000):
                with self.assertRaises(ValueError): validate_overrides([override(scope='home', target=prefix+part, property=prop, value=value)])

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
