"""Explicit integration check: creates/removes only a uniquely named test schema.

Run inside API container: python - < tests/check_appearance_postgres.py
Never writes production appearance/account tables or submits analysis jobs.
"""
import asyncio
import uuid
import constants
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Base,Account,AppearancePreset,AppearanceSetting,AppearanceOverride
from chessism_api.operations.appearance import read_appearance,change_appearance,AppearanceConflict


async def check():
    schema='appearance_test_'+uuid.uuid4().hex
    control=create_async_engine(constants.CONN_STRING)
    engine=create_async_engine(constants.CONN_STRING,connect_args={'server_settings':{'search_path':schema}})
    try:
        async with control.begin() as conn:await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with engine.begin() as conn:
            await conn.run_sync(lambda c:Base.metadata.create_all(c,tables=[Account.__table__,AppearancePreset.__table__,AppearanceSetting.__table__,AppearanceOverride.__table__]))
        AsyncDBSession.configure(bind=engine)
        async with AsyncDBSession.begin() as session:
            session.add(Account(id='test-admin',name='Test',email='test@example.invalid',password_hash='not-a-real-password',role='admin'))
        state=await read_appearance()
        assert state['revision']==0 and not state['presets'] and state['active_preset_id'] is None
        rows=[
            {'scope':'scored_positions','target':'unique.slice','property':'fill','value':'#abcdef80'},
            {'scope':'positions','target':'cloud.panel','property':'background','value':'#123456'},
            {'scope':'home','target':'heading','property':'color','value':'#abcdef'},
            {'scope':'navigation','target':'button','property':'hoverColor','value':'#fedcba'},
            {'scope':'home','target':'chart.accuracy','property':'chartColor1','value':'#c0123480'},
        ]
        state=await change_appearance('save',{'revision':0,'name':'Saved test','overrides':rows},'test-admin')
        preset=state['active_preset_id']
        sort_rows=lambda items:sorted(items,key=lambda row:(row['scope'],row['target'],row['property']))
        assert state['revision']==1 and sort_rows(state['overrides'])==sort_rows(rows)
        await engine.dispose() # New DB connection must see the committed preset.
        assert sort_rows((await read_appearance())['overrides'])==sort_rows(rows)
        try:await change_appearance('activate',{'revision':0,'preset_id':None},'test-admin')
        except AppearanceConflict:pass
        else:raise AssertionError('Stale writes must conflict')
        assert (await read_appearance())['revision']==1
        state=await change_appearance('rename',{'revision':1,'preset_id':preset,'name':'Renamed'},'test-admin')
        assert state['presets'][0]['name']=='Renamed'
        state=await change_appearance('activate',{'revision':2,'preset_id':None},'test-admin')
        assert state['overrides']==[] and state['active_preset_id'] is None
        state=await change_appearance('activate',{'revision':3,'preset_id':preset},'test-admin')
        assert sort_rows(state['overrides'])==sort_rows(rows)
        state=await change_appearance('save',{'revision':4,'preset_id':preset,'overrides':[]},'test-admin')
        assert len(state['presets'])==1 and state['overrides']==[]
        print('PASS: PostgreSQL multi-page typed storage, committed reload, Default preservation, conflict rollback, rename, activation and preset update.',flush=True)
    finally:
        await engine.dispose()
        async with control.begin() as conn:await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await control.dispose()


if __name__=='__main__':asyncio.run(check())
