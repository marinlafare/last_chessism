"""Balanced-source integration extension for the private PostgreSQL fixture."""
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from unittest.mock import AsyncMock


async def verify(connection, engine):
    from sqlalchemy.ext.asyncio import async_sessionmaker
    from chessism_api.database.relational_storage import RelationalStorage
    from chessism_api.operations.research_algorithms import blunder_balanced_sampling as b
    from chessism_api.operations.research_algorithms import blunder_v2_source as s
    from chessism_api.operations.research_algorithms.blunder_v2_job import configuration
    from tests.test_blunder_balanced import REF
    regular=[f'reg{i}' for i in range(1000) if not b.held_player(f'reg{i}')][:60]
    held=[f'held{i}' for i in range(1000) if b.held_player(f'held{i}')][:20]
    await connection.executemany('INSERT INTO player VALUES($1,$2,1,NULL)',[(1000+i,n) for i,n in enumerate(regular+held)])
    games=[]
    for i in range(1800):
        wi=i%30; bi=60+i%20 if i%7==0 else 30+(i//30)%30
        games.append((10000+i,'blitz',1800,1800+(i%3)*300,1.,0.,'win','resigned',1,'chess',1000+wi,1000+bi,
                      regular[wi],(regular+held)[bi],datetime(2020+i%3,6,1,tzinfo=timezone.utc)+timedelta(minutes=i)))
    await connection.executemany('''INSERT INTO game(link,mode,white_elo,black_elo,white_result,black_result,
        white_str_result,black_str_result,n_moves,rules,white_player_id,black_player_id,white,black,played_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)''',games)
    await connection.execute('''INSERT INTO game_analysis_summary SELECT link,true,2 FROM game WHERE link>=10000;
        INSERT INTO game_fen_association SELECT link,1,'white',1 FROM game WHERE link>=10000;
        INSERT INTO game_fen_association SELECT link,1,'black',2 FROM game WHERE link>=10000;
        INSERT INTO candidate.player SELECT * FROM player WHERE id>=1000;
        INSERT INTO candidate.game(link,white_elo,black_elo,white_result,black_result,white_str_result,black_str_result,n_moves,white_player_id,black_player_id,mode_id,played_at)
            SELECT link,white_elo,black_elo,white_result,black_result,white_str_result,black_str_result,n_moves,white_player_id,black_player_id,1,played_at FROM game WHERE link>=10000;
        INSERT INTO candidate.game_analysis_coverage SELECT * FROM game_analysis_summary WHERE link>=10000;
        INSERT INTO candidate.game_fen_association SELECT game_link,n_move,move_color='white',fen_id FROM game_fen_association WHERE game_link>=10000;
        CREATE TABLE algorithm_run(id text,status text,config jsonb,result jsonb,created_at timestamptz DEFAULT now());''')
    ids=list(range(1,12))
    previous={'format':'chessism_rating_blunder_candidate_v2','source':{'sha256':REF['sha256'],
        'split':{k:REF[k] for k in ('training_before','test_from')},'seen_game_ids':ids,
        'seen_game_ids_sha256':hashlib.sha256(json.dumps(ids).encode()).hexdigest()}}
    await connection.execute('INSERT INTO algorithm_run(id,status,config,result) VALUES($1,\'complete\',$2::jsonb,$3::jsonb)',
                             REF['id'],json.dumps(configuration(100)),json.dumps(previous))
    outputs=[]
    for layout in (None,RelationalStorage('candidate','archive','reads','relational_v3')):
        stream=io.BytesIO()
        meta,arrays,frozen=await s.freeze(configuration(1000,b.plan([REF])),AsyncMock(),stream,
            session_factory=async_sessionmaker(engine,info={'relational_storage_v2':layout}))
        assert meta and all(g['link']>=10000 for g in meta)
        assert frozen['sampling_audit']['prior_game_overlap']==0
        assert sum(arrays['cp_count'])==len(meta)*2
        assert all(r['status']!='failed' for r in frozen['sampling_audit']['cp_concentration'])
        assert set(ids).issubset(frozen['seen_game_ids'])
        assert all(not any(b.held_player(n) for n in b.names(g)) for g in meta if g['split'] in (0,1))
        outputs.append(stream.getvalue())
    assert outputs[0]==outputs[1],'Balanced native/legacy transitions changed'
    from chessism_api.operations.research_algorithms.blunder_refinement import policy
    stream=io.BytesIO()
    await s.freeze(configuration(1000,b.plan([REF]),policy()),AsyncMock(),stream,
        session_factory=async_sessionmaker(engine))
    assert stream.getvalue()==outputs[0],'Refinement altered the frozen source contract'
    newer={**previous,'source':{**previous['source'],'sha256':'b'*64}}
    await connection.execute("INSERT INTO algorithm_run(id,status,config,result,created_at) VALUES($1,'complete',$2::jsonb,$3::jsonb,now()+interval '1 second')",
        '00000000-0000-0000-0000-000000000099',json.dumps(configuration(100)),json.dumps(newer))
    try:
        await s.freeze(configuration(1000,b.plan([REF]),policy()),AsyncMock(),io.BytesIO(),session_factory=async_sessionmaker(engine))
    except ValueError as error: assert 'history changed' in str(error)
    else: raise AssertionError('Queued refinement reused stale cohort exclusions')
    await connection.execute("DELETE FROM algorithm_run WHERE id='00000000-0000-0000-0000-000000000099'")
    # Historical reconstruction must match the actual input hash, not just IDs.
    raw=io.BytesIO()
    meta,_,frozen=await s.freeze(configuration(100),AsyncMock(),raw,session_factory=async_sessionmaker(engine))
    historic={**REF,'sha256':frozen['sha256'],**{k:frozen['split'][k] for k in ('training_before','test_from')}}
    await connection.execute('UPDATE algorithm_run SET config=$1::jsonb,result=$2::jsonb',json.dumps(configuration(100)),json.dumps({'format':'chessism_rating_blunder_candidate_v2','source':frozen}))
    from chessism_api.operations.research_algorithms.blunder_balanced_source import exclusions
    async with async_sessionmaker(engine)() as session:
        found,proof=await exclusions(session,b.plan([historic]),AsyncMock())
        assert found=={g['link'] for g in meta}
        assert proof[0]['method']=='full_frozen_source_hash_match'
    from tests.development_reuse_postgres_fixture import verify as verify_reuse
    await verify_reuse(connection,engine,historic,meta)
    await connection.execute('UPDATE fen SET score=101 WHERE fen_id=1')
    async with async_sessionmaker(engine)() as session:
        try: await exclusions(session,b.plan([historic]),AsyncMock())
        except ValueError as error: assert 'Historical source changed' in str(error)
        else: raise AssertionError('Changed historical inputs were accepted')
    print('PASS: balanced/refined native/legacy extraction, stale-queue rejection, identity manifest, historical replay, leakage exclusion and both-player caps')
