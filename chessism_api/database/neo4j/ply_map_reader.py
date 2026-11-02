"""Small indexed snapshot reads. No graph scan, CUDA work or layout on GET."""
from sqlalchemy import text
from . import ply_map_store as store


async def snapshot(session, requested=None):
    status=await store.status(session)
    sid=requested or status.get('active_snapshot')
    if not sid: return status,None
    row=(await session.execute(text('SELECT * FROM graph_map.snapshot WHERE id=:id'),{'id':sid})).mappings().first()
    if not row: raise ValueError('This map snapshot expired. Refresh the map.')
    return status,dict(row)


async def players(session, sid, search):
    return [dict(r) for r in (await session.execute(text('''SELECT player_id,name,
        cardinality(white_games)+cardinality(black_games) AS games FROM graph_map.player
        WHERE snapshot=:id AND strpos(name,lower(:search))>0 ORDER BY games DESC,name LIMIT 40'''),
        {'id':sid,'search':search})).mappings()]


async def memberships(session,sid,ids):
    return [dict(r) for r in (await session.execute(text('''SELECT * FROM graph_map.player
        WHERE snapshot=:id AND player_id=ANY(CAST(:players AS bigint[])) ORDER BY name'''),
        {'id':sid,'players':ids})).mappings()]


def highlight(rows,selected):
    sets={str(p['player_id']):set(p['white_games'])|set(p['black_games']) for p in selected}
    union=set().union(*sets.values()) if sets else set()
    for row in rows:
        members=set(row.pop('members',[]))
        row['players']={pid:len(members & games) for pid,games in sets.items() if members & games}
        # Two selected opponents meeting in one game count as one game.
        row['selected_games']=len(members & union)


async def view(session,sid,first,last,offset,ids):
    params={'id':sid,'first':first,'last':last,'offset':offset}
    # Neutral overview does not transfer occurrence memberships at all.
    node_columns='key,ply,kind,fen_key,label,games,x,y' + (',members' if ids else '')
    rows=[dict(r) for r in (await session.execute(text(f'''SELECT {node_columns} FROM graph_map.node
        WHERE snapshot=:id AND ply BETWEEN :first AND :last ORDER BY ply,y,key LIMIT 301 OFFSET :offset'''),params)).mappings()]
    more=len(rows)>300;rows=rows[:300]
    # Only draw edges whose endpoints are visible. Return an explicit boundary
    # count instead of pretending these are full, disconnected game paths.
    keys=[r['key'] for r in rows]
    edge_columns='key,source,target,kind,hidden,reason,label,games' + (',members' if ids else '')
    edges=[dict(r) for r in (await session.execute(text(f'''SELECT {edge_columns} FROM graph_map.connection
        WHERE snapshot=:id AND source=ANY(CAST(:keys AS text[]))
          AND target=ANY(CAST(:keys AS text[])) ORDER BY key'''),{'id':sid,'keys':keys})).mappings()]
    selected=await memberships(session,sid,ids)
    highlight(rows,selected);highlight(edges,selected)
    layers=[dict(r) for r in (await session.execute(text('''SELECT ply,count(*) AS nodes FROM graph_map.node
        WHERE snapshot=:id GROUP BY ply ORDER BY ply'''),{'id':sid})).mappings()]
    crossing=await session.scalar(text('''SELECT count(*) FROM graph_map.connection WHERE snapshot=:id
        AND (source=ANY(CAST(:keys AS text[])) OR target=ANY(CAST(:keys AS text[])))
        AND NOT(source=ANY(CAST(:keys AS text[])) AND target=ANY(CAST(:keys AS text[])))'''),{'id':sid,'keys':keys})
    for row in [*rows,*edges]: row.pop('snapshot',None)
    return dict(nodes=rows,connections=edges,layers=layers,has_more=more,offset=offset,
                boundary_connections=crossing,selected=[{'id':p['player_id'],'name':p['name']} for p in selected])


async def detail(session,sid,key,offset,ids):
    node=(await session.execute(text('SELECT * FROM graph_map.node WHERE snapshot=:id AND key=:key'),{'id':sid,'key':key})).mappings().first()
    edge=None if node else (await session.execute(text('SELECT * FROM graph_map.connection WHERE snapshot=:id AND key=:key'),{'id':sid,'key':key})).mappings().first()
    if not node and not edge: raise ValueError('This item is not in the saved map.')
    item=dict(node or edge);members=set(item.pop('members'));item.pop('snapshot')
    endpoint_ply=item.get('ply',0)
    if edge:
        endpoint_ply=await session.scalar(text('SELECT ply FROM graph_map.node WHERE snapshot=:id AND key=:key'),
            {'id':sid,'key':edge['target']})
    if ids:
        selected=await memberships(session,sid,ids)
        union=set().union(*(set(p['white_games'])|set(p['black_games']) for p in selected))
        members &= union
    params={'id':sid,'members':sorted(members),'offset':offset}
    games=[dict(r) for r in (await session.execute(text('''SELECT * FROM graph_map.game
        WHERE snapshot=:id AND ordinal=ANY(CAST(:members AS integer[]))
        ORDER BY nullif(played_at,'')::timestamptz DESC NULLS LAST,game_id DESC LIMIT 11 OFFSET :offset'''),params)).mappings()]
    more=len(games)>10;games=games[:10]
    ordinals=[g['ordinal'] for g in games]
    segments=[dict(r) for r in (await session.execute(text('''SELECT s.* FROM graph_map.segment s
        JOIN graph_map.connection c ON c.snapshot=s.snapshot AND c.key=s.connection
        WHERE s.snapshot=:id AND (c.key=:key OR c.target=:key) AND s.ordinal=ANY(CAST(:ordinals AS integer[]))'''),
        {'id':sid,'key':key,'ordinals':ordinals})).mappings()]
    by_game={r['ordinal']:r for r in segments}
    for game in games:
        segment=by_game.get(game['ordinal'])
        game.update(start_ply=segment['start_ply'] if segment else endpoint_ply,
                    end_ply=segment['end_ply'] if segment else endpoint_ply,
                    reason=segment['reason'] if segment else 'shared')
        game.pop('snapshot',None)
    return dict(item=item,rows=games,has_more=more,total=len(members),offset=offset)
