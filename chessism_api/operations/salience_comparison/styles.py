"""Page-local experimental controls; no shared edits to the official hero."""
from copy import deepcopy


def register(registry):
    targets=registry['scopes']['salience_comparison']['targets']
    properties=['background','hoverBackground','color','hoverColor','fontFamily','fontSize',
        'textAlign','borderColor','hoverBorderColor','borderWidth','radius','padding']
    names=['content','intro.panel','intro.back','intro.title','intro.text','snapshot.field','snapshot.summary',
        'error','distributions.panel','games.panel','games.filters','games.sort','games.direction','games.mode',
        'games.count','games.table','games.head','games.row','games.open','games.pagination','games.previous','games.next',
        'review.panel','review.heading','review.position','review.form','review.note','review.save','review.status']
    names += [f'review.choice.{choice}' for choice in ('repetitive','distinctive','uncertain')]
    for variant in ('current','equal','opening'):
        names.extend(f'variant.{variant}.{part}' for part in ('panel','title','summary','chart','axis','bar'))
        names.append(f'review.score.{variant}')
    for name in names:
        targets[name]={'label':name.replace('.',' · ').title(),'properties':list(properties)}
        if name.endswith('.bar'):
            targets[name]['properties']+=['fill','stroke','strokeWidth','barHoverColor','barHoverBorder']
            targets[name]['chartInteraction']=[{'kind':'svg','marks':['&'],'hover':['&:hover']}]
        elif name.endswith('.axis'):
            targets[name]['properties']+=['fill']
    targets['games.row']['hoverState']=':is(:hover,[data-highlighted="true"])'
    for name,spec in registry['scopes']['home']['targets'].items():
        if name.startswith('gameReview.'):
            targets[name]=deepcopy(spec)
