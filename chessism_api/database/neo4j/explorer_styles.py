"""Explicit independent Positions-page targets for the graph explorer."""
BOX = ['background','hoverBackground','borderColor','hoverBorderColor','borderWidth','radius','padding']
TEXT = ['color','hoverColor','fontFamily','fontSize','textAlign']


def register(registry):
    targets = registry['scopes']['scored_positions']['targets']
    groups = {
        'explorer': 'Explorer background', 'toolbar': 'Filter toolbar',
        'playerLabel': 'Player label', 'playerInput': 'Player search', 'apply': 'Explore player button',
        'all': 'All imported games button', 'colorLabel': 'Color label', 'colorInput': 'Color selector',
        'coverage': 'Sample coverage', 'ranking': 'Ranking container', 'rankingTitle': 'Ranking title',
        'chart': 'Appearances chart', 'barRow': 'Position bar row', 'barLabel': 'Position bar label',
        'barTrack': 'Position bar track', 'barFill': 'Position bar fill', 'barValue': 'Position bar count',
        'rankingPrevious': 'Previous ranking page', 'rankingNext': 'Next ranking page',
        'preview': 'Hover preview', 'previewBoard': 'Hover board', 'previewText': 'Hover count',
        'selection': 'Selected position container', 'positionTitle': 'Selected position title',
        'back': 'Back button', 'reset': 'Reset button', 'seeGames': 'See games button', 'flip': 'Flip board button',
        'board': 'Board frame', 'board.light': 'Light squares', 'board.dark': 'Dark squares',
        'board.whitePiece': 'White pieces', 'board.blackPiece': 'Black pieces',
        'board.rank': 'Rank coordinates', 'board.file': 'File coordinates',
        'evaluation': 'Evaluation', 'counts': 'Position counts container', 'appearances': 'Appearance count',
        'distinctGames': 'Distinct games count', 'rootNote': 'Starting position note',
        'neighborhood': 'Connections container', 'network': 'Graph canvas', 'centerNode': 'Selected graph node',
        'node': 'Neighbor node', 'nodeMove': 'Move label', 'nodeCount': 'Move count', 'moveGames': 'Move games button',
        'incomingTitle': 'Incoming moves title', 'outgoingTitle': 'Outgoing moves title',
        'incomingPrevious': 'Previous incoming moves', 'incomingNext': 'More incoming moves',
        'outgoingPrevious': 'Previous outgoing moves', 'outgoingNext': 'More outgoing moves',
        'queryTime': 'Query timing', 'status': 'Loading message', 'error': 'Error message', 'empty': 'Empty state',
        'retry': 'Retry button', 'gamesOverlay': 'Games backdrop', 'gamesPanel': 'Games popup',
        'gamesTitle': 'Games popup title', 'gamesClose': 'Games close button', 'gamesScope': 'Games sample scope',
        'gamesList': 'Games list background', 'gameRow': 'Game row', 'gameDate': 'Game date',
        'gamePlayers': 'Game players', 'gameMode': 'Game mode', 'gameResult': 'Game result',
        'gamesPrevious': 'Previous games page', 'gamesNext': 'Next games page',
        'compareLabel': 'Comparison player label', 'compareInput': 'Comparison player input',
        'mapRefresh': 'Refresh map', 'mapScope': 'Scope toolbar', 'mapScope.a': 'Player A filter',
        'mapScope.b': 'Player B filter', 'mapScope.both': 'Both-player filter', 'mapSample': 'Sample counts',
        'mapLimit': 'Map limit notice', 'mapDetailScope': 'Position count scope',
        'mapPanel': 'Map container', 'mapControls': 'Map controls', 'mapOverview': 'Overview button',
        'mapReset': 'Fit map button', 'mapMinimumLabel': 'Minimum games label', 'mapMinimum': 'Minimum games input',
        'mapColorLabel': 'Color mode label', 'mapColors': 'Color mode selector', 'mapHint': 'Map instructions',
        'mapCanvas': 'Map background', 'mapHover': 'Hovered node description', 'mapLegend': 'Map legend',
        'mapExplanation': 'Map size and weight legend', 'mapNodeList': 'Accessible node list',
        'mapNodeRow': 'Map node row', 'mapNodeName': 'Map node name', 'mapNodeDescription': 'Map node counts',
        'mapPrevious': 'Previous nodes', 'mapNext': 'Next nodes', 'mapNode': 'Position node',
        'mapEdge': 'Map connections', 'mapLabel': 'Map labels',
    }
    for key, label in groups.items():
        targets[f'commonGraph.{key}'] = {'label': f'Graph explorer · {label}', 'properties': [*BOX, *TEXT]}
    for key in ('a','b','both','other','root', *(f'community{i}' for i in range(8))):
        targets[f'commonGraph.mapPalette.{key}'] = {
            'label': f'Opening map · {key} color', 'properties': [*BOX, *TEXT]}
    for key in ('barRow','node','gameRow','mapNodeRow'):
        targets[f'commonGraph.{key}'].update(hoverState='[data-active="true"]')
    targets['commonGraph.edge'] = {'label': 'Graph explorer · Move arrows',
        'properties': ['color','hoverColor','stroke','strokeWidth']}
    for key in ('panel','toolbar','title','refresh','status','error','retry','empty','coverage','stale',
                'players','searchLabel','search','searchButton','clear','legend','neutral','playerResults',
                'searchClose','playerResult','limit','explanation','range','earlier','rangeLabel','later',
                'canvas','column','edge','node','bundle','nodeLabel','pagination','previous','next',
                'detail','detailTitle','detailClose','detailCount','paths','path','game','pathReason',
                'moves','move','preview','detailPrevious','detailNext',*(f'player{i}' for i in range(8))):
        targets[f'commonGraph.ply.{key}'] = {'label':f'Collapsed ply map · {key}',
            'properties':[*BOX,*TEXT]}
    targets['commonGraph.ply.node']['hoverState']='[data-active="true"]'
    for key in ('metric.ply_map','progress.ply_map'):
        registry['scopes'].setdefault('positions',{'targets':{}})['targets'][f'cuda.{key}'] = {
            'label':f'CUDA summaries · {key}', 'properties':[*BOX,*TEXT]}
