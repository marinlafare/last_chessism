import os

CONN_STRING = os.environ.get("DATABASE_URL")

if not CONN_STRING:
    raise ValueError("DATABASE_URL environment variable is not set.")

# --- Game Results Constants ---
DRAW_RESULTS = ['50move', 'agreed', 'insufficient', 'repetition', 'stalemate', 'timevsinsufficient']
LOSE_RESULTS = ['checkmated', 'resigned', 'threecheck', 'timeout', 'abandoned']
WINING_RESULT = ['win', 'kingofthehill']

# --- Other Constants ---
USER_AGENT = "ChessismApp/1.0 (marinlafare@gmail.com)"
LEADERBOARD = "https://api.chess.com/pub/leaderboards"
STATS = "https://api.chess.com/pub/player/{username}/stats"
PLAYER="https://api.chess.com/pub/player/{player}"
DOWNLOAD_MONTH = "https://api.chess.com/pub/player/{player}/games/{year}/{month}"


#USER_AGENT="Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0"
