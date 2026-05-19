BEGIN;

WITH ranked AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY player_name, year, month
            ORDER BY n_games DESC, id DESC
        ) AS row_number
    FROM months
)
DELETE FROM months
WHERE id IN (
    SELECT id
    FROM ranked
    WHERE row_number > 1
);

WITH player_month_counts AS (
    SELECT
        player_name,
        year,
        month,
        COUNT(*)::int AS n_games
    FROM (
        SELECT white AS player_name, year, month
        FROM game
        UNION ALL
        SELECT black AS player_name, year, month
        FROM game
    ) game_players
    GROUP BY player_name, year, month
)
UPDATE months
SET n_games = player_month_counts.n_games
FROM player_month_counts
WHERE months.player_name = player_month_counts.player_name
  AND months.year = player_month_counts.year
  AND months.month = player_month_counts.month;

ALTER TABLE months
ADD CONSTRAINT _player_year_month_uc UNIQUE (player_name, year, month);

COMMIT;
