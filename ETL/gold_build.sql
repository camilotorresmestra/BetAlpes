
CREATE SCHEMA IF NOT EXISTS main.gold;

-- ---------- Dimensiones ----------
CREATE OR REPLACE TABLE main.gold.dim_team AS
SELECT DISTINCT
  team_api_id AS id_team,
  COALESCE(team_long_name, team_short_name) AS team_name
FROM main.silver.team
WHERE team_api_id IS NOT NULL;

CREATE OR REPLACE TABLE main.gold.dim_player AS
SELECT DISTINCT
  player_api_id AS id_player,
  player_name
FROM main.silver.player
WHERE player_api_id IS NOT NULL;

INSERT INTO main.gold.dim_player (id_player, player_name)
SELECT 0, 'Unknown / Not Available'
WHERE NOT EXISTS (SELECT 1 FROM main.gold.dim_player WHERE id_player = 0);

CREATE OR REPLACE TABLE main.gold.league AS
SELECT DISTINCT
  id          AS id_league,
  name        AS name,
  country_id  AS country
FROM main.silver.league;

CREATE OR REPLACE TABLE main.gold.dim_period AS
WITH
dates_m AS (
  SELECT DISTINCT to_date(match_date) AS date
  FROM main.silver.match
  WHERE match_date IS NOT NULL
),
dates_e AS (
  SELECT DISTINCT date_key AS date
  FROM main.silver.events_long
),
all_dates AS (
  SELECT date FROM dates_m
  UNION
  SELECT date FROM dates_e
)
SELECT
  CAST(date AS STRING) AS id_period,
  year(date)  AS year,
  month(date) AS month,
  day(date)   AS day,
  date,
  CAST(NULL AS INT)   AS minute,
  CAST(NULL AS INT)   AS seconds,
  to_timestamp(date)  AS timestamp
FROM all_dates;


CREATE OR REPLACE TABLE main.gold.dim_match AS
SELECT DISTINCT
  id                  AS id_match,
  match_api_id        AS match,
  season
FROM main.silver.match;


CREATE OR REPLACE TABLE main.gold.dim_betting_office (
  id_betting_office BIGINT,
  abbr STRING,
  name STRING,
  url STRING,
  country STRING
);
INSERT OVERWRITE main.gold.dim_betting_office VALUES
  (3424915662, 'WH',  'William Hill',                 'https://www.williamhill.com/',       'United Kingdom'),
  (2506957540, 'IW',  'Interwetten',                  'https://www.interwetten.com/',       'Austria'),
  (3923460405, 'B365','Bet365',                       'https://www.bet365.com/',            'United Kingdom'),
  (1179038950, 'SJ',  'Stan James',                   'https://www.stanjames.com/',         'United Kingdom'),
  (1122891783, 'VC',  'Victor Chandler (BetVictor)',  'https://www.betvictor.com/',         'Gibraltar'),
  (1989802799, 'BW',  'Bwin',                         'https://www.bwin.com/',              'Austria'),
  (1714678657, 'GB',  'Gamebookers',                  'https://www.gamebookers.com/',       'Antigua and Barbuda'),
  (2244424266, 'LB',  'Ladbrokes',                    'https://www.ladbrokes.com/',         'United Kingdom'),
  (151015397,  'PS',  'Pinnacle Sports',              'https://www.pinnacle.com/',          'Curaçao'),
  (1911832374, 'BS',  'Betsson',                      'https://www.betsson.com/',           'Sweden');

CREATE OR REPLACE TABLE main.gold.dim_event (
  id_event BIGINT,
  event_abb STRING,
  event_description STRING
);
INSERT OVERWRITE main.gold.dim_event VALUES
  (10001, 'opening', 'Odds at the start of the match'),
  (10002, 'closing', 'Odds at the end of the match');


CREATE OR REPLACE TEMP VIEW gold_tmp_lineups_base AS
WITH
home AS (
  SELECT
    id AS id_match,
    1  AS local,
    array(
      COALESCE(CAST(regexp_extract(CAST(home_player_1  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(home_player_2  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(home_player_3  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(home_player_4  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(home_player_5  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(home_player_6  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(home_player_7  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(home_player_8  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(home_player_9  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(home_player_10 AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(home_player_11 AS STRING), '(\\d+)', 1) AS INT), 0)
    ) AS players
  FROM main.silver.match
),
away AS (
  SELECT
    id AS id_match,
    0  AS local,
    array(
      COALESCE(CAST(regexp_extract(CAST(away_player_1  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(away_player_2  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(away_player_3  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(away_player_4  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(away_player_5  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(away_player_6  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(away_player_7  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(away_player_8  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(away_player_9  AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(away_player_10 AS STRING), '(\\d+)', 1) AS INT), 0),
      COALESCE(CAST(regexp_extract(CAST(away_player_11 AS STRING), '(\\d+)', 1) AS INT), 0)
    ) AS players
  FROM main.silver.match
)
SELECT * FROM home
UNION ALL
SELECT * FROM away;

CREATE OR REPLACE TEMP VIEW gold_tmp_lineups_map AS
WITH nz AS (
  SELECT
    id_match,
    local,
    players,
    array_sort(filter(players, x -> x <> 0)) AS players_nonzero_sorted
  FROM gold_tmp_lineups_base
)
SELECT
  id_match,
  local,
  players,
  CASE
    WHEN size(players_nonzero_sorted) >= 3
      THEN sha2(array_join(transform(players_nonzero_sorted, x -> CAST(x AS STRING)), '|'), 256)
    ELSE sha2(concat_ws('|', CAST(id_match AS STRING), CAST(local AS STRING)), 256)
  END AS id_lineup
FROM nz;


CREATE OR REPLACE TABLE main.gold.id_lineup AS
SELECT id_lineup, players
FROM gold_tmp_lineups_map
GROUP BY id_lineup, players;

CREATE OR REPLACE TABLE main.gold.id_bridge_lineup AS
SELECT
  l.id_lineup,
  pos AS position,
  l.players[pos] AS id_player
FROM main.gold.id_lineup l
LATERAL VIEW posexplode(l.players) p AS pos, id_player;

CREATE OR REPLACE TABLE main.gold.lineup_names AS
WITH named AS (
  SELECT
    b.id_lineup,
    b.position,
    CASE
      WHEN b.id_player = 0 THEN '0'
      WHEN p.player_name IS NULL OR TRIM(p.player_name) = '' THEN '0'
      ELSE p.player_name
    END AS name_or_zero
  FROM main.gold.id_bridge_lineup b
  LEFT JOIN main.gold.dim_player p
    ON b.id_player = p.id_player
)
SELECT
  id_lineup,
  transform(
    array_sort(collect_list(named_struct('pos', position, 'nm', name_or_zero))),
    x -> x.nm
  ) AS players_names
FROM named
GROUP BY id_lineup;

CREATE OR REPLACE TABLE main.gold.fact_match AS
WITH base AS (
  SELECT
    m.league_id                           AS id_league,
    CAST(to_date(m.match_date) AS STRING) AS id_period,
    m.id                                  AS id_match,
    1                                     AS local,
    m.home_team_api_id                    AS id_team,
    CASE WHEN m.home_team_goal > m.away_team_goal THEN 'H'
         WHEN m.home_team_goal < m.away_team_goal THEN 'A'
         ELSE 'D' END                      AS match_result,
    m.home_team_goal                      AS num_goals_favor,
    m.away_team_goal                      AS num_goals_against
  FROM main.silver.match m
  UNION ALL
  SELECT
    m.league_id                           AS id_league,
    CAST(to_date(m.match_date) AS STRING) AS id_period,
    m.id                                  AS id_match,
    0                                     AS local,
    m.away_team_api_id                    AS id_team,
    CASE WHEN m.home_team_goal > m.away_team_goal THEN 'H'
         WHEN m.home_team_goal < m.away_team_goal THEN 'A'
         ELSE 'D' END                      AS match_result,
    m.away_team_goal                      AS num_goals_favor,
    m.home_team_goal                      AS num_goals_against
  FROM main.silver.match m
),
map AS (
  SELECT DISTINCT id_match, local, id_lineup
  FROM gold_tmp_lineups_map
)
SELECT
  b.id_league, b.id_period, b.id_match, b.local, b.id_team, b.match_result,
  b.num_goals_favor, b.num_goals_against,
  m.id_lineup
FROM base b
LEFT JOIN map m
  ON b.id_match = m.id_match AND b.local = m.local
WHERE b.num_goals_favor >= 0 AND b.num_goals_against >= 0;

CREATE OR REPLACE TABLE main.gold.fact_events AS
WITH joined AS (
  SELECT
    e.id_league,
    d.id_period,
    e.id_match,
    bo.id_betting_office,
    10001 AS id_event,
    e.home_odd, e.draw_odd, e.visitor_odd
  FROM main.silver.events_long e
  LEFT JOIN main.gold.dim_period d
    ON e.date_key = d.date
  LEFT JOIN main.gold.dim_betting_office bo
    ON e.betting_office = bo.abbr
)
SELECT
  id_league, id_period, id_match, id_betting_office, id_event,
  home_odd, draw_odd, visitor_odd,
  (1/home_odd + 1/draw_odd + 1/visitor_odd) - 1.0 AS spread
FROM joined
WHERE home_odd > 1 AND draw_odd > 1 AND visitor_odd > 1;

CREATE OR REPLACE VIEW main.gold.v_match_lineup_names AS
SELECT
  f.id_match, f.local, f.id_lineup,
  dt.team_name,
  ln.players_names
FROM main.gold.fact_match f
LEFT JOIN main.gold.lineup_names ln
  ON f.id_lineup = ln.id_lineup
LEFT JOIN main.gold.dim_team dt
  ON f.id_team = dt.id_team;
