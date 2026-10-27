-- Demo en la interfaz de DuckDB (http://localhost:4213, se abre con `duckdb -ui`).
-- Pegá este archivo en una celda del notebook y ejecutalo UNA vez; después corré las consultas de abajo.
--
-- 1) Cambiá esta ruta por la carpeta data/gold de tu máquina:
SET VARIABLE gold = '/Users/alexmarassi/tp-transfermarkt/pipeline-transfermarkt/data/gold';

-- 2) Vistas sobre el Gold particionado (las mismas que crea el DAG en data/catalog.duckdb,
--    pero con la ruta de tu máquina en vez de la del contenedor)
CREATE OR REPLACE VIEW player_appearances AS
SELECT * FROM read_parquet(getvariable('gold') || '/player_appearances/*/*/*.parquet', hive_partitioning = true);
CREATE OR REPLACE VIEW match_results AS
SELECT * FROM read_parquet(getvariable('gold') || '/match_results/*/*/*.parquet', hive_partitioning = true);
CREATE OR REPLACE VIEW player_month_stats AS
SELECT * FROM read_parquet(getvariable('gold') || '/player_month_stats/*/*/*.parquet', hive_partitioning = true);
CREATE OR REPLACE VIEW club_month_stats AS
SELECT * FROM read_parquet(getvariable('gold') || '/club_month_stats/*/*/*.parquet', hive_partitioning = true);

CREATE OR REPLACE VIEW top_scorers AS
SELECT season, competition_id, competition_name, player_id, player_name,
       arg_max(club_name, game_date) AS club_name,
       count(*) AS matches, sum(minutes_played) AS minutes_played,
       sum(goals) AS goals, sum(assists) AS assists,
       round(sum(goals) * 90.0 / nullif(sum(minutes_played), 0), 2) AS goals_per_90
FROM player_appearances
GROUP BY season, competition_id, competition_name, player_id, player_name;

CREATE OR REPLACE VIEW highest_scoring_matches AS
SELECT game_date, competition_name, home_club_name, away_club_name, home_goals, away_goals,
       total_goals, decided_by, attendance
FROM match_results WHERE total_goals IS NOT NULL
ORDER BY total_goals DESC, game_date;

CREATE OR REPLACE VIEW league_table AS
SELECT season, competition_id, competition_name, club_id, club_name,
       sum(matches) AS matches, sum(wins) AS wins, sum(draws) AS draws, sum(losses) AS losses,
       sum(goals_for) AS goals_for, sum(goals_against) AS goals_against,
       sum(goal_difference) AS goal_difference, sum(points) AS points
FROM club_month_stats WHERE competition_type = 'first_tier'
GROUP BY season, competition_id, competition_name, club_id, club_name;
