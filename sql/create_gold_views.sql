-- Vistas del catálogo DuckDB sobre el Gold particionado. {{GOLD_DIR}} lo reemplaza la tarea update_catalog.
-- hive_partitioning expone year y month (de las carpetas year=YYYY/month=MM) como columnas,
-- y DuckDB las usa para leer solo las carpetas que pide el WHERE (partition pruning).

-- Los cuatro datasets de Gold
CREATE OR REPLACE VIEW player_appearances AS
SELECT * FROM read_parquet('{{GOLD_DIR}}/player_appearances/*/*/*.parquet', hive_partitioning = true);

CREATE OR REPLACE VIEW match_results AS
SELECT * FROM read_parquet('{{GOLD_DIR}}/match_results/*/*/*.parquet', hive_partitioning = true);

CREATE OR REPLACE VIEW player_month_stats AS
SELECT * FROM read_parquet('{{GOLD_DIR}}/player_month_stats/*/*/*.parquet', hive_partitioning = true);

CREATE OR REPLACE VIEW club_month_stats AS
SELECT * FROM read_parquet('{{GOLD_DIR}}/club_month_stats/*/*/*.parquet', hive_partitioning = true);

-- Máximos goleadores por temporada y competición (desde el detalle de apariciones).
-- Los goles de las apariciones son individuales: no los afecta la definición por penales.
CREATE OR REPLACE VIEW top_scorers AS
SELECT
    a.season,
    a.competition_id,
    a.competition_name,
    a.player_id,
    a.player_name,
    arg_max(a.club_name, a.game_date) AS club_name,
    count(*) AS matches,
    sum(a.minutes_played) AS minutes_played,
    sum(a.goals) AS goals,
    sum(a.assists) AS assists,
    round(sum(a.goals) * 90.0 / nullif(sum(a.minutes_played), 0), 2) AS goals_per_90
FROM player_appearances a
GROUP BY a.season, a.competition_id, a.competition_name, a.player_id, a.player_name;

-- Partidos con más goles (sin los definidos por penales, donde el crudo mezcla los penales)
CREATE OR REPLACE VIEW highest_scoring_matches AS
SELECT game_date, competition_name, home_club_name, away_club_name, home_goals, away_goals,
       total_goals, decided_by, attendance
FROM match_results
WHERE total_goals IS NOT NULL
ORDER BY total_goals DESC, game_date;

-- Tabla de posiciones por temporada y competición (solo ligas)
CREATE OR REPLACE VIEW league_table AS
SELECT
    season,
    competition_id,
    competition_name,
    club_id,
    club_name,
    sum(matches) AS matches,
    sum(wins) AS wins,
    sum(draws) AS draws,
    sum(losses) AS losses,
    sum(goals_for) AS goals_for,
    sum(goals_against) AS goals_against,
    sum(goal_difference) AS goal_difference,
    sum(points) AS points
FROM club_month_stats
WHERE competition_type = 'first_tier'
GROUP BY season, competition_id, competition_name, club_id, club_name;
