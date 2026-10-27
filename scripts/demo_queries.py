"""
Consultas de demostración sobre el catálogo DuckDB (las vistas de Gold).

Uso (dentro del contenedor, desde la raíz del repo):
    docker compose exec airflow-scheduler python /opt/airflow/scripts/demo_queries.py
"""
import duckdb

CATALOG = "/opt/airflow/data/catalog.duckdb"

QUERIES = [
    ("Vistas del catálogo y sus filas", None),
    (
        "Máximos goleadores de la Premier League 2024",
        "SELECT player_name, club_name, matches, goals, assists, goals_per_90 FROM top_scorers "
        "WHERE competition_id = 'GB1' AND season = 2024 ORDER BY goals DESC, assists DESC LIMIT 5",
    ),
    (
        "Tabla de LaLiga 2024",
        "SELECT club_name, matches, wins, draws, losses, goals_for, goals_against, points FROM league_table "
        "WHERE competition_id = 'ES1' AND season = 2024 ORDER BY points DESC, goal_difference DESC LIMIT 5",
    ),
    (
        "Partidos con más goles (sin los definidos por penales)",
        "SELECT game_date, competition_name, home_club_name, away_club_name, home_goals, away_goals, total_goals "
        "FROM highest_scoring_matches LIMIT 5",
    ),
    (
        "Goles por partido en cada competición",
        "SELECT competition_name, count(*) AS partidos, round(avg(total_goals), 2) AS goles_por_partido "
        "FROM match_results WHERE total_goals IS NOT NULL GROUP BY 1 ORDER BY partidos DESC LIMIT 6",
    ),
    (
        "ARGENTINA: tabla del Torneo Apertura (datos parciales, ene-may 2025)",
        "SELECT club_name, matches, wins, draws, losses, goals_for, goals_against, goal_difference, points "
        "FROM league_table WHERE competition_id = 'ARG1' ORDER BY points DESC, goal_difference DESC LIMIT 8",
    ),
    (
        "ARGENTINA: máximos goleadores del Torneo Apertura",
        "SELECT player_name, club_name, matches, goals, assists, goals_per_90 FROM top_scorers "
        "WHERE competition_id = 'ARG1' ORDER BY goals DESC, assists DESC LIMIT 5",
    ),
    (
        "ARGENTINA: partidos de Boca y River",
        "SELECT game_date, home_club_name, away_club_name, home_goals, away_goals FROM match_results "
        "WHERE competition_id = 'ARG1' AND (home_club_name IN ('CA Boca Juniors', 'CA River Plate') "
        "OR away_club_name IN ('CA Boca Juniors', 'CA River Plate')) ORDER BY game_date LIMIT 8",
    ),
    (
        "ARGENTINA: partidos con más asistencia",
        "SELECT game_date, home_club_name, away_club_name, home_goals, away_goals, attendance FROM match_results "
        "WHERE competition_id = 'ARG1' AND attendance IS NOT NULL ORDER BY attendance DESC LIMIT 5",
    ),
    (
        "Una sola partición (marzo 2024): solo lee esa carpeta",
        "SELECT competition_name, count(*) AS apariciones, sum(goals) AS goles FROM player_appearances "
        "WHERE year = 2024 AND month = 3 GROUP BY 1 ORDER BY apariciones DESC",
    ),
]


def main():
    con = duckdb.connect(CATALOG, read_only=True)
    for title, sql in QUERIES:
        print(f"\n=== {title} ===")
        if sql is None:
            views = [r[0] for r in con.sql("SELECT view_name FROM duckdb_views() WHERE NOT internal ORDER BY 1").fetchall()]
            for v in views:
                print(f"{v:<26}{con.sql(f'SELECT count(*) FROM {v}').fetchone()[0]:>8} filas")
        else:
            print(con.sql(sql).df().to_string(index=False))


if __name__ == "__main__":
    main()
