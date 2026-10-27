"""
Capa Gold: modelo analítico, particionado por la fecha del partido (no la de ingesta).

Cuatro datasets, cada uno en gold/{dataset}/year=YYYY/month=MM/part-0.parquet:

    player_appearances   una fila por jugador por partido (el detalle, desnormalizado)
    match_results        una fila por partido, con resultado, clubes y competición
    player_month_stats   una fila por jugador y club por mes (goles, asistencias, minutos)
    club_month_stats     una fila por club, competición y mes (partidos, puntos, goles)

player_appearances une appearances ⋈ games ⋈ players ⋈ clubs ⋈ competitions (5 entidades de Silver).
Los otros tres se derivan de Silver y de ese mismo detalle, siempre sobre el mismo mes.

Cada mes se reescribe entero: se borra la carpeta y se vuelve a escribir, nunca se agrega.
"""
import logging
import shutil
from datetime import date

import duckdb
import pyarrow.parquet as pq

from transfermarkt.config import CATALOG_PATH, GOLD_DIR, SILVER_DIR, SQL_DIR

log = logging.getLogger(__name__)

# games y appearances ya llegan filtrados al mes en Silver. players, clubs y competitions
# se unen con LEFT JOIN: un club o una competición con in_catalog = false (copas, rivales de
# otras divisiones) puede tener menos datos, pero la aparición no se pierde.
# El valor de mercado no está: el crudo no lo trae.
PLAYER_APPEARANCES_QUERY = """
SELECT
    a.appearance_id,
    a.game_id,
    a.player_id,
    g.game_date,
    -- partido
    g.competition_id,
    comp.competition_name,
    comp.competition_type,
    comp.country_name,
    g.season,
    g.matchday,
    g.round,
    g.stadium,
    g.attendance,
    g.referee,
    g.decided_by,
    -- club y rival
    a.club_id,
    c.club_name,
    a.opponent_club_id,
    o.club_name AS opponent_club_name,
    a.is_home,
    CASE WHEN a.is_home THEN g.home_goals ELSE g.away_goals END AS goals_for,
    CASE WHEN a.is_home THEN g.away_goals ELSE g.home_goals END AS goals_against,
    CASE
        WHEN g.home_goals = g.away_goals THEN 'D'
        WHEN (g.home_goals > g.away_goals) = a.is_home THEN 'W'
        ELSE 'L'
    END AS result,
    -- jugador
    p.player_name,
    p.date_of_birth,
    date_diff('year', p.date_of_birth, g.game_date) AS player_age,
    p.citizenship,
    p.position AS player_position,
    p.sub_position AS player_sub_position,
    p.foot,
    p.height_cm,
    -- lo que hizo en el partido
    a.position AS match_position,
    a.minutes_played,
    a.goals,
    a.assists,
    a.yellow_cards,
    a.second_yellow_cards,
    a.red_cards
FROM read_parquet($appearances) a
JOIN read_parquet($games) g USING (game_id)
LEFT JOIN read_parquet($players) p ON p.player_id = a.player_id
LEFT JOIN read_parquet($clubs) c ON c.club_id = a.club_id
LEFT JOIN read_parquet($clubs) o ON o.club_id = a.opponent_club_id
LEFT JOIN read_parquet($competitions) comp ON comp.competition_id = g.competition_id
ORDER BY g.game_date, a.game_id, a.player_id
"""

# Un partido por fila. En los partidos definidos por penales el resultado del crudo incluye los
# penales (14:13 en vez de 2:2), así que total_goals queda NULL: no se conoce el resultado real.
MATCH_RESULTS_QUERY = """
SELECT
    g.game_id,
    g.game_date,
    g.competition_id,
    comp.competition_name,
    comp.competition_type,
    comp.country_name,
    g.season,
    g.matchday,
    g.round,
    g.home_club_id,
    h.club_name AS home_club_name,
    g.away_club_id,
    a.club_name AS away_club_name,
    g.home_goals,
    g.away_goals,
    CASE WHEN g.decided_by = 'penalties' THEN NULL ELSE g.home_goals + g.away_goals END AS total_goals,
    CASE
        WHEN g.home_goals > g.away_goals THEN 'home'
        WHEN g.home_goals < g.away_goals THEN 'away'
        ELSE 'draw'
    END AS winner,
    g.decided_by,
    g.attendance,
    g.stadium,
    g.referee,
    coalesce(ap.players_used, 0) AS players_used,
    coalesce(ap.yellow_cards, 0) AS yellow_cards,
    coalesce(ap.red_cards, 0) AS red_cards
FROM read_parquet($games) g
LEFT JOIN read_parquet($clubs) h ON h.club_id = g.home_club_id
LEFT JOIN read_parquet($clubs) a ON a.club_id = g.away_club_id
LEFT JOIN read_parquet($competitions) comp ON comp.competition_id = g.competition_id
LEFT JOIN (
    SELECT game_id, count(*) AS players_used,
           sum(yellow_cards + second_yellow_cards) AS yellow_cards, sum(red_cards) AS red_cards
    FROM read_parquet($appearances) GROUP BY game_id
) ap USING (game_id)
ORDER BY g.game_date, g.game_id
"""

# Se agrega desde el detalle de player_appearances del mismo mes. Un jugador que cambió de club
# en el mes tiene una fila por club.
PLAYER_MONTH_STATS_QUERY = """
SELECT
    player_id,
    player_name,
    player_position,
    citizenship,
    club_id,
    club_name,
    count(*) AS matches,
    sum(minutes_played) AS minutes_played,
    sum(goals) AS goals,
    sum(assists) AS assists,
    sum(yellow_cards + second_yellow_cards) AS yellow_cards,
    sum(red_cards) AS red_cards,
    round(sum(goals) * 90.0 / nullif(sum(minutes_played), 0), 2) AS goals_per_90
FROM read_parquet($player_appearances)
GROUP BY player_id, player_name, player_position, citizenship, club_id, club_name
ORDER BY player_id, club_id
"""

# Una fila por club, competición y mes. Los partidos definidos por penales cuentan como empate
# (el empate del tiempo reglamentario) y no suman goles, porque el resultado del crudo los incluye.
CLUB_MONTH_STATS_QUERY = """
WITH sides AS (
    SELECT game_date, season, competition_id, decided_by,
           home_club_id AS club_id, home_goals AS goals_for, away_goals AS goals_against
    FROM read_parquet($games)
    UNION ALL
    SELECT game_date, season, competition_id, decided_by,
           away_club_id, away_goals, home_goals
    FROM read_parquet($games)
),
scored AS (
    SELECT *,
        CASE WHEN decided_by = 'penalties' THEN 0 ELSE goals_for END AS gf,
        CASE WHEN decided_by = 'penalties' THEN 0 ELSE goals_against END AS ga,
        CASE WHEN decided_by = 'penalties' OR goals_for = goals_against THEN 'D'
             WHEN goals_for > goals_against THEN 'W' ELSE 'L' END AS outcome
    FROM sides
)
SELECT
    s.club_id,
    c.club_name,
    s.competition_id,
    comp.competition_name,
    comp.competition_type,
    s.season,
    count(*) AS matches,
    sum((outcome = 'W')::INT) AS wins,
    sum((outcome = 'D')::INT) AS draws,
    sum((outcome = 'L')::INT) AS losses,
    sum(gf) AS goals_for,
    sum(ga) AS goals_against,
    sum(gf) - sum(ga) AS goal_difference,
    sum(CASE outcome WHEN 'W' THEN 3 WHEN 'D' THEN 1 ELSE 0 END) AS points
FROM scored s
LEFT JOIN read_parquet($clubs) c ON c.club_id = s.club_id
LEFT JOIN read_parquet($competitions) comp ON comp.competition_id = s.competition_id
GROUP BY s.club_id, c.club_name, s.competition_id, comp.competition_name, comp.competition_type, s.season
ORDER BY s.competition_id, s.club_id, s.season
"""

DATASETS = ["player_appearances", "match_results", "player_month_stats", "club_month_stats"]


def partition_dir(dataset: str, year: int, month: int):
    return GOLD_DIR / dataset / f"year={year:04d}" / f"month={month:02d}"


def _write(con, query: str, params: dict, dataset: str, start: date) -> int:
    """Ejecuta la consulta y escribe la partición del mes. Devuelve las filas escritas."""
    used = {k: v for k, v in params.items() if f"${k}" in query}   # DuckDB rechaza parámetros que la consulta no usa
    table = con.execute(query, used).fetch_arrow_table()
    target = partition_dir(dataset, start.year, start.month)
    target.mkdir(parents=True)
    pq.write_table(table, target / "part-0.parquet")
    log.info("Gold %s %04d-%02d: %d filas en %s", dataset, start.year, start.month, table.num_rows, target)
    return table.num_rows


def build_gold(start: date) -> dict:
    """Escribe las particiones del mes de `start` de los cuatro datasets. Si existían, las borra primero."""
    ym = f"{start.year:04d}-{start.month:02d}"

    # Idempotencia: se borra la partición del mes antes de escribir (nunca append)
    for dataset in DATASETS:
        target = partition_dir(dataset, start.year, start.month)
        if target.exists():
            shutil.rmtree(target)

    appearances = SILVER_DIR / "appearances" / f"{ym}.parquet"
    games = SILVER_DIR / "games" / f"{ym}.parquet"
    if not (appearances.exists() and games.exists()):
        log.info("Gold %s: el mes no tiene partidos en Silver, no se escribe ninguna partición", ym)
        return {"month": ym, **{d: 0 for d in DATASETS}}

    silver = {
        "appearances": str(appearances),
        "games": str(games),
        "players": str(SILVER_DIR / "players" / "players.parquet"),
        "clubs": str(SILVER_DIR / "clubs" / "clubs.parquet"),
        "competitions": str(SILVER_DIR / "competitions" / "competitions.parquet"),
    }
    con = duckdb.connect()
    rows = {"month": ym}

    rows["player_appearances"] = _write(con, PLAYER_APPEARANCES_QUERY, silver, "player_appearances", start)
    rows["match_results"] = _write(con, MATCH_RESULTS_QUERY, silver, "match_results", start)

    detail = {"player_appearances": str(partition_dir("player_appearances", start.year, start.month) / "part-0.parquet")}
    rows["player_month_stats"] = _write(con, PLAYER_MONTH_STATS_QUERY, detail, "player_month_stats", start)
    rows["club_month_stats"] = _write(con, CLUB_MONTH_STATS_QUERY, silver, "club_month_stats", start)

    # Un join que duplica o pierde filas rompe la regla "una fila por aparición / por partido"
    silver_appearances = con.execute("SELECT count(*) FROM read_parquet($f)", {"f": silver["appearances"]}).fetchone()[0]
    silver_games = con.execute("SELECT count(*) FROM read_parquet($f)", {"f": silver["games"]}).fetchone()[0]
    if rows["player_appearances"] != silver_appearances:
        raise ValueError(f"Gold {ym}: {rows['player_appearances']} filas, pero Silver tiene {silver_appearances} apariciones")
    if rows["match_results"] != silver_games:
        raise ValueError(f"Gold {ym}: {rows['match_results']} partidos, pero Silver tiene {silver_games}")
    return rows


def update_catalog() -> dict:
    """Crea (o reemplaza) las vistas del catálogo DuckDB con el .sql de /sql."""
    if not any((GOLD_DIR / "player_appearances").glob("year=*/month=*/*.parquet")):
        log.warning("No hay particiones Gold todavía: no se crean las vistas")
        return {"views": 0}

    sql = (SQL_DIR / "create_gold_views.sql").read_text(encoding="utf-8")
    sql = sql.replace("{{GOLD_DIR}}", str(GOLD_DIR))
    with duckdb.connect(str(CATALOG_PATH)) as con:
        con.execute(sql)
        views = [r[0] for r in con.execute("SELECT view_name FROM duckdb_views() WHERE NOT internal ORDER BY 1").fetchall()]
        counts = {v: con.execute(f"SELECT count(*) FROM {v}").fetchone()[0] for v in views}
    log.info("Catálogo %s: %s", CATALOG_PATH, counts)
    return {"views": counts}
