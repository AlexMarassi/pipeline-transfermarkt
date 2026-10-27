"""
Capa Gold: una fila por jugador por partido, con todo el contexto para analizar sin joins.

Salida (Parquet, particionado por la fecha del partido, no por la de ingesta):
    gold/player_appearances/year=YYYY/month=MM/part-0.parquet

Join: appearances ⋈ games ⋈ players ⋈ clubs (club y rival) ⋈ competitions.
Cada mes se reescribe entero: se borra la carpeta y se vuelve a escribir, nunca se agrega.
"""
import logging
import shutil
from datetime import date

import duckdb
import pyarrow.parquet as pq

from transfermarkt.config import CATALOG_PATH, GOLD_DIR, SILVER_DIR, SQL_DIR

log = logging.getLogger(__name__)

GOLD_TABLE = "player_appearances"

# games y appearances ya llegan filtrados al mes en Silver. players, clubs y competitions
# se unen con LEFT JOIN: un club o una competición con in_catalog = false (copas, rivales de
# otras divisiones) puede tener menos datos, pero la aparición no se pierde.
# El valor de mercado no está: el crudo no lo trae.
GOLD_QUERY = """
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
FROM read_parquet(?) a
JOIN read_parquet(?) g USING (game_id)
LEFT JOIN read_parquet(?) p ON p.player_id = a.player_id
LEFT JOIN read_parquet(?) c ON c.club_id = a.club_id
LEFT JOIN read_parquet(?) o ON o.club_id = a.opponent_club_id
LEFT JOIN read_parquet(?) comp ON comp.competition_id = g.competition_id
ORDER BY g.game_date, a.game_id, a.player_id
"""


def partition_dir(year: int, month: int):
    return GOLD_DIR / GOLD_TABLE / f"year={year:04d}" / f"month={month:02d}"


def build_gold(start: date) -> dict:
    """Escribe la partición del mes de `start`. Si ya existía, la borra primero."""
    ym = f"{start.year:04d}-{start.month:02d}"
    target = partition_dir(start.year, start.month)

    # Idempotencia: se borra la partición del mes antes de escribir (nunca append)
    if target.exists():
        shutil.rmtree(target)

    facts = [SILVER_DIR / "appearances" / f"{ym}.parquet", SILVER_DIR / "games" / f"{ym}.parquet"]
    if not all(f.exists() for f in facts):
        log.info("Gold %s: el mes no tiene partidos en Silver, no se escribe partición", ym)
        return {"month": ym, "rows": 0}

    params = [str(f) for f in facts] + [
        str(SILVER_DIR / "players" / "players.parquet"),
        str(SILVER_DIR / "clubs" / "clubs.parquet"),
        str(SILVER_DIR / "clubs" / "clubs.parquet"),
        str(SILVER_DIR / "competitions" / "competitions.parquet"),
    ]
    silver_rows = duckdb.sql("SELECT count(*) FROM read_parquet(?)", params=[params[0]]).fetchone()[0]
    table = duckdb.execute(GOLD_QUERY, params).fetch_arrow_table()

    # Un join que duplica o pierde filas rompe la regla "una fila por aparición"
    if table.num_rows != silver_rows:
        raise ValueError(f"Gold {ym}: {table.num_rows} filas, pero Silver tiene {silver_rows} apariciones")

    target.mkdir(parents=True)
    pq.write_table(table, target / "part-0.parquet")
    log.info("Gold %s: %d filas en %s", ym, table.num_rows, target)
    return {"month": ym, "rows": table.num_rows}


def update_catalog() -> dict:
    """Crea (o reemplaza) la vista player_appearances en el catálogo DuckDB, con el .sql de /sql."""
    if not any((GOLD_DIR / GOLD_TABLE).glob("year=*/month=*/*.parquet")):
        log.warning("No hay particiones Gold todavía: no se crea la vista")
        return {"view": None, "rows": 0}

    sql = (SQL_DIR / "create_player_appearances_view.sql").read_text(encoding="utf-8")
    sql = sql.replace("{{GOLD_GLOB}}", str(GOLD_DIR / GOLD_TABLE / "*" / "*" / "*.parquet"))
    with duckdb.connect(str(CATALOG_PATH)) as con:
        con.execute(sql)
        rows = con.execute(f"SELECT count(*) FROM {GOLD_TABLE}").fetchone()[0]
    log.info("Catálogo %s: vista %s con %d filas", CATALOG_PATH, GOLD_TABLE, rows)
    return {"view": GOLD_TABLE, "rows": rows}
