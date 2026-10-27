"""
Prueba de partition pruning: EXPLAIN ANALYZE sobre todo Gold vs. un solo mes.

Gold está particionado por year=YYYY/month=MM (fecha del partido). Con un filtro por esas
columnas DuckDB descarta las carpetas que no coinciden sin abrir sus archivos.

Uso (dentro del contenedor, desde la raíz del repo):
    docker compose exec airflow-scheduler python /opt/airflow/scripts/finops_validation.py 2024 3
"""
import re
import sys

import duckdb

GOLD = "/opt/airflow/data/gold/player_appearances/*/*/*.parquet"

QUERY = "SELECT count(*) AS apariciones, sum(goals) AS goles FROM read_parquet('{glob}', hive_partitioning = true){where}"


def explain(con, where: str) -> str:
    return con.execute("EXPLAIN ANALYZE " + QUERY.format(glob=GOLD, where=where)).fetchall()[0][1]


def summarize(plan: str) -> tuple[int, int, float]:
    """Archivos leídos, filas leídas por el TABLE_SCAN y tiempo total, tal como los informa DuckDB."""
    scan = re.search(r"TABLE_SCAN.*?Total Files Read:\s*([\d,]+).*?([\d,]+) Rows", plan, re.DOTALL)
    total = re.search(r"Total Time:\s*([\d.]+)s", plan)
    return int(scan.group(1).replace(",", "")), int(scan.group(2).replace(",", "")), float(total.group(1))


def main(year: int, month: int):
    con = duckdb.connect()
    con.execute("SET threads = 1")  # tiempos comparables entre las dos consultas
    cases = [
        ("Todo Gold (sin filtro)", ""),
        (f"Un mes (year={year} AND month={month})", f" WHERE year = {year} AND month = {month}"),
    ]
    results = []
    for title, where in cases:
        plan = explain(con, where)
        print("=" * 100)
        print(f"{title}\n{QUERY.format(glob=GOLD, where=where)}")
        print("=" * 100)
        print(plan)
        results.append((title, *summarize(plan)))

    print("=" * 100)
    print("Resumen")
    print("=" * 100)
    print(f"{'consulta':<44}{'archivos leídos':>17}{'filas leídas':>14}{'tiempo (s)':>12}")
    for title, files, rows, secs in results:
        print(f"{title:<44}{files:>17}{rows:>14}{secs:>12.4f}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 2024, int(sys.argv[2]) if len(sys.argv) > 2 else 3)
