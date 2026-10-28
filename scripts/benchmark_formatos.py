"""
Benchmark: la misma pregunta sobre el JSON crudo y sobre el Parquet de Gold, con pandas y con DuckDB.

Pregunta: los cinco goleadores de la Serie A 2023/24 (Lautaro Martínez, 24 goles).
- "Método normal": leer el JSON lines de la fuente (una temporada, 65,7 MB) y calcular a mano.
- Con el pipeline: leer el Parquet que Airflow deja en Gold (21 archivos, 2,8 MB, ya tipado).

Separa los dos efectos: el formato (JSON vs. Parquet) y el motor (pandas vs. DuckDB).

Uso (fuera de Docker, desde la raíz del repo, con el entorno virtual):
    .venv/bin/python scripts/benchmark_formatos.py
"""
import statistics
import time
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "tm_source" / "appearances" / "2023.json"
GOLD = ROOT / "data" / "gold" / "player_appearances"


def pandas_json():
    df = pd.read_json(RAW, lines=True)
    df = df[df["competition_code"] == "IT1"]
    # En el crudo el id del jugador está al final del href y los goles llegan como texto ("" = 0)
    player_id = df["parent"].map(lambda p: p["href"].rsplit("/", 1)[-1])
    goals = pd.to_numeric(df["goals"].replace("", "0"), errors="coerce").fillna(0)
    out = pd.DataFrame({"player_id": player_id, "goals": goals}).groupby("player_id")["goals"].sum()
    return out.sort_values(ascending=False).head(5)


def duckdb_json():
    return duckdb.sql(f"""
        SELECT split_part(parent.href, '/', -1) AS player_id,
               sum(coalesce(try_cast(nullif(goals, '') AS INT), 0)) AS goals
        FROM read_json('{RAW.as_posix()}', format = 'newline_delimited')
        WHERE competition_code = 'IT1'
        GROUP BY 1 ORDER BY goals DESC LIMIT 5""").fetchall()


def pandas_parquet():
    df = pd.read_parquet(GOLD)
    df = df[(df.competition_id == "IT1") & (df.season == 2023)]
    return df.groupby("player_id")["goals"].sum().sort_values(ascending=False).head(5)


def duckdb_parquet():
    return duckdb.sql(f"""
        SELECT player_id, sum(goals) AS goals
        FROM read_parquet('{GOLD.as_posix()}/*/*/*.parquet', hive_partitioning = true)
        WHERE competition_id = 'IT1' AND season = 2023
        GROUP BY 1 ORDER BY goals DESC LIMIT 5""").fetchall()


CASES = {
    "pandas_json": ("pandas + JSON crudo", pandas_json),
    "duckdb_json": ("DuckDB + JSON crudo", duckdb_json),
    "pandas_parquet": ("pandas + Parquet Gold", pandas_parquet),
    "duckdb_parquet": ("DuckDB + Parquet Gold", duckdb_parquet),
}


def measure(case: str, repeat: int = 11) -> dict:
    """Mediana de `repeat` corridas, después de una corrida de calentamiento."""
    label, fn = CASES[case]
    fn()
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return {"case": case, "label": label, "ms": round(statistics.median(times) * 1000, 1)}


def main():
    raw_mb = RAW.stat().st_size / 1e6
    gold_files = list(GOLD.glob("*/*/*.parquet"))
    gold_mb = sum(f.stat().st_size for f in gold_files) / 1e6
    print(f"JSON crudo: {raw_mb:.1f} MB (1 archivo) · Parquet Gold: {gold_mb:.1f} MB ({len(gold_files)} archivos)")
    print(f"{'variante':<26}{'mediana (ms)':>14}")
    results = [measure(case) for case in CASES]
    for r in results:
        print(f"{r['label']:<26}{r['ms']:>14.1f}")
    print(f"El pipeline completo (Parquet + DuckDB) es {results[0]['ms'] / results[-1]['ms']:.0f} veces más rápido que pandas sobre el crudo")


if __name__ == "__main__":
    main()
