"""
Prueba de idempotencia de un mes: cuenta filas en Silver y Gold y calcula el SHA-256 de cada archivo.

Se corre antes y después de re-ejecutar el mes (Clear en la web de Airflow). Si el pipeline es
idempotente, las dos salidas son idénticas: mismas filas, mismos hashes, mismos archivos.

Uso (dentro del contenedor, desde la raíz del repo):
    docker compose exec airflow-scheduler python /opt/airflow/scripts/idempotency_check.py 2024-03
"""
import hashlib
import sys
from pathlib import Path

import pyarrow.parquet as pq

DATA = Path("/opt/airflow/data")
GOLD_DATASETS = ["player_appearances", "match_results", "player_month_stats", "club_month_stats"]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(ym: str):
    year, month = ym.split("-")
    files = {
        "silver games": [DATA / "silver" / "games" / f"{ym}.parquet"],
        "silver appearances": [DATA / "silver" / "appearances" / f"{ym}.parquet"],
    }
    for dataset in GOLD_DATASETS:
        files[f"gold {dataset}"] = sorted((DATA / "gold" / dataset / f"year={year}" / f"month={month}").glob("*.parquet"))
    print(f"Idempotencia del mes {ym}")
    print(f"{'tabla':<28}{'filas':>8}  {'archivo':<62}  sha256")
    for table, paths in files.items():
        for f in paths:
            rows = pq.read_metadata(f).num_rows
            print(f"{table:<28}{rows:>8}  {f.relative_to(DATA)!s:<62}  {sha256(f)[:16]}…")
    for dataset in GOLD_DATASETS:
        gold_dir = DATA / "gold" / dataset / f"year={year}" / f"month={month}"
        print(f"archivos en la partición Gold de {dataset}: {len(list(gold_dir.iterdir()))}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "2024-03")
