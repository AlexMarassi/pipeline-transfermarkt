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


def snapshot(data: Path, ym: str) -> dict:
    """Estado del mes: filas, SHA-256 y fecha de modificación de cada archivo, y cuántos archivos hay por partición Gold."""
    year, month = ym.split("-")
    files = {
        "silver games": [data / "silver" / "games" / f"{ym}.parquet"],
        "silver appearances": [data / "silver" / "appearances" / f"{ym}.parquet"],
    }
    for dataset in GOLD_DATASETS:
        files[f"gold {dataset}"] = sorted((data / "gold" / dataset / f"year={year}" / f"month={month}").glob("*.parquet"))
    out = []
    for table, paths in files.items():
        for f in paths:
            out.append({
                "table": table,
                "file": str(f.relative_to(data)),
                "rows": pq.read_metadata(f).num_rows,
                "sha256": sha256(f),
                "mtime": f.stat().st_mtime,
            })
    partitions = {}
    for dataset in GOLD_DATASETS:
        gold_dir = data / "gold" / dataset / f"year={year}" / f"month={month}"
        partitions[dataset] = len(list(gold_dir.iterdir())) if gold_dir.exists() else 0
    return {"ym": ym, "files": out, "partitions": partitions}


def main(ym: str):
    snap = snapshot(DATA, ym)
    print(f"Idempotencia del mes {ym}")
    print(f"{'tabla':<28}{'filas':>8}  {'archivo':<62}  sha256")
    for f in snap["files"]:
        print(f"{f['table']:<28}{f['rows']:>8}  {f['file']:<62}  {f['sha256'][:16]}…")
    for dataset, n in snap["partitions"].items():
        print(f"archivos en la partición Gold de {dataset}: {n}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "2024-03")
