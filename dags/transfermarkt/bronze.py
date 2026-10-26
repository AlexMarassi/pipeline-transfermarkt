"""
Capa Bronze: copia el snapshot fuente sin modificar.

Destino: bronze/{tabla}/ingested_year=YYYY/ingested_month=MM/{archivo}

La fecha de ingesta sale de manifest.json (cuándo se congeló el snapshot), no del reloj.
Así, re-ejecutar el DAG cae siempre en la misma carpeta y no crea copias nuevas.
"""
import csv
import hashlib
import json
import logging
import shutil
from datetime import date
from pathlib import Path

from transfermarkt.config import BRONZE_DIR, MANIFEST_PATH, SOURCE_DIR, TABLES

log = logging.getLogger(__name__)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def ingestion_partition(manifest: dict) -> tuple[str, str]:
    """('2026', '10') a partir de snapshot_date del manifest."""
    d = date.fromisoformat(manifest["snapshot_date"])
    return f"{d.year:04d}", f"{d.month:02d}"


def bronze_partition_dir(table: str, year: str, month: str) -> Path:
    return BRONZE_DIR / table / f"ingested_year={year}" / f"ingested_month={month}"


def validate_source() -> dict:
    """Verifica que cada archivo del snapshot exista y coincida con el manifest (tamaño y SHA-256)."""
    manifest = load_manifest()
    errors = []
    for f in manifest["files"]:
        p = SOURCE_DIR / f["path"]
        if not p.exists():
            errors.append(f"falta {f['path']}")
            continue
        if p.stat().st_size != f["bytes"]:
            errors.append(f"{f['path']}: tamaño {p.stat().st_size} != {f['bytes']}")
            continue
        if sha256_of(p) != f["sha256"]:
            errors.append(f"{f['path']}: SHA-256 distinto al del manifest")

    entities = {f["entity"] for f in manifest["files"]}
    missing_tables = [t for t in TABLES if t not in entities]
    if missing_tables:
        errors.append(f"tablas sin archivos en el manifest: {missing_tables}")

    if errors:
        raise ValueError("El snapshot fuente no coincide con el manifest:\n" + "\n".join(errors))

    log.info("Snapshot OK: %d archivos, %.1f MB, congelado el %s",
             len(manifest["files"]), manifest["total_bytes"] / 1e6, manifest["snapshot_date"])
    return {"snapshot_date": manifest["snapshot_date"], "files": len(manifest["files"])}


def land_bronze() -> dict:
    """Copia cada archivo del snapshot a su partición de Bronze. Reescribe la partición entera."""
    manifest = load_manifest()
    year, month = ingestion_partition(manifest)
    landed = {}

    for table in TABLES:
        files = [f for f in manifest["files"] if f["entity"] == table]
        target = bronze_partition_dir(table, year, month)

        # Idempotencia: si la partición ya existe, se borra y se vuelve a escribir
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True)

        for f in files:
            src = SOURCE_DIR / f["path"]
            shutil.copyfile(src, target / src.name)   # copia byte a byte, sin transformar
        landed[table] = [str(target / Path(f["path"]).name) for f in files]
        log.info("%s: %d archivo(s) en %s", table, len(files), target)

    return {"ingested_year": year, "ingested_month": month, "tables": list(landed)}


def validate_bronze() -> dict:
    """Bronze tiene que ser una copia exacta: mismos SHA-256, sin vacíos, JSON/CSV legibles."""
    manifest = load_manifest()
    year, month = ingestion_partition(manifest)
    errors = []
    summary = {}

    for f in manifest["files"]:
        p = bronze_partition_dir(f["entity"], year, month) / Path(f["path"]).name
        if not p.exists():
            errors.append(f"falta en Bronze: {p}")
            continue
        if p.stat().st_size == 0:
            errors.append(f"archivo vacío: {p}")
            continue
        if sha256_of(p) != f["sha256"]:
            errors.append(f"{p}: el contenido cambió respecto de la fuente")

        if p.suffix == ".json":
            n = 0
            with open(p, encoding="utf-8") as fh:
                for i, line in enumerate(fh, start=1):
                    if not line.strip():
                        continue
                    try:
                        json.loads(line)
                    except json.JSONDecodeError as e:
                        errors.append(f"{p.name} línea {i}: JSON inválido ({e.msg})")
                        break
                    n += 1
        else:  # CSV
            with open(p, encoding="utf-8", newline="") as fh:
                n = sum(1 for _ in csv.DictReader(fh))

        if n != f["lines"]:
            errors.append(f"{p.name}: {n} registros, el manifest dice {f['lines']}")
        summary[f["path"]] = n

    if errors:
        raise ValueError("Bronze no pasó la validación:\n" + "\n".join(errors))

    log.info("Bronze OK: %d archivos validados", len(summary))
    return {"files": len(summary), "records": sum(summary.values())}
