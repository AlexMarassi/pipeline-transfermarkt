"""Lectura de Bronze y escritura de Parquet (pyarrow)."""
import json
import logging
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from transfermarkt.config import BRONZE_DIR

log = logging.getLogger(__name__)


def latest_bronze_dir(table: str) -> Path:
    """Partición de ingesta más reciente de una tabla: bronze/{tabla}/ingested_year=…/ingested_month=…/"""
    parts = sorted((BRONZE_DIR / table).glob("ingested_year=*/ingested_month=*"))
    if not parts:
        raise FileNotFoundError(f"No hay datos en Bronze para '{table}'. ¿Corrió land_bronze?")
    return parts[-1]


def iter_bronze_json(table: str):
    """Recorre los JSON lines de una tabla en Bronze. Devuelve (nombre_archivo, registro)."""
    for f in sorted(latest_bronze_dir(table).glob("*.json")):
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    yield f.stem, json.loads(line)


def read_bronze_csv(table: str) -> pd.DataFrame:
    files = sorted(latest_bronze_dir(table).glob("*.csv"))
    return pd.concat([pd.read_csv(f, dtype=str, keep_default_na=False) for f in files], ignore_index=True)


def _column(values, typ: pa.DataType) -> pa.Array:
    """Convierte una columna de pandas al tipo exacto del esquema (nulos de pandas -> null)."""
    vals = [None if (v is None or (isinstance(v, float) and v != v) or v is pd.NaT) else v for v in values]
    if pa.types.is_integer(typ):
        vals = [None if v is None else int(v) for v in vals]
    elif pa.types.is_floating(typ):
        vals = [None if v is None else float(v) for v in vals]
    elif pa.types.is_boolean(typ):
        vals = [None if v is None else bool(v) for v in vals]
    elif pa.types.is_timestamp(typ):
        vals = [None if v is None else pd.Timestamp(v).to_pydatetime() for v in vals]
    return pa.array(vals, type=typ)


def to_arrow(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    """DataFrame -> tabla de pyarrow con el esquema fijo (mismas columnas y tipos en todos los meses)."""
    return pa.table({f.name: _column(df[f.name].tolist(), f.type) for f in schema}, schema=schema)


def write_parquet(df: pd.DataFrame, path: Path, schema: pa.Schema) -> int:
    """
    Escribe df con el esquema fijo, reemplazando el archivo si ya existe.
    Primero escribe a un temporal y después lo renombra: nunca queda un archivo a medias.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    table = to_arrow(df, schema)
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, tmp)
    os.replace(tmp, path)
    log.info("Escrito %s (%d filas)", path, table.num_rows)
    return table.num_rows


def remove_if_exists(path: Path):
    """Para meses sin datos: si quedó un archivo de una corrida anterior, se borra."""
    if path.exists():
        path.unlink()
        log.info("Borrado %s (el mes no tiene filas)", path)
