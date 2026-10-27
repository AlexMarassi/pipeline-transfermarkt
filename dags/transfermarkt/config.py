"""
Rutas y parámetros del pipeline.

Las rutas se pueden cambiar con variables de entorno, sin tocar el código
(por ejemplo, para correr fuera de Docker o, más adelante, apuntar a S3).
"""
import os
from pathlib import Path

# Snapshot fuente (sistema "origen", solo lectura)
SOURCE_DIR = Path(os.environ.get("TM_SOURCE_DIR", "/opt/airflow/tm_source"))
MANIFEST_PATH = SOURCE_DIR / "manifest.json"

# Capas del medallion
DATA_DIR = Path(os.environ.get("TM_DATA_DIR", "/opt/airflow/data"))
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Tablas que entran a Bronze (cada una es una subcarpeta de tm_source/)
TABLES = ["competitions", "clubs", "games", "players", "appearances", "player_contacts"]

# Catálogo DuckDB: vista sobre el Gold particionado, y los .sql que la crean
SQL_DIR = Path(os.environ.get("TM_SQL_DIR", "/opt/airflow/sql"))
CATALOG_PATH = DATA_DIR / "catalog.duckdb"
