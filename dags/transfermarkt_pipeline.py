"""
Pipeline Transfermarkt: Bronze -> Silver -> Gold, una corrida por mes de partidos.

Cada corrida procesa la ventana [data_interval_start, data_interval_end), es decir, un mes.
Los meses históricos se cargan con backfill.

Estado:
  - Bronze: implementado (scripts en dags/transfermarkt/bronze.py)
  - Silver y Gold: todavía de prueba, solo muestran la ventana que van a procesar
"""
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, get_current_context, task

# Permite importar dags/transfermarkt/ cuando la tarea corre (Airflow 3 no lo agrega solo)
sys.path.insert(0, str(Path(__file__).parent))

log = logging.getLogger(__name__)


def window():
    """
    Mes que procesa esta corrida: [primer día del mes, primer día del mes siguiente).

    Se toma el mes calendario de data_interval_start. En corridas programadas y backfills
    coincide con la ventana de Airflow; en corridas manuales (donde Airflow 3 da una ventana
    vacía) usa el mes de la fecha lógica elegida.
    """
    ctx = get_current_context()
    start = ctx["data_interval_start"].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (start + timedelta(days=32)).replace(day=1)
    return start, end


@dag(
    dag_id="transfermarkt_pipeline",
    schedule="@monthly",
    start_date=datetime(2023, 8, 1),     # primer mes con partidos en el recorte
    catchup=False,                       # los meses viejos se cargan con backfill
    max_active_runs=1,                   # una corrida a la vez: Bronze reescribe la misma partición
    tags=["tp", "transfermarkt", "medallion"],
)
def transfermarkt_pipeline():

    # ---------------- BRONZE ----------------
    @task
    def validate_source():
        from transfermarkt.bronze import validate_source as run
        return run()

    @task
    def land_bronze():
        from transfermarkt.bronze import land_bronze as run
        return run()

    @task
    def validate_bronze():
        from transfermarkt.bronze import validate_bronze as run
        return run()

    # ---------------- SILVER (de prueba) ----------------
    @task
    def silver_dimensions():
        log.info("TODO Fase 4: competitions, clubs y players -> Parquet")

    @task
    def silver_facts():
        start, end = window()
        log.info("TODO Fase 4: games y appearances con fecha en [%s, %s)", start.date(), end.date())

    @task
    def mask_pii():
        log.info("TODO Fase 4: player_contacts con email hasheado (SHA-256 + PII_SALT)")

    # ---------------- GOLD (de prueba) ----------------
    @task
    def build_gold():
        start, _ = window()
        log.info("TODO Fase 5: gold/player_appearances/year=%04d/month=%02d/", start.year, start.month)

    @task
    def update_catalog():
        log.info("TODO Fase 5: vista player_appearances en DuckDB")

    (
        validate_source()
        >> land_bronze()
        >> validate_bronze()
        >> [silver_dimensions(), silver_facts(), mask_pii()]
        >> build_gold()
        >> update_catalog()
    )


transfermarkt_pipeline()
