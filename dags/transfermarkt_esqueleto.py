"""
Esqueleto del pipeline Transfermarkt: Bronze -> Silver -> Gold.
Las tareas solo imprimen lo que harían. La lógica real se agrega después.
"""
import time
from datetime import datetime

from airflow.sdk import dag, task, task_group, get_current_context


def periodo():
    """Devuelve el año y mes que procesa esta corrida."""
    ctx = get_current_context()
    fecha = ctx.get("logical_date") or ctx["dag_run"].run_after
    return fecha.year, fecha.month


def simular(mensaje):
    anio, mes = periodo()
    print(f"[{anio}-{mes:02d}] {mensaje}")
    time.sleep(2)


@dag(
    dag_id="transfermarkt_esqueleto",
    schedule="0 0 1 * *",          # una vez por mes, el día 1
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["tp", "transfermarkt", "medallion"],
)
def transfermarkt_esqueleto():

    # ---------- BRONZE ----------
    @task_group(group_id="bronze")
    def bronze():

        @task
        def validate_source():
            simular("Comparo checksums de los archivos fuente contra el manifest")

        @task
        def land_bronze():
            simular("Copio los JSON sin cambios a bronze/{tabla}/ingested_year=/ingested_month=")

        @task
        def validate_bronze():
            simular("Verifico que cada JSON sea válido y no esté vacío")

        validate_source() >> land_bronze() >> validate_bronze()

    # ---------- SILVER ----------
    @task_group(group_id="silver")
    def silver():

        @task
        def silver_dimensions():
            simular("Limpio players, clubs y competitions -> Parquet")

        @task
        def silver_facts():
            simular("Limpio games y appearances del mes -> Parquet")

        @task
        def mask_pii():
            simular("Hasheo emails con SHA-256 + salt")

        # Las tres corren en paralelo (sin >> entre ellas)
        silver_dimensions()
        silver_facts()
        mask_pii()

    # ---------- GOLD ----------
    @task_group(group_id="gold")
    def gold():

        @task
        def build_gold():
            simular("Uno las tablas de Silver y escribo gold/.../year=/month=")

        @task
        def update_catalog():
            simular("Actualizo la vista en DuckDB")

        build_gold() >> update_catalog()

    # Orden general: Bronze -> Silver -> Gold
    bronze() >> silver() >> gold()


transfermarkt_esqueleto()
