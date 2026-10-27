# Pipeline Transfermarkt

Medallion pipeline (Bronze → Silver → Gold) orchestrated with Airflow over the raw Transfermarkt data, queried with DuckDB.

Cómo levantarlo: [docs/levantar_airflow.md](docs/levantar_airflow.md) · Qué falta: [docs/pendientes.md](docs/pendientes.md)

## Dataset and recorte
TODO (step 3)

**Estado:** ✅ Hecho — falta pasarlo acá. Fuente: [docs/procedimiento.md](docs/procedimiento.md) (Fase 0 y 1).

## Architecture and DAG
TODO (step 2+)

**Estado:** 🟡 Parcial — DAG de 8 tareas armado; Bronze y Silver funcionan, Gold pendiente.

## Silver decisions: nulls and deduplication
TODO (steps 6–8)

**Estado:** ✅ Hecho — falta pasarlo acá. Fuente: [docs/silver_decisiones.md](docs/silver_decisiones.md) y [docs/pii_y_salt.md](docs/pii_y_salt.md).

## Gold and partitioning
TODO (step 9)

**Estado:** ⏳ Pendiente (Fase 5).

## Idempotency and pruning
TODO (steps 10–11)

**Estado:** 🟡 Parcial — idempotencia verificada en Bronze y Silver; falta Gold, la prueba formal y `EXPLAIN ANALYZE` (Fase 6).

## Migration to AWS
TODO (step 12)

**Estado:** ⏳ Pendiente — borrador al final de [docs/plan_de_trabajo.md](docs/plan_de_trabajo.md).
