# Pendientes

Qué está hecho y qué falta. Actualizado: 2026-10-27.
Detalle de cada fase en [plan_de_trabajo.md](plan_de_trabajo.md); historial y decisiones en [procedimiento.md](procedimiento.md).

## Estado

| Fase | Estado |
|---|---|
| 0 — Datos crudos y recorte | ✅ Hecho |
| 1 — Snapshot fuente y PII sintética | ✅ Hecho |
| 2 — Entorno y repo | ✅ Hecho |
| 3 — Bronze | ✅ Hecho |
| 4 — Silver | ✅ Hecho |
| 5 — Gold y DuckDB | 🟡 Hecho; falta acelerar cada corrida antes del backfill |
| 6 — Idempotencia y pruning | ⏳ Pendiente |
| 7 — README y defensa | 🟡 Parcial (los docs están; falta pasarlos al README) |

## Antes de empezar (cada integrante, una vez)

- [ ] Levantar Airflow con el `docker-compose.yaml` del repo ([levantar_airflow.md](levantar_airflow.md)).
- [ ] **Armar `tm_source/` en tu máquina**, porque no está en el repo:
  1. Bajar el crudo (`git clone` + `dvc pull`, ver Fase 0 en [procedimiento.md](procedimiento.md)).
  2. `python scripts/build_snapshot.py ... --snapshot-date 2026-10-26` (misma fecha para todos).
  3. `docker compose exec airflow-scheduler python /opt/airflow/scripts/generate_pii.py`
  4. Verificar que los SHA-256 de `tm_source/manifest.json` coincidan con los de un compañero.
- [ ] Cargar `PII_SALT` con el **mismo valor** que el resto ([pii_y_salt.md](pii_y_salt.md)).
- [ ] El DAG `transfermarkt_pipeline` puede estar en pausa: activarlo con el interruptor de la web.

## Fase 5 — Gold y DuckDB

- [x] Implementar `build_gold` (hoy solo muestra un mensaje): una fila por aparición, uniendo appearances + games + players + clubs + competitions.
- [x] Escribir en `data/gold/player_appearances/year=YYYY/month=MM/`. Antes de escribir, borrar la carpeta del mes.
- [x] Clubes y competiciones con `in_catalog = false` (copas, rivales de ascenso): unir con LEFT JOIN para no perder filas.
- [x] El valor de mercado no existe en el crudo: no ponerlo en Gold.
- [x] `update_catalog`: archivo `.sql` en `sql/` que crea la vista `player_appearances` en DuckDB con `read_parquet(..., hive_partitioning=true)`.
- [ ] **Acelerar cada corrida antes del backfill** (hoy ~2,5 min). Hoy Bronze y las dimensiones se reprocesan enteros en cada mes; por ejemplo, saltear `land_bronze` si la partición ya coincide con el manifest.
- [x] Borrar el DAG de prueba `transfermarkt_esqueleto`.
- [ ] Acordar el `dag_id`: el DAG real se llama `transfermarkt_pipeline`. Si alguien tiene otro archivo con ese nombre, se pisan.

## Fase 6 — Pruebas que pide la consigna

- [x] Backfill de todos los meses (hecho el 2026-10-27: 32 corridas, 21 meses con partidos, 79.978 filas en Gold = Silver):
  `docker compose exec airflow-scheduler airflow backfill create --dag-id transfermarkt_pipeline --from-date 2023-08-01 --to-date 2026-03-01 --max-active-runs 1`
  **Importante el `--max-active-runs 1`:** el backfill ignora el `max_active_runs` del DAG (usa 10) y varias corridas a la vez se pisan al reescribir Bronze (`Directory not empty`).
- [ ] Idempotencia: correr un mes dos veces (ej. 2024-03, con Clear en la web), contar filas en Silver y Gold y guardar ambas salidas en `docs/`.
- [ ] Pruning: `scripts/finops_validation.py` con `EXPLAIN ANALYZE` de la consulta completa vs. un solo mes. Guardar la salida en `docs/`.

## Fase 7 — Cierre

- [ ] Completar el README, cada sección a partir de los docs:
  - Dataset → [procedimiento.md](procedimiento.md);
  - Silver → [silver_decisiones.md](silver_decisiones.md);
  - PII → [pii_y_salt.md](pii_y_salt.md).
- [ ] Sección "Migration to AWS" (texto corto; ver el final de [plan_de_trabajo.md](plan_de_trabajo.md)).
- [ ] Pull Requests: `feature/*` → `develop`, y al final `develop` → `main`.
- [ ] Ensayar la defensa con las preguntas de la consigna (tabla de la Fase 7 del plan).

## Opcional

- [ ] Sumar la temporada 2025 del crudo. Cambió de formato, así que Silver necesita adaptarse (ver Fase 0 en [procedimiento.md](procedimiento.md)).
- [ ] Pasar de CeleryExecutor a LocalExecutor en el `docker-compose.yaml`: saca `redis` y `airflow-worker` y ahorra memoria.
