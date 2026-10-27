# Guía para la defensa

Qué mostrar, en qué orden y qué responder. Las preguntas son las de la sección 7 de la consigna. Cada respuesta dice dónde está la prueba.

## 1. Antes de entrar (10 minutos antes)

- [ ] Docker Desktop abierto y `docker compose up -d`. Esperar a que `docker compose ps` muestre todo en `healthy`.
- [ ] Entrar a http://localhost:8080 (usuario y contraseña `airflow`). Confirmar que `transfermarkt_pipeline` aparece y está activado (interruptor encendido).
- [ ] Confirmar que existen `data/gold/` y `data/catalog.duckdb`. Si no, correr el backfill (ver más abajo).
- [ ] Abrir la interfaz de DuckDB (`duckdb -ui`, http://localhost:4213) y ejecutar una vez [demo_duckdb_ui.sql](demo_duckdb_ui.sql), con la ruta de `data/gold` de tu máquina en la primera línea.
- [ ] Tener a mano una terminal parada en la carpeta del repo.

Backfill, si hiciera falta (tarda unos 10 minutos):

```bash
docker compose exec airflow-scheduler airflow backfill create \
  --dag-id transfermarkt_pipeline --from-date 2023-08-01 --to-date 2026-03-01 --max-active-runs 1
```

## 2. Recorrido de la demo

| # | Qué hacés | Dónde | Qué decís |
|---|---|---|---|
| 1 | Mostrar el grafo del DAG | Airflow, DAG `transfermarkt_pipeline`, vista **Graph** | 8 tareas en 3 capas. Las tres de Silver corren en paralelo |
| 2 | Disparar una corrida | **▶ Trigger**, esperar el verde (unos 15 s) | Cada corrida procesa un mes calendario |
| 3 | Re-ejecutar el mismo mes | **Clear** sobre la corrida terminada | Es la prueba de idempotencia |
| 4 | Contar filas antes y después | Terminal: `docker compose exec airflow-scheduler python /opt/airflow/scripts/idempotency_check.py 2024-03` | Mismas filas y mismos hashes (ver [idempotencia_2024-03.md](idempotencia_2024-03.md)) |
| 5 | Mostrar el pruning | DuckDB UI (abajo) o `finops_validation.py 2024 3` | 21 archivos contra 1 |
| 6 | Mostrar datos | DuckDB UI: goleadores, tabla de LaLiga, Argentina | El Gold ya responde sin joins |

### Consultas para la interfaz de DuckDB

```sql
-- Máximos goleadores de la Premier League 2024
SELECT player_name, club_name, matches, goals, assists, goals_per_90
FROM top_scorers WHERE competition_id = 'GB1' AND season = 2024
ORDER BY goals DESC, assists DESC LIMIT 5;

-- Tabla de LaLiga 2024
SELECT club_name, matches, wins, draws, losses, points
FROM league_table WHERE competition_id = 'ES1' AND season = 2024
ORDER BY points DESC, goal_difference DESC LIMIT 5;

-- Argentina: Torneo Apertura (datos parciales)
SELECT club_name, matches, points, goal_difference
FROM league_table WHERE competition_id = 'ARG1'
ORDER BY points DESC, goal_difference DESC LIMIT 5;
```

### EXPLAIN ANALYZE: sin filtro y con filtro de partición

Correr las dos y buscar **Total Files Read** en el plano:

```sql
-- Sin filtro: lee todas las particiones
EXPLAIN ANALYZE
SELECT year, month, COUNT(*) AS apariciones
FROM read_parquet(getvariable('gold') || '/player_appearances/*/*/*.parquet', hive_partitioning = true)
GROUP BY year, month;

-- Con filtro: lee una sola
EXPLAIN ANALYZE
SELECT year, month, COUNT(*) AS apariciones
FROM read_parquet(getvariable('gold') || '/player_appearances/*/*/*.parquet', hive_partitioning = true)
WHERE year = 2024 AND month = 3
GROUP BY year, month;
```

Resultado esperado: **Total Files Read: 21** contra **Total Files Read: 1**, y 79.978 filas contra 3.445. La salida guardada está en [pruning_explain_analyze.txt](pruning_explain_analyze.txt).

## 3. Respuestas a las preguntas

### Idempotencia: ¿qué pasa si re-ejecuto un mes ya cargado?

Sale lo mismo: ningún dato se duplica. Cada capa **reemplaza** su salida del mes, nunca agrega:

- **Bronze:** borra su carpeta y vuelve a copiar el snapshot ([bronze.py](../dags/transfermarkt/bronze.py)).
- **Silver:** escribe el Parquet a un archivo temporal y lo renombra encima del anterior (`write_parquet` en [storage.py](../dags/transfermarkt/storage.py)). Si el mes no tiene filas, borra el archivo viejo.
- **Gold:** `build_gold` hace `shutil.rmtree` de la carpeta `year=/month=` de cada dataset **antes** de escribir ([gold.py](../dags/transfermarkt/gold.py)).

Como la partición se borra entera y se vuelve a escribir, un reintento a mitad de camino o un Clear no pueden dejar filas repetidas. Prueba: [idempotencia_2024-03.md](idempotencia_2024-03.md), con filas y SHA-256 idénticos antes y después, en Silver y en los cuatro datasets de Gold.

### Composición de tareas: ¿por qué estas tareas separadas?

- **Validar antes de transformar:** `validate_source` y `validate_bronze` fallan rápido si la fuente cambió o la copia está dañada, antes de gastar tiempo en limpiar.
- **Silver en tres tareas** (`silver_dimensions`, `silver_facts`, `mask_pii`): no dependen entre sí, así que corren en paralelo, y si una falla se reintenta solo esa.
- **`mask_pii` aislada:** es la única tarea que usa el secreto. Queda claro dónde se lee el salt.
- **Gold y catálogo separados:** `update_catalog` solo se ejecuta si Gold terminó.

### Higiene de datos: nulos y deduplicación

Abrir el README, sección *Silver decisions*. Tres ejemplos para elegir:

- **`attendance` vacío → NULL, no 0.** Falta el dato, no hubo cero espectadores; un 0 bajaría los promedios.
- **`goals` y `assists` vacíos → 0.** La fila existe porque el jugador jugó, y el campo vacío significa que no convirtió.
- **Email nulo → se descarta el contacto, no el jugador.** Sin email no hay nada que proteger ni con qué unir, pero el jugador sigue existiendo en `players`.

Deduplicación: por clave natural, y de forma programática (`drop_duplicates`).

| Tabla | Clave | Se queda con |
|---|---|---|
| appearances | `game_id` + `player_id` | La de más minutos (29 duplicados, copias exactas) |
| players | `player_id` | El perfil de la temporada más reciente |
| player_contacts | `player_id` | El de `updated_at` más reciente |

### Particionado y rendimiento

- **Clave elegida:** `year/month` de la **fecha del partido**.
- **Por qué no otras:** `competition_id` tiene pocos valores y mezcla ligas de tamaño muy distinto; `club_id` genera cientos de archivos chicos; `season` es demasiado grueso y casi no permite descartar nada. El mes da 21 particiones y es el filtro natural al analizar.
- **Prueba:** 21 archivos contra 1 ([pruning_explain_analyze.txt](pruning_explain_analyze.txt)).
- **Fecha de ingesta contra fecha de negocio:** la de ingesta es cuándo se congeló el snapshot (octubre 2026) y es una sola, por eso particiona **Bronze**. La de negocio es cuándo se jugó el partido (agosto 2023 a marzo 2026) y particiona **Gold**. Son muy distintas: si Gold usara la ingesta, todo caería en una sola carpeta y no habría pruning.

### Seguridad y gobernanza

- **Cómo se obtiene el salt:** `Variable.get("PII_SALT")` **dentro** de la tarea `mask_pii` ([transfermarkt_pipeline.py](../dags/transfermarkt_pipeline.py)), no arriba del archivo. Airflow relee los DAGs cada ~30 segundos; a nivel de módulo consultaría la base en cada relectura.
- **Si un analista lee los Parquet de Silver:** (1) SHA-256 es de una sola vía; (2) sin el salt no puede armar un diccionario de emails conocidos y comparar hashes; (3) en Silver no queda ningún dato en claro, porque nombre y teléfono se eliminan.
- **Por qué una Variable y no una constante:** una constante queda en el repositorio y en el historial de Git para siempre, aunque se borre después. La Variable vive solo en la base de Airflow, se cambia sin tocar código y en la nube se reemplaza por Secrets Manager.

Demostración rápida: `docker compose exec airflow-scheduler airflow variables get PII_SALT`. Más detalle en [pii_y_salt.md](pii_y_salt.md).

### Modelo de datos

- **Join:** `appearances ⋈ games ⋈ players ⋈ clubs (club y rival) ⋈ competitions`: 5 entidades de Silver (la consigna pide 4).
- **Por qué desnormalizar:** el analista no repite cinco joins en cada consulta, y el formato columnar de Parquet hace barato repetir el nombre del club en cada fila. Se paga con más espacio y con redundancia, a cambio de consultas simples.
- **LEFT JOIN** con players, clubs y competitions, para no perder apariciones de copas o rivales de otras divisiones.
- **Datasets derivados:** `match_results`, `player_month_stats` y `club_month_stats` salen de Silver y de ese mismo detalle. Así los goleadores y las tablas no recalculan los joins.

### Local contra nube

| Hoy | En AWS |
|---|---|
| Carpetas `data/` | S3 (landing, bronze, silver, gold) |
| Airflow en Docker | Amazon MWAA |
| Variable `PII_SALT` | AWS Secrets Manager |
| DuckDB sobre archivos | Athena con Glue Data Catalog |

En el código cambian las rutas (`s3://`, concentradas en [config.py](../dags/transfermarkt/config.py)) y el backend de secretos. La lógica de pandas y SQL no cambia.

## 4. Preguntas incómodas (mejor tenerlas pensadas)

| Si te preguntan… | Respuesta |
|---|---|
| ¿Por qué Docker y no `airflow standalone`? ¿Y Airflow 3.1.0 en vez de 3.0.4? | Para que todo el equipo tenga el mismo entorno con un solo archivo. Es un desvío documentado en [procedimiento.md](procedimiento.md) |
| ¿Por qué usás DuckDB para construir Gold? La consigna dice "sin motores externos" | Esa restricción está en la sección de Silver, que sí usa solo pandas y pyarrow. DuckDB ya es parte del stack y el join de 5 tablas es más simple en SQL |
| ¿Por qué Gold tiene goles raros en partidos de copa? | El crudo suma los penales al resultado (14:13 en vez de 2:2). Silver detecta esos partidos con `decided_by` y Gold deja `total_goals` en NULL y no les suma goles |
| ¿Por qué la tabla de Argentina está incompleta? | El crudo de 2025 viene incompleto, y las dos zonas del Apertura quedaron en una sola tabla. Es un ejemplo, no una tabla oficial |
| La asistencia de River dice siempre 85.018 | Transfermarkt carga la capacidad del estadio, no el público real |
| ¿Por qué el backfill necesita `--max-active-runs 1`? | Ignora el `max_active_runs` del DAG, y varias corridas a la vez se pisan al reescribir Bronze |
| ¿Dónde está el valor de mercado? | El crudo no lo trae (siempre vacío), así que no está en Silver ni en Gold |
| ¿Cómo sabés que los resultados son correctos? | Coinciden con la realidad: Liverpool 84 puntos en la Premier 2024, Barcelona 88 en LaLiga, Lautaro Martínez 24 goles en la Serie A 2023 |

## 5. Mapa de archivos

| Para mostrar… | Abrir |
|---|---|
| El DAG | [transfermarkt_pipeline.py](../dags/transfermarkt_pipeline.py) |
| Bronze, Silver y Gold | [bronze.py](../dags/transfermarkt/bronze.py), [silver.py](../dags/transfermarkt/silver.py), [gold.py](../dags/transfermarkt/gold.py) |
| Las vistas de DuckDB | [create_gold_views.sql](../sql/create_gold_views.sql) |
| Las reglas de nulos | README, sección *Silver decisions*, y [silver_decisiones.md](silver_decisiones.md) |
| Idempotencia | [idempotencia_2024-03.md](idempotencia_2024-03.md) |
| Pruning | [pruning_explain_analyze.txt](pruning_explain_analyze.txt) |
| PII y salt | [pii_y_salt.md](pii_y_salt.md) |
