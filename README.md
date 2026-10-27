# Pipeline Transfermarkt

Pipeline medallion (Bronze → Silver → Gold) orquestado con Airflow sobre datos crudos de Transfermarkt y consultado con DuckDB. El Gold es una tabla de apariciones de jugadores por partido, particionada por la fecha del partido.

- Cómo levantarlo: [docs/levantar_airflow.md](docs/levantar_airflow.md)
- Qué falta: [docs/pendientes.md](docs/pendientes.md)
- Historial y desvíos respecto de la consigna: [docs/procedimiento.md](docs/procedimiento.md)

## Dataset and recorte

**Fuente.** JSON lines crudos del scraper open source `transfermarkt-scraper`, tal como los publica el proyecto [`transfermarkt-datasets`](https://github.com/dcaribou/transfermarkt-datasets) (se bajan con `dvc pull`). Nunca se usan los CSV ya limpios.

**Recorte.** Liga Profesional Argentina (ARG1), LaLiga (ES1), Premier League (GB1) y Serie A (IT1), temporadas 2023 y 2024: **166 MB sin comprimir** (la consigna pide 50–300 MB). El crudo completo pesa ~15 GB.

| Entidad | Líneas | Qué es |
|---|---|---|
| appearances | 80.007 | Un jugador en un partido |
| games | 3.238 | Partidos |
| players | 5.393 (4.017 jugadores) | Perfil del jugador |
| clubs | 150 | Plantel, estadio, DT |
| competitions | 10 | Ligas, copas y supercopas |
| player_contacts | 4.101 | Contactos **sintéticos** (Faker, semilla fija): `player_id`, nombre, email, teléfono |

**Sistema fuente congelado.** El recorte se copia una sola vez a `tm_source/` (con `scripts/build_snapshot.py`), con un `manifest.json` de tamaños y SHA-256. El DAG lee solo de ahí y nunca de Internet, así que el pipeline es reproducible. `tm_source/` no está en el repo: el procedimiento para armarlo está en [docs/pendientes.md](docs/pendientes.md).

**Fecha de negocio vs. fecha de ingesta.** Son distintas a propósito:

| | Qué es | Valor | Se usa para |
|---|---|---|---|
| Fecha de negocio | Fecha del partido (`games.date`) | ago 2023 – mar 2026 | Partición de **Gold** y de los hechos de Silver |
| Fecha de ingesta | Cuándo se congeló el snapshot | 2026-10 | Partición de **Bronze** |

Los partidos cubren 32 meses, de los cuales 21 tienen partidos: de ahí salen las particiones mensuales.

Más detalle (exploración, imperfecciones del crudo, qué quedó afuera y por qué): [docs/procedimiento.md](docs/procedimiento.md).

## Architecture and DAG

```
validate_source >> land_bronze >> validate_bronze >> [silver_dimensions, silver_facts, mask_pii] >> build_gold >> update_catalog
```

DAG `transfermarkt_pipeline`, `schedule="@monthly"`, `start_date=2023-08-01`, `catchup=False`, `max_active_runs=1`. Cada corrida procesa **un mes calendario**; los meses históricos se cargan con backfill.

| Tarea | Qué hace | Por qué es una tarea aparte |
|---|---|---|
| `validate_source` | Compara cada archivo de `tm_source/` con el manifest (tamaño y SHA-256) | Si la fuente cambió, falla antes de tocar nada |
| `land_bronze` | Copia el snapshot sin modificar a `bronze/{tabla}/ingested_year=2026/ingested_month=10/` | Bronze es la copia fiel; no transforma |
| `validate_bronze` | Mismo hash que la fuente, sin archivos vacíos, cada línea es JSON válido | Detecta una copia corrupta antes de limpiar |
| `silver_dimensions` | Competitions, clubs y players: tipado y deduplicación | Las 3 tareas de Silver no dependen entre sí: corren en paralelo y se reintenta solo la que falla |
| `silver_facts` | Games y appearances del mes de la corrida | Idem |
| `mask_pii` | Enmascara `player_contacts` | Aísla el único paso que usa el secreto |
| `build_gold` | Join de 5 entidades, escribe la partición del mes | Necesita las tres tareas de Silver terminadas |
| `update_catalog` | Crea la vista `player_appearances` en DuckDB | Se ejecuta al final, cuando Gold ya existe |

Decisiones de diseño:

- **El mes de la corrida** sale de `data_interval_start`. En Airflow 3 las corridas manuales traen inicio igual a fin; con esta regla corrida manual, programada y backfill procesan el mismo mes.
- **Bronze copia el snapshot completo en cada corrida.** Su fecha de ingesta es fija, así que todas las corridas reescriben la misma partición con el mismo contenido.
- **El backfill necesita `--max-active-runs 1`.** Ignora el `max_active_runs` del DAG y varias corridas a la vez se pisarían al reescribir Bronze.
- La lógica vive en `dags/transfermarkt/` (módulos importados por el DAG, no DAGs); el DAG solo define las tareas.

```
tm_source/ ──► bronze/ ──► silver/ ──► gold/ ──► catalog.duckdb
 (fuente)    (copia fiel)  (limpio)  (analítico)   (vista SQL)
```

## Silver decisions: nulls and deduplication

Resumen. Cada regla, con su justificación y los casos reales que afecta, está en [docs/silver_decisiones.md](docs/silver_decisiones.md).

**Deduplicación**

| Tabla | Clave | Se queda con | Casos |
|---|---|---|---|
| games | `game_id` | Temporada más reciente | 0 |
| appearances | `game_id` + `player_id` | La de más minutos (los duplicados son copias exactas) | 29 |
| players | `player_id` | Perfil de la temporada más reciente | 1.379 versiones |
| clubs | `club_id` | Temporada más reciente | 51 |
| player_contacts | `player_id` | `updated_at` más reciente | 79 |

**Nulos**

| Campo | Regla | Por qué |
|---|---|---|
| Partido sin fecha o resultado ilegible | Se descarta | Sin fecha no se puede particionar (hoy: 0 casos) |
| `attendance` vacío | NULL | Falta el dato; un 0 bajaría los promedios |
| `goals`, `assists` vacíos | 0 | La fila existe porque el jugador jugó; vacío = no convirtió |
| Tarjetas | 1 si hay algo, 0 si vacío | El crudo trae el minuto o `✔`, no una cantidad |
| `current_market_value` | No pasa a Silver | Siempre vacío en el crudo |
| Email nulo en `player_contacts` | Se descarta el contacto, no el jugador | Sin email no hay nada que proteger ni con qué unir |

**Huérfanos.** Se recuperan en vez de descartarse: los jugadores que aparecen en un partido pero no en `players` se reconstruyen desde el perfil de la aparición (1.140 apariciones de 190 jugadores), y las competiciones y clubes sin ficha se agregan con `in_catalog = false`. Descartarlos perdería goles y minutos reales.

**Fechas.** El crudo mezcla `mes/día/año` y `día/mes/año`. Se prueban los dos formatos y se elige el que coincide con el día de la semana; si quedan ambos, gana el formato dominante del archivo. Cada partido guarda `date_raw` y `date_resolved_by` para auditar.

### PII

Transfermarkt no trae datos de contacto: se generan con `scripts/generate_pii.py`. En Silver:

- `email_hash = SHA-256(salt + email.strip().lower())`.
- `full_name` y `phone_number` **se eliminan**: no queda ningún dato en claro.
- El salt es una **Variable de Airflow** (`PII_SALT`), leída con `Variable.get` **dentro** de la tarea `mask_pii`, nunca a nivel de módulo ni en el repo.

Sin el salt, un analista con acceso a los Parquet no puede revertir un hash con un diccionario de emails conocidos. Detalle y respuestas de defensa: [docs/pii_y_salt.md](docs/pii_y_salt.md).

## Gold and partitioning

**Tabla `player_appearances`**: una fila por jugador por partido, desnormalizada para analizar sin joins.

```
appearances ⋈ games ⋈ players ⋈ clubs (club) ⋈ clubs (rival) ⋈ competitions
```

- `appearances` y `games` se unen con JOIN; `players`, `clubs` y `competitions` con **LEFT JOIN**, para no perder apariciones de copas o rivales de otras divisiones (`in_catalog = false`).
- Se agregan columnas derivadas: `goals_for`, `goals_against`, `result` (W/D/L desde el punto de vista del club) y `player_age`.
- Desnormalizar evita repetir cinco joins en cada consulta, y el Parquet columnar hace barato repetir el nombre del club en cada fila.
- El valor de mercado no está: el crudo no lo trae.
- `build_gold` falla si el join duplica o pierde filas respecto de Silver.

**Partición:** `gold/player_appearances/year=YYYY/month=MM/`, por la **fecha del partido** (no la de ingesta). Es la columna por la que se filtra al analizar y la que da un número razonable de particiones (21).

**Catálogo:** [sql/create_player_appearances_view.sql](sql/create_player_appearances_view.sql) crea en `data/catalog.duckdb` la vista `player_appearances` con `read_parquet(..., hive_partitioning = true)`. `year` y `month` aparecen como columnas.

```sql
SELECT club_name, sum(goals) FROM player_appearances
WHERE year = 2024 AND month = 3 GROUP BY 1;
```

## Idempotency and pruning

**Idempotencia.** Cada capa reemplaza su salida en lugar de agregar: Bronze borra su carpeta y la reescribe, Silver reemplaza el Parquet del mes (escribe a un temporal y renombra), y Gold borra la carpeta del mes (`shutil.rmtree`) antes de escribir. Nunca hay append.

Prueba con 2024-03 y **Clear** de la corrida (las 8 tareas se ejecutan de nuevo): mismas filas y mismos SHA-256 antes y después.

| | Filas | SHA-256 |
|---|---|---|
| Silver games | 112 | idéntico |
| Silver appearances | 3.445 | idéntico |
| Gold (1 archivo) | 3.445 | idéntico |

Salida completa: [docs/idempotencia_2024-03.md](docs/idempotencia_2024-03.md). Script: `scripts/idempotency_check.py`.

**Pruning.** `scripts/finops_validation.py` corre `EXPLAIN ANALYZE` sobre todo Gold y sobre un solo mes:

| Consulta | Archivos leídos | Filas leídas | Tiempo |
|---|---|---|---|
| Todo Gold | 21 | 79.978 | 0,027 s |
| `year = 2024 AND month = 3` | 1 | 3.445 | 0,005 s |

El filtro por las columnas de la partición hace que DuckDB descarte las otras 20 carpetas sin abrirlas. Salida completa: [docs/pruning_explain_analyze.txt](docs/pruning_explain_analyze.txt).

**Backfill.** 32 corridas (2023-08 a 2026-03), 21 meses con datos, **79.978 filas en Gold = las de Silver**:

```bash
docker compose exec airflow-scheduler airflow backfill create \
  --dag-id transfermarkt_pipeline --from-date 2023-08-01 --to-date 2026-03-01 --max-active-runs 1
```

## Migration to AWS

| Hoy (local) | En AWS |
|---|---|
| Carpetas `tm_source/`, `bronze/`, `silver/`, `gold/` | Buckets de S3 (landing, bronze, silver, gold) |
| Airflow en Docker | Amazon MWAA |
| Variable `PII_SALT` | AWS Secrets Manager como backend de secretos de Airflow |
| DuckDB sobre archivos locales | Athena con AWS Glue Data Catalog (las particiones `year=/month=` se reconocen igual) |
| Snapshot fuente | Bucket de landing |

En el código cambian las rutas (`s3://` con `pyarrow.fs` o `s3fs`, centralizadas en `config.py`) y el backend de secretos; la lógica de pandas, DuckDB y el diseño del DAG siguen igual.

## Estructura del repo

```
dags/            DAG transfermarkt_pipeline y módulos (bronze, silver, gold, parsing, storage, config)
scripts/         build_snapshot, generate_pii, idempotency_check, finops_validation
sql/             create_player_appearances_view.sql
docs/            procedimiento, decisiones de Silver, PII y salt, pruebas de la Fase 6, plan y pendientes
docker-compose.yaml
```

`data/`, `tm_source/`, `*.csv`, `*.parquet`, `*.duckdb` y `.env` están en `.gitignore`: ni datos ni secretos van al repo.
