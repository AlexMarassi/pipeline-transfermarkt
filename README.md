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
| `build_gold` | Escribe la partición del mes de los 4 datasets de Gold | Necesita las tres tareas de Silver terminadas |
| `update_catalog` | Crea las vistas del catálogo DuckDB | Se ejecuta al final, cuando Gold ya existe |

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

Resumen de las reglas de Silver. El detalle completo (incluido el tipado y la resolución de fechas) está en [docs/silver_decisiones.md](docs/silver_decisiones.md).

**Deduplicación**

| Tabla | Clave | Se queda con | Casos |
|---|---|---|---|
| games | `game_id` | Temporada más reciente | 0 |
| appearances | `game_id` + `player_id` | La de más minutos (los duplicados son copias exactas) | 29 |
| players | `player_id` | Perfil de la temporada más reciente | 1.379 versiones |
| clubs | `club_id` | Temporada más reciente | 51 |
| player_contacts | `player_id` | `updated_at` más reciente | 79 |

**Nulos y vacíos.** Cada decisión, con su justificación de negocio (las cantidades son las observadas en el crudo):

| Tabla.campo | Regla | Justificación |
|---|---|---|
| games.`game_date` sin fecha válida | **Se descarta el partido** | Sin fecha no se puede particionar ni ubicar en el tiempo. Hoy son 0 casos |
| games.`result` no parseable | **Se descarta el partido** | Un partido sin resultado no sirve para análisis. Hoy son 0 casos |
| games.`attendance` vacío (247) | **NULL** | Falta el dato, no hubo 0 espectadores. Un 0 bajaría los promedios |
| games.`referee`, `stadium` vacíos | **NULL** | Dato descriptivo; no afecta uniones ni métricas |
| games.`half_time_score` | **No pasa a Silver** como tal; se usa para `decided_by` | Viene roto (`"("` en 1.623 filas, vacío en 1.526), pero cuando dice `on pens` o `AET` indica cómo se definió el partido |
| games.`kickoff_time` | **No pasa a Silver** | Siempre vacío |
| appearances.`goals`, `assists` vacíos | **0** | La fila existe porque el jugador jugó; el campo vacío es "no convirtió". Los valores no vacíos son siempre números ≥ 1 |
| appearances.tarjetas | **1 si hay algo, 0 si está vacío** | El crudo trae el *minuto* de la tarjeta (`"90 + 4'"`) o `✔`, no una cantidad |
| appearances.`pos` vacío (530 en marzo 2024) | **NULL** | Posición desconocida; la del perfil del jugador sigue disponible en players |
| players.`current_market_value` | **No pasa a Silver** | Siempre vacío en el crudo |
| players.`contract_expires` = `-` | **NULL** | Sin contrato informado (ej. libre). No se inventa una fecha |
| players.`current_club` = club 515 | **NULL** | 515 es el club "sin club" de Transfermarkt |
| players.`height`, `foot`, `number` vacíos | **NULL** | Dato descriptivo faltante |
| players.`name` vacío (130) | `player_name` = solo el apellido | Jugadores conocidos por un solo nombre |
| clubs.`total_market_value` | **No pasa a Silver** | Siempre vacío |
| clubs.`coach_name` vacío | **NULL** | Dato descriptivo |
| competitions.`total_clubs` en copas | **NULL** | Viene copiado de la liga del país (Copa del Rey = 20 clubes), no es real |
| player_contacts.`email` vacío (106) | **Se descarta el contacto, no el jugador** | Sin email no hay nada que proteger ni con qué unir. El jugador sigue en players |

**Huérfanos.** Se recuperan en vez de descartarse: los jugadores que aparecen en un partido pero no en `players` se reconstruyen desde el perfil de la aparición (1.140 apariciones de 190 jugadores), y las competiciones y clubes sin ficha se agregan con `in_catalog = false`. Descartarlos perdería goles y minutos reales.

**Fechas.** El crudo mezcla `mes/día/año` y `día/mes/año`. Se prueban los dos formatos y se elige el que coincide con el día de la semana; si quedan ambos, gana el formato dominante del archivo. Cada partido guarda `date_raw` y `date_resolved_by` para auditar.

### PII

Transfermarkt no trae datos de contacto: se generan con `scripts/generate_pii.py`. En Silver:

- `email_hash = SHA-256(salt + email.strip().lower())`.
- `full_name` y `phone_number` **se eliminan**: no queda ningún dato en claro.
- El salt es una **Variable de Airflow** (`PII_SALT`), leída con `Variable.get` **dentro** de la tarea `mask_pii`, nunca a nivel de módulo ni en el repo.

Sin el salt, un analista con acceso a los Parquet no puede revertir un hash con un diccionario de emails conocidos. Detalle y respuestas de defensa: [docs/pii_y_salt.md](docs/pii_y_salt.md).

## Gold and partitioning

Gold tiene **cuatro datasets** en `gold/{dataset}/year=YYYY/month=MM/`, todos particionados por la **fecha del partido** (no la de ingesta). Cada uno responde a un tipo de pregunta:

| Dataset | Una fila por… | Filas | Sirve para |
|---|---|---|---|
| `player_appearances` | jugador por partido | 79.978 | El detalle: cualquier análisis sin joins |
| `match_results` | partido | 3.145 | Resultados, goleadas, asistencia, tarjetas |
| `player_month_stats` | jugador y club por mes | 26.589 | Goles, asistencias y minutos mensuales |
| `club_month_stats` | club, competición y mes | 2.465 | Partidos, puntos, goles a favor y en contra |

**`player_appearances`** es el dataset principal: une `appearances ⋈ games ⋈ players ⋈ clubs (club) ⋈ clubs (rival) ⋈ competitions`, o sea 5 entidades de Silver (la consigna pide 4).

- `appearances` y `games` se unen con JOIN; `players`, `clubs` y `competitions` con **LEFT JOIN**, para no perder apariciones de copas o rivales de otras divisiones (`in_catalog = false`).
- Se agregan columnas derivadas: `goals_for`, `goals_against`, `result` (W/D/L desde el punto de vista del club), `player_age` y `decided_by`.
- Desnormalizar evita repetir cinco joins en cada consulta, y el Parquet columnar hace barato repetir el nombre del club en cada fila.
- El valor de mercado no está: el crudo no lo trae.

Los otros tres se derivan de Silver y de ese mismo detalle (`player_month_stats` agrega `player_appearances`; `match_results` y `club_month_stats` salen de `games`). Así la analítica común (goleadores, tabla de posiciones, goleadas) no recalcula los joins cada vez.

**Penales.** En los partidos de copa definidos por penales el crudo suma los penales al resultado. Esos partidos tienen `total_goals` en NULL y no suman goles a los clubes (ver [docs/silver_decisiones.md](docs/silver_decisiones.md)).

**Partición:** `year=/month=` por la fecha del partido. Es la columna por la que se filtra al analizar y la que da un número razonable de particiones (21 meses con partidos). Ingesta y negocio son fechas distintas: Bronze se particiona por cuándo se congeló el snapshot (2026-10) y Gold por cuándo se jugó el partido.

**Catálogo:** [sql/create_gold_views.sql](sql/create_gold_views.sql) crea en `data/catalog.duckdb` una vista por dataset, con `read_parquet(..., hive_partitioning = true)`, y tres vistas analíticas listas para consultar:

| Vista | Qué muestra |
|---|---|
| `top_scorers` | Máximos goleadores por temporada y competición |
| `highest_scoring_matches` | Partidos con más goles (sin los definidos por penales) |
| `league_table` | Tabla de posiciones por temporada y liga |

```sql
-- Máximos goleadores de la Premier League 2024
SELECT player_name, club_name, matches, goals, assists, goals_per_90
FROM top_scorers WHERE competition_id = 'GB1' AND season = 2024
ORDER BY goals DESC LIMIT 3;

-- Tabla de LaLiga 2024
SELECT club_name, matches, points, goal_difference
FROM league_table WHERE competition_id = 'ES1' AND season = 2024
ORDER BY points DESC, goal_difference DESC LIMIT 3;
```

Los resultados coinciden con la realidad, lo que valida el modelo: Liverpool 84 puntos en la Premier League 2024, Barcelona 88 en LaLiga y Lautaro Martínez con 24 goles en la Serie A 2023.

## Idempotency and pruning

**Idempotencia.** Cada capa reemplaza su salida en lugar de agregar: Bronze borra su carpeta y la reescribe, Silver reemplaza el Parquet del mes (escribe a un temporal y renombra), y Gold borra la carpeta del mes (`shutil.rmtree`) antes de escribir. Nunca hay append.

Prueba con 2024-03 y **Clear** de la corrida (las 8 tareas se ejecutan de nuevo): mismas filas y mismos SHA-256 antes y después, en Silver y en los cuatro datasets de Gold, cada uno con un solo archivo por partición.

| | Filas | SHA-256 |
|---|---|---|
| Silver games | 112 | idéntico |
| Silver appearances | 3.445 | idéntico |
| Gold `player_appearances` | 3.445 | idéntico |
| Gold `match_results` | 112 | idéntico |
| Gold `player_month_stats` | 1.216 | idéntico |
| Gold `club_month_stats` | 68 | idéntico |

Salida completa: [docs/idempotencia_2024-03.md](docs/idempotencia_2024-03.md). Script: `scripts/idempotency_check.py`.

**Pruning.** `scripts/finops_validation.py` corre `EXPLAIN ANALYZE` sobre todo Gold y sobre un solo mes:

| Consulta | Archivos leídos | Filas leídas | Tiempo |
|---|---|---|---|
| Todo Gold | 21 | 79.978 | 0,027 s |
| `year = 2024 AND month = 3` | 1 | 3.445 | 0,005 s |

El filtro por las columnas de la partición hace que DuckDB descarte las otras 20 carpetas sin abrirlas. Salida completa: [docs/pruning_explain_analyze.txt](docs/pruning_explain_analyze.txt).

**Backfill.** 32 corridas (2023-08 a 2026-03), 21 meses con datos, **79.978 apariciones en Gold = las de Silver**:

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
sql/             create_gold_views.sql
docs/            procedimiento, decisiones de Silver, PII y salt, pruebas de la Fase 6, plan y pendientes
docker-compose.yaml
```

`data/`, `tm_source/`, `*.csv`, `*.parquet`, `*.duckdb` y `.env` están en `.gitignore`: ni datos ni secretos van al repo.
