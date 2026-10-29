# Procedimiento

Registro de lo que se fue haciendo, en orden, y de **en qué nos apartamos del [plan de trabajo](plan_de_trabajo.pdf) o de la consigna**, con el motivo.

## Diferencias con el plan y la consigna

| # | Plan / consigna | Qué hicimos | Motivo |
|---|---|---|---|
| 1 | Airflow con WSL2 + `airflow standalone` | Airflow con Docker (`docker-compose.yaml` en la raíz del repo) | Todos usan el mismo entorno con un solo archivo. Ver [levantar_airflow.md](levantar_airflow.md) |
| 2 | Airflow 3.0.4 | Airflow 3.1.0 (imagen `apache/airflow:3.1.0`) | Es la versión con la que se instaló. Se puede cambiar en la línea `image:` |
| 3 | Datos en `~/airflow/data/` | `./data/` del repo (dentro de Airflow: `/opt/airflow/data/`) | Con Docker, la carpeta se comparte entre la PC y el contenedor. Mismo layout bronze / silver / gold |
| 4 | Snapshot en `~/tm_source/`, fuera del repo | `./tm_source/` en la raíz del repo, ignorado por Git (dentro de Airflow: `/opt/airflow/tm_source/`) | Airflow en Docker solo ve carpetas montadas. Sigue sin subirse al repo |
| 5 | Snapshot comprimido (`.json.gz`) | JSON lines **sin comprimir**, un archivo por entidad y temporada | El tamaño de la consigna (50–300 MB) es sin comprimir, y Bronze queda en formato nativo legible |
| 6 | Recorte: Argentina + 2 o 3 ligas europeas | España, Inglaterra, Italia y Argentina; temporadas 2023 y 2024 | Argentina solo está en el crudo para 2024 y 2025. 2025 cambió de formato y está incompleta. Ver Fase 0 |
| 7 | Opción B (correr el scraper) | No se hizo | La opción A alcanza para cumplir la consigna |
| 8 | `player_contacts`: `player_id, full_name, email, phone_number` | Se agregó `updated_at` y algunas imperfecciones a propósito | Da un criterio para deduplicar (el más reciente) y casos reales de limpieza en Silver |
| 9 | Scripts corren en la máquina de cada uno | `generate_pii.py` corre dentro del contenedor de Airflow | Faker ya está instalado ahí; nadie tiene que instalar nada extra |
| 10 | Ventana de cada corrida = `data_interval_start` → `data_interval_end` | Cada corrida procesa el **mes calendario de `data_interval_start`** | En Airflow 3 las corridas manuales traen inicio = fin (ventana vacía). Con esta regla, manual, programada y backfill procesan el mismo mes |
| 11 | — | Bronze copia el snapshot completo en cada corrida (no solo el mes) | El snapshot es uno solo y su fecha de ingesta es fija: todas las corridas reescriben la misma partición con el mismo contenido. `max_active_runs=1` evita que dos corridas escriban a la vez |
| 12 | Silver con pandas + pyarrow | Silver con pandas + pyarrow; **Gold** arma sus joins con SQL de DuckDB dentro de la tarea y escribe con pyarrow | La restricción de pandas + pyarrow es para Silver. En Gold, los joins de 5 entidades son más claros en SQL, y DuckDB es el mismo motor que después consulta el catálogo |
| 13 | Un dataset Gold | Cuatro datasets Gold con la misma partición (`player_appearances`, `match_results`, `player_month_stats`, `club_month_stats`) | `player_appearances` cumple el join de 4+ entidades; los otros tres dejan listas las consultas habituales (resultados, goleadores, tabla de posiciones) |
| 14 | — | Los meses con partidos pero sin apariciones (Copa de la Liga inglesa, julio 2025 a marzo 2026) no generan partición Gold | `build_gold` necesita las apariciones del mes. Esos 93 partidos quedan en Silver. Gold cubre agosto 2023 a mayo 2025 (21 particiones) |

## Registro

### 2026-10-26 — Fase 0: extracción y exploración del crudo

**Descarga (opción A).**
- Se clonó `https://github.com/dcaribou/transfermarkt-datasets` en `C:\Users\Leandro\tm_work\` (fuera del repo).
- Se instaló DVC en un entorno aparte y se corrió `dvc pull data/raw/transfermarkt-scraper.dvc`.
- Resultado: 74 archivos `.json.gz`, 527 MB comprimidos, **~15 GB sin comprimir**. Temporadas 2012–2025; por temporada hay games, appearances, clubs, players y game_lineups.
- `data/competitions.json` del mismo proyecto también es salida cruda del scraper (JSON lines): se usa como entidad `competitions`.
- En Windows, 4 archivos de la app Streamlit del proyecto no se pudieron escribir (tienen `:` en el nombre). No afectan a los datos.

**Tamaño por liga y temporada (sin comprimir).** Cada liga europea pesa ~20–27 MB por temporada en 2023–2024. En 2022 y antes pesa ~4 veces más, porque cada aparición trae el historial de valor de mercado del jugador. Casi todo el peso está en `appearances`, porque cada línea repite el perfil completo del jugador.

**Recorte elegido:** ES1, GB1, IT1, ARG1 × temporadas 2023 y 2024.
- **Argentina (ARG1)** solo existe en el crudo para 2024 y 2025.
- **2025** tiene otro formato:
  - `game_id` directo;
  - fechas `DD/MM/YYYY`;
  - `result` como texto;
  - la temporada está incompleta.

  Queda fuera para no mezclar dos esquemas.
- **Total: 166 MB sin comprimir.**

**Snapshot.** Se armó con [`scripts/build_snapshot.py`](../scripts/build_snapshot.py):
- filtra por liga y temporada y copia cada línea **sin modificar**;
- escribe `tm_source/{entidad}/{temporada}.json` y `tm_source/manifest.json` (tamaño, líneas, SHA-256, origen, commit y fecha de congelamiento `2026-10-26`).

| Archivo | MB | Líneas |
|---|---|---|
| appearances/2023.json | 65.7 | 38.728 |
| appearances/2024.json | 68.9 | 41.279 |
| games/2023.json | 10.5 | 1.526 |
| games/2024.json | 12.3 | 1.712 |
| players/2023.json | 3.5 | 2.308 |
| players/2024.json | 5.3 | 3.085 |
| clubs/2023.json | 0.04 | 60 |
| clubs/2024.json | 0.06 | 90 |
| competitions/competitions.json | 0.01 | 10 |

Para rearmarlo en otra compu (después de bajar el crudo como arriba):

```powershell
python scripts/build_snapshot.py `
  --raw C:/Users/<usuario>/tm_work/transfermarkt-datasets/data/raw/transfermarkt-scraper `
  --competitions-file C:/Users/<usuario>/tm_work/transfermarkt-datasets/data/competitions.json `
  --out tm_source --snapshot-date 2026-10-26
```

Usar la misma `--snapshot-date` para que la fecha de ingesta de Bronze sea igual en todas las máquinas.

**Imperfecciones encontradas (insumo para Silver).**

| Tabla | Hallazgo | Ejemplo |
|---|---|---|
| games | **Fechas en dos formatos** en 2024: `%a, %m/%d/%y` y `%a, %d/%m/%y`. El día de la semana del prefijo permite desambiguar | `Sat, 2/24/24` vs. `Wed, 30/10/24` |
| games | Resultado como texto | `"2:2"` |
| games | Asistencia como texto, a veces vacía (19 en 2023, 228 en 2024) | `"Attendance: 21.341"` |
| games | Posición en la tabla con espacios y saltos de línea | `"\n   Position:   14   "` |
| games | ~570 partidos con club local que no está en `clubs` (huérfanos) | — |
| appearances | Goles, asistencias y tarjetas vacíos en vez de 0 | `"goals": ""` |
| appearances | Minutos como texto con apóstrofe | `"90'"` |
| appearances | 29 duplicados (mismo jugador, mismo partido) en 2024 | — |
| appearances | ~1.970 apariciones de jugadores que no están en `players` | — |
| players | 1 jugador duplicado en 2024 | — |
| players | Altura como texto con espacio especial (NBSP) | `"1,90 m"` |
| players | Fechas como texto | `"Jun 18, 2002"` |
| players | Valor de mercado siempre vacío | `null` |
| clubs | Números como texto | `"8.396 Seats"`, `"58.6 %"`, `"+€1.40m"` |
| competitions | ~~Cada liga europea aparece 3 veces~~ **Corregido en Fase 4:** no son duplicados. `country_code` es el código del país; las 3 filas son liga, copa y supercopa | `ES1` = LaLiga, Copa del Rey, Supercopa |
| games | Incluye partidos de copas nacionales (FA Cup, Copa del Rey, Copa de la Liga, etc.), no solo de liga | 780 de 3.238 partidos (2.458 son de liga) |
| competitions | Espacios sobrantes en nombres | `"Torneo Apertura "` |

**Cobertura temporal (fecha del partido):** de agosto 2023 a mediados de 2025, más algunos partidos sueltos. Da más de 20 particiones mensuales para mostrar pruning. El conteo exacto por mes se confirma en Silver, después de resolver el formato de fechas.

### 2026-10-26 — Fase 1: PII sintética

Transfermarkt no trae datos de contacto, así que se generan con [`scripts/generate_pii.py`](../scripts/generate_pii.py).

- **Qué genera**: `tm_source/player_contacts/player_contacts.csv` con columnas `player_id, full_name, email, phone_number, updated_at`. Toma los 4.017 jugadores únicos de `tm_source/players/`.
- **Determinístico**: la semilla de Faker depende del `player_id`. Dos corridas dan el mismo archivo (SHA-256 `94e95e7e…d3d239`). El script también registra el archivo en `manifest.json`.
- **Dónde corre**: dentro de Airflow, que ya tiene Faker:
  ```powershell
  docker compose exec airflow-scheduler python /opt/airflow/scripts/generate_pii.py
  ```

**Imperfecciones agregadas a propósito** (para que Silver tenga reglas que aplicar):

| Imperfección | Cantidad | Regla prevista en Silver |
|---|---|---|
| Email nulo | 106 | Se descarta el contacto, no el jugador |
| Email en mayúsculas o con espacios | ~10% | `strip().lower()` antes de hashear |
| Mismo jugador con 2 filas (contacto actualizado) | 84 | Se queda la fila con `updated_at` más reciente |

**Diferencia con el plan:** se agregó la columna `updated_at`, que no estaba en el plan. Sirve para que la deduplicación tenga un criterio claro ("el más reciente").

**Salt:** cargado como Variable `PII_SALT` (ver [pii_y_salt.md](pii_y_salt.md)). Cada integrante lo carga en su Airflow (el valor se comparte por fuera del repo):
```powershell
docker compose exec airflow-scheduler airflow variables set PII_SALT "<valor-acordado>"
```

### 2026-10-26 — Fase 3: Bronze

**Código.**
- El DAG real es [`dags/transfermarkt_pipeline.py`](../dags/transfermarkt_pipeline.py) (`dag_id = transfermarkt_pipeline`).
- La lógica de Bronze está en [`dags/transfermarkt/bronze.py`](../dags/transfermarkt/bronze.py).
- Las rutas están en [`dags/transfermarkt/config.py`](../dags/transfermarkt/config.py). Se pueden cambiar con las variables de entorno `TM_SOURCE_DIR` y `TM_DATA_DIR`.

**Tareas.**

| Tarea | Qué hace | Falla si… |
|---|---|---|
| `validate_source` | Compara cada archivo de `tm_source/` con el manifest | Falta un archivo, cambió el tamaño o el SHA-256 |
| `land_bronze` | Copia cada archivo sin modificar a `bronze/{tabla}/ingested_year=2026/ingested_month=10/`. Si la carpeta existe, la borra y la reescribe | — |
| `validate_bronze` | Revisa la copia: mismo SHA-256 que la fuente, ningún archivo vacío, cada línea es JSON válido, misma cantidad de registros | Algo no coincide |

**Configuración del DAG:**
- `schedule="@monthly"`;
- `start_date=2023-08-01` (primer mes con partidos);
- `catchup=False`;
- `max_active_runs=1`.

Silver y Gold todavía son tareas de prueba: solo muestran el mes que van a procesar.

**Prueba.**
```powershell
docker compose exec airflow-scheduler airflow dags test transfermarkt_pipeline 2024-03-01
```
- La primera corrida terminó en `success`, con los 10 archivos en Bronze (166,7 MB).
- La segunda corrida dejó los mismos 10 archivos con los mismos SHA-256: **Bronze es idempotente**.
- Las tareas de Silver y Gold mostraron la ventana `[2024-03-01, 2024-04-01)` y la partición `year=2024/month=03`.

**Problemas resueltos.**
- **`ModuleNotFoundError: transfermarkt`**: al ejecutar una tarea, Airflow 3 no agrega `dags/` a las rutas de Python. El DAG agrega su propia carpeta a `sys.path`.
- **Ventana vacía en corridas manuales**: ver la diferencia 10 de la tabla.

### 2026-10-26 — Fase 4: Silver

**Código.**
- [`dags/transfermarkt/silver.py`](../dags/transfermarkt/silver.py): las tres tareas (`silver_dimensions`, `silver_facts`, `mask_pii`).
- [`dags/transfermarkt/parsing.py`](../dags/transfermarkt/parsing.py): funciones puras de limpieza, probadas con 36 casos reales del crudo.
- [`dags/transfermarkt/storage.py`](../dags/transfermarkt/storage.py): lectura de Bronze y escritura atómica de Parquet con esquema fijo.

Todas las reglas, con su justificación y los conteos, están en [silver_decisiones.md](silver_decisiones.md).

**Hallazgos de la exploración que cambiaron las reglas del plan.**
- `country_code` no identifica la competición (ver la tabla de imperfecciones, corregida). El id sale del link.
- **El día de la semana no alcanza para todas las fechas**: 182 partidos son ambiguos aunque se mire el día. Se resuelven con el formato dominante de su archivo (2023 = mes/día, 2024 = día/mes, sin votos en contra).
- **Las tarjetas traen el minuto, no una cantidad.**
- Los huérfanos se **recuperan** (jugadores desde el perfil de la aparición; competiciones y clubes desde los partidos) en vez de descartarse. El plan decía "se descarta como huérfano"; se cambió para no perder estadísticas reales.

**Rendimiento.** La primera versión cargaba las 80.000 apariciones completas en memoria, dos veces. Superó los 5 GB de Docker y colgó el motor (hubo que reiniciar Docker Desktop). Se cambió a lectura línea por línea, guardando solo los campos necesarios:
- **memoria:** de más de 1 GB a ~140 MB;
- **tiempo:** ~15 s por tarea.

También se agregó `dags/.airflowignore` para que Airflow no trate los módulos de `dags/transfermarkt/` como DAGs.

**Prueba de idempotencia (local).** Se corrieron las 3 tareas dos veces para 2024-03: los 6 Parquet salen **idénticos byte a byte**.

| Diferencia con el plan | Motivo |
|---|---|
| Huérfanos recuperados en vez de descartados | No perder goles y minutos de jugadores reales |
| `half_time_score`, valor de mercado y `kickoff_time` no pasan a Silver | Vienen rotos o siempre vacíos |
| Gold podrá incluir partidos de copa (con clubes `in_catalog = false`) | Son partidos reales del recorte |

**Validación de fechas en Airflow y casos borde.**
- **Corrida completa** (`airflow dags test transfermarkt_pipeline 2024-03-01`): `success` en 157 s, con los mismos conteos que en local.
- **Mes sin partidos** (2023-07): no escribe nada y borra un archivo viejo si existía.
- **Cobertura real:** 29 meses con partidos entre agosto 2023 y marzo 2026 (ver la tabla en [silver_decisiones.md](silver_decisiones.md)).
- **Partidos "fuera de temporada"** (ej. agosto 2025) que parecían un error: son la Copa de la Liga inglesa, que en el crudo está rotulada con la temporada anterior, y un partido postergado de la Serie A (Atalanta–Fiorentina 2–3, 2/6/2024). Las fechas son correctas.

**Incidente de entorno.** Después de reiniciar Docker Desktop, los contenedores arrancaron todos a la vez (por `restart: always`), sin esperar a `airflow-init`. Los chequeos de salud se acumularon (~20 procesos por contenedor) y Airflow no terminaba de arrancar.
- **Solución:** `docker compose down` y `docker compose up -d`, que respeta el orden.
- **Si pasa después de reiniciar Docker:** hacer lo mismo.

### 2026-10-27 — Fase 5: Gold y catálogo DuckDB

**Código.**
- [`dags/transfermarkt/gold.py`](../dags/transfermarkt/gold.py): tareas `build_gold` y `update_catalog`.
- [`sql/create_gold_views.sql`](../sql/create_gold_views.sql): vistas del catálogo `data/catalog.duckdb`.

**Qué hace.**
- `build_gold` arma los cuatro datasets del mes con SQL de DuckDB sobre los Parquet de Silver, y los escribe con pyarrow en `gold/{dataset}/year=YYYY/month=MM/part-0.parquet`. Antes de escribir borra la carpeta del mes (`shutil.rmtree`).
- Controla que `player_appearances` tenga tantas filas como apariciones hay en Silver, y `match_results` tantas como partidos. Si un join duplica o pierde filas, la tarea falla.
- En los partidos definidos por penales, `total_goals` queda en NULL y no suman goles a los clubes (ver [silver_decisiones.md](silver_decisiones.md)).
- `update_catalog` ejecuta el `.sql` y crea 7 vistas: una por dataset y tres de consulta (`top_scorers`, `highest_scoring_matches`, `league_table`).

**Validación con la realidad.** Liverpool 84 puntos (Premier League 2024/25), Barcelona 88 (LaLiga 2024/25) y Lautaro Martínez 24 goles (Serie A 2023/24) coinciden con las tablas oficiales.

Diferencias con la consigna: 12, 13 y 14 de la tabla de arriba.

### 2026-10-27 — Fase 6: backfill, idempotencia y pruning

- **Backfill** de 32 corridas (agosto 2023 a marzo 2026) con `--max-active-runs 1`: el backfill ignora el `max_active_runs` del DAG y sin esa opción varias corridas se pisaban al reescribir Bronze (`Directory not empty`). Resultado: 21 particiones y 79.978 apariciones en Gold, las mismas que en Silver.
- **Idempotencia:** marzo 2024 con **Clear** sobre la corrida; mismas filas y mismos SHA-256 antes y después, en Silver y en los cuatro datasets de Gold. Salida en [idempotencia_2024-03.md](idempotencia_2024-03.md).
- **Pruning:** [`scripts/finops_validation.py`](../scripts/finops_validation.py) con `EXPLAIN ANALYZE`. Sin filtro lee 21 archivos y 79.978 filas; con `year = 2024 AND month = 3` lee 1 archivo y 3.445 filas. Salida en [pruning_explain_analyze.txt](pruning_explain_analyze.txt).

### 2026-10-29 — Orden del repositorio para la entrega

- La presentación y su captura pasaron a [`docs/presentacion/`](presentacion/), y el plan original a [`docs/plan_de_trabajo.pdf`](plan_de_trabajo.pdf).
- Se agregó [`docs/tm_source_manifest.json`](tm_source_manifest.json), una copia del manifest del snapshot (solo tamaños y SHA-256, sin datos), para verificar que otra máquina arma exactamente los mismos datos.
- El README tiene ahora los pasos para reproducir todo desde cero, con el commit del proyecto fuente fijado (`e44f186`).
