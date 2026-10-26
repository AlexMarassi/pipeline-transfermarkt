# Plan de trabajo — Pipeline Transfermarkt

Versión en Markdown del plan original (`work_plan/Plan de trabajo — Pipeline Transfermarkt.pdf`), para ir marcando el avance.
Lo que cambia respecto al plan se anota en [procedimiento.md](procedimiento.md). Lo que falta, resumido, está en [pendientes.md](pendientes.md).

**Estado (2026-10-26):** fases 0 a 4 hechas; faltan la 5 (Gold), la 6 (pruebas) y la 7 (README y defensa).

## Resumen

Pipeline medallion local, orquestado con Airflow, sobre datos crudos de Transfermarkt: el JSON tal como sale del scraper, nunca los CSV ya limpios. El Gold es una tabla de apariciones de jugadores por partido, particionada por fecha del partido y consultada con DuckDB.

| Decisión | Propuesta |
|---|---|
| Fuente | JSON lines crudos del scraper open source `transfermarkt-scraper` (snapshot del proyecto `transfermarkt-datasets`) |
| Entidades | competitions, clubs, players, games, appearances + tabla sintética de PII |
| Recorte | Liga Profesional Argentina + 2 o 3 ligas europeas, varias temporadas; se ajusta para quedar en 50–300 MB sin comprimir |
| Business date | Fecha del partido (`games.date`) |
| Partición Gold | `year=YYYY/month=MM` de la fecha del partido |
| Ingestion date | Fecha en que se congeló el snapshot fuente (ej. 2026-10) |
| Gold | appearances ⋈ games ⋈ players ⋈ clubs (+ competitions) |
| PII | Contactos sintéticos por `player_id`; email hasheado con SHA-256 + salt de Airflow Variable |

**Regla de oro:** los datos se consiguen una vez, se congelan como "sistema fuente" y el DAG nunca vuelve a tocar Transfermarkt. Eso hace al pipeline reproducible.

## DAG

8 tareas en 3 capas:

```
validate_source >> land_bronze >> validate_bronze >> [silver_dimensions, silver_facts, mask_pii] >> build_gold >> update_catalog
```

- Las tres tareas de Silver corren en paralelo porque no dependen entre sí; separarlas permite reintentar solo la que falla.
- `schedule="@monthly"`, `start_date` en el primer mes del recorte, `catchup=False`.
- Los meses históricos se cargan con `airflow backfill create --dag-id <id> --from-date <inicio> --to-date <fin>`.
- Cada tarea trabaja sobre su ventana `data_interval_start` – `data_interval_end`.

## Fase 0 — Validación y factibilidad

Objetivo: saber si el dataset cumple la consigna y cuánto hay que recortar. Sin Airflow todavía.

- [x] Bajar el snapshot crudo (opción A: `dvc pull` del proyecto `transfermarkt-datasets`).
- [x] Medir el tamaño sin comprimir por entidad y temporada.
- [x] Inspeccionar el crudo: cómo vienen fechas, valores de mercado, resultados, nulos y duplicados.
- [ ] ~~(Opcional) Probar la opción B~~ — no se hizo, la opción A alcanza.
- [x] Definir el recorte: ES1, GB1, IT1, ARG1 × 2023–2024 = 166 MB.
- [x] Confirmar que hay al menos 4 entidades que se unen bien por ID (competitions, clubs, games, players, appearances).

**Salida:** exploración del crudo y decisión escrita de alcance.

## Fase 1 — Snapshot fuente y PII sintética

El crudo se congela en `tm_source/`, que hace de "sistema fuente". El DAG lee de ahí y nunca de Internet.

- [x] Crear `tm_source/` con una subcarpeta por entidad y un archivo por temporada, tal como sale del scraper (`scripts/build_snapshot.py`).
- [x] Escribir `manifest.json`: archivo, tamaño, SHA-256, origen y fecha de obtención.
- [x] Generar `player_contacts` con Faker y semilla fija: `player_id`, `full_name`, `email`, `phone_number`, `updated_at`. Un registro por jugador del recorte (más algunos duplicados a propósito). Se guarda como CSV en `tm_source/` (ignorado por Git).
- [x] Dejar el script de generación en `/scripts` (`scripts/generate_pii.py`).

## Fase 2 — Entorno y repositorio

- [x] Airflow andando en cada máquina (Docker, ver [levantar_airflow.md](levantar_airflow.md)).
- [x] `pandas`, `pyarrow`, `duckdb`, `faker` disponibles en Airflow.
- [x] `PII_SALT` cargado como Variable (ver [pii_y_salt.md](pii_y_salt.md)). El valor se comparte por fuera del repo; cada integrante lo carga en su máquina.
- [x] Carpeta de datos `data/` (bronze, silver, gold) montada en Airflow.
- [x] Repo con `/dags`, `/scripts`, `/sql`, `/docs` y `README.md`.
- [x] `.gitignore` con `data/`, `tm_source/`, `*.csv`, `*.duckdb`, `.env`, `logs/`.
- [x] Ramas: `main`, `develop` y `feature/*` con Pull Request.
- [x] DAG de prueba (`transfermarkt_esqueleto`) corriendo.

## Fase 3 — Bronze

Copia los archivos del snapshot sin tocar un byte, en `bronze/{table}/ingested_year=YYYY/ingested_month=MM/`.

- [x] La fecha de ingesta sale del `manifest.json` (cuándo se congeló el snapshot), no del reloj. Así re-ejecutar no crea carpetas nuevas.
- [x] Si la carpeta ya existe, se borra y se vuelve a escribir (idempotente).
- [x] Formato: JSON lines del scraper, sin comprimir (y `player_contacts` en CSV).
- [x] Tarea de validación: checksums iguales al manifest, sin archivos vacíos, cada línea es JSON válido.
- [x] Probado con `airflow dags test` y re-ejecutado: mismos 10 archivos, mismos hashes.

**Para la defensa:** la fecha de ingesta (octubre 2026) es muy distinta de la fecha de negocio (partidos de agosto 2023 a marzo 2026). Por eso Bronze y Gold se particionan por fechas distintas.

## Fase 4 — Silver

Tipa, deduplica, resuelve nulos y enmascara PII con pandas + pyarrow, y escribe Parquet. Las dimensiones se reescriben enteras; los hechos, solo para el mes de la corrida.

Tabla original del plan (hipótesis). **Las reglas finales, que cambiaron en varios puntos al ver el crudo, están en [silver_decisiones.md](silver_decisiones.md).**

| Tabla | Deduplicación | Tipado esperado | Nulos y huérfanos (a validar con los datos) |
|---|---|---|---|
| players | Uno por `player_id`: el de la temporada más reciente | Fecha de nacimiento, altura a cm, valor de mercado de texto a euros | Sin valor de mercado: queda NULL, nunca 0 |
| clubs | Uno por `club_id`, temporada más reciente | Valores de mercado y capacidades a número | Sin estadio o DT: se mantiene |
| games | Uno por `game_id` | Fecha a datetime, resultado a goles local y visitante | Sin fecha: se descarta (no se puede particionar). Sin asistencia: NULL |
| appearances | Uno por (`player_id`, `game_id`) | Minutos, goles, tarjetas a enteros | Jugador o partido inexistente: huérfano, se descarta y se loguea el conteo |
| competitions | Uno por `competition_id` | Normalizar nombres y mayúsculas | Sin país (torneos internacionales): "Internacional" |
| player_contacts | Uno por `player_id` | — | Email nulo: se descarta el contacto, no el jugador |

- [x] Reglas definitivas (confirmadas con el crudo) en [silver_decisiones.md](silver_decisiones.md). Al final se pasan al README.
- [x] PII: `email_hash = sha256(salt + email.strip().lower())`. `full_name` y `phone_number` se eliminan. En Silver no queda ningún dato en claro.
- [x] El salt se lee con `Variable.get("PII_SALT")` dentro de la función `@task`, nunca a nivel de módulo.
- [x] Layout `silver/{table}/`. Para los hechos, un Parquet por mes (ej. `silver/games/2024-03.parquet`).
- [x] Idempotencia verificada: dos corridas del mismo mes dan archivos idénticos byte a byte.

## Fase 5 — Gold y catálogo DuckDB

Una fila por jugador por partido, con todo el contexto necesario para analizar sin joins.

- [ ] Join: `appearances` ⋈ `games` ⋈ `players` ⋈ `clubs` ⋈ `competitions` (5 entidades; la consigna pide 4).
- [ ] Partición `gold/player_appearances/year=YYYY/month=MM/`, mes en dos dígitos.
- [ ] Escritura: se borra la carpeta del mes (`shutil.rmtree`) y se reescribe. Nunca append.
- [ ] `update_catalog` ejecuta un `.sql` de `/sql` que crea la vista `player_appearances` en un archivo `.duckdb` con `read_parquet(..., hive_partitioning=True)`.

## Fase 6 — Idempotencia y pruning

Cada prueba con su script en `/scripts` y su salida guardada en `/docs`.

**Idempotencia**
- [ ] Correr el DAG para un mes con muchos partidos (ej. 2024-03).
- [ ] Contar filas en Silver (games y appearances del mes) y en la partición Gold.
- [ ] Re-ejecutar el mismo mes desde la UI (Clear) y volver a contar.
- [ ] Los conteos tienen que ser idénticos. Guardar ambas salidas.

**Pruning con DuckDB**
- [ ] Backfill de varias temporadas para tener muchas particiones.
- [ ] `scripts/finops_validation.py` con `EXPLAIN ANALYZE` sobre todo Gold y sobre un solo mes.
- [ ] La consulta filtrada lee menos archivos y filas. Guardar ambas salidas lado a lado.

## Fase 7 — Documentación y defensa

| Pregunta de la defensa | Dónde se responde |
|---|---|
| Idempotencia y re-ejecución | Salidas de la Fase 6 + función de escritura que borra la partición |
| Por qué estas tareas separadas | Diagrama del DAG + una línea por tarea en el README |
| Nulos y deduplicación | Tabla de decisiones de Silver con justificación |
| Clave de partición y pruning | Salida de `EXPLAIN ANALYZE` + ingesta vs. fecha de negocio |
| Salt y seguridad | Código de la tarea de PII; sin el salt, un hash no se revierte con diccionarios |
| Modelo Gold | Diagrama de joins y razón para desnormalizar |
| Migración a AWS | Sección corta en el README |

**Migración a AWS:** carpetas locales → S3; Airflow → MWAA; `PII_SALT` → Secrets Manager; DuckDB → Athena con Glue Data Catalog; snapshot fuente → bucket de landing. En el código cambian las rutas (`s3://` vía `pyarrow.fs` o `s3fs`) y el backend de secretos; la lógica de pandas sigue igual.

## Riesgos y planes B

| Riesgo | Qué se hace |
|---|---|
| El scraper (opción B) se bloquea | Se sigue solo con la opción A |
| `dvc pull` falla o el bucket desaparece | Guardar una copia propia del crudo apenas se baja |
| El recorte queda fuera de 50–300 MB | Sumar o quitar temporadas o ligas |
| El crudo viene más limpio de lo esperado | Igual hay dedup entre temporadas, huérfanos y tipado; documentarlo bien |
| El backfill tarda demasiado | Reducir temporadas; para la demo alcanzan dos o tres |
| Diferencias de entorno entre integrantes | Mismo `docker-compose.yaml` para todos |
