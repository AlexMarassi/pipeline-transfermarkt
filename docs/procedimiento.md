# Procedimiento

Registro de lo que se fue haciendo, en orden, y de **en qué nos apartamos del [plan de trabajo](plan_de_trabajo.md) o de la consigna**, con el motivo.

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
| competitions | Cada liga europea aparece 3 veces | — |
| competitions | Espacios sobrantes en nombres | `"Torneo Apertura "` |

**Cobertura temporal (fecha del partido):** de agosto 2023 a mediados de 2025, más algunos partidos sueltos. Da más de 20 particiones mensuales para mostrar pruning. El conteo exacto por mes se confirma en Silver, después de resolver el formato de fechas.

**Pendiente de Fase 1:** generar `player_contacts` (PII sintética con Faker) y guardarla en `tm_source/`.
