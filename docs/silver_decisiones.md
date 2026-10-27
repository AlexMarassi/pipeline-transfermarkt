# Silver: decisiones de limpieza

Reglas que aplica la capa Silver ([`dags/transfermarkt/silver.py`](../dags/transfermarkt/silver.py)), con su justificación. Cada regla sale de lo observado en el crudo (ver [procedimiento.md](procedimiento.md), Fase 0). Este contenido va a la sección *Silver decisions* del README.

## Qué escribe Silver

| Tarea | Archivo | Se reescribe |
|---|---|---|
| `silver_dimensions` | `silver/competitions/competitions.parquet` | Entero, en cada corrida |
| `silver_dimensions` | `silver/clubs/clubs.parquet` | Entero, en cada corrida |
| `silver_dimensions` | `silver/players/players.parquet` | Entero, en cada corrida |
| `silver_facts` | `silver/games/YYYY-MM.parquet` | Solo el mes de la corrida |
| `silver_facts` | `silver/appearances/YYYY-MM.parquet` | Solo el mes de la corrida |
| `mask_pii` | `silver/player_contacts/player_contacts.parquet` | Entero, en cada corrida |

- **Formato:** Parquet escrito con pyarrow, con un esquema fijo por tabla (mismos tipos en todos los meses).
- **Escritura atómica:** se escribe a un archivo temporal y después se reemplaza el definitivo.
- **Idempotencia:** dos corridas del mismo mes generan archivos idénticos byte a byte (verificado).

## Deduplicación

| Tabla | Clave | Qué se queda | Por qué | Casos |
|---|---|---|---|---|
| games | `game_id` | Temporada (archivo) más reciente | Un partido es único; si se repitiera, la versión más nueva es la canónica | 0 |
| appearances | (`game_id`, `player_id`) | La de más minutos | Un jugador juega un partido una sola vez. Los duplicados del crudo son copias exactas | 29 |
| players | `player_id` | Temporada más reciente; empatando, la del plantel (`players`) antes que la recuperada; después, la más completa | El perfil más nuevo es el vigente | 1.379 versiones descartadas (1.374 jugadores en 2023 y 2024, más 1 duplicado dentro de 2024) |
| clubs | `club_id` | Temporada más reciente | Igual que players | 51 |
| competitions | `competition_id` | Primera | No hay duplicados reales (ver abajo) | 0 |
| player_contacts | `player_id` | `updated_at` más reciente | Es el contacto vigente | 79 |

**`competitions` no tiene duplicados.** Parecía que cada liga europea aparecía 3 veces, pero `country_code` es el código del *país*, no de la competición. Las 3 filas de `ES1` son LaLiga, Copa del Rey y Supercopa. El id real sale del link (`/wettbewerb/CDR`).

## Nulos y vacíos

| Tabla.campo | Regla | Justificación |
|---|---|---|
| games.`game_date` sin fecha válida | **Se descarta el partido** | Sin fecha no se puede particionar ni ubicar en el tiempo. Hoy son 0 casos |
| games.`result` no parseable | **Se descarta el partido** | Un partido sin resultado no sirve para análisis. Hoy son 0 casos |
| games.`attendance` vacío (247) | **NULL** | Falta el dato, no hubo 0 espectadores. Un 0 bajaría los promedios |
| games.`referee`, `stadium` vacíos | **NULL** | Dato descriptivo; no afecta uniones ni métricas |
| games.`half_time_score` | **No pasa a Silver** | Viene roto (`"("` en 1.623 filas, vacío en 1.526) |
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

## Huérfanos

| Caso | Cantidad | Regla | Por qué |
|---|---|---|---|
| Apariciones de jugadores que no están en players | 1.140 apariciones de 190 jugadores | **Se recupera el jugador** desde el perfil que trae cada aparición (`source = 'appearances'`) | Son jugadores reales (ej. transferidos a mitad de temporada). Descartarlos perdería goles y minutos reales |
| Partidos de una competición sin ficha (Copa de la Liga inglesa, CGB) | 153 partidos | **Se reconstruye la competición** desde los datos del partido (`in_catalog = false`) | El partido es real; falta solo el catálogo |
| Partidos de copa contra clubes de otras divisiones | 303 clubes | **Se agrega el club** con id y nombre (`in_catalog = false`) | El rival existe; solo falta su ficha. Así Gold puede mostrar contra quién se jugó |
| Apariciones de partidos que no quedaron en Silver | 0 | Se descartan y se cuentan | No se pueden ubicar en el tiempo |

## Tipado

| Crudo | Silver | Ejemplo |
|---|---|---|
| Fecha de partido con dos formatos | `date` | `Sat, 2/24/24` (mes/día) y `Wed, 30/10/24` (día/mes) → 2024-02-24 y 2024-10-30 |
| Fecha de perfil | `date` | `Jun 18, 2002` / `18/06/2002` → 2002-06-18 |
| Resultado | 2 enteros | `"2:1"` → `home_goals = 2`, `away_goals = 1` |
| Jornada | entero o texto | `"23. Matchday"` → `matchday = 23`; `"First Round"` → `round` |
| Minutos | entero | `"90'"` → 90 |
| Asistencia de público | entero | `"Attendance: 21.341"` → 21341 |
| Capacidad de estadio | entero | `"8.396 Seats"` → 8396 |
| Altura | entero (cm) | `"1,85 m"` (con espacio especial NBSP) → 185 |
| Dinero | decimal (€) | `"+€1.40m"` → 1400000 |
| Porcentaje | decimal | `"58.6 %"` → 58.6 |
| Posición | 2 textos | `"Attack - Centre-Forward"` → `position = Attack`, `sub_position = Centre-Forward` |
| Textos | sin espacios sobrantes | `"Torneo Apertura "` → `"Torneo Apertura"` |

### Cómo se resuelve la fecha del partido

El crudo mezcla `mes/día/año` (archivo 2023) y `día/mes/año` (archivo 2024). Para cada partido:

1. **Se prueban los dos formatos** y se quedan los que coinciden con el día de la semana que trae adelante (`Sat`, `Wed`…).
2. **Si queda uno solo, es ese.** Pasa en 3.056 de 3.238 partidos.
3. **Si quedan los dos**, gana el formato dominante del archivo. Pasa en 182 partidos: por ejemplo, `Sun, 12/3/23` puede ser 3 de diciembre o 12 de marzo, y los dos son domingo.
4. **El formato dominante del archivo** se calcula con las fechas no ambiguas (las que tienen un número mayor a 12). En 2023 hay 832 votos para mes/día y ninguno en contra; en 2024, 1.011 votos para día/mes y ninguno en contra.

Cada partido guarda `date_raw` y `date_resolved_by` (`weekday` o `file_format`) para poder auditar la conversión.

**La fecha de una aparición es la de su partido.** Así la tabla de hechos y la de partidos nunca quedan desfasadas.

### Cobertura temporal resultante

| Período | Partidos | Qué es |
|---|---|---|
| ago 2023 – may 2024 | 1.465 | Temporada 2023/24 de las ligas europeas y sus copas |
| jun 2024 | 1 | Atalanta–Fiorentina 2–3 (Serie A, fecha 29), postergado y jugado el 2/6/2024 |
| ago 2024 – may 2025 | 1.679 | Temporada 2024/25 + Torneo Apertura 2025 de Argentina (ene–may 2025) |
| jul 2025 – mar 2026 | 93 | Copa de la Liga inglesa (CGB) edición 2025/26 |

**La Copa de la Liga inglesa (CGB) está corrida un año en el crudo.** El archivo de temporada "2023" trae la edición 2024/25 (primera ronda: 13/8/2024) y el "2024" trae la 2025/26. Son partidos reales con fecha resuelta por día de la semana, así que se mantienen. El rótulo `season` conserva el del crudo.

## PII

Ver [pii_y_salt.md](pii_y_salt.md). En Silver, `player_contacts` queda solo con `player_id`, `email_hash` y `updated_at`:
- **El email** se normaliza (`strip().lower()`) y se hashea con SHA-256 + `PII_SALT`.
- **`full_name` y `phone_number`** no pasan a Silver.

## Control de calidad

Cada tarea devuelve un resumen (filas que entran, descartadas por motivo, filas que salen). Se ve en el log de la tarea y en su XCom (`return_value`). Ejemplo de `silver_facts` para 2024-03:

```
games:       3238 crudos, 3056 fechas por día de semana, 182 por formato de archivo, 0 descartados
appearances: 80007 crudas, 29 duplicadas descartadas, 0 huérfanas
mes 2024-03: 112 partidos, 3445 apariciones
```
