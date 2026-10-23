# Cómo levantar Airflow

Airflow corre con Docker usando el `docker-compose.yaml` de la raíz del repo. Los DAGs se escriben en `dags/` y Airflow los detecta solo.

## Requisitos

- **Docker Desktop** instalado.
- **Al menos 5 GB de memoria para Docker.** En Windows, crear el archivo `C:\Users\<tu_usuario>\.wslconfig` con:
  ```ini
  [wsl2]
  memory=5GB
  swap=4GB
  ```
  Después correr `wsl --shutdown` y volver a abrir Docker Desktop. Para verificar: `docker info --format "{{.MemTotal}}"` tiene que dar unos 5.000.000.000 o más.

## Primera vez (después de clonar)

1. **Si ya tenías Airflow andando desde otra carpeta** (por ejemplo `airflow-docker`), apagalo primero:
   ```powershell
   cd $HOME\airflow-docker
   docker compose down
   ```
   Desde ahí esa carpeta no se usa más.
2. **Crear el `.env`** a partir del ejemplo, desde la carpeta del repo:
   ```powershell
   Copy-Item .env.example .env      # Windows
   cp .env.example .env             # Mac / Linux
   ```
   En Linux, cambiar `AIRFLOW_UID=50000` por el resultado de `id -u`.
3. Seguir con el uso diario. La primera vez tarda 2 o 3 minutos más, porque crea la base de datos de Airflow e instala duckdb y faker.

## Uso diario

1. **Abrir Docker Desktop** y esperar a que diga **Engine running**.
2. **Abrir una terminal en la carpeta del repo** (en VS Code: *Terminal → New Terminal*) y correr:
   ```powershell
   docker compose up -d
   docker compose ps
   ```
   Esperar 1 o 2 minutos hasta que los servicios digan `healthy`.
3. **Entrar a http://localhost:8080** con usuario `airflow` y contraseña `airflow`.

Los DAGs nuevos aparecen en pausa: hay que activarlos con el interruptor antes de apretar ▶ Trigger.

## Al terminar

```powershell
docker compose down
```

Después cerrar Docker Desktop (clic derecho en la ballena → *Quit Docker Desktop*).

## Comandos útiles

| Para qué | Comando |
|---|---|
| Ver el estado de los servicios | `docker compose ps` |
| Forzar que Airflow lea un DAG nuevo (si no aparece después de 5 minutos) | `docker compose exec airflow-dag-processor airflow dags reserialize` |
| Ver errores en los DAGs | `docker compose exec airflow-scheduler airflow dags list-import-errors` |
| Cargar el salt de PII (se hace una vez, nunca va en el repo) | `docker compose exec airflow-scheduler airflow variables set PII_SALT "<secreto>"` |

**No usar** `docker compose down -v`: el `-v` borra la base de datos de Airflow, con las corridas y las variables.

## Problemas comunes

- **`airflow-init` falla con "Cannot allocate memory"** o hay servicios en `unhealthy`: a Docker le falta memoria. Ver los requisitos de arriba.
- **El DAG no aparece en la web**: revisar que el archivo esté en `dags/`, esperar 5 minutos o usar `reserialize`, y mirar si hay errores con `list-import-errors`.
- **"failed to connect to the docker API"**: Docker Desktop no está abierto.
