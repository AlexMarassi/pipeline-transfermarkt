# PII, hash y salt

Cómo se protegen los datos personales en el pipeline y cómo configurar el salt.

## Qué datos personales hay

Transfermarkt no trae datos de contacto, así que se generan con [`scripts/generate_pii.py`](../scripts/generate_pii.py) (ver [procedimiento.md](procedimiento.md), Fase 1). Queda una tabla `player_contacts` con una fila por jugador:

| Columna | Es PII | Qué pasa en Silver |
|---|---|---|
| `player_id` | No | Se mantiene (es la clave para unir) |
| `full_name` | Sí | Se elimina |
| `email` | Sí | Se reemplaza por `email_hash` |
| `phone_number` | Sí | Se elimina |
| `updated_at` | No | Se usa para deduplicar y después se puede descartar |

El CSV vive en `tm_source/` y **nunca se sube al repo** (`.gitignore` excluye `tm_source/` y `*.csv`).

## Qué es el hash

Un hash (SHA-256) convierte un texto en un código fijo de 64 caracteres:

```
"juan.perez@gmail.com"  →  "3f1a9c…e72b"
```

- **Es de una sola vía**: con el código no se puede volver al email.
- **Es estable**: el mismo email siempre da el mismo código. Por eso los analistas pueden unir tablas por `email_hash` sin ver nunca el email.

## Para qué sirve el salt

Un hash solo no alcanza. Un atacante puede tomar una lista de millones de emails conocidos, hashearlos todos y comparar con nuestra tabla: eso es un **ataque de diccionario**.

El salt es un **texto secreto** que se agrega antes de hashear:

```
email_hash = SHA-256(salt + email)
```

Sin conocer el salt, el atacante no puede armar su diccionario, porque sus hashes nunca coinciden con los nuestros.

## Cómo se configura

El salt se guarda como **Variable de Airflow**, dentro de la base de datos de Airflow. No está en ningún archivo.

1. **Inventar un valor largo y aleatorio.** En PowerShell:
   ```powershell
   [guid]::NewGuid().ToString() + [guid]::NewGuid().ToString()
   ```
2. **Cargarlo**, desde la carpeta del repo, con Airflow levantado:
   ```powershell
   docker compose exec airflow-scheduler airflow variables set PII_SALT "<valor>"
   ```
3. **Verificarlo:** `docker compose exec airflow-scheduler airflow variables get PII_SALT`, o en la web, **Admin → Variables**.

Reglas:
- **Todo el grupo usa el mismo valor.** Si no, los hashes salen distintos en cada máquina. Se comparte por un canal privado, nunca por el repo.
- **Se carga una vez**: sobrevive a `docker compose down` / `up`. Se pierde con `docker compose down -v`, y en ese caso hay que volver a cargarlo.
- **El valor no aparece en ningún archivo commiteado**: ni en el DAG, ni en `scripts/`, ni en `.env`, ni en `docs/`.

## Cómo se usa en el DAG

Se lee **dentro** de la función `@task`, nunca arriba de todo en el archivo:

```python
import hashlib
from airflow.sdk import task, Variable

@task
def mask_pii():
    salt = Variable.get("PII_SALT")          # se lee solo cuando la tarea corre

    def hash_email(email):
        if email is None:
            return None
        normalized = email.strip().lower()   # "  JUAN@X.COM " y "juan@x.com" dan el mismo hash
        return hashlib.sha256((salt + normalized).encode("utf-8")).hexdigest()
    ...
```

- **Por qué dentro de la tarea:** Airflow relee los archivos de DAGs cada ~30 segundos. Si `Variable.get` estuviera a nivel de módulo, consultaría la base de datos en cada relectura, aunque no corra nada. La consigna lo pide explícitamente.
- **Por qué normalizar antes de hashear:** el crudo trae emails en mayúsculas y con espacios. Sin `strip().lower()`, el mismo email daría hashes distintos y no se podría unir.

## Respuestas para la defensa

**¿Cómo obtienen el salt dentro del DAG?**
Con `Variable.get("PII_SALT")` dentro de la tarea `mask_pii`. Mostrar el código de arriba.

**Si un analista tiene acceso de lectura a los Parquet de Silver, ¿qué le impide revertir los emails?**
1. SHA-256 es de una sola vía.
2. Por el salt, no puede usar un diccionario de hashes precalculados: necesitaría conocer el secreto, que no está en los archivos ni en el repo.
3. En Silver no queda ningún dato en claro: nombre y teléfono se eliminan.

**¿Por qué una Variable de Airflow y no una constante en Python?**
- **Una constante quedaría en el repositorio**, y cualquiera con acceso al código vería el secreto, también en el historial de Git, aunque después se borre.
- **La Variable vive solo en la base de datos de Airflow**: se puede cambiar sin tocar el código, cada entorno (local, producción) puede tener la suya, y en la nube se reemplaza por un gestor de secretos (AWS Secrets Manager) sin cambiar la lógica.
