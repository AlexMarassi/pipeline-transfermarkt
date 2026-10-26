"""
Genera la tabla sintética de PII (player_contacts) para los jugadores del snapshot.

Transfermarkt no trae datos de contacto, así que se inventan con Faker: nombre completo,
email y teléfono por player_id. Es determinístico: la semilla depende del player_id,
así que correrlo de nuevo da exactamente los mismos datos.

Para que Silver tenga trabajo de limpieza real, se agregan imperfecciones a propósito
(siempre las mismas, por la semilla):
  - ~3% de emails nulos
  - ~10% de emails con mayúsculas o espacios alrededor
  - ~2% de jugadores con una segunda fila (contacto actualizado, updated_at más nuevo)

Salida: tm_source/player_contacts/player_contacts.csv (no se sube al repo)
y se agrega/actualiza su entrada en tm_source/manifest.json.

Uso (con Airflow levantado, desde la raíz del repo):
    docker compose exec airflow-scheduler python /opt/airflow/scripts/generate_pii.py
"""
import csv
import hashlib
import json
import random
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

SOURCE = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/opt/airflow/tm_source")
OUT = SOURCE / "player_contacts" / "player_contacts.csv"
BASE_SEED = 2026
PLAYER_RE = re.compile(r"/spieler/(\d+)")
COLUMNS = ["player_id", "full_name", "email", "phone_number", "updated_at"]


def player_ids():
    """player_id únicos de todos los archivos de players del snapshot."""
    ids = set()
    for f in sorted((SOURCE / "players").glob("*.json")):
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    m = PLAYER_RE.search(json.loads(line)["href"])
                    if m:
                        ids.add(int(m.group(1)))
    return sorted(ids)


def contact(fake, rng, pid, updated_at, name=None):
    first, last = name or (fake.first_name(), fake.last_name())
    user = re.sub(r"[^a-z0-9.]", "", f"{first}.{last}".lower()) or f"player{pid}"
    email = f"{user}{rng.randint(1, 999)}@{fake.free_email_domain()}"
    roll = rng.random()
    if roll < 0.03:
        email = None                                  # email faltante
    elif roll < 0.08:
        email = email.upper()                         # casing inconsistente
    elif roll < 0.13:
        email = f"  {email} "                         # espacios sobrantes
    return {
        "player_id": pid,
        "full_name": f"{first} {last}",
        "email": email,
        "phone_number": fake.phone_number(),
        "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S"),
    }


def main():
    ids = player_ids()
    rows = []
    base = datetime(2026, 10, 1, 12, 0, 0)
    fake = Faker(["es_AR", "es_ES", "en_GB", "it_IT"])
    for pid in ids:
        rng = random.Random(BASE_SEED + pid)
        fake.seed_instance(BASE_SEED + pid)           # misma semilla por jugador = mismos datos
        first_ts = base - timedelta(days=rng.randint(30, 900))
        rows.append(contact(fake, rng, pid, first_ts))
        if rng.random() < 0.02:                       # contacto actualizado (duplicado)
            # misma persona, nuevo email/teléfono
            name = tuple(rows[-1]["full_name"].split(" ", 1))
            rows.append(contact(fake, rng, pid, first_ts + timedelta(days=rng.randint(1, 25)), name))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)

    # Registrar el archivo en el manifest
    manifest_path = SOURCE / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    rel = "player_contacts/player_contacts.csv"
    entry = {
        "path": rel,
        "entity": "player_contacts",
        "bytes": OUT.stat().st_size,
        "lines": len(rows),
        "sha256": hashlib.sha256(OUT.read_bytes()).hexdigest(),
    }
    manifest["files"] = [f for f in manifest["files"] if f["path"] != rel] + [entry]
    manifest["total_bytes"] = sum(f["bytes"] for f in manifest["files"])
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    n_null = sum(1 for r in rows if r["email"] is None)
    print(f"{len(ids)} jugadores, {len(rows)} filas ({len(rows) - len(ids)} duplicadas, {n_null} sin email)")
    print(f"Escrito {OUT} ({entry['bytes'] / 1e6:.2f} MB) y actualizado manifest.json")


if __name__ == "__main__":
    main()
