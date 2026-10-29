#!/bin/bash
# Doble click para presentar: levanta Airflow (Docker) y el servidor de la presentación, y abre el navegador.
# Este archivo está en docs/presentacion/: se sube dos carpetas hasta la raíz del repo
cd "$(dirname "$0")/../.." || exit 1

if ! docker info >/dev/null 2>&1; then
  echo "Docker Desktop no está abierto. Abrilo, esperá a 'Engine running' y volvé a ejecutar esto."
  open -a Docker
  read -r -p "Enter para cerrar..." _
  exit 1
fi

echo "Levantando Airflow..."
docker compose up -d

echo "Iniciando la presentación en http://localhost:8000/ (Ctrl+C para terminar)"
.venv/bin/python scripts/presentar.py
