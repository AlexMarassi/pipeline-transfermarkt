"""
Arma el snapshot fuente (tm_source/) a partir del crudo de transfermarkt-datasets.

Toma los JSON lines del scraper (un .json.gz por temporada y entidad), se queda solo con
las ligas y temporadas del recorte y los guarda sin modificar: cada línea se copia byte a byte.
Al final escribe manifest.json con tamaño, cantidad de líneas y SHA-256 de cada archivo.

Este script se corre UNA vez, fuera de Airflow. El DAG solo lee tm_source/.

Uso (desde la raíz del repo):
    python scripts/build_snapshot.py \
        --raw  C:/Users/<usuario>/tm_work/transfermarkt-datasets/data/raw/transfermarkt-scraper \
        --competitions-file C:/Users/<usuario>/tm_work/transfermarkt-datasets/data/competitions.json \
        --out  tm_source

Solo usa la biblioteca estándar de Python.
"""
import argparse
import gzip
import hashlib
import json
import re
from datetime import date
from pathlib import Path

# Recorte por defecto (ver docs/procedimiento.md)
DEFAULT_COMPETITIONS = ["ES1", "GB1", "IT1", "ARG1"]
DEFAULT_SEASONS = ["2023", "2024"]

CLUB_RE = re.compile(r"/verein/(\d+)")
GAME_RE = re.compile(r"/spielbericht/(\d+)")
COMP_RE = re.compile(r"/wettbewerb/([^/?]+)")


def competition_of(parent):
    """Código de la competición de un registro (ej. 'ES1')."""
    code = parent.get("country_code")
    if code:
        return code
    m = COMP_RE.search(parent.get("href") or "")
    return m.group(1) if m else None


def club_id_of(href):
    m = CLUB_RE.search(href or "")
    return m.group(1) if m else None


def game_id_of_appearance(rec):
    """El id del partido viene como game_id (formato nuevo) o dentro de result.href (formato viejo)."""
    if rec.get("game_id"):
        return str(rec["game_id"])
    res = rec.get("result")
    if isinstance(res, dict):
        m = GAME_RE.search(res.get("href") or "")
        if m:
            return m.group(1)
    return None


def filter_lines(src_lines, keep, dst):
    """Copia a dst las líneas para las que keep(registro) es True, sin tocar los bytes."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    kept = 0
    with open(dst, "wb") as out:
        for line in src_lines:
            if not line.strip():
                continue
            if keep(json.loads(line)):
                out.write(line if line.endswith(b"\n") else line + b"\n")
                kept += 1
    return kept


def gz_lines(path):
    with gzip.open(path, "rb") as fh:
        yield from fh


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw", required=True, help="Carpeta data/raw/transfermarkt-scraper")
    ap.add_argument("--competitions-file", required=True, help="Archivo data/competitions.json")
    ap.add_argument("--out", default="tm_source", help="Carpeta destino (default: tm_source)")
    ap.add_argument("--competitions", default=",".join(DEFAULT_COMPETITIONS))
    ap.add_argument("--seasons", default=",".join(DEFAULT_SEASONS))
    ap.add_argument("--origin", default="transfermarkt-datasets (dvc pull data/raw/transfermarkt-scraper)")
    ap.add_argument("--origin-commit", default=None, help="Commit de transfermarkt-datasets usado")
    ap.add_argument("--snapshot-date", default=date.today().isoformat(),
                    help="Fecha de congelamiento (YYYY-MM-DD). Define la fecha de ingesta de Bronze.")
    args = ap.parse_args()

    raw = Path(args.raw)
    out = Path(args.out)
    comps = set(args.competitions.split(","))
    seasons = args.seasons.split(",")
    counts = {}

    # competitions: un solo archivo, sin temporada
    n = filter_lines(open(args.competitions_file, "rb"),
                     lambda r: competition_of(r) in comps,
                     out / "competitions" / "competitions.json")
    counts["competitions/competitions.json"] = n

    for season in seasons:
        d = raw / season

        # clubs de las ligas elegidas
        club_ids = set()

        def keep_club(r):
            if competition_of(r["parent"]) in comps:
                club_ids.add(club_id_of(r["href"]))
                return True
            return False

        counts[f"clubs/{season}.json"] = filter_lines(
            gz_lines(d / "clubs.json.gz"), keep_club, out / "clubs" / f"{season}.json")

        # partidos de las ligas elegidas
        game_ids = set()

        def keep_game(r):
            if competition_of(r["parent"]) in comps:
                game_ids.add(str(r["game_id"]))
                return True
            return False

        counts[f"games/{season}.json"] = filter_lines(
            gz_lines(d / "games.json.gz"), keep_game, out / "games" / f"{season}.json")

        # jugadores de esos clubes
        counts[f"players/{season}.json"] = filter_lines(
            gz_lines(d / "players.json.gz"),
            lambda r: club_id_of(r["parent"].get("href")) in club_ids,
            out / "players" / f"{season}.json")

        # apariciones en esos partidos
        counts[f"appearances/{season}.json"] = filter_lines(
            gz_lines(d / "appearances.json.gz"),
            lambda r: game_id_of_appearance(r) in game_ids,
            out / "appearances" / f"{season}.json")

        print(f"Temporada {season}: {len(club_ids)} clubes, {len(game_ids)} partidos")

    # manifest
    files = []
    total = 0
    for rel, lines in sorted(counts.items()):
        p = out / rel
        size = p.stat().st_size
        total += size
        files.append({"path": rel, "entity": rel.split("/")[0], "bytes": size,
                      "lines": lines, "sha256": sha256_of(p)})

    manifest = {
        "snapshot_date": args.snapshot_date,
        "origin": args.origin,
        "origin_commit": args.origin_commit,
        "competitions": sorted(comps),
        "seasons": seasons,
        "format": "JSON lines, sin comprimir, líneas copiadas sin modificar del crudo del scraper",
        "total_bytes": total,
        "files": files,
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\nSnapshot en {out}/ — {total / 1e6:.1f} MB sin comprimir")
    for f in files:
        print(f"  {f['path']:28s} {f['bytes'] / 1e6:7.1f} MB {f['lines']:8d} líneas")


if __name__ == "__main__":
    main()
