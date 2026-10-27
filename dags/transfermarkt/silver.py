"""
Capa Silver: tipado, deduplicación, nulos, huérfanos y enmascarado de PII (pandas + pyarrow).

Salidas (Parquet):
    silver/competitions/competitions.parquet      dimensión, se reescribe entera
    silver/clubs/clubs.parquet                     dimensión, se reescribe entera
    silver/players/players.parquet                 dimensión, se reescribe entera
    silver/games/YYYY-MM.parquet                   hechos del mes de la corrida
    silver/appearances/YYYY-MM.parquet             hechos del mes de la corrida
    silver/player_contacts/player_contacts.parquet PII enmascarada

Cada regla de nulos y deduplicación está justificada en el README.
"""
import hashlib
import logging
from collections import Counter
from datetime import date

import pandas as pd
import pyarrow as pa

from transfermarkt import parsing as p
from transfermarkt.config import SILVER_DIR
from transfermarkt.storage import iter_bronze_json, read_bronze_csv, remove_if_exists, write_parquet

log = logging.getLogger(__name__)

# ---------------------------------------------------------------- esquemas
COMPETITIONS_SCHEMA = pa.schema([
    ("competition_id", pa.string()),
    ("competition_name", pa.string()),
    ("competition_type", pa.string()),
    ("country_name", pa.string()),
    ("confederation", pa.string()),
    ("total_clubs", pa.int32()),
    ("in_catalog", pa.bool_()),          # False = competición reconstruida desde games (huérfana)
])

CLUBS_SCHEMA = pa.schema([
    ("club_id", pa.int64()),
    ("club_name", pa.string()),
    ("club_code", pa.string()),
    ("competition_id", pa.string()),     # liga del club en su temporada más reciente
    ("season", pa.int32()),
    ("squad_size", pa.int32()),
    ("average_age", pa.float64()),
    ("foreigners_number", pa.int32()),
    ("foreigners_pct", pa.float64()),
    ("national_team_players", pa.int32()),
    ("stadium_name", pa.string()),
    ("stadium_seats", pa.int32()),
    ("net_transfer_record_eur", pa.float64()),
    ("coach_name", pa.string()),
    ("in_catalog", pa.bool_()),          # False = club de otra división, visto solo en partidos de copa
])

PLAYERS_SCHEMA = pa.schema([
    ("player_id", pa.int64()),
    ("player_name", pa.string()),
    ("first_name", pa.string()),
    ("last_name", pa.string()),
    ("date_of_birth", pa.date32()),
    ("citizenship", pa.string()),
    ("position", pa.string()),
    ("sub_position", pa.string()),
    ("foot", pa.string()),
    ("height_cm", pa.int32()),
    ("current_club_id", pa.int64()),
    ("contract_expires", pa.date32()),
    ("shirt_number", pa.int32()),
    ("international_caps", pa.int32()),
    ("season", pa.int32()),
    ("source", pa.string()),             # 'players' o 'appearances' (jugador recuperado)
])

GAMES_SCHEMA = pa.schema([
    ("game_id", pa.int64()),
    ("competition_id", pa.string()),
    ("season", pa.int32()),
    ("game_date", pa.date32()),
    ("home_club_id", pa.int64()),
    ("away_club_id", pa.int64()),
    ("home_goals", pa.int32()),
    ("away_goals", pa.int32()),
    ("decided_by", pa.string()),         # 'regular', 'extra_time' o 'penalties' (ver silver_decisiones.md)
    ("matchday", pa.int32()),
    ("round", pa.string()),
    ("stadium", pa.string()),
    ("attendance", pa.int32()),
    ("referee", pa.string()),
    ("date_raw", pa.string()),           # fecha original, para auditar la conversión
    ("date_resolved_by", pa.string()),   # 'weekday' o 'file_format'
])

APPEARANCES_SCHEMA = pa.schema([
    ("appearance_id", pa.string()),      # '{game_id}_{player_id}'
    ("game_id", pa.int64()),
    ("player_id", pa.int64()),
    ("club_id", pa.int64()),
    ("opponent_club_id", pa.int64()),
    ("competition_id", pa.string()),
    ("game_date", pa.date32()),
    ("is_home", pa.bool_()),
    ("position", pa.string()),
    ("minutes_played", pa.int32()),
    ("goals", pa.int32()),
    ("assists", pa.int32()),
    ("yellow_cards", pa.int32()),
    ("second_yellow_cards", pa.int32()),
    ("red_cards", pa.int32()),
])

CONTACTS_SCHEMA = pa.schema([
    ("player_id", pa.int64()),
    ("email_hash", pa.string()),
    ("updated_at", pa.timestamp("ms")),   # Parquet no guarda segundos: milisegundos
])


def _season(file_stem):
    return int(file_stem) if str(file_stem).isdigit() else None


# ---------------------------------------------------------------- games (se usa en dims y hechos)
def load_games():
    """
    Todos los partidos de Bronze, tipados y deduplicados (sin filtrar por mes).
    Devuelve (DataFrame, estadísticas).
    """
    raw = list(iter_bronze_json("games"))
    stats = Counter(raw_rows=len(raw))

    # El formato de fecha (mes/día o día/mes) cambia según el archivo: se infiere por archivo
    formats = {}
    for f in sorted({f for f, _ in raw}):
        fmt, votes = p.infer_date_format(r["date"] for ff, r in raw if ff == f)
        formats[f] = fmt
        log.info("games/%s: formato de fecha %s (votos %s)", f, fmt, votes)

    rows = []
    for f, r in raw:
        gdate, how = p.game_date(r.get("date"), formats[f])
        home, away = p.score(r.get("result"))
        md, rnd = p.matchday(r.get("matchday"))
        rows.append({
            "game_id": int(r["game_id"]),
            "competition_id": p.competition_id(r["parent"].get("href")),
            "competition_type": r["parent"].get("competition_type"),
            "country_name": p.clean_text(r["parent"].get("country_name")),
            "competition_href": r["parent"].get("href"),
            "season": _season(f),
            "game_date": gdate,
            "date_resolved_by": how,
            "date_raw": r.get("date"),
            "home_club_id": p.club_id(r["home_club"].get("href")),
            "away_club_id": p.club_id(r["away_club"].get("href")),
            "home_club_href": r["home_club"].get("href"),
            "away_club_href": r["away_club"].get("href"),
            "home_club_name": p.clean_text(r.get("home_club_name")),
            "away_club_name": p.clean_text(r.get("away_club_name")),
            "home_goals": home,
            "away_goals": away,
            "decided_by": p.decided_by(r.get("half_time_score"), r.get("events")),
            "matchday": md,
            "round": rnd,
            "stadium": p.clean_text(r.get("stadium")),
            "attendance": p.thousands_int(r.get("attendance")),
            "referee": p.clean_text(r.get("referee")),
        })
    df = pd.DataFrame(rows)
    stats.update(f"date_by_{k}" for k in df["date_resolved_by"])

    # Dedup: un registro por game_id, el de la temporada (archivo) más reciente
    before = len(df)
    df = df.sort_values(["game_id", "season"], ascending=[True, False]).drop_duplicates("game_id", keep="first")
    stats["dropped_duplicate_game_id"] = before - len(df)

    # Sin fecha válida no se puede particionar ni ubicar en el tiempo: se descarta
    invalid = df["game_date"].isna()
    stats["dropped_invalid_date"] = int(invalid.sum())
    df = df[~invalid]

    # Sin resultado parseable el partido no sirve para análisis: se descarta
    no_score = df["home_goals"].isna() | df["away_goals"].isna()
    stats["dropped_no_score"] = int(no_score.sum())
    df = df[~no_score]

    stats["rows_out"] = len(df)
    return df.reset_index(drop=True), dict(stats)


# ---------------------------------------------------------------- dimensiones
def _player_row(rec, season, source):
    """Perfil de jugador. Sirve para registros de players y para el perfil embebido en appearances."""
    first = p.clean_text(rec.get("name"))
    last = p.clean_text(rec.get("last_name"))
    position, sub_position = p.split_position(rec.get("position"))
    current_club = p.club_id((rec.get("current_club") or {}).get("href"))
    return {
        "player_id": p.player_id(rec.get("href")),
        "player_name": " ".join(x for x in (first, last) if x) or None,
        "first_name": first,
        "last_name": last,
        "date_of_birth": p.profile_date(rec.get("date_of_birth")),
        "citizenship": p.clean_text(rec.get("citizenship")),
        "position": position,
        "sub_position": sub_position,
        "foot": p.clean_text(rec.get("foot")),
        "height_cm": p.height_cm(rec.get("height")),
        # el club 515 de Transfermarkt significa "sin club": se guarda como NULL
        "current_club_id": None if current_club == p.WITHOUT_CLUB_ID else current_club,
        "contract_expires": p.profile_date(rec.get("contract_expires")),
        "shirt_number": p.to_int(rec.get("number")),
        "international_caps": p.to_int(rec.get("international_caps")),
        "season": season,
        "source": source,
    }


def scan_appearances_for_dimensions(known_players):
    """
    Una sola pasada por appearances (sin guardarlas en memoria) para juntar:
      - el perfil de jugadores que no están en players (huérfanos a recuperar)
      - los clubes ('for' y 'opponent') con su link, para detectar clubes huérfanos
    """
    recovered = {}
    club_hrefs = {}
    for f, a in iter_bronze_json("appearances"):
        pid = p.player_id(a["parent"].get("href"))
        if pid not in known_players:
            recovered[(pid, _season(f))] = _player_row(a["parent"], _season(f), "appearances")
        for key in ("for", "opponent"):
            href = (a.get(key) or {}).get("href")
            cid = p.club_id(href)
            if cid is not None and cid not in club_hrefs:
                club_hrefs[cid] = href
    return recovered, club_hrefs


def build_players(recovered):
    stats = Counter()
    rows = [_player_row(r, _season(f), "players") for f, r in iter_bronze_json("players")]
    stats["rows_from_players"] = len(rows)

    # Jugadores que aparecen en partidos pero no en los planteles (ej. transferidos a mitad de
    # temporada). En vez de perder sus apariciones, se recupera su perfil del registro de appearance.
    rows.extend(recovered.values())
    stats["rows_recovered_from_appearances"] = len(recovered)

    df = pd.DataFrame(rows)
    df = df[df["player_id"].notna()]

    # Dedup: un registro por jugador. Gana la temporada más reciente; dentro de la misma
    # temporada, el que viene del plantel ('players') y después el más completo (menos nulos).
    df["_completeness"] = df.notna().sum(axis=1)
    df["_source_rank"] = (df["source"] != "players").astype(int)
    before = len(df)
    df = (df.sort_values(["player_id", "season", "_source_rank", "_completeness"],
                         ascending=[True, False, True, False])
            .drop_duplicates("player_id", keep="first"))
    stats["dropped_older_or_duplicate_versions"] = before - len(df)
    stats["rows_out"] = len(df)
    return df.reset_index(drop=True), dict(stats)


def build_clubs(games, appearance_club_hrefs):
    stats = Counter()
    rows = []
    for f, r in iter_bronze_json("clubs"):
        rows.append({
            "club_id": p.club_id(r.get("href")),
            "club_name": p.clean_text(r.get("name")),
            "club_code": p.clean_text(r.get("code")),
            "competition_id": p.competition_id(r["parent"].get("href")),
            "season": _season(f),
            "squad_size": p.to_int(r.get("squad_size")),
            "average_age": p.to_float(r.get("average_age")),
            "foreigners_number": p.to_int(r.get("foreigners_number")),
            "foreigners_pct": p.to_float(r.get("foreigners_percentage")),
            "national_team_players": p.to_int(r.get("national_team_players")),
            "stadium_name": p.clean_text(r.get("stadium_name")),
            "stadium_seats": p.thousands_int(r.get("stadium_seats")),
            "net_transfer_record_eur": p.money_eur(r.get("net_transfer_record")),
            "coach_name": p.clean_text(r.get("coach_name")),
            "in_catalog": True,
        })
    stats["rows_raw"] = len(rows)
    df = pd.DataFrame(rows)

    # Dedup: el mismo club aparece una vez por temporada; queda la más reciente
    before = len(df)
    df = df.sort_values(["club_id", "season"], ascending=[True, False]).drop_duplicates("club_id", keep="first")
    stats["dropped_older_seasons"] = before - len(df)

    # Huérfanos: rivales de otras divisiones que solo aparecen en partidos de copa.
    # Se agregan como clubes "fuera de catálogo" (solo id y nombre) para no perder esos partidos.
    known = set(df["club_id"])
    stubs = {}
    for _, g in games.iterrows():
        for side in ("home", "away"):
            cid = g[f"{side}_club_id"]
            if cid not in known and cid not in stubs:
                name = g[f"{side}_club_name"]
                stubs[cid] = name if isinstance(name, str) else p.name_from_slug(g[f"{side}_club_href"])
    for cid, href in appearance_club_hrefs.items():
        if cid not in known and cid not in stubs:
            stubs[cid] = p.name_from_slug(href)
    stub_df = pd.DataFrame([{"club_id": k, "club_name": v, "in_catalog": False} for k, v in stubs.items()])
    stats["stubs_added"] = len(stub_df)
    df = pd.concat([df, stub_df], ignore_index=True)
    stats["rows_out"] = len(df)
    return df, dict(stats)


def build_competitions(games):
    stats = Counter()
    rows = []
    for _, r in iter_bronze_json("competitions"):
        conf = (r.get("parent") or {}).get("href") or ""
        rows.append({
            # El id es el código del link (FAC, CDR, ES1…). country_code NO sirve: es el del país.
            "competition_id": p.competition_id(r.get("href")),
            "competition_name": p.clean_text(r.get("competition_name")),   # 'Torneo Apertura ' -> sin espacio
            "competition_type": p.clean_text(r.get("competition_type")),
            "country_name": p.clean_text(r.get("country_name")),
            "confederation": conf.rstrip("/").split("/")[-1] or None,
            "total_clubs": p.to_int(r.get("total_clubs")),
            "in_catalog": True,
        })
    stats["rows_raw"] = len(rows)
    df = pd.DataFrame(rows)
    before = len(df)
    df = df.drop_duplicates("competition_id", keep="first")
    stats["dropped_duplicates"] = before - len(df)

    # En copas, total_clubs viene copiado de la liga del país (ej. Copa del Rey = 20): no es real
    df.loc[df["competition_type"] != "first_tier", "total_clubs"] = None

    # Huérfanas: competiciones con partidos pero sin ficha (ej. la Copa de la Liga inglesa, CGB).
    # Se reconstruyen desde los datos del partido.
    missing = games[~games["competition_id"].isin(df["competition_id"])]
    stubs = (missing.drop_duplicates("competition_id")
             .assign(competition_name=lambda x: x["competition_href"].map(p.name_from_slug), in_catalog=False)
             [["competition_id", "competition_name", "competition_type", "country_name", "in_catalog"]])
    # La confederación de una competición reconstruida es la de su país
    conf_by_country = dict(zip(df["country_name"], df["confederation"]))
    stubs["confederation"] = stubs["country_name"].map(conf_by_country)
    stats["stubs_added"] = len(stubs)
    df = pd.concat([df, stubs], ignore_index=True)
    stats["rows_out"] = len(df)
    return df, dict(stats)


def build_dimensions() -> dict:
    """Tarea silver_dimensions: competitions, clubs y players completos."""
    games, _ = load_games()
    known_players = {p.player_id(r.get("href")) for _, r in iter_bronze_json("players")}
    recovered, club_hrefs = scan_appearances_for_dimensions(known_players)

    competitions, s_comp = build_competitions(games)
    clubs, s_clubs = build_clubs(games, club_hrefs)
    players, s_players = build_players(recovered)

    write_parquet(competitions, SILVER_DIR / "competitions" / "competitions.parquet", COMPETITIONS_SCHEMA)
    write_parquet(clubs, SILVER_DIR / "clubs" / "clubs.parquet", CLUBS_SCHEMA)
    write_parquet(players, SILVER_DIR / "players" / "players.parquet", PLAYERS_SCHEMA)

    report = {"competitions": s_comp, "clubs": s_clubs, "players": s_players}
    log.info("Calidad silver_dimensions: %s", report)
    return report


# ---------------------------------------------------------------- hechos del mes
def build_appearances(games_all):
    """Todas las apariciones de Bronze, tipadas y deduplicadas. Se leen de a una (no se guarda el crudo)."""
    stats = Counter()
    rows = []
    for f, a in iter_bronze_json("appearances"):
        res = a.get("result")
        gid = p.game_id(res.get("href")) if isinstance(res, dict) else p.to_int(a.get("game_id"))
        pid = p.player_id(a.get("href"))
        rows.append({
            "_file": f,
            "_date_raw": a.get("date"),
            "appearance_id": f"{gid}_{pid}",
            "game_id": gid,
            "player_id": pid,
            "club_id": p.club_id((a.get("for") or {}).get("href")),
            "opponent_club_id": p.club_id((a.get("opponent") or {}).get("href")),
            "competition_id": p.clean_text(a.get("competition_code")),
            "is_home": {"H": True, "A": False}.get(a.get("venue")),
            "position": p.clean_text(a.get("pos")),
            "minutes_played": p.minutes(a.get("minutes_played")),
            # '' en goles/asistencias = no convirtió (0); las tarjetas traen el minuto -> 1/0
            "goals": p.count_or_zero(a.get("goals")),
            "assists": p.count_or_zero(a.get("assists")),
            "yellow_cards": p.card_flag(a.get("yellow_cards")),
            "second_yellow_cards": p.card_flag(a.get("second_yellow_cards")),
            "red_cards": p.card_flag(a.get("red_cards")),
        })
    stats["raw_rows"] = len(rows)
    df = pd.DataFrame(rows)

    # La fecha de la aparición es la del partido. Si el partido no está, se usa la fecha
    # cruda con el formato dominante del archivo (mismo criterio que games).
    formats = {f: p.infer_date_format(g["_date_raw"])[0] for f, g in df.groupby("_file")}
    game_dates = dict(zip(games_all["game_id"], games_all["game_date"]))
    df["game_date"] = [game_dates.get(gid) or p.plain_date(raw, formats[f])
                       for gid, raw, f in zip(df["game_id"], df["_date_raw"], df["_file"])]
    df = df.drop(columns=["_file", "_date_raw"])

    # Sin jugador o sin partido identificable no se puede unir con nada: se descarta
    no_key = df["game_id"].isna() | df["player_id"].isna()
    stats["dropped_no_key"] = int(no_key.sum())
    df = df[~no_key]

    # Dedup: una fila por (partido, jugador). Los duplicados del crudo son copias exactas;
    # si difirieran, gana la de más minutos.
    before = len(df)
    df = (df.sort_values(["appearance_id", "minutes_played"], ascending=[True, False])
            .drop_duplicates("appearance_id", keep="first"))
    stats["dropped_duplicate_appearance"] = before - len(df)

    # Huérfanas: apariciones de partidos que no quedaron en Silver (ej. partido sin fecha válida)
    orphan = ~df["game_id"].isin(set(games_all["game_id"]))
    stats["dropped_orphan_game"] = int(orphan.sum())
    df = df[~orphan]

    stats["rows_out_all_months"] = len(df)
    return df.reset_index(drop=True), dict(stats)


def build_facts(start: date, end: date) -> dict:
    """Tarea silver_facts: games y appearances con fecha en [start, end)."""
    month = f"{start:%Y-%m}"
    games_all, s_games = load_games()
    appearances_all, s_apps = build_appearances(games_all)

    in_month_g = (games_all["game_date"] >= start) & (games_all["game_date"] < end)
    in_month_a = (appearances_all["game_date"] >= start) & (appearances_all["game_date"] < end)
    games, apps = games_all[in_month_g], appearances_all[in_month_a]

    games_path = SILVER_DIR / "games" / f"{month}.parquet"
    apps_path = SILVER_DIR / "appearances" / f"{month}.parquet"
    if len(games):
        write_parquet(games, games_path, GAMES_SCHEMA)
    else:
        remove_if_exists(games_path)
    if len(apps):
        write_parquet(apps, apps_path, APPEARANCES_SCHEMA)
    else:
        remove_if_exists(apps_path)

    report = {"month": month, "games_month": len(games), "appearances_month": len(apps),
              "games_quality": s_games, "appearances_quality": s_apps}
    log.info("Calidad silver_facts: %s", report)
    return report


# ---------------------------------------------------------------- PII
def hash_email(email: str, salt: str) -> str:
    """SHA-256(salt + email normalizado). El mismo email siempre da el mismo hash."""
    return hashlib.sha256((salt + email).encode("utf-8")).hexdigest()


def mask_pii(salt: str) -> dict:
    """Tarea mask_pii: player_contacts sin datos en claro."""
    if not salt:
        raise ValueError("PII_SALT está vacío: cargarlo con `airflow variables set PII_SALT ...`")
    df = read_bronze_csv("player_contacts")
    stats = Counter(raw_rows=len(df))

    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
    # Normalizar antes de hashear: '  JUAN@X.COM ' y 'juan@x.com' tienen que dar el mismo hash
    email = df["email"].str.strip().str.lower()
    df["email"] = email.where(email != "", None)

    # Sin email no hay nada que proteger ni con qué unir: se descarta el contacto (no el jugador)
    no_email = df["email"].isna() | df["player_id"].isna()
    stats["dropped_no_email"] = int(no_email.sum())
    df = df[~no_email]

    # Dedup: un contacto por jugador, el de updated_at más reciente
    before = len(df)
    df = df.sort_values(["player_id", "updated_at"], ascending=[True, False]).drop_duplicates("player_id")
    stats["dropped_older_contacts"] = before - len(df)

    df["email_hash"] = df["email"].map(lambda e: hash_email(e, salt))
    # full_name, phone_number y el email en claro NO pasan a Silver
    df = df[["player_id", "email_hash", "updated_at"]]

    write_parquet(df, SILVER_DIR / "player_contacts" / "player_contacts.parquet", CONTACTS_SCHEMA)
    stats["rows_out"] = len(df)
    log.info("Calidad mask_pii: %s", dict(stats))
    return dict(stats)
