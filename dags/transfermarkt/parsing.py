"""
Funciones de limpieza de valores sueltos del crudo de Transfermarkt.

Son funciones puras (texto -> valor limpio o None) para poder probarlas solas.
Cada una documenta el formato crudo que resuelve.
"""
import re
from collections import Counter
from datetime import date, datetime

# ---------- IDs a partir de links ----------
_CLUB = re.compile(r"/verein/(\d+)")
_GAME = re.compile(r"/spielbericht/(\d+)")
_PLAYER = re.compile(r"/spieler/(\d+)")
_COMP = re.compile(r"/(?:wettbewerb|pokalwettbewerb)/([^/?]+)")
_SLUG = re.compile(r"^/([^/]+)/")

# Transfermarkt usa el club 515 para "sin club"
WITHOUT_CLUB_ID = 515


def club_id(href):
    m = _CLUB.search(href or "")
    return int(m.group(1)) if m else None


def game_id(href):
    m = _GAME.search(href or "")
    return int(m.group(1)) if m else None


def player_id(href):
    m = _PLAYER.search(href or "")
    return int(m.group(1)) if m else None


def competition_id(href):
    """'/fa-cup/startseite/pokalwettbewerb/FAC' -> 'FAC'. OJO: country_code NO es la competición."""
    m = _COMP.search(href or "")
    return m.group(1) if m else None


def name_from_slug(href):
    """'/cd-castellon/startseite/verein/2502' -> 'Cd Castellon'. Solo para registros sin nombre."""
    m = _SLUG.search(href or "")
    return m.group(1).replace("-", " ").title() if m else None


# ---------- texto ----------
_SPACES = re.compile(r"\s+")


def clean_text(value):
    """Saca espacios (incluido NBSP) y saltos de línea; vacío -> None."""
    if value is None:
        return None
    s = _SPACES.sub(" ", str(value).replace("\xa0", " ")).strip()
    return s or None


# ---------- números ----------
def to_int(value):
    """'29' -> 29; '' / None / '-' -> None."""
    s = clean_text(value)
    if s is None:
        return None
    m = re.search(r"-?\d+", s)
    return int(m.group(0)) if m else None


def count_or_zero(value):
    """Goles/asistencias: '' significa 0 (el jugador jugó y no convirtió), '2' -> 2."""
    n = to_int(value)
    return 0 if n is None else n


def card_flag(value):
    """Tarjetas: el crudo trae el MINUTO ("90 + 4'") o '✔', no una cantidad. Algo -> 1, vacío -> 0."""
    return 1 if clean_text(value) else 0


def minutes(value):
    """"90'" -> 90."""
    return to_int(value)


def thousands_int(value):
    """Separador de miles con punto: 'Attendance: 21.341' -> 21341, '8.396 Seats' -> 8396."""
    s = clean_text(value)
    if s is None:
        return None
    m = re.search(r"\d[\d.]*", s)
    return int(m.group(0).replace(".", "")) if m else None


def to_float(value):
    """'25.8' -> 25.8, '58.6 %' -> 58.6."""
    s = clean_text(value)
    if s is None:
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group(0)) if m else None


def money_eur(value):
    """'+€1.40m' -> 1400000.0, '-€500k' -> -500000.0, '+-0' -> 0.0, '€5.44bn' -> 5.44e9."""
    s = clean_text(value)
    if s is None:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)\s*(bn|m|k)?", s)
    if not m:
        return None
    amount = float(m.group(1)) * {"bn": 1e9, "m": 1e6, "k": 1e3, None: 1}[m.group(2)]
    return -amount if s.startswith("-") else amount


def height_cm(value):
    """'1,85\\xa0m' -> 185, '1,9 m' -> 190."""
    s = clean_text(value)
    if s is None:
        return None
    m = re.search(r"(\d+),(\d+)", s)
    if not m:
        return None
    return int(m.group(1)) * 100 + int(m.group(2).ljust(2, "0")[:2])


def score(result):
    """'2:1' -> (2, 1)."""
    m = re.match(r"^\s*(\d+)\s*:\s*(\d+)\s*$", result or "")
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)


def decided_by(half_time_score, events):
    """
    Cómo se definió el partido. Cuando hubo tanda de penales el resultado del crudo ya los incluye
    (14:13 en vez de 2:2). Se detecta por los eventos "Shootout" del partido y, si faltan, por
    "on pens" en half_time_score. "AET" marca el alargue.
    """
    text = (half_time_score or "").lower()
    has_shootout = any(isinstance(e, dict) and e.get("type") == "Shootout" for e in (events or []))
    if has_shootout or "pens" in text:
        return "penalties"
    if "aet" in text:
        return "extra_time"
    return "regular"


def matchday(value):
    """'23. Matchday' -> (23, None); 'First Round' -> (None, 'First Round')."""
    s = clean_text(value)
    if s is None:
        return None, None
    m = re.match(r"^(\d+)\.?\s*Matchday$", s)
    return (int(m.group(1)), None) if m else (None, s)


def split_position(value):
    """'Attack - Centre-Forward' -> ('Attack', 'Centre-Forward'); 'Goalkeeper' -> ('Goalkeeper', 'Goalkeeper')."""
    s = clean_text(value)
    if s is None:
        return None, None
    parts = [p.strip() for p in s.split(" - ", 1)]
    return (parts[0], parts[1]) if len(parts) == 2 else (parts[0], parts[0])


# ---------- fechas ----------
def profile_date(value):
    """Fechas de perfil: 'Jun 18, 2002' o '18/06/2002' (día/mes). '-' o vacío -> None."""
    s = clean_text(value)
    if s is None or s == "-":
        return None
    for fmt in ("%b %d, %Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


MD = "%m/%d/%y"   # mes/día/año
DM = "%d/%m/%y"   # día/mes/año


def infer_date_format(raw_dates):
    """
    Formato dominante de un archivo a partir de las fechas NO ambiguas
    (las que tienen un número mayor a 12). Devuelve (formato, votos).
    """
    votes = Counter()
    for raw in raw_dates:
        rest = raw.split(", ")[-1]
        try:
            a, b, _ = (int(x) for x in rest.split("/"))
        except ValueError:
            continue
        if a > 12 >= b:
            votes[DM] += 1
        elif b > 12 >= a:
            votes[MD] += 1
    fmt = DM if votes[DM] > votes[MD] else MD
    return fmt, dict(votes)


def game_date(raw, default_fmt):
    """
    Fecha de partido 'Sat, 2/24/24' (mes/día) o 'Wed, 30/10/24' (día/mes).

    1. Se prueban ambos formatos y se quedan los que coinciden con el día de la semana.
    2. Si queda uno solo, es ese.
    3. Si quedan los dos (ej. 'Sun, 12/3/23': 3-dic y 12-mar son domingo), gana el formato del archivo.
    Devuelve (fecha, cómo se resolvió) o (None, 'invalid').
    """
    if not raw or ", " not in raw:
        return None, "invalid"
    weekday, rest = raw.split(", ", 1)
    matches = {}
    for fmt in (MD, DM):
        try:
            d = datetime.strptime(rest.strip(), fmt).date()
        except ValueError:
            continue
        if d.strftime("%a") == weekday.strip():
            matches[fmt] = d
    if len(set(matches.values())) == 1:
        return next(iter(matches.values())), "weekday"
    if len(matches) == 2:
        return matches[default_fmt], "file_format"
    return None, "invalid"


def plain_date(raw, fmt):
    """Fecha sin día de semana ('5/19/24'), con el formato del archivo."""
    try:
        return datetime.strptime((raw or "").strip(), fmt).date()
    except ValueError:
        return None


def month_bounds(start: date):
    """(primer día del mes, primer día del mes siguiente)."""
    first = start.replace(day=1)
    nxt = date(first.year + (first.month == 12), first.month % 12 + 1, 1)
    return first, nxt
