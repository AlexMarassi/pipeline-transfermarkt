"""
Servidor local para la presentación: sirve el HTML y deja correr DuckDB en vivo desde las diapositivas.

- En / sirve la presentación (Pipeline Transfermarkt.html, en la raíz del repo o en la carpeta de arriba).
- El resto de las rutas son archivos del repo, así la diapositiva de código lee el DAG tal como está.
- /api/sql corre una consulta de solo lectura sobre las vistas de Gold (las mismas de sql/create_gold_views.sql).
- /api/bench corre el benchmark JSON crudo vs. Parquet de Gold (ver benchmark_formatos.py).
- Al servirse desde localhost, el iframe de Airflow (localhost:8080) queda en el mismo sitio y la sesión funciona.

Uso (fuera de Docker, desde la raíz del repo, con el entorno virtual):
    .venv/bin/python scripts/presentar.py
y abrir http://localhost:8000/
"""
import json
import math
import re
import sys
import time
import webbrowser
from decimal import Decimal
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb

ROOT = Path(__file__).resolve().parent.parent
GOLD_DIR = ROOT / "data" / "gold"
HTML_NAME = "Pipeline Transfermarkt.html"
PORT = 8000
MAX_ROWS = 200

sys.path.insert(0, str(Path(__file__).resolve().parent))
import benchmark_formatos  # noqa: E402

# Solo consultas de lectura: la conexión es en memoria, pero no queremos COPY, ATTACH ni INSTALL desde el navegador
READ_ONLY = re.compile(r"^\s*(select|with|from|explain|describe|summarize|show|pragma\s+show)\b", re.IGNORECASE)


def find_html() -> Path:
    for folder in (ROOT, ROOT.parent):
        if (folder / HTML_NAME).exists():
            return folder / HTML_NAME
    sys.exit(f"No encontré {HTML_NAME} en {ROOT} ni en {ROOT.parent}")


def catalog() -> duckdb.DuckDBPyConnection:
    """Conexión en memoria con las vistas de Gold apuntando a data/gold de esta máquina."""
    con = duckdb.connect()
    sql = (ROOT / "sql" / "create_gold_views.sql").read_text(encoding="utf-8")
    con.execute(sql.replace("{{GOLD_DIR}}", GOLD_DIR.as_posix()))
    return con


def to_json(v):
    """Lo que devuelve DuckDB, en algo que JSON acepte (las sumas llegan como Decimal y las fechas como date)."""
    if isinstance(v, Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, (int, float, str, type(None))):
        return v
    return str(v)


def run_sql(sql: str) -> dict:
    sql = sql.strip().rstrip(";")
    if ";" in sql or not READ_ONLY.match(sql):
        return {"error": "Solo se permite una consulta de lectura (SELECT, WITH, EXPLAIN, DESCRIBE, SUMMARIZE)."}
    con = catalog()
    try:
        t0 = time.perf_counter()
        rel = con.execute(sql)
        rows = rel.fetchmany(MAX_ROWS + 1)
        ms = (time.perf_counter() - t0) * 1000
        cols = [d[0] for d in rel.description]
    except duckdb.Error as e:
        return {"error": str(e)}
    finally:
        con.close()
    return {
        "columns": cols,
        "rows": [[to_json(v) for v in r] for r in rows[:MAX_ROWS]],
        "truncated": len(rows) > MAX_ROWS,
        "ms": round(ms, 2),
    }


class Handler(SimpleHTTPRequestHandler):
    def log_message(self, fmt, *args):
        if "/api/" in (args[0] if args else ""):
            super().log_message(fmt, *args)

    def send_json(self, payload: dict, status: int = 200):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/api/health":
            return self.send_json({"ok": True, "duckdb": duckdb.__version__})
        if url.path == "/api/bench":
            case = parse_qs(url.query).get("case", [""])[0]
            if case not in benchmark_formatos.CASES:
                return self.send_json({"error": f"caso desconocido: {case}"}, 400)
            return self.send_json(benchmark_formatos.measure(case, repeat=3))
        if url.path in ("/", "/index.html"):
            body = self.server.html.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
            return
        return super().do_GET()

    def do_POST(self):
        if urlparse(self.path).path != "/api/sql":
            return self.send_json({"error": "no encontrado"}, 404)
        length = int(self.headers.get("Content-Length", 0))
        try:
            sql = json.loads(self.rfile.read(length) or b"{}").get("sql", "")
        except json.JSONDecodeError:
            return self.send_json({"error": "JSON inválido"}, 400)
        return self.send_json(run_sql(sql))


def main():
    server = ThreadingHTTPServer(("127.0.0.1", PORT), partial(Handler, directory=str(ROOT)))
    server.html = find_html()
    url = f"http://localhost:{PORT}/"
    print(f"Presentación ({server.html}) en {url}  (Ctrl+C para cortar)")
    webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
