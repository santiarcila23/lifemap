"""
API Flask de LifeMap: reputación, ubicación y supervivencia de negocios.

  GET  /health                 estado de MongoDB, Spark, Dask y la API + hash del commit desplegado
  GET  /businesses/near        lat, lon, radio (m), categoria?, limit?        -> $near
  GET|POST /businesses/within  polígono GeoJSON (cuerpo o ?poligono=), categoria?  -> $geoWithin
  GET  /zones/summary          lat, lon, radio, categoria?, min_dias?          -> agregación $geoNear
  GET  /stats/closure          por=categoria|celda, min_dias, orden, limit     -> tasas de Spark
  GET|POST /predict            texto -> sentimiento (modelo entrenado en Spark)
Otros: GET / (página de inicio), GET /meta, GET /reports, GET /reports/<nombre>
"""
from __future__ import annotations

import json
import os
import time
import urllib.request

from flask import Flask, jsonify, request, send_from_directory
from pymongo import MongoClient
from pymongo.errors import OperationFailure

import queries as q
from sentiment import ModeloSentimiento

APP_VERSION = os.environ.get("APP_VERSION", "local")
GIT_COMMIT = os.environ.get("GIT_COMMIT", "local")
SPARK_UI = os.environ.get("SPARK_MASTER_UI", "http://spark-master:8080")
DASK_UI = os.environ.get("DASK_DASHBOARD", "http://dask-scheduler:8787")
REPORT_DIR = os.environ.get("REPORT_DIR", "/data/reports")


def _http_json(url: str, timeout: float = 3.0):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        body = r.read().decode("utf-8", "replace")
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return body


def create_app(db=None, modelo=None) -> Flask:
    app = Flask(__name__, static_folder="static", static_url_path="/static")
    app.json.ensure_ascii = False
    app.json.sort_keys = False

    if db is None:
        client = MongoClient(os.environ.get("MONGO_URI", "mongodb://mongo:27017"), serverSelectionTimeoutMS=4000)
        db = client[os.environ.get("MONGO_DB", "lifemap")]
    if modelo is None:
        modelo = ModeloSentimiento(os.environ.get("MODELS_DIR", "/models"))

    # ------------------------------------------------------------------ utilidades
    @app.errorhandler(q.ValidationError)
    def bad_request(err):
        return jsonify(error=str(err)), 400

    @app.errorhandler(OperationFailure)
    def mongo_rejected(err):
        msg = (getattr(err, "details", None) or {}).get("errmsg", str(err))
        if "Loop is not valid" in msg or "Edges" in msg or "Duplicate vertices" in msg:
            return jsonify(error="El polígono no es válido: sus lados se cruzan o tiene vértices repetidos"), 400
        return jsonify(error=f"MongoDB rechazó la consulta: {msg}"), 400

    @app.errorhandler(404)
    def not_found(_):
        return jsonify(error="Ruta no encontrada", rutas=["/health", "/businesses/near", "/businesses/within",
                                                          "/zones/summary", "/stats/closure", "/predict"]), 404

    @app.before_request
    def start_timer():
        request._t0 = time.perf_counter()

    @app.after_request
    def add_headers(resp):
        t0 = getattr(request, "_t0", None)
        if t0 is not None:
            resp.headers["X-Response-Time-ms"] = f"{(time.perf_counter() - t0) * 1000:.1f}"
        resp.headers["X-App-Version"] = APP_VERSION
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    # ------------------------------------------------------------------ generales
    @app.get("/")
    def index():
        return send_from_directory(app.static_folder, "index.html")

    @app.get("/ping")
    def ping():
        return jsonify(status="ok")

    @app.get("/health")
    def health():
        servicios = {"api": {"estado": "up"}}
        try:
            db.command("ping")
            servicios["mongodb"] = {"estado": "up", "negocios": db.businesses.estimated_document_count(),
                                    "resenas": db.reviews.estimated_document_count()}
        except Exception as e:  # noqa: BLE001
            servicios["mongodb"] = {"estado": "down", "detalle": str(e)[:200]}
        try:
            s = _http_json(f"{SPARK_UI}/json/")
            vivos = s.get("aliveworkers", 0)
            servicios["spark"] = {"estado": "up" if s.get("status") == "ALIVE" and vivos > 0 else "down",
                                  "master": s.get("status"), "workers": vivos, "cores": s.get("cores")}
        except Exception as e:  # noqa: BLE001
            servicios["spark"] = {"estado": "down", "detalle": str(e)[:200]}
        try:
            ok = _http_json(f"{DASK_UI}/health")
            servicios["dask"] = {"estado": "up" if str(ok).strip() == "ok" else "down"}
            try:
                w = _http_json(f"{DASK_UI}/json/counts.json")
                if isinstance(w, dict) and "workers" in w:
                    servicios["dask"]["workers"] = w["workers"]
            except Exception:  # noqa: BLE001
                pass
        except Exception as e:  # noqa: BLE001
            servicios["dask"] = {"estado": "down", "detalle": str(e)[:200]}
        servicios["modelo_sentimiento"] = {"estado": "up" if modelo.cargado else "down", **modelo.info()}
        todo_ok = all(v.get("estado") == "up" for v in servicios.values())
        return jsonify(status="ok" if todo_ok else "degradado", commit=GIT_COMMIT, version=APP_VERSION,
                       servicios=servicios)

    @app.get("/meta")
    def meta():
        rep = db.reports.find_one({"_id": "ingesta"}, {"area": 1, "fecha_corte": 1, "negocios": 1, "resenas": 1,
                                                        "cierres_observados": 1, "censurados": 1}) or {}
        rep.pop("_id", None)
        cats = db.tasas_categoria.find({}, {"_id": 1}).sort("negocios", -1).limit(30)
        rep["categorias"] = [c["_id"] for c in cats]
        return jsonify(rep)

    # ------------------------------------------------------------------ geoespaciales
    @app.get("/businesses/near")
    def near():
        lat, lon = q.punto(request.args)
        r, lim, cat = q.radio(request.args), q.limite(request.args), q.categoria(request.args)
        docs = list(db.businesses.find(q.near_query(lat, lon, r, cat), q.PROYECCION_NEGOCIO).limit(lim))
        return jsonify(centro={"lat": lat, "lon": lon}, radio_m=r, categoria=cat, total=len(docs),
                       negocios=[q.negocio_salida(d) for d in docs])

    @app.route("/businesses/within", methods=["GET", "POST"])
    def within():
        if request.method == "POST":
            body = request.get_json(silent=True)
            if body is None:
                raise q.ValidationError("Envíe un GeoJSON en el cuerpo (Content-Type: application/json)")
            poly_src = body.get("poligono", body) if isinstance(body, dict) else body
        else:
            poly_src = request.args.get("poligono")
            if not poly_src:
                raise q.ValidationError("Falta 'poligono' (GeoJSON Polygon) en el cuerpo POST o en ?poligono=")
        poly = q.poligono(poly_src)
        lim, cat = q.limite(request.args, 200), q.categoria(request.args)
        docs = list(db.businesses.find(q.within_query(poly, cat), q.PROYECCION_NEGOCIO).limit(lim))
        return jsonify(poligono=poly, categoria=cat, total=len(docs), negocios=[q.negocio_salida(d) for d in docs])

    @app.get("/zones/summary")
    def zones_summary():
        lat, lon = q.punto(request.args)
        r, cat, md = q.radio(request.args, 2000), q.categoria(request.args), q.min_dias(request.args)
        res = list(db.businesses.aggregate(q.zone_pipeline(lat, lon, r, cat)))
        return jsonify(centro={"lat": lat, "lon": lon}, radio_m=r, categoria=cat, min_dias=md,
                       resumen=q.resumen_zona(res[0] if res else None, md))

    # ------------------------------------------------------------------ resultados de Spark
    @app.get("/stats/closure")
    def stats_closure():
        p, md, o, lim = q.por(request.args), q.min_dias(request.args), q.orden(request.args), q.limite(request.args, 20)
        coll = db.tasas_categoria if p == "categoria" else db.tasas_celda
        campo = {"tasa": "tasa_cierre_anual", "negocios": "negocios", "cierres": "cierres"}[o]
        proy = {"geometry": 0} if request.args.get("geometria", "no") == "no" else {}
        filtro = {"dias_negocio": {"$gte": md}}
        docs = list(coll.find(filtro, proy).sort(campo, -1).limit(lim))
        excluidos = coll.count_documents({"dias_negocio": {"$lt": md}})
        return jsonify(por=p, min_dias=md, anios_minimos=round(md / 365, 1), orden=o,
                       grupos=len(docs), grupos_excluidos_por_minimo=excluidos,
                       formula="tasa = cierres / (días-negocio / 365) * 100", resultados=docs)

    # ------------------------------------------------------------------ sentimiento
    @app.route("/predict", methods=["GET", "POST"])
    def predict():
        payload = request.get_json(silent=True) if request.method == "POST" else request.args
        t = q.texto(payload)
        if not modelo.cargado:
            return jsonify(error="El modelo de sentimiento todavía no está entrenado (corra el job de Spark)"), 503
        return jsonify(texto=t, **modelo.predecir(t))

    # ------------------------------------------------------------------ reportes
    @app.get("/reports")
    def reports():
        try:
            nombres = sorted(f for f in os.listdir(REPORT_DIR) if f.endswith(".json"))
        except FileNotFoundError:
            nombres = []
        return jsonify(reportes=[f"/reports/{n[:-5]}" for n in nombres])

    @app.get("/reports/<nombre>")
    def report(nombre):
        if not nombre.replace("_", "").isalnum():
            raise q.ValidationError("Nombre de reporte inválido")
        ruta = os.path.join(REPORT_DIR, f"{nombre}.json")
        if not os.path.exists(ruta):
            return jsonify(error=f"No existe el reporte '{nombre}'"), 404
        with open(ruta, encoding="utf-8") as f:
            return jsonify(json.load(f))

    return app
