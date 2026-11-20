"""
API Flask de LifeMap: reputación, ubicación y supervivencia de negocios.

  GET  /health                 estado de MongoDB, Spark, Dask y la API + hash del commit desplegado
  GET  /businesses/near        lat, lon, radio (m), categoria?, limit?        -> $near
  GET|POST /businesses/within  polígono GeoJSON (cuerpo o ?poligono=), categoria?  -> $geoWithin
  GET  /zones/summary          lat, lon, radio, categoria?, min_dias?          -> agregación $geoNear
  GET  /stats/closure          por=categoria|celda, min_dias, orden, limit     -> tasas de Spark
  GET|POST /predict            texto -> sentimiento (modelo entrenado en Spark)
Entrega 2:
  GET  /live/hotspots          fecha?, lat?, lon?, radio?, solo_hotspots?   -> ventanas de 30 días por celda
  GET  /live/evolucion         geohash?                                      -> % negativo por ventana
  GET|POST /alerts             lat/lon/radio o polígono, activas?            -> alertas del streaming
  GET  /survival/<categoria>   zona?, poligono?, entrada_tardia?, comparar?  -> Kaplan-Meier
  GET  /risk/near              lat, lon, radio, categoria?, limit?           -> probabilidad de cierre
Otros: GET / (página de inicio), GET /meta, GET /reports, GET /reports/<nombre>
"""
from __future__ import annotations

import json
import os
import socket
import time
import urllib.request
from datetime import datetime, timezone

from flask import Flask, jsonify, request, send_from_directory
from pymongo import MongoClient
from pymongo.errors import OperationFailure

import km
import queries as q
from sentiment import ModeloSentimiento
from zonas import ZONAS

APP_VERSION = os.environ.get("APP_VERSION", "local")
GIT_COMMIT = os.environ.get("GIT_COMMIT", "local")
SPARK_UI = os.environ.get("SPARK_MASTER_UI", "http://spark-master:8080")
DASK_UI = os.environ.get("DASK_DASHBOARD", "http://dask-scheduler:8787")
REPORT_DIR = os.environ.get("REPORT_DIR", "/data/reports")
KAFKA_HOST = os.environ.get("KAFKA_HOST", "kafka:9092")
VENTANA_INICIO = datetime.fromisoformat(os.environ.get("VENTANA_INICIO", "2016-01-01"))
MIN_NEGOCIOS_KM = int(os.environ.get("MIN_NEGOCIOS_KM", "100"))


def _kafka_arriba(timeout: float = 2.0) -> bool:
    host, _, port = KAFKA_HOST.partition(":")
    with socket.create_connection((host, int(port or 9092)), timeout=timeout):
        return True


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
                                                          "/zones/summary", "/stats/closure", "/predict",
                                                          "/live/hotspots", "/live/evolucion", "/alerts",
                                                          "/survival/<categoria>", "/risk/near"]), 404

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
        try:
            _kafka_arriba()
            servicios["kafka"] = {"estado": "up", "broker": KAFKA_HOST}
        except Exception as e:  # noqa: BLE001
            servicios["kafka"] = {"estado": "down", "broker": KAFKA_HOST, "detalle": str(e)[:200]}
        try:
            est = {d["_id"]: d for d in db.stream_estado.find()}
            r = est.get("reviews")
            if r:
                act = r.get("actualizado")
                edad = (datetime.now(timezone.utc) - act.replace(tzinfo=timezone.utc)).total_seconds() if act else None
                servicios["kafka"]["consumidor"] = {
                    "estado": "up" if edad is not None and edad < 60 else "down",
                    "retraso_mensajes": r.get("retraso_mensajes"), "offsets_procesados": r.get("offsets_procesados"),
                    "offsets_kafka": r.get("offsets_kafka"), "ultimo_lote": r.get("lote"),
                    "segundos_desde_ultimo_reporte": round(edad, 1) if edad is not None else None,
                    "modelo_cargado": r.get("modelo_sentimiento"), "fecha_corte_modelo": r.get("fecha_corte_modelo"),
                    "watermark_ventanas": (est.get("ventanas") or {}).get("watermark")}
            else:
                servicios["kafka"]["consumidor"] = {"estado": "sin datos", "detalle": "el streaming aún no reporta avance"}
        except Exception as e:  # noqa: BLE001
            servicios["kafka"]["consumidor"] = {"estado": "desconocido", "detalle": str(e)[:200]}
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

    # ------------------------------------------------------------------ Entrega 2: streaming
    @app.get("/live/hotspots")
    def live_hotspots():
        a = request.args
        f, centro = q.fecha(a, "fecha"), q.punto_opcional(a)
        solo, lim = q.si_no(a, "solo_hotspots", False), q.limite(a, 200)
        filtro = {}
        if centro:
            filtro.update(q.circulo(*centro, q.radio(a, 10_000), "centro"))
        if f is None:                       # la ventana más reciente con datos
            ult = list(db.ventanas_celda.find(filtro, {"inicio": 1}).sort("inicio", -1).limit(1))
            f = ult[0]["inicio"] if ult else None
        if f is not None:
            filtro.update({"inicio": {"$lte": f}, "fin": {"$gt": f}})
        if solo:
            filtro["hotspot"] = True
        docs = list(db.ventanas_celda.find(filtro).sort("pct_negativas", -1).limit(lim)) if f is not None else []
        return jsonify(fecha=f.isoformat() if f else None, centro=centro and {"lat": centro[0], "lon": centro[1]},
                       solo_hotspots=solo, celdas=len(docs), hotspots=sum(1 for d in docs if d.get("hotspot")),
                       regla="hotspot = % negativo sube más del umbral frente a la ventana anterior, con un mínimo de reseñas",
                       ventanas=[q.ventana_salida(d) for d in docs])

    @app.get("/live/evolucion")
    def live_evolucion():
        gh = (request.args.get("geohash") or "").strip()
        filtro = {"geohash": gh} if gh else {}
        res = list(db.ventanas_celda.aggregate([
            {"$match": filtro},
            {"$group": {"_id": "$inicio", "fin": {"$first": "$fin"}, "resenas": {"$sum": "$resenas"},
                        "negativas": {"$sum": "$negativas"}, "hotspots": {"$sum": {"$cond": ["$hotspot", 1, 0]}},
                        "celdas": {"$sum": 1}}},
            {"$sort": {"_id": 1}}]))
        filas = [{"inicio": r["_id"].isoformat(), "fin": r["fin"].isoformat(), "resenas": r["resenas"],
                  "negativas": r["negativas"], "celdas": r["celdas"], "hotspots": r["hotspots"],
                  "pct_negativas": round(r["negativas"] / r["resenas"] * 100, 2) if r["resenas"] else None}
                 for r in res]
        return jsonify(geohash=gh or None, ventanas=len(filas), evolucion=filas)

    @app.route("/alerts", methods=["GET", "POST"])
    def alerts():
        a = request.args
        filtro = {"activa": True} if q.si_no(a, "activas", True) else {}
        body = request.get_json(silent=True) if request.method == "POST" else None
        if body or a.get("poligono"):
            src = body.get("poligono", body) if isinstance(body, dict) else (body or a.get("poligono"))
            poly = q.poligono(src)
            filtro["geometry"] = {"$geoWithin": {"$geometry": poly}}
            zona_desc = {"poligono": poly}
        else:
            centro = q.punto_opcional(a)
            zona_desc = None
            if centro:
                r = q.radio(a, 5000)
                filtro.update(q.circulo(*centro, r, "geometry"))
                zona_desc = {"lat": centro[0], "lon": centro[1], "radio_m": r}
        cat = q.categoria(a)
        if cat:
            filtro["categoria_principal"] = cat
        docs = list(db.alerts.find(filtro).sort("probabilidad", -1).limit(q.limite(a, 200)))
        return jsonify(zona=zona_desc, categoria=cat, total=len(docs), alertas=[q.alerta_salida(d) for d in docs])

    @app.get("/survival/<categoria>")
    def survival(categoria):
        a = request.args
        comp = q.comparar(a)
        if comp:                                   # curvas precalculadas para comparar (Power BI)
            docs = list(db.supervivencia.find({"tipo": comp}))
            filas = [{"grupo": d.get("nombre", d["grupo"]), "anios": p["anios"], "supervivencia": p["supervivencia"],
                      "en_riesgo": p["en_riesgo"], "confiable": d["confiable"]} for d in docs for p in d["curva"]]
            return jsonify(comparar=comp, grupos=[{k: d.get(k) for k in ("grupo", "nombre", "negocios", "cierres",
                                                                          "mediana_anios", "confiable", "en_riesgo")}
                                                  for d in docs], puntos=filas)
        tardia = q.si_no(a, "entrada_tardia", True)
        minimo = q._num(a, "min_negocios", MIN_NEGOCIOS_KM, lo=1, cast=int)
        filtro = {} if categoria.lower() in ("todas", "all") else {"categoria_principal": categoria}
        z = q.zona(a, ZONAS)
        body_poly = a.get("poligono")
        if z:
            filtro["location"] = {"$geoWithin": {"$geometry": ZONAS[z]["poligono"]}}
        elif body_poly:
            filtro["location"] = {"$geoWithin": {"$geometry": q.poligono(body_poly)}}
        docs = list(db.businesses.find(filtro, {"vida": 1}))
        r = km.kaplan_meier(*km.datos_negocios(docs, VENTANA_INICIO if tardia else None))
        r["curva"] = km.reducir(r["curva"], q._num(a, "puntos", 150, lo=10, hi=2000, cast=int))
        confiable = r["negocios"] >= minimo
        return jsonify(categoria=categoria, zona=z and {"clave": z, "nombre": ZONAS[z]["nombre"]},
                       entrada_tardia=tardia, ventana_inicio=VENTANA_INICIO.date().isoformat() if tardia else None,
                       min_negocios=minimo, confiable=confiable,
                       aviso=None if confiable else f"Solo {r['negocios']} negocios: no se deben sacar conclusiones",
                       **r)

    @app.get("/risk/near")
    def risk_near():
        lat, lon = q.punto(request.args)
        r, lim, cat = q.radio(request.args, 1000), q.limite(request.args, 50), q.categoria(request.args)
        filtro = {"location": {"$near": {"$geometry": {"type": "Point", "coordinates": [lon, lat]}, "$maxDistance": r}}}
        if cat:
            filtro["categoria_principal"] = cat
        docs = list(db.riesgo_negocios.find(filtro, {"estado": 0, "estado_corte": 0, "variables_corte": 0}).limit(lim))
        docs.sort(key=lambda d: -(d.get("probabilidad") or 0))
        return jsonify(centro={"lat": lat, "lon": lon}, radio_m=r, categoria=cat, total=len(docs),
                       en_alerta=sum(1 for d in docs if d.get("alerta")), negocios=[q.riesgo_salida(d) for d in docs])

    @app.get("/zonas")
    def zonas():
        return jsonify(zonas=[{"clave": k, "nombre": v["nombre"], "poligono": v["poligono"]} for k, v in ZONAS.items()])

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
