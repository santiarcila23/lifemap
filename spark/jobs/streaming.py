"""
Streaming de LifeMap (Entrega 2): Kafka -> Spark Structured Streaming -> MongoDB.

Dos consultas sobre el topic `reviews` (clave business_id):

1) Clasificación y alertas (foreachBatch)
   - Carga /models/sentiment_v1 (el de la Entrega 1, entrenado solo hasta T) SIN reentrenar.
   - Cada reseña se guarda en `reviews_stream` con su predicción, probabilidad y la marca de
     procesamiento.
   - Los negocios que recibieron reseñas actualizan su estado en `riesgo_negocios`, se recalculan
     sus variables (riesgo_vars.py) y se puntúan con /models/riesgo_v1. Si superan el umbral
     quedan en `alerts` con su geometría, probabilidad y fecha.
   Los mensajes con prueba=true (prueba de humo de Jenkins) se clasifican y guardan, pero no
   tocan las alertas ni las ventanas.

2) Ventanas geoespaciales
   - Ventanas de 30 días por celda de geohash sobre el campo date de la reseña (tiempo del
     evento) con watermark de 7 días; conteo y % de reseñas negativas (según el modelo).
   - Cada ventana se guarda en `ventanas_celda` con la geometría de su celda. Es hotspot si su %
     negativo sube más de UMBRAL_HOTSPOT puntos frente a la ventana anterior de la misma celda y
     tiene al menos MIN_RESENAS_VENTANA reseñas.

Cada 5 s se guarda en `stream_estado` el avance de cada consulta (offsets y retraso del
consumidor) para /health.
"""
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone

from pymongo import GEOSPHERE, MongoClient, ReplaceOne, UpdateOne
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

import geohash
import riesgo_vars as rv
from comun import CHECKPOINTS, KAFKA, MODELS, MONGO_DB, MONGO_URI, TOPIC, log, sesion

T = "STREAM"
VENTANA = os.environ.get("VENTANA", "30 days")
WATERMARK = os.environ.get("WATERMARK", "7 days")
UMBRAL_HOTSPOT = float(os.environ.get("UMBRAL_HOTSPOT", "15"))        # puntos porcentuales
MIN_RESENAS_VENTANA = int(os.environ.get("MIN_RESENAS_VENTANA", "30"))
MAX_OFFSETS = int(os.environ.get("MAX_OFFSETS_POR_LOTE", "5000"))
TRIGGER = os.environ.get("TRIGGER", "5 seconds")
INICIO_OFFSETS = os.environ.get("STARTING_OFFSETS", "latest")
CLASES = ["negativo", "neutral", "positivo"]

db = MongoClient(MONGO_URI)[MONGO_DB]
spark = sesion("lifemap_streaming")
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------- modelos (se cargan, no se entrenan)
modelo_sent = PipelineModel.load(os.path.join(MODELS, "sentiment_v1"))
with open(os.path.join(MODELS, "sentiment_v1", "lifemap_version.json"), encoding="utf-8") as f:
    meta_sent = json.load(f)
modelo_riesgo = PipelineModel.load(os.path.join(MODELS, "riesgo_v1"))
with open(os.path.join(MODELS, "riesgo_v1.json"), encoding="utf-8") as f:
    meta_riesgo = json.load(f)
UMBRAL_RIESGO = float(os.environ.get("UMBRAL_RIESGO", meta_riesgo["umbral"]))
log(T, f"Modelo de sentimiento {meta_sent['version']} ({meta_sent['modelo']}, corte {meta_sent['fecha_corte']}) "
       f"y de riesgo {meta_riesgo['version']} ({meta_riesgo['modelo']}, umbral {UMBRAL_RIESGO}) cargados")

db.alerts.create_index([("geometry", GEOSPHERE)])
db.alerts.create_index([("probabilidad", -1)])
db.ventanas_celda.create_index([("geometry", GEOSPHERE)])
db.ventanas_celda.create_index([("centro", GEOSPHERE)])
db.ventanas_celda.create_index([("inicio", 1), ("geohash", 1)])
db.reviews_stream.create_index([("corrida", 1)])
db.reviews_stream.create_index([("procesado_en", -1)])

# ---------------------------------------------------------------- fuente Kafka
esquema = StructType([
    StructField("review_id", StringType()), StructField("business_id", StringType()),
    StructField("stars", IntegerType()), StructField("text", StringType()), StructField("date", StringType()),
    StructField("geohash", StringType()), StructField("categoria_principal", StringType()),
    StructField("prueba", BooleanType()), StructField("corrida", StringType()),
])
crudo = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", KAFKA).option("subscribe", TOPIC)
         .option("startingOffsets", INICIO_OFFSETS).option("maxOffsetsPerTrigger", MAX_OFFSETS)
         .option("failOnDataLoss", "false").load())
mensajes = (crudo.select(F.col("key").cast("string").alias("clave"), "partition", "offset",
                         F.from_json(F.col("value").cast("string"), esquema).alias("m"))
            .select("m.*", "clave", "partition", "offset")
            .withColumn("fecha", F.to_timestamp("date"))
            .withColumn("prueba", F.coalesce("prueba", F.lit(False)))
            .filter(F.col("text").isNotNull() & F.col("fecha").isNotNull()))
clasificadas = (modelo_sent.transform(mensajes)
                .select("review_id", "business_id", "stars", "text", "fecha", "geohash", "categoria_principal",
                        "prueba", "corrida", "partition", "offset", F.col("prediction").cast("int").alias("pred"),
                        vector_to_array("probability").alias("probs")))


# ---------------------------------------------------------------- consulta 1: reseñas + alertas
def puntuar(negocios: dict) -> dict:
    """negocio -> probabilidad de cierre con el modelo de riesgo (un DataFrame pequeño por lote)."""
    if not negocios:
        return {}
    filas = [(bid, *[float(v[k]) for k in rv.VARIABLES]) for bid, v in negocios.items()]
    df = spark.createDataFrame(filas, ["business_id"] + rv.VARIABLES)
    out = modelo_riesgo.transform(df).withColumn("p1", vector_to_array("probability")[1])
    return {r["business_id"]: float(r["p1"]) for r in out.select("business_id", "p1").collect()}


def procesar_lote(df, lote_id):
    filas = df.collect()
    if not filas:
        return
    ahora = datetime.now(timezone.utc)
    ops = []
    for r in filas:
        probs = list(r["probs"])
        _id = f"{r['corrida']}:{r['review_id']}" if r["prueba"] else r["review_id"]
        ops.append(ReplaceOne({"_id": _id}, {
            "_id": _id, "review_id": r["review_id"], "business_id": r["business_id"], "stars": r["stars"],
            "date": r["fecha"], "geohash": r["geohash"], "categoria_principal": r["categoria_principal"],
            "text": (r["text"] or "")[:500], "prediccion": r["pred"], "sentimiento": CLASES[r["pred"]],
            "probabilidad": round(max(probs), 4), "probabilidades": dict(zip(CLASES, [round(p, 4) for p in probs])),
            "procesado_en": ahora, "lote": int(lote_id), "modelo": meta_sent["version"],
            "kafka": {"particion": r["partition"], "offset": r["offset"]},
            "prueba": bool(r["prueba"]), "corrida": r["corrida"]}, upsert=True))
    db.reviews_stream.bulk_write(ops, ordered=False)

    reales = [r for r in filas if not r["prueba"]]
    if not reales:
        log(T, f"Lote {lote_id}: {len(filas)} reseñas de prueba clasificadas")
        return
    # estado y variables de los negocios que recibieron reseñas
    por_negocio = {}
    for r in reales:
        por_negocio.setdefault(r["business_id"], []).append(r)
    docs = {d["_id"]: d for d in db.riesgo_negocios.find({"_id": {"$in": list(por_negocio)}})}
    variables, fechas = {}, {}
    for bid, rs in por_negocio.items():
        d = docs.get(bid)
        if d is None:
            continue           # negocio sin reseñas antes de T: el modelo no tiene su historia
        e = d["estado"]
        for r in rs:
            rv.acumular(e, r["fecha"], r["stars"], r["pred"])
        fecha_ref = max(r["fecha"] for r in rs).replace(tzinfo=None)
        variables[bid], fechas[bid] = rv.variables(e, fecha_ref, d.get("competidores_500m", 0)), fecha_ref
        d["estado"] = e
    probs = puntuar(variables)
    upd, alertas, n_alertas = [], [], 0
    for bid, p in probs.items():
        d, p = docs[bid], round(p, 4)
        alerta = p >= UMBRAL_RIESGO
        upd.append(UpdateOne({"_id": bid}, {"$set": {"estado": d["estado"], "variables": variables[bid],
                                                     "probabilidad": p, "alerta": alerta, "fecha_ref": fechas[bid],
                                                     "actualizado": ahora}}))
        if alerta:
            n_alertas += 1
            alertas.append(UpdateOne({"_id": bid}, {
                "$set": {"business_id": bid, "nombre": d["nombre"], "categoria_principal": d["categoria_principal"],
                         "geometry": d["location"], "probabilidad": p, "umbral": UMBRAL_RIESGO, "activa": True,
                         "fecha": fechas[bid], "procesado_en": ahora, "lote": int(lote_id),
                         "variables": variables[bid], "modelo": meta_riesgo["version"]},
                "$setOnInsert": {"primera_alerta": ahora}}, upsert=True))
        else:
            alertas.append(UpdateOne({"_id": bid, "activa": True},
                                     {"$set": {"activa": False, "probabilidad": p, "procesado_en": ahora}}))
    if upd:
        db.riesgo_negocios.bulk_write(upd, ordered=False)
    if alertas:
        db.alerts.bulk_write(alertas, ordered=False)
    hasta = max(r["fecha"] for r in reales)
    log(T, f"Lote {lote_id}: {len(filas)} reseñas (hasta {hasta:%Y-%m-%d}) | {len(probs)} negocios puntuados | "
           f"{n_alertas} sobre el umbral")


# ---------------------------------------------------------------- consulta 2: ventanas por celda
ventanas = (clasificadas.filter(~F.col("prueba") & F.col("geohash").isNotNull())
            .withWatermark("fecha", WATERMARK)
            .groupBy(F.window("fecha", VENTANA).alias("w"), "geohash")
            .agg(F.count("*").alias("resenas"), F.sum(F.when(F.col("pred") == 0, 1).otherwise(0)).alias("negativas")))
DUR = timedelta(days=int(VENTANA.split()[0]))


def es_hotspot(pct, pct_ant, n) -> bool:
    return pct_ant is not None and n >= MIN_RESENAS_VENTANA and (pct - pct_ant) > UMBRAL_HOTSPOT


def _clave(gh, inicio):
    return f"{gh}|{inicio:%Y-%m-%d}"


def guardar_ventanas(df, lote_id):
    filas = df.collect()
    if not filas:
        return
    ahora = datetime.now(timezone.utc)
    ops = []
    for r in filas:
        ini, fin = r["w"]["start"], r["w"]["end"]
        pct = round(r["negativas"] / r["resenas"] * 100, 2) if r["resenas"] else 0.0
        lat, lon = geohash.center(r["geohash"])
        ops.append(UpdateOne({"_id": _clave(r["geohash"], ini)}, {"$set": {
            "geohash": r["geohash"], "inicio": ini, "fin": fin, "resenas": int(r["resenas"]),
            "negativas": int(r["negativas"]), "pct_negativas": pct, "geometry": geohash.polygon(r["geohash"]),
            "centro": {"type": "Point", "coordinates": [lon, lat]}, "actualizado": ahora, "lote": int(lote_id)}},
            upsert=True))
    db.ventanas_celda.bulk_write(ops, ordered=False)
    # regla de hotspot: frente a la ventana anterior; también se revisa la siguiente, por si ya existía
    tocadas = set()
    for r in filas:
        ini = r["w"]["start"]
        tocadas.update({_clave(r["geohash"], ini), _clave(r["geohash"], ini + DUR)})
    hot = 0
    for v in db.ventanas_celda.find({"_id": {"$in": list(tocadas)}}):
        ant = db.ventanas_celda.find_one({"_id": _clave(v["geohash"], v["inicio"] - DUR)}, {"pct_negativas": 1})
        pct_ant = ant["pct_negativas"] if ant else None
        h = es_hotspot(v["pct_negativas"], pct_ant, v["resenas"])
        hot += h
        db.ventanas_celda.update_one({"_id": v["_id"]}, {"$set": {
            "pct_anterior": pct_ant, "cambio": None if pct_ant is None else round(v["pct_negativas"] - pct_ant, 2),
            "hotspot": h}})
    log(T, f"Ventanas lote {lote_id}: {len(filas)} celda-ventana actualizadas, {hot} hotspots")


q1 = (clasificadas.writeStream.queryName("reviews").foreachBatch(procesar_lote)
      .option("checkpointLocation", os.path.join(CHECKPOINTS, "reviews")).trigger(processingTime=TRIGGER).start())
q2 = (ventanas.writeStream.queryName("ventanas").outputMode("update").foreachBatch(guardar_ventanas)
      .option("checkpointLocation", os.path.join(CHECKPOINTS, "ventanas")).trigger(processingTime=TRIGGER).start())
log(T, f"Escuchando el topic '{TOPIC}' en {KAFKA} (ventana {VENTANA}, watermark {WATERMARK}, "
       f"hotspot > {UMBRAL_HOTSPOT} puntos con >= {MIN_RESENAS_VENTANA} reseñas)")


# ---------------------------------------------------------------- avance y retraso del consumidor
def _offsets(x):
    if isinstance(x, str):
        try:
            x = json.loads(x)
        except ValueError:
            return {}
    return (x or {}).get(TOPIC, {}) if isinstance(x, dict) else {}


def reportar():
    while True:
        for q in (q1, q2):
            try:
                p = q.lastProgress
                if not p:
                    continue
                src = (p.get("sources") or [{}])[0]
                fin, ult = _offsets(src.get("endOffset")), _offsets(src.get("latestOffset"))
                retraso = sum(max(int(ult.get(k, 0)) - int(v), 0) for k, v in fin.items()) if ult else None
                db.stream_estado.replace_one({"_id": q.name}, {
                    "_id": q.name, "activa": q.isActive, "lote": p.get("batchId"),
                    "filas_ultimo_lote": p.get("numInputRows"),
                    "filas_por_segundo": round(p.get("processedRowsPerSecond") or 0.0, 1),
                    "offsets_procesados": fin, "offsets_kafka": ult, "retraso_mensajes": retraso,
                    "watermark": (p.get("eventTime") or {}).get("watermark"),
                    "modelo_sentimiento": meta_sent["version"], "fecha_corte_modelo": meta_sent["fecha_corte"],
                    "modelo_riesgo": meta_riesgo["version"], "actualizado": datetime.now(timezone.utc)}, upsert=True)
            except Exception as e:  # noqa: BLE001
                log(T, f"AVISO: no se pudo guardar el avance ({type(e).__name__}: {e})")
        time.sleep(5)


threading.Thread(target=reportar, daemon=True).start()
spark.streams.awaitAnyTermination()
