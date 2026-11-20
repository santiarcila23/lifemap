"""
Modelo de riesgo de cierre (Entrega 2): el patrón de KafkaMed aplicado a negocios.

  Datos: solo reseñas ANTES de la fecha de corte T (FECHA_CORTE).
  Variables por negocio (riesgo_vars.py): sentimiento medio, tendencia del % negativo en los
    últimos 6 meses, estrellas medias, log(reseñas), log(reseñas de los últimos 6 meses), edad,
    días sin reseñas y competidores de la
    misma categoría a 500 m (MongoDB $geoNear, contando solo los activos en T).
  Etiqueta: 1 si cerró (is_open = 0) y su última reseña cae entre T y T + 12 meses;
    0 si siguió con actividad después de ese plazo o sigue abierto; los negocios sin actividad
    después de T se excluyen.
  Partición: por negocio (mismo hash que el sentimiento), 80 % entrenamiento / 20 % prueba.
  Desbalance: pesos por clase; se reportan AUC-ROC, AUC-PR (línea base = tasa de positivos) y
    recall/precisión en el umbral elegido: el que maximiza F1 en entrenamiento (equilibra
    avisar a tiempo con no llenar el mapa de falsas alarmas).

Salidas
  /models/riesgo_v1 (PipelineModel) + /models/riesgo_v1.json (variables, umbral, métricas)
  MongoDB riesgo_negocios: estado acumulado a T, variables y probabilidad (lo usa el streaming)
  /data/reports/riesgo.json
"""
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from pymongo import GEOSPHERE, MongoClient, ReplaceOne
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

import riesgo_vars as rv
from comun import (BUSINESSES, FECHA_CORTE, MODELS, MONGO_DB, MONGO_URI, REVIEWS, guardar_json, log,
                   reporte_actual, sesion)

T = "RIESGO"
DIAS_ACTIVO = int(os.environ.get("DIAS_ACTIVO_COMPETIDOR", "180"))
VERSION = "riesgo_v1"

if reporte_actual("riesgo.json", fecha_corte=FECHA_CORTE):
    sys.exit(0)

corte = datetime.fromisoformat(FECHA_CORTE)
spark = sesion("lifemap_riesgo")
spark.sparkContext.setLogLevel("WARN")
db = MongoClient(MONGO_URI)[MONGO_DB]

# ---------------------------------------------------------------- estado de cada negocio hasta T
rev = (spark.read.parquet(REVIEWS).filter(F.col("date") < F.to_timestamp(F.lit(FECHA_CORTE)))
       .withColumn("mes", F.date_format("date", "yyyy-MM")))
por_mes = (rev.groupBy("business_id", "mes")
           .agg(F.count("*").alias("n"), F.sum(F.when(F.col("sentimiento") == 0, 1).otherwise(0)).alias("neg"),
                F.sum(F.col("sentimiento") - 1).alias("suma_sent"), F.sum("stars").alias("suma_estrellas"),
                F.min("date").alias("primera"), F.max("date").alias("ultima"))
           .collect())
estados = {}
for r in por_mes:
    e = estados.setdefault(r["business_id"], rv.estado_vacio())
    rv.acumular(e, r["primera"], 0, 0, n=r["n"], negativas=r["neg"], suma_sent=float(r["suma_sent"]),
                suma_estrellas=float(r["suma_estrellas"]))
    e["ultima"] = max(e["ultima"], r["ultima"].replace(tzinfo=None))
log(T, f"T = {FECHA_CORTE}: {len(estados):,} negocios con reseñas antes de T")

neg = {r["business_id"]: r for r in spark.read.parquet(BUSINESSES)
       .select("business_id", "name", "latitude", "longitude", "categoria_principal", "is_open", "ultima").collect()}

# ---------------------------------------------------------------- documentos + competidores ($geoNear)
docs = []
limite_activo = corte - timedelta(days=DIAS_ACTIVO)
for bid, e in estados.items():
    b = neg.get(bid)
    if b is None:
        continue
    docs.append({"_id": bid, "nombre": b["name"], "categoria_principal": b["categoria_principal"],
                 "location": {"type": "Point", "coordinates": [float(b["longitude"]), float(b["latitude"])]},
                 "activo_corte": e["ultima"] >= limite_activo, "estado": e, "estado_corte": e,
                 "etiqueta": rv.etiqueta(int(b["is_open"]), b["ultima"], corte)})
db.riesgo_negocios.drop()
for i in range(0, len(docs), 2000):
    db.riesgo_negocios.insert_many(docs[i:i + 2000])
db.riesgo_negocios.create_index([("location", GEOSPHERE)])
db.riesgo_negocios.create_index([("categoria_principal", 1)])

log(T, f"Contando competidores a 500 m con $geoNear para {len(docs):,} negocios ...")
competidores = {}
for d in docs:
    res = list(db.riesgo_negocios.aggregate([
        {"$geoNear": {"near": d["location"], "distanceField": "distancia", "maxDistance": 500, "spherical": True,
                      "query": {"categoria_principal": d["categoria_principal"], "activo_corte": True,
                                "_id": {"$ne": d["_id"]}}}},
        {"$count": "n"}]))
    competidores[d["_id"]] = res[0]["n"] if res else 0

filas = []
for d in docs:
    v = rv.variables(d["estado"], corte, competidores[d["_id"]])
    d["competidores_500m"], d["variables"] = competidores[d["_id"]], v
    filas.append((d["_id"], d["etiqueta"], *[float(v[k]) for k in rv.VARIABLES]))
datos = spark.createDataFrame(filas, ["business_id", "label_raw"] + rv.VARIABLES)
datos = datos.withColumn("fold", F.abs(F.hash("business_id")) % 10)

etiquetados = datos.filter(F.col("label_raw").isNotNull()).withColumn("label", F.col("label_raw").cast("double"))
train, test = etiquetados.filter("fold < 8"), etiquetados.filter("fold >= 8")
n_tr, n_te = train.count(), test.count()
pos_tr = train.filter("label = 1").count()
pos_te = test.filter("label = 1").count()
excluidos = datos.filter(F.col("label_raw").isNull()).count()
log(T, f"Etiquetados: entrenamiento {n_tr:,} ({pos_tr} cierres, {pos_tr / max(n_tr, 1):.1%}) | "
       f"prueba {n_te:,} ({pos_te} cierres, {pos_te / max(n_te, 1):.1%}) | excluidos sin actividad después de T: {excluidos:,}")

w_pos = (n_tr - pos_tr) / max(pos_tr, 1)
train = train.withColumn("peso", F.when(F.col("label") == 1, w_pos).otherwise(1.0)).cache()
ensamblador = VectorAssembler(inputCols=rv.VARIABLES, outputCol="x")
candidatos = {
    "regresion_logistica": [StandardScaler(inputCol="x", outputCol="features", withMean=True),
                            LogisticRegression(featuresCol="features", weightCol="peso", maxIter=100, regParam=0.01)],
    "random_forest": [RandomForestClassifier(featuresCol="x", weightCol="peso", numTrees=200, maxDepth=6, seed=42)],
}
ev_roc = BinaryClassificationEvaluator(metricName="areaUnderROC", rawPredictionCol="rawPrediction")
ev_pr = BinaryClassificationEvaluator(metricName="areaUnderPR", rawPredictionCol="rawPrediction")


def en_umbral(pred, u):
    p = pred.withColumn("alerta", (F.col("p1") >= u).cast("int"))
    c = {(int(r["label"]), r["alerta"]): r["count"] for r in p.groupBy("label", "alerta").count().collect()}
    tp, fp, fn, tn = c.get((1, 1), 0), c.get((0, 1), 0), c.get((1, 0), 0), c.get((0, 0), 0)
    rec = tp / (tp + fn) if tp + fn else 0.0
    pre = tp / (tp + fp) if tp + fp else 0.0
    return {"umbral": round(u, 4), "recall": round(rec, 4), "precision": round(pre, 4),
            "f1": round(2 * pre * rec / (pre + rec), 4) if pre + rec else 0.0,
            "matriz": {"vp": tp, "fp": fp, "fn": fn, "vn": tn}}


resultados, modelos = {}, {}
for nombre, etapas in candidatos.items():
    modelo = Pipeline(stages=[ensamblador] + etapas).fit(train)
    p_tr = modelo.transform(train).withColumn("p1", vector_to_array("probability")[1])
    p_te = modelo.transform(test).withColumn("p1", vector_to_array("probability")[1]).cache()
    # umbral: el que maximiza F1 en entrenamiento (nunca se mira la prueba para elegirlo)
    pares = sorted(((float(r["p1"]), int(r["label"])) for r in p_tr.select("p1", "label").collect()), reverse=True)
    total_pos, vp, mejor_f1, umbral = sum(l for _, l in pares), 0, -1.0, 0.5
    for i, (p, l) in enumerate(pares, start=1):
        vp += l
        f1 = 2 * vp / (i + total_pos) if total_pos else 0.0
        if f1 > mejor_f1:
            mejor_f1, umbral = f1, p
    m = {"auc_roc": round(ev_roc.evaluate(p_te), 4), "auc_pr": round(ev_pr.evaluate(p_te), 4),
         "auc_pr_linea_base": round(pos_te / max(n_te, 1), 4), "en_umbral_prueba": en_umbral(p_te, umbral)}
    if nombre == "regresion_logistica":
        lr = modelo.stages[-1]
        m["coeficientes_estandarizados"] = dict(zip(rv.VARIABLES, [round(float(c), 4) for c in lr.coefficients]))
    else:
        m["importancia"] = dict(zip(rv.VARIABLES, [round(float(c), 4) for c in modelo.stages[-1].featureImportances]))
    resultados[nombre], modelos[nombre] = m, modelo
    u = m["en_umbral_prueba"]
    log(T, f"{nombre}: AUC-ROC={m['auc_roc']} AUC-PR={m['auc_pr']} (base {m['auc_pr_linea_base']}) | "
           f"umbral {u['umbral']}: recall={u['recall']} precisión={u['precision']} {u['matriz']}")

mejor = max(resultados, key=lambda x: resultados[x]["auc_pr"])
modelo, umbral = modelos[mejor], resultados[mejor]["en_umbral_prueba"]["umbral"]
modelo.write().overwrite().save(os.path.join(MODELS, VERSION))
meta = {"version": VERSION, "modelo": mejor, "variables": rv.VARIABLES, "umbral": umbral,
        "fecha_corte": FECHA_CORTE, "criterio_umbral": "max F1 en entrenamiento",
        "auc_roc": resultados[mejor]["auc_roc"], "auc_pr": resultados[mejor]["auc_pr"],
        "entrenado": datetime.now(timezone.utc).isoformat(timespec="seconds")}
with open(os.path.join(MODELS, f"{VERSION}.json"), "w", encoding="utf-8") as f:
    json.dump(meta, f, ensure_ascii=False, indent=1)
log(T, f"Mejor por AUC-PR: {mejor} -> /models/{VERSION} (umbral {umbral})")

# ---------------------------------------------------------------- probabilidad a T de todos los negocios
probs = {r["business_id"]: float(r["p1"]) for r in
         modelo.transform(datos).withColumn("p1", vector_to_array("probability")[1]).select("business_id", "p1").collect()}
ops = []
for d in docs:
    p = round(probs[d["_id"]], 4)
    ops.append(ReplaceOne({"_id": d["_id"]}, {**d, "probabilidad": p, "probabilidad_corte": p,
                                              "alerta": p >= umbral, "fecha_ref": corte, "fecha_corte": corte,
                                              "variables_corte": d["variables"]}))
for i in range(0, len(ops), 2000):
    db.riesgo_negocios.bulk_write(ops[i:i + 2000], ordered=False)
en_alerta = sum(1 for p in probs.values() if p >= umbral)
log(T, f"{len(ops):,} negocios puntuados a T; {en_alerta:,} sobre el umbral")

guardar_json("riesgo.json", {
    "fecha_corte": FECHA_CORTE, "version": VERSION,
    "etiqueta": "1 = cerrado con última reseña entre T y T + 12 meses; 0 = activo después o abierto; "
                "se excluyen los sin actividad después de T",
    "variables": rv.VARIABLES, "negocios_con_resenas_antes_de_t": len(docs), "excluidos": excluidos,
    "entrenamiento": {"n": n_tr, "cierres": pos_tr}, "prueba": {"n": n_te, "cierres": pos_te},
    "peso_clase_positiva": round(w_pos, 3), "criterio_umbral": "max F1 en entrenamiento",
    "modelos": resultados, "mejor": mejor, "umbral": umbral, "negocios_sobre_umbral_en_t": en_alerta,
})
log(T, "Listo")
spark.stop()
