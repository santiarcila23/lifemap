"""
Sentimiento de las reseñas con Spark MLlib (pipeline de SentimentStream sobre texto real).

  Etiqueta: 1-2 estrellas = negativo (0), 3 = neutral (1), 4-5 = positivo (2).
  Pipeline: limpieza (SQLTransformer) -> RegexTokenizer -> StopWordsRemover (inglés)
            -> HashingTF -> IDF -> clasificador.
  Clasificadores: Naive Bayes multinomial y regresión logística multinomial, ambos con
  pesos por clase (inverso de la frecuencia) para compensar el desbalance.
  Partición SIN FUGA: por negocio (hash del business_id), 80 % entrenamiento y 20 % prueba;
  ningún negocio queda en los dos lados.

Salidas
  /models/sentiment_nb, /models/sentiment_lr   PipelineModel de cada clasificador
  /models/sentiment_v1                         el mejor por F1 macro (lo usa la Entrega 2)
  /models/sentiment_export.npz + .json         pesos para /predict de la API (sin JVM)
  /data/reports/sentimiento.json               métricas, matrices de confusión, desbalance
  MongoDB: metricas_sentimiento
"""
import json
import os
import random

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.feature import IDF, HashingTF, RegexTokenizer, SQLTransformer, StopWordsRemover
from pyspark.sql import functions as F

import sys

from comun import ya_hecho, MODELS, REVIEWS, Cronometro, escribir_mongo, guardar_json, log, sesion
import textomodelo

NUM_FEATURES = int(os.environ.get("NUM_FEATURES", str(1 << 16)))
MAX_TRAIN = int(os.environ.get("MAX_TRAIN", "0"))          # 0 = todas las reseñas de entrenamiento
CLASES = ["negativo", "neutral", "positivo"]
T = "SENT"

if ya_hecho("sentimiento.json"):
    sys.exit(0)
spark = sesion("lifemap_sentimiento")
spark.sparkContext.setLogLevel("WARN")

df = (spark.read.parquet(REVIEWS)
      .select("review_id", "business_id", "text", F.col("sentimiento").cast("double").alias("label")))
df = df.withColumn("fold", F.abs(F.hash("business_id")) % 10)
train, test = df.filter("fold < 8"), df.filter("fold >= 8")
if MAX_TRAIN:
    n = train.count()
    if n > MAX_TRAIN:
        train = train.sample(False, MAX_TRAIN / n, seed=42)

# Comprobación de que no hay fuga: ningún negocio en entrenamiento y prueba a la vez
cruce = train.select("business_id").distinct().join(test.select("business_id").distinct(), "business_id").count()
log(T, f"Negocios en entrenamiento y prueba a la vez: {cruce} (debe ser 0)")

dist_train = {int(r["label"]): r["count"] for r in train.groupBy("label").count().collect()}
dist_test = {int(r["label"]): r["count"] for r in test.groupBy("label").count().collect()}
n_train, n_test = sum(dist_train.values()), sum(dist_test.values())
log(T, f"Entrenamiento: {n_train:,} reseñas {dist_train} | prueba: {n_test:,} {dist_test}")

# Pesos por clase: N / (K * n_c). La clase rara (neutral) pesa más.
pesos = {c: n_train / (len(CLASES) * dist_train.get(c, 1)) for c in range(len(CLASES))}
expr_peso = F.when(F.col("label") == 0, pesos[0]).when(F.col("label") == 1, pesos[1]).otherwise(pesos[2])
train = train.withColumn("peso", expr_peso).cache()
log(T, "Pesos por clase: " + ", ".join(f"{CLASES[c]}={w:.2f}" for c, w in pesos.items()))

stages = [
    SQLTransformer(statement="SELECT *, regexp_replace(lower(text), '[^a-z ]', ' ') AS texto_limpio FROM __THIS__"),
    RegexTokenizer(inputCol="texto_limpio", outputCol="tokens", pattern="\\s+", minTokenLength=2),
    StopWordsRemover(inputCol="tokens", outputCol="palabras", stopWords=StopWordsRemover.loadDefaultStopWords("english")),
    HashingTF(inputCol="palabras", outputCol="tf", numFeatures=NUM_FEATURES),
    IDF(inputCol="tf", outputCol="features", minDocFreq=5),
]
modelos = {
    "naive_bayes": NaiveBayes(modelType="multinomial", smoothing=1.0, weightCol="peso"),
    "regresion_logistica": LogisticRegression(family="multinomial", maxIter=30, regParam=0.01,
                                              elasticNetParam=0.0, weightCol="peso"),
}


def metricas(pred) -> dict:
    cm = np.zeros((3, 3), dtype=int)
    for r in pred.groupBy("label", "prediction").count().collect():
        cm[int(r["label"]), int(r["prediction"])] = r["count"]
    por_clase = {}
    for c, nombre in enumerate(CLASES):
        tp, fp, fn = cm[c, c], cm[:, c].sum() - cm[c, c], cm[c, :].sum() - cm[c, c]
        p = tp / (tp + fp) if tp + fp else 0.0
        r = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * p * r / (p + r) if p + r else 0.0
        por_clase[nombre] = {"precision": round(float(p), 4), "recall": round(float(r), 4),
                             "f1": round(float(f1), 4), "soporte": int(cm[c, :].sum())}
    total = cm.sum()
    return {"accuracy": round(float(np.trace(cm) / total), 4),
            "f1_macro": round(float(np.mean([v["f1"] for v in por_clase.values()])), 4),
            "f1_ponderado": round(float(sum(v["f1"] * v["soporte"] for v in por_clase.values()) / total), 4),
            "por_clase": por_clase,
            "matriz_confusion": {"filas_real_columnas_predicha": CLASES, "valores": cm.tolist()}}


resultados, ajustados = {}, {}
for nombre, clf in modelos.items():
    with Cronometro() as c:
        modelo = Pipeline(stages=stages + [clf]).fit(train)
    pred = modelo.transform(test).select("label", "prediction", "text", "probability").cache()
    m = metricas(pred)
    m["segundos_entrenamiento"] = round(c.s, 1)
    resultados[nombre], ajustados[nombre] = m, (modelo, pred)
    modelo.write().overwrite().save(os.path.join(MODELS, f"sentiment_{'nb' if nombre == 'naive_bayes' else 'lr'}"))
    log(T, f"{nombre}: accuracy={m['accuracy']} F1 macro={m['f1_macro']} ({c.s:.0f} s)")
    for clase, v in m["por_clase"].items():
        log(T, f"   {clase:<9} P={v['precision']:.3f} R={v['recall']:.3f} F1={v['f1']:.3f} (n={v['soporte']:,})")
    log(T, f"   matriz (filas = real neg/neu/pos): {m['matriz_confusion']['valores']}")

mejor = max(resultados, key=lambda k: resultados[k]["f1_macro"])
modelo_mejor, pred_mejor = ajustados[mejor]
modelo_mejor.write().overwrite().save(os.path.join(MODELS, "sentiment_v1"))
log(T, f"Mejor por F1 macro: {mejor} -> guardado como /models/sentiment_v1")

# Línea base: predecir siempre la clase mayoritaria (positivo)
base_acc = round(dist_test.get(2, 0) / n_test, 4) if n_test else None

# ---------------------------------------------------------------- exportación para la API
idf = modelo_mejor.stages[4].idf.toArray()
clf = modelo_mejor.stages[-1]
if mejor == "naive_bayes":
    W, b = clf.theta.toArray(), clf.pi.toArray()
else:
    W, b = clf.coefficientMatrix.toArray(), clf.interceptVector.toArray()
stop = StopWordsRemover.loadDefaultStopWords("english")
np.savez_compressed(os.path.join(MODELS, "sentiment_export.npz"), idf=idf, W=W, b=b)
meta = {"modelo": mejor, "num_features": NUM_FEATURES, "clases": CLASES, "stopwords": stop,
        "f1_macro": resultados[mejor]["f1_macro"], "version": "sentiment_v1"}
with open(os.path.join(MODELS, "sentiment_export.json"), "w", encoding="utf-8") as f:
    json.dump(meta, f, ensure_ascii=False)

# Verificación: la réplica en Python predice lo mismo que Spark
muestra = pred_mejor.limit(300).collect()
m_py = {"num_features": NUM_FEATURES, "stopwords": set(stop), "idf": idf, "W": W, "b": b}
iguales = sum(1 for r in muestra if textomodelo.predecir(r["text"], m_py)[0] == int(r["prediction"]))
log(T, f"Réplica de /predict coincide con Spark en {iguales} de {len(muestra)} reseñas de prueba")

guardar_json("sentimiento.json", {
    "particion": "por negocio: fold = abs(hash(business_id)) % 10; entrenamiento fold < 8, prueba fold >= 8",
    "negocios_en_ambos_lados": cruce,
    "distribucion_entrenamiento": {CLASES[k]: v for k, v in sorted(dist_train.items())},
    "distribucion_prueba": {CLASES[k]: v for k, v in sorted(dist_test.items())},
    "pesos_por_clase": {CLASES[k]: round(v, 3) for k, v in pesos.items()},
    "accuracy_linea_base_siempre_positivo": base_acc,
    "num_features": NUM_FEATURES,
    "modelos": resultados,
    "mejor": mejor,
    "replica_predict_coincide": f"{iguales}/{len(muestra)}",
})
filas = [(k, clase, v["precision"], v["recall"], v["f1"], v["soporte"], resultados[k]["accuracy"], resultados[k]["f1_macro"])
         for k in resultados for clase, v in resultados[k]["por_clase"].items()]
try:
    escribir_mongo(spark.createDataFrame(filas, ["modelo", "clase", "precision", "recall", "f1", "soporte",
                                                 "accuracy", "f1_macro"]), "metricas_sentimiento")
except Exception as e:  # noqa: BLE001
    log(T, f"AVISO: no se pudo escribir en MongoDB ({type(e).__name__})")
log(T, "Listo")
spark.stop()
