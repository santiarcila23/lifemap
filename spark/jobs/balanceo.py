"""
Balanceo de carga del MapReduce de tasas.

1. Tiempos: la operación del paso 5 (cierres y días-negocio por categoría sobre los negocios, y
   reseñas negativas por celda sobre las reseñas) con 2, 8, 32 y 200 particiones, REPETICIONES
   veces cada una. El reparto (repartition + cache) se materializa antes y no se cronometra.
   Se reporta media, desviación estándar, mínimo y máximo.
2. Sesgo: las reseñas se reparten por categoría, por ciudad y sin clave (round-robin), y se
   cuentan las filas de cada partición con spark_partition_id().

Salidas: /data/reports/balanceo.json; MongoDB balanceo_tiempos, balanceo_particiones
"""
import os
import statistics

from pyspark.sql import functions as F

import sys

from comun import ya_hecho, BUSINESSES, REVIEWS, Cronometro, escribir_mongo, guardar_json, log, sesion

PARTICIONES = [int(x) for x in os.environ.get("PARTICIONES", "2,8,32,200").split(",")]
REPETICIONES = int(os.environ.get("REPETICIONES", "3"))
N_SESGO = int(os.environ.get("N_SESGO", "8"))
T = "BAL"

if ya_hecho("balanceo.json"):
    sys.exit(0)
spark = sesion("lifemap_balanceo")
spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext
biz = spark.read.parquet(BUSINESSES).select("categoria_principal", "evento", "dias_negocio").cache()
rev = spark.read.parquet(REVIEWS).select("geohash", "stars", "categoria_principal", "city").cache()
n_biz, n_rev = biz.count(), rev.count()
log(T, f"Negocios: {n_biz:,} | reseñas: {n_rev:,} | núcleos (defaultParallelism): {sc.defaultParallelism}")


def operacion(b, r, n):
    cierres = (b.rdd.map(lambda x: (x[0], (int(x[1]), int(x[2]))))
               .reduceByKey(lambda a, c: (a[0] + c[0], a[1] + c[1]), numPartitions=n).collect())
    negativas = (r.rdd.map(lambda x: (x[0], (1 if x[1] <= 2 else 0, 1)))
                 .reduceByKey(lambda a, c: (a[0] + c[0], a[1] + c[1]), numPartitions=n).collect())
    return sorted(cierres), sorted(negativas)


filas_t, resumen_t, ref = [], [], None
for n in PARTICIONES:
    b, r = biz.repartition(n).cache(), rev.select("geohash", "stars").repartition(n).cache()
    b.count(), r.count()
    tiempos = []
    for rep in range(1, REPETICIONES + 1):
        with Cronometro() as c:
            res = operacion(b, r, n)
        tiempos.append(c.s)
        filas_t.append((n, rep, round(c.s, 3)))
        log(T, f"  {n:>3} particiones, repetición {rep}: {c.s:.2f} s")
    ref = ref or res
    media, desv = statistics.mean(tiempos), statistics.stdev(tiempos) if len(tiempos) > 1 else 0.0
    resumen_t.append({"particiones": n, "media_s": round(media, 3), "desviacion_s": round(desv, 3),
                      "min_s": round(min(tiempos), 3), "max_s": round(max(tiempos), 3),
                      "mediana_s": round(statistics.median(tiempos), 3), "mismo_resultado": res == ref})
    log(T, f"{n:>3} particiones -> media {media:.2f} s ± {desv:.2f} (min {min(tiempos):.2f}, max {max(tiempos):.2f})")
    b.unpersist(), r.unpersist()


def filas_por_particion(df) -> list[int]:
    d = {x["pid"]: x["filas"] for x in
         df.groupBy(F.spark_partition_id().alias("pid")).agg(F.count("*").alias("filas")).collect()}
    return [d.get(i, 0) for i in range(N_SESGO)]


def estad(v: list[int]) -> dict:
    media = sum(v) / len(v)
    return {"max": max(v), "min": min(v), "media": round(media), "max_sobre_media": round(max(v) / media, 2),
            "particiones_vacias": sum(1 for x in v if x == 0)}


repartos = {"por_categoria": filas_por_particion(rev.repartition(N_SESGO, "categoria_principal")),
            "por_ciudad": filas_por_particion(rev.repartition(N_SESGO, "city")),
            "sin_clave": filas_por_particion(rev.repartition(N_SESGO))}
sesgo = {k: {"filas_por_particion": v, **estad(v)} for k, v in repartos.items()}
for k, v in sesgo.items():
    log(T, f"Reparto {k:<14} en {N_SESGO}: filas {v['filas_por_particion']} -> máx/media = {v['max_sobre_media']}")

conc = {}
for col in ("categoria_principal", "city"):
    top = rev.groupBy(col).count().orderBy(F.desc("count")).limit(10).collect()
    conc[col] = [{"valor": x[col], "resenas": x["count"], "pct": round(x["count"] / n_rev * 100, 2)} for x in top]
    log(T, f"Más frecuentes por {col}: " + ", ".join(f"{x['valor']} ({x['pct']} %)" for x in conc[col][:5]))

guardar_json("balanceo.json", {"negocios": n_biz, "resenas": n_rev, "nucleos": sc.defaultParallelism,
                               "repeticiones": REPETICIONES, "tiempos": resumen_t, "sesgo_particiones": N_SESGO,
                               "sesgo": sesgo, "concentracion": conc})
try:
    escribir_mongo(spark.createDataFrame(filas_t, ["particiones", "repeticion", "segundos"]), "balanceo_tiempos")
    escribir_mongo(spark.createDataFrame([(k, i, v) for k, vals in repartos.items() for i, v in enumerate(vals)],
                                         ["reparto", "particion", "filas"]), "balanceo_particiones")
except Exception as e:  # noqa: BLE001
    log(T, f"AVISO: no se pudo escribir en MongoDB ({type(e).__name__})")
log(T, "Listo")
spark.stop()
