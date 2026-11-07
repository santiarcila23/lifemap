"""
MapReduce de tasas de cierre (como la AFR del ejercicio de discos, con días-negocio).

  Map:    cada negocio emite (clave, (cierre, días_negocio, 1)), con cierre = 1 si is_open = 0.
  Reduce: reduceByKey suma por clave -> (cierres, días-negocio, negocios).
  Claves: categoría principal y celda de geohash.
  Validación: el mismo cálculo con groupBy de DataFrames debe dar lo mismo en todas las claves.
  Tasa de cierre anualizada (%) = cierres / (días-negocio / 365) * 100.
  Mínimo de días-negocio (MIN_DIAS_NEGOCIO, por defecto 36.500 = 100 años-negocio): con esa
  exposición un cierre más o menos cambia la tasa a lo sumo 1 punto; los grupos por debajo
  se guardan pero se marcan como no confiables.
  Además, por celda: porcentaje de reseñas negativas (1-2 estrellas) con MapReduce.

Salidas: MongoDB tasas_categoria, tasas_celda (con geometría de la celda);
         /data/reports/mapreduce.json
"""
from pyspark.sql import functions as F
from pyspark.sql import types as Ty

import geohash
import sys

from comun import ya_hecho, BUSINESSES, MIN_DIAS, REVIEWS, Cronometro, escribir_mongo, guardar_json, log, sesion, tasa_cierre

T = "MR"
if ya_hecho("mapreduce.json"):
    sys.exit(0)
spark = sesion("lifemap_mapreduce")
spark.sparkContext.setLogLevel("WARN")

biz = spark.read.parquet(BUSINESSES).select("business_id", "categoria_principal", "geohash", "evento",
                                            "dias_negocio").cache()
rev = spark.read.parquet(REVIEWS).select("geohash", "stars")
n_biz = biz.count()
log(T, f"Negocios: {n_biz:,}")


def mapreduce(clave: str):
    return (biz.select(clave, "evento", "dias_negocio").rdd
            .map(lambda r: (r[0], (int(r[1]), int(r[2]), 1)))                 # Map
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])))  # Reduce


def groupby(clave: str):
    return {r[clave]: (r["cierres"], r["dias"], r["negocios"]) for r in
            biz.groupBy(clave).agg(F.sum("evento").alias("cierres"), F.sum("dias_negocio").alias("dias"),
                                   F.count("*").alias("negocios")).collect()}


resumen = {"min_dias_negocio": MIN_DIAS, "justificacion_minimo":
           "100 años-negocio: con esa exposición un cierre más o menos mueve la tasa a lo sumo 1 punto porcentual"}
tablas = {}
for clave, nombre in [("categoria_principal", "categoria"), ("geohash", "celda")]:
    with Cronometro() as c_rdd:
        rdd = dict(mapreduce(clave).collect())
    with Cronometro() as c_df:
        gb = groupby(clave)
    distintas = [k for k in set(rdd) | set(gb) if rdd.get(k) != gb.get(k)]
    log(T, f"Por {nombre}: {len(rdd)} claves | RDD {c_rdd.s:.2f} s, DataFrame {c_df.s:.2f} s | "
           f"¿coinciden? {'SÍ' if not distintas else 'NO'} ({len(rdd) - len(distintas)} de {len(rdd)})")
    filas = []
    for k, (cierres, dias, negocios) in rdd.items():
        filas.append({"clave": k, "negocios": negocios, "cierres": cierres, "dias_negocio": dias,
                      "anios_negocio": round(dias / 365, 1), "tasa_cierre_anual": tasa_cierre(cierres, dias),
                      "confiable": dias >= MIN_DIAS})
    tablas[nombre] = filas
    conf = sorted([f for f in filas if f["confiable"]], key=lambda f: -f["tasa_cierre_anual"])
    resumen[f"por_{nombre}"] = {"claves": len(filas), "confiables": len(conf), "coincide_groupby": not distintas,
                                "claves_distintas": distintas[:10], "segundos_rdd": round(c_rdd.s, 2),
                                "segundos_dataframe": round(c_df.s, 2), "top10_tasa": conf[:10],
                                "bottom5_tasa": conf[-5:]}
    for f in conf[:5]:
        log(T, f"   {str(f['clave'])[:28]:<28} negocios={f['negocios']:>5} cierres={f['cierres']:>4} "
               f"años-negocio={f['anios_negocio']:>8,.0f} tasa={f['tasa_cierre_anual']:.2f} %")

tot_c, tot_d = biz.agg(F.sum("evento"), F.sum("dias_negocio")).first()
resumen["total"] = {"negocios": n_biz, "cierres": int(tot_c), "dias_negocio": int(tot_d),
                    "tasa_cierre_anual": tasa_cierre(tot_c, tot_d)}
log(T, f"TOTAL: {tot_c:,} cierres en {tot_d:,} días-negocio -> tasa = {tasa_cierre(tot_c, tot_d):.2f} % anual")

# Porcentaje de reseñas negativas por celda (MapReduce sobre las reseñas)
neg = dict(rev.rdd.map(lambda r: (r[0], (1 if r[1] <= 2 else 0, 1)))
           .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).collect())

# ---------------------------------------------------------------- guardar en MongoDB
esq_cat = Ty.StructType([Ty.StructField("_id", Ty.StringType()), Ty.StructField("categoria", Ty.StringType()),
                         Ty.StructField("negocios", Ty.LongType()), Ty.StructField("cierres", Ty.LongType()),
                         Ty.StructField("dias_negocio", Ty.LongType()), Ty.StructField("anios_negocio", Ty.DoubleType()),
                         Ty.StructField("tasa_cierre_anual", Ty.DoubleType()), Ty.StructField("confiable", Ty.BooleanType())])
cat_rows = [(f["clave"], f["clave"], f["negocios"], f["cierres"], f["dias_negocio"], float(f["anios_negocio"]),
             float(f["tasa_cierre_anual"] or 0.0), f["confiable"]) for f in tablas["categoria"]]

geom = Ty.StructType([Ty.StructField("type", Ty.StringType()),
                      Ty.StructField("coordinates", Ty.ArrayType(Ty.ArrayType(Ty.ArrayType(Ty.DoubleType()))))])
punto = Ty.StructType([Ty.StructField("type", Ty.StringType()),
                       Ty.StructField("coordinates", Ty.ArrayType(Ty.DoubleType()))])
esq_cel = Ty.StructType([Ty.StructField("_id", Ty.StringType()), Ty.StructField("geohash", Ty.StringType()),
                         Ty.StructField("geometry", geom), Ty.StructField("centro", punto),
                         Ty.StructField("negocios", Ty.LongType()), Ty.StructField("cierres", Ty.LongType()),
                         Ty.StructField("dias_negocio", Ty.LongType()), Ty.StructField("anios_negocio", Ty.DoubleType()),
                         Ty.StructField("tasa_cierre_anual", Ty.DoubleType()), Ty.StructField("confiable", Ty.BooleanType()),
                         Ty.StructField("resenas", Ty.LongType()), Ty.StructField("resenas_negativas", Ty.LongType()),
                         Ty.StructField("pct_negativas", Ty.DoubleType())])
cel_rows = []
for f in tablas["celda"]:
    gh = f["clave"]
    n_neg, n_rev = neg.get(gh, (0, 0))
    lat, lon = geohash.center(gh)
    cel_rows.append((gh, gh, geohash.polygon(gh), {"type": "Point", "coordinates": [lon, lat]}, f["negocios"],
                     f["cierres"], f["dias_negocio"], float(f["anios_negocio"]), float(f["tasa_cierre_anual"] or 0.0),
                     f["confiable"], n_rev, n_neg, round(n_neg / n_rev * 100, 2) if n_rev else 0.0))
resumen["por_celda"]["pct_negativas_celda_max"] = max((r[-1] for r in cel_rows), default=None)

try:
    escribir_mongo(spark.createDataFrame(cat_rows, esq_cat), "tasas_categoria")
    escribir_mongo(spark.createDataFrame(cel_rows, esq_cel), "tasas_celda")
    log(T, "Guardado en MongoDB: tasas_categoria, tasas_celda")
except Exception as e:  # noqa: BLE001
    log(T, f"AVISO: no se pudo escribir en MongoDB ({type(e).__name__}: {str(e)[:200]})")

guardar_json("mapreduce.json", resumen)
log(T, "Listo")
spark.stop()
