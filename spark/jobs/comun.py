"""Utilidades compartidas por los trabajos de Spark de LifeMap."""
import json
import os
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession

CLEAN = os.environ.get("CLEAN_DIR", "/data/clean")
REVIEWS = os.path.join(CLEAN, "reviews")
BUSINESSES = os.path.join(CLEAN, "businesses")
REPORTS = os.environ.get("REPORT_DIR", "/data/reports")
MODELS = os.environ.get("MODELS_DIR", "/models")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "lifemap")
# Mínimo de días-negocio para comparar tasas entre grupos: 36.500 días = 100 años-negocio.
# Con esa exposición, un cierre más o menos mueve la tasa a lo sumo 1 punto porcentual.
MIN_DIAS = int(os.environ.get("MIN_DIAS_NEGOCIO", "36500"))


def ya_hecho(reporte: str) -> bool:
    """True si el reporte ya existe y no se pidió recalcular (FORCE_SPARK=true)."""
    forzar = os.environ.get("FORCE_SPARK", "false").lower() in ("1", "true", "yes")
    if not forzar and os.path.exists(os.path.join(REPORTS, reporte)):
        print(f"[skip] {reporte} ya existe; FORCE_SPARK=true para recalcular", flush=True)
        return True
    return False


def sesion(nombre: str) -> SparkSession:
    return (SparkSession.builder.appName(nombre)
            .config("spark.mongodb.read.connection.uri", MONGO_URI)
            .config("spark.mongodb.write.connection.uri", MONGO_URI)
            .getOrCreate())


def log(tag: str, msg: str) -> None:
    print(f"[{tag} {datetime.now():%H:%M:%S}] {msg}", flush=True)


def tasa_cierre(cierres: float, dias_negocio: float):
    """Tasa de cierre anualizada (%) = cierres / (días-negocio / 365) * 100 (la AFR de los discos)."""
    if not dias_negocio:
        return None
    return round(cierres / (dias_negocio / 365.0) * 100.0, 4)


def guardar_json(nombre: str, datos: dict) -> str:
    os.makedirs(REPORTS, exist_ok=True)
    datos = {"generado": datetime.now(timezone.utc).isoformat(timespec="seconds"), **datos}
    ruta = os.path.join(REPORTS, nombre)
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2, default=str)
    return ruta


def escribir_mongo(df, coleccion: str, modo: str = "overwrite") -> None:
    (df.write.format("mongodb").mode(modo)
       .option("database", MONGO_DB).option("collection", coleccion).save())


class Cronometro:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.s = time.perf_counter() - self.t0
