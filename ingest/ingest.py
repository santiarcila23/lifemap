"""
Ingesta de LifeMap: Kaggle -> limpieza distribuida con Dask -> MongoDB (GeoJSON + 2dsphere).

Pasos
  1. Descarga automática de business.json y review.json del Yelp Open Dataset con la API
     de Kaggle (credenciales KAGGLE_USERNAME / KAGGLE_KEY por variables de entorno).
  2. Negocios (pandas, ~150 mil filas): coordenadas nulas, fuera de rango o fuera del área
     metropolitana; negocios que no son de la familia de categorías; duplicados.
  3. Reseñas (Dask, ~7 millones de líneas, lectura particionada por bloques del archivo):
     cada worker filtra las reseñas de los negocios del área sin parsear todo el JSON,
     descarta texto vacío, estrellas o fechas inválidas y escribe Parquet. Luego se quitan
     duplicados por review_id y se une cada reseña con su negocio.
  4. Vida de cada negocio: primera y última reseña, número de reseñas, días-negocio,
     evento (is_open = 0) y censura (is_open = 1), en el mismo documento GeoJSON.
  5. Carga por lotes a MongoDB (cada partición la escribe un worker) e índices.
  6. Reporte con conteos antes y después de cada regla: /data/reports/ingesta.json.
"""
from __future__ import annotations

import glob
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
import zipfile
from datetime import datetime, timezone

import dask
import dask.bag as db
import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.distributed import Client
from pymongo import ASCENDING, GEOSPHERE, MongoClient
from pymongo.errors import BulkWriteError

import geohash


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


CFG = {
    "dataset": env("KAGGLE_DATASET", "yelp-dataset/yelp-dataset"),
    "business_file": env("YELP_BUSINESS_FILE", "yelp_academic_dataset_business.json"),
    "review_file": env("YELP_REVIEW_FILE", "yelp_academic_dataset_review.json"),
    "raw_dir": env("RAW_DIR", "/data/raw"),
    "clean_dir": env("CLEAN_DIR", "/data/clean"),
    "report_dir": env("REPORT_DIR", "/data/reports"),
    "mongo_uri": env("MONGO_URI", "mongodb://mongo:27017"),
    "mongo_db": env("MONGO_DB", "lifemap"),
    "scheduler": env("DASK_SCHEDULER", "tcp://dask-scheduler:8786"),
    # Área metropolitana: centro y radio (km). Por defecto, Tampa (Florida).
    "area_name": env("AREA_NAME", "Tampa, FL"),
    "area_lat": float(env("AREA_LAT", "27.9506")),
    "area_lon": float(env("AREA_LON", "-82.4572")),
    "area_radius_km": float(env("AREA_RADIUS_KM", "45")),
    # Familia de categorías (texto que debe aparecer en `categories`)
    "family": env("CATEGORY_FAMILY", "Restaurants"),
    # Categorías genéricas que no sirven como categoría principal
    "generic": [c.strip() for c in env("GENERIC_CATEGORIES", "Restaurants,Food").split(",") if c.strip()],
    "geohash_precision": int(env("GEOHASH_PRECISION", "5")),
    "blocksize": env("REVIEW_BLOCKSIZE", "64MB"),
    "batch_size": int(env("BATCH_SIZE", "5000")),
    "force": env("FORCE_INGEST", "false").lower() in ("1", "true", "yes"),
}

RE_BID = re.compile(r'"business_id"\s*:\s*"([^"]+)"')


def log(msg: str) -> None:
    print(f"[ingesta {datetime.now():%H:%M:%S}] {msg}", flush=True)


# --------------------------------------------------------------------------- descarga
def find_file(name: str) -> str | None:
    hits = glob.glob(os.path.join(CFG["raw_dir"], "**", name), recursive=True)
    return hits[0] if hits else None


def download(name: str) -> str:
    """Descarga un archivo del dataset con la API de Kaggle (si no está ya descargado)."""
    os.makedirs(CFG["raw_dir"], exist_ok=True)
    path = find_file(name)
    if path and not CFG["force"]:
        log(f"Ya descargado: {path} ({os.path.getsize(path) / 1e9:.2f} GB)")
        return path
    if not (os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY")):
        sys.exit("ERROR: faltan KAGGLE_USERNAME / KAGGLE_KEY (credencial 'kaggle-api' de Jenkins o archivo .env)")
    log(f"Descargando {name} de kaggle.com/datasets/{CFG['dataset']} ...")
    t0 = time.time()
    try:
        subprocess.run(["kaggle", "datasets", "download", "-d", CFG["dataset"], "-f", name,
                        "-p", CFG["raw_dir"], "--force"], check=True)
    except subprocess.CalledProcessError:
        log("La descarga de un solo archivo falló; se descarga el dataset completo")
        subprocess.run(["kaggle", "datasets", "download", "-d", CFG["dataset"], "-p", CFG["raw_dir"], "--force"],
                       check=True)
    for z in glob.glob(os.path.join(CFG["raw_dir"], "*.zip")):
        log(f"Descomprimiendo {os.path.basename(z)} ...")
        with zipfile.ZipFile(z) as zf:
            for miembro in zf.namelist():
                if os.path.basename(miembro) in (CFG["business_file"], CFG["review_file"]):
                    zf.extract(miembro, CFG["raw_dir"])     # solo lo que se usa: ahorra unos 4 GB
        os.remove(z)
    path = find_file(name)
    if not path:
        sys.exit(f"ERROR: no se encontró {name} después de descargar")
    log(f"Listo en {time.time() - t0:.0f} s: {path} ({os.path.getsize(path) / 1e9:.2f} GB)")
    return path


# --------------------------------------------------------------------------- negocios
def haversine_km(lat, lon, lat0, lon0):
    lat, lon = np.radians(lat), np.radians(lon)
    lat0, lon0 = math.radians(lat0), math.radians(lon0)
    a = np.sin((lat - lat0) / 2) ** 2 + np.cos(lat) * math.cos(lat0) * np.sin((lon - lon0) / 2) ** 2
    return 6371.0 * 2 * np.arcsin(np.sqrt(a))


def main_category(cats: list[str]) -> str:
    for c in cats:
        if c not in CFG["generic"]:
            return c
    return CFG["family"]


def clean_businesses(path: str) -> tuple[pd.DataFrame, list[dict]]:
    cols = ["business_id", "name", "city", "state", "latitude", "longitude",
            "stars", "review_count", "is_open", "categories"]
    b = pd.read_json(path, lines=True)[cols]
    steps = [{"regla": "Negocios en business.json", "quedan": len(b), "descartados": 0,
              "por_que": "Archivo completo"}]

    def step(rule, why, mask):
        nonlocal b
        antes = len(b)
        b = b[mask]
        steps.append({"regla": rule, "quedan": len(b), "descartados": antes - len(b), "por_que": why})

    b["latitude"] = pd.to_numeric(b["latitude"], errors="coerce")
    b["longitude"] = pd.to_numeric(b["longitude"], errors="coerce")
    step("Coordenadas nulas", "Sin ubicación no se puede guardar como GeoJSON ni consultar por zona",
         b["latitude"].notna() & b["longitude"].notna())
    step("Coordenadas fuera de rango", "Latitud fuera de ±90 o longitud fuera de ±180: MongoDB rechaza el punto",
         b["latitude"].between(-90, 90) & b["longitude"].between(-180, 180))
    dist = haversine_km(b["latitude"].to_numpy(), b["longitude"].to_numpy(), CFG["area_lat"], CFG["area_lon"])
    step(f"Fuera del área ({CFG['area_name']}, radio {CFG['area_radius_km']:g} km)",
         "El equipo trabaja solo su área metropolitana", dist <= CFG["area_radius_km"])
    b["categories"] = b["categories"].fillna("")
    step(f"No pertenece a la familia '{CFG['family']}'", "El equipo trabaja solo su familia de categorías",
         b["categories"].str.contains(CFG["family"], regex=False))
    step("Duplicados por business_id", "Un negocio debe aparecer una sola vez", ~b["business_id"].duplicated())

    b = b.copy()
    b["categories"] = b["categories"].map(lambda s: [c.strip() for c in s.split(",") if c.strip()])
    b["categoria_principal"] = b["categories"].map(main_category)
    b["geohash"] = [geohash.encode(la, lo, CFG["geohash_precision"]) for la, lo in zip(b["latitude"], b["longitude"])]
    b["is_open"] = b["is_open"].astype(int)
    return b, steps


# --------------------------------------------------------------------------- reseñas
def filter_partition(lines: list[str], ids: frozenset, out_dir: str) -> list[dict]:
    """Corre en un worker de Dask sobre un bloque del archivo de reseñas.

    Solo parsea el JSON de las líneas cuyo business_id es del área; así no se convierte a
    objetos de Python el 95 % del archivo que no se usa.
    """
    c = {"leidas": 0, "fuera_area": 0, "sin_texto": 0, "estrellas_invalidas": 0, "fecha_invalida": 0, "ok": 0}
    rows = []
    for line in lines:
        if not line.strip():
            continue
        c["leidas"] += 1
        m = RE_BID.search(line)
        if not m or m.group(1) not in ids:
            c["fuera_area"] += 1
            continue
        r = json.loads(line)
        text = (r.get("text") or "").strip()
        if not text:
            c["sin_texto"] += 1
            continue
        try:
            stars = int(float(r.get("stars")))
        except (TypeError, ValueError):
            stars = -1
        if stars < 1 or stars > 5:
            c["estrellas_invalidas"] += 1
            continue
        fecha = pd.to_datetime(r.get("date"), errors="coerce")
        if pd.isna(fecha):
            c["fecha_invalida"] += 1
            continue
        rows.append({"review_id": r.get("review_id"), "business_id": r["business_id"],
                     "stars": stars, "text": text, "date": fecha})
        c["ok"] += 1
    if rows:
        os.makedirs(out_dir, exist_ok=True)
        pd.DataFrame(rows).to_parquet(os.path.join(out_dir, f"part-{uuid.uuid4().hex}.parquet"), index=False)
    return [c]


def clean_reviews(path: str, ids: frozenset, client: Client) -> tuple[dd.DataFrame, list[dict]]:
    out = os.path.join(CFG["clean_dir"], "reviews_stage")
    shutil.rmtree(out, ignore_errors=True)
    bag = db.read_text(path, blocksize=CFG["blocksize"])
    log(f"review.json en {bag.npartitions} particiones de {CFG['blocksize']} (Dask)")
    # los ids van una sola vez en el grafo (delayed) y no copiados en cada tarea
    ids_d = dask.delayed(ids, pure=True)
    counts = bag.map_partitions(filter_partition, ids_d, out).compute()
    tot = {k: sum(c[k] for c in counts) for k in counts[0]}

    r = dd.read_parquet(out)
    antes_dup = tot["ok"]
    r = r.drop_duplicates(subset="review_id")
    r = r.persist()
    despues_dup = len(r)

    steps, quedan = [], tot["leidas"]
    steps.append({"regla": "Reseñas en review.json", "quedan": quedan, "descartados": 0, "por_que": "Archivo completo"})
    for key, rule, why in [
        ("fuera_area", "De negocios fuera del área o de la familia", "Unión reseña–negocio: solo interesan los negocios limpios"),
        ("sin_texto", "Sin texto", "El modelo de sentimiento necesita texto"),
        ("estrellas_invalidas", "Estrellas fuera de 1 a 5", "La etiqueta de sentimiento sale de las estrellas"),
        ("fecha_invalida", "Fecha inválida", "La vida del negocio se construye con las fechas"),
    ]:
        quedan -= tot[key]
        steps.append({"regla": rule, "quedan": quedan, "descartados": tot[key], "por_que": why})
    steps.append({"regla": "Duplicados por review_id", "quedan": despues_dup, "descartados": antes_dup - despues_dup,
                  "por_que": "Una reseña repetida contaría dos veces en el sentimiento y en la vida"})
    return r, steps


def sentiment_label(stars: pd.Series) -> pd.Series:
    """1-2 estrellas = negativo (0), 3 = neutral (1), 4-5 = positivo (2)."""
    return pd.cut(stars, bins=[0, 2, 3, 5], labels=[0, 1, 2]).astype(int)


# --------------------------------------------------------------------------- vida
def business_life(r: dd.DataFrame, b: pd.DataFrame, cutoff: pd.Timestamp) -> pd.DataFrame:
    r = r.assign(negativa=(r["stars"] <= 2).astype(int),
                 sent=r["stars"].map_partitions(sentiment_label, meta=("sent", "int64")))
    g = r.groupby("business_id").agg(
        primera=("date", "min"), ultima=("date", "max"), n_resenas=("review_id", "count"),
        negativas=("negativa", "sum"), estrellas_resenas=("stars", "mean"), sent_medio=("sent", "mean")).compute()
    b = b.merge(g, left_on="business_id", right_index=True, how="inner")
    b["dias_negocio"] = (b["ultima"].dt.normalize() - b["primera"].dt.normalize()).dt.days + 1
    b["evento"] = (b["is_open"] == 0).astype(int)        # cierre observado
    b["censurado"] = (b["is_open"] == 1).astype(int)     # sigue abierto en la fecha de corte
    b["pct_negativas"] = (b["negativas"] / b["n_resenas"] * 100).round(2)
    b["sentimiento_medio"] = (b["sent_medio"] - 1).round(4)   # -1 = todo negativo, +1 = todo positivo
    b["dias_hasta_corte"] = (cutoff.normalize() - b["ultima"].dt.normalize()).dt.days
    return b


# --------------------------------------------------------------------------- MongoDB
def biz_document(row) -> dict:
    return {
        "_id": row.business_id,
        "name": row.name,
        "city": row.city,
        "state": row.state,
        "location": {"type": "Point", "coordinates": [float(row.longitude), float(row.latitude)]},
        "stars": float(row.stars),
        "review_count": int(row.review_count),
        "is_open": int(row.is_open),
        "categories": list(row.categories),
        "categoria_principal": row.categoria_principal,
        "geohash": row.geohash,
        "vida": {
            "primera_resena": row.primera.to_pydatetime(),
            "ultima_resena": row.ultima.to_pydatetime(),
            "n_resenas": int(row.n_resenas),
            "dias_negocio": int(row.dias_negocio),
            "evento": int(row.evento),
            "censurado": int(row.censurado),
            "entrada_tardia": True,
            "dias_hasta_corte": int(row.dias_hasta_corte),
        },
        "sentimiento": {
            "medio": float(row.sentimiento_medio),
            "negativas": int(row.negativas),
            "pct_negativas": float(row.pct_negativas),
            "estrellas_resenas": round(float(row.estrellas_resenas), 3),
        },
    }


def write_reviews_partition(pdf: pd.DataFrame, uri: str, dbname: str, batch: int) -> pd.DataFrame:
    """Corre en un worker de Dask: escribe su partición de reseñas en lotes."""
    coll = MongoClient(uri)[dbname]["reviews"]
    n, docs = 0, []
    for r in pdf.itertuples(index=False):
        docs.append({"_id": r.review_id, "business_id": r.business_id, "stars": int(r.stars),
                     "sentimiento": int(r.sentimiento), "text": r.text, "date": r.date.to_pydatetime(),
                     "geohash": r.geohash, "categoria_principal": r.categoria_principal})
        if len(docs) >= batch:
            n += _insert(coll, docs)
            docs = []
    if docs:
        n += _insert(coll, docs)
    return pd.DataFrame({"insertados": [n]})


def _insert(coll, docs) -> int:
    try:
        return len(coll.insert_many(docs, ordered=False).inserted_ids)
    except BulkWriteError as e:
        return e.details.get("nInserted", 0)


# --------------------------------------------------------------------------- main
def main() -> None:
    t_total = time.time()
    mongo = MongoClient(CFG["mongo_uri"], serverSelectionTimeoutMS=20000)
    mdb = mongo[CFG["mongo_db"]]
    if mdb.businesses.estimated_document_count() > 0 and not CFG["force"]:
        log("MongoDB ya tiene negocios cargados (FORCE_INGEST=true para recargar). Nada que hacer.")
        return

    biz_path = download(CFG["business_file"])
    rev_path = download(CFG["review_file"])

    client = Client(CFG["scheduler"])
    log(f"Dask: {len(client.scheduler_info()['workers'])} workers en {CFG['scheduler']}")

    t0 = time.time()
    b, steps_b = clean_businesses(biz_path)
    log(f"Negocios limpios: {len(b):,} ({time.time() - t0:.0f} s)")

    t0 = time.time()
    r, steps_r = clean_reviews(rev_path, frozenset(b["business_id"]), client)
    log(f"Reseñas limpias: {steps_r[-1]['quedan']:,} ({time.time() - t0:.0f} s)")

    cutoff = r["date"].max().compute()
    b = business_life(r, b, cutoff)
    antes = steps_b[-1]["quedan"]
    steps_b.append({"regla": "Sin reseñas válidas", "quedan": len(b), "descartados": antes - len(b),
                    "por_que": "Sin reseñas no hay vida (primera y última reseña) ni sentimiento"})

    cols = ["business_id", "geohash", "categoria_principal", "city"]
    r = r.merge(b[cols], on="business_id", how="inner")
    r = r.assign(sentimiento=r["stars"].map_partitions(sentiment_label, meta=("sentimiento", "int64")))

    # Parquet para Spark (sentimiento, MapReduce, balanceo)
    t0 = time.time()
    rev_out = os.path.join(CFG["clean_dir"], "reviews")
    biz_out = os.path.join(CFG["clean_dir"], "businesses")
    shutil.rmtree(rev_out, ignore_errors=True)
    shutil.rmtree(biz_out, ignore_errors=True)
    # Spark lee timestamps en microsegundos (pandas los escribe en nanosegundos)
    r = r.assign(date=r["date"].astype("datetime64[us]"))
    r.to_parquet(rev_out, write_index=False)
    bp = b.drop(columns=["categories"]).copy()
    bp["primera"], bp["ultima"] = bp["primera"].astype("datetime64[us]"), bp["ultima"].astype("datetime64[us]")
    os.makedirs(biz_out, exist_ok=True)
    bp.to_parquet(os.path.join(biz_out, "part-0.parquet"), index=False)
    shutil.rmtree(os.path.join(CFG["clean_dir"], "reviews_stage"), ignore_errors=True)
    log(f"Parquet escrito en {CFG['clean_dir']} ({time.time() - t0:.0f} s)")

    # MongoDB
    t0 = time.time()
    if CFG["force"]:
        mdb.businesses.drop()
        mdb.reviews.drop()
    docs = [biz_document(row) for row in b.itertuples(index=False)]
    for i in range(0, len(docs), CFG["batch_size"]):
        mdb.businesses.insert_many(docs[i:i + CFG["batch_size"]], ordered=False)
    rev = dd.read_parquet(rev_out)
    ins = rev.map_partitions(write_reviews_partition, CFG["mongo_uri"], CFG["mongo_db"], CFG["batch_size"],
                             meta={"insertados": "int64"}).compute()
    n_rev = int(ins["insertados"].sum())
    mdb.businesses.create_index([("location", GEOSPHERE)])
    mdb.businesses.create_index([("categoria_principal", ASCENDING)])
    mdb.businesses.create_index([("geohash", ASCENDING)])
    mdb.reviews.create_index([("business_id", ASCENDING), ("date", ASCENDING)])
    mdb.reviews.create_index([("geohash", ASCENDING)])
    mdb.reviews.create_index([("date", ASCENDING)])
    log(f"MongoDB: {len(docs):,} negocios y {n_rev:,} reseñas cargados en lotes de "
        f"{CFG['batch_size']} ({time.time() - t0:.0f} s)")

    dist = rev["sentimiento"].value_counts().compute().sort_index()
    nombres = {0: "negativo", 1: "neutral", 2: "positivo"}
    total = int(dist.sum())
    reporte = {
        "generado": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "area": {"nombre": CFG["area_name"], "lat": CFG["area_lat"], "lon": CFG["area_lon"],
                 "radio_km": CFG["area_radius_km"], "familia": CFG["family"]},
        "fecha_corte": cutoff.isoformat(),
        "limpieza_negocios": steps_b,
        "limpieza_resenas": steps_r,
        "negocios": len(b),
        "resenas": n_rev,
        "cierres_observados": int(b["evento"].sum()),
        "censurados": int(b["censurado"].sum()),
        "dias_negocio_total": int(b["dias_negocio"].sum()),
        "dias_negocio_mediana": float(b["dias_negocio"].median()),
        "negocios_con_una_resena": int((b["n_resenas"] == 1).sum()),
        "sentimiento": {nombres[int(k)]: {"n": int(v), "pct": round(v / total * 100, 2)} for k, v in dist.items()},
        "categorias_principales_top": b["categoria_principal"].value_counts().head(15).to_dict(),
        "segundos_total": round(time.time() - t_total, 1),
    }
    os.makedirs(CFG["report_dir"], exist_ok=True)
    with open(os.path.join(CFG["report_dir"], "ingesta.json"), "w", encoding="utf-8") as f:
        json.dump(reporte, f, ensure_ascii=False, indent=2, default=str)
    mdb.reports.replace_one({"_id": "ingesta"}, {"_id": "ingesta", **json.loads(json.dumps(reporte, default=str))},
                            upsert=True)
    log("Limpieza de negocios:")
    for s in steps_b:
        log(f"  {s['regla']:<55} quedan {s['quedan']:>9,}  (-{s['descartados']:,})")
    log("Limpieza de reseñas:")
    for s in steps_r:
        log(f"  {s['regla']:<55} quedan {s['quedan']:>9,}  (-{s['descartados']:,})")
    log(f"Sentimiento (por estrellas): {reporte['sentimiento']}")
    log(f"Cierres observados: {reporte['cierres_observados']:,} | censurados: {reporte['censurados']:,}")
    log(f"Ingesta completa en {reporte['segundos_total']:.0f} s")
    client.close()


if __name__ == "__main__":
    main()
