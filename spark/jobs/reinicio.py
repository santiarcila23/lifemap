"""Deja el streaming como recién arrancado en la fecha de corte T, para repetir la demo.

Uso (con el streaming detenido):
  docker compose stop spark-streaming
  docker compose run --rm spark-reinicio
  docker compose up -d spark-streaming

Borra las reseñas clasificadas, las ventanas, las alertas y los checkpoints, y devuelve cada
negocio de riesgo_negocios a su estado y probabilidad en T. No toca nada de la Entrega 1.
"""
import json
import os
import shutil

from pymongo import MongoClient

db = MongoClient(os.environ.get("MONGO_URI", "mongodb://mongo:27017"))[os.environ.get("MONGO_DB", "lifemap")]
for c in ("reviews_stream", "ventanas_celda", "alerts", "stream_estado"):
    print(f"Borrando {c}: {db[c].estimated_document_count():,} documentos", flush=True)
    db[c].drop()
with open(os.path.join(os.environ.get("MODELS_DIR", "/models"), "riesgo_v1.json"), encoding="utf-8") as f:
    umbral = json.load(f)["umbral"]
r = db.riesgo_negocios.update_many({}, [{"$set": {"estado": "$estado_corte", "probabilidad": "$probabilidad_corte",
                                                   "variables": "$variables_corte", "fecha_ref": "$fecha_corte",
                                                   "alerta": {"$gte": ["$probabilidad_corte", umbral]}}},
                                        {"$unset": ["actualizado"]}])
print(f"riesgo_negocios: {r.modified_count:,} negocios devueltos a su estado en T", flush=True)
cp = os.environ.get("CHECKPOINT_DIR", "/data/checkpoints")
shutil.rmtree(cp, ignore_errors=True)
print(f"Checkpoints borrados ({cp}). Ya puede levantar spark-streaming y correr el productor.", flush=True)
