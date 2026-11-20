"""El modelo que usa el sistema es el versionado en el repositorio (modelos_esperados.json).

Corre dentro del contenedor de staging, que monta /models, y consulta en MongoDB qué versión
cargó el job de streaming. Si alguien cambia la versión esperada o se despliega un modelo
distinto, esta prueba falla y Jenkins no despliega.
"""
import json
import os

import pytest
from pymongo import MongoClient

MODELS = os.environ.get("MODELS_DIR", "/models")
ESPERADO = json.load(open(os.path.join(os.path.dirname(__file__), "..", "..", "modelos_esperados.json"), encoding="utf-8"))


def _json(ruta):
    with open(ruta, encoding="utf-8") as f:
        return json.load(f)


def test_modelo_de_sentimiento_guardado_es_el_versionado():
    meta = _json(os.path.join(MODELS, ESPERADO["sentimiento"], "lifemap_version.json"))
    assert meta["version"] == ESPERADO["sentimiento"]
    assert meta["fecha_corte"] == ESPERADO["fecha_corte"], "el modelo no se entrenó con la fecha de corte esperada"
    assert os.path.isdir(os.path.join(MODELS, ESPERADO["sentimiento"], "stages"))


def test_export_de_la_api_corresponde_al_mismo_modelo():
    meta = _json(os.path.join(MODELS, "sentiment_export.json"))
    assert meta["version"] == ESPERADO["sentimiento"] and meta["fecha_corte"] == ESPERADO["fecha_corte"]


def test_modelo_de_riesgo_es_el_versionado():
    meta = _json(os.path.join(MODELS, f"{ESPERADO['riesgo']}.json"))
    assert meta["version"] == ESPERADO["riesgo"] and meta["fecha_corte"] == ESPERADO["fecha_corte"]
    assert 0 < meta["umbral"] < 1 and os.path.isdir(os.path.join(MODELS, ESPERADO["riesgo"]))


def test_el_streaming_cargo_el_modelo_versionado():
    db = MongoClient(os.environ.get("MONGO_URI", "mongodb://mongo:27017"), serverSelectionTimeoutMS=5000)[
        os.environ.get("MONGO_DB", "lifemap")]
    est = db.stream_estado.find_one({"_id": "reviews"})
    if est is None:
        pytest.fail("El streaming no ha reportado qué modelo cargó (¿está corriendo spark-streaming?)")
    assert est["modelo_sentimiento"] == ESPERADO["sentimiento"]
    assert est["fecha_corte_modelo"] == ESPERADO["fecha_corte"]
    assert est["modelo_riesgo"] == ESPERADO["riesgo"]
