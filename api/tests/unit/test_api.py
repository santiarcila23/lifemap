"""Pruebas de la API sin MongoDB: validación (400) y /predict con un modelo pequeño."""
import numpy as np
import pytest

import textomodelo
from app import create_app
from sentiment import ModeloSentimiento


class _ColeccionVacia:
    def find(self, *a, **k):
        return self

    def sort(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return []

    def find_one(self, *a, **k):
        return None

    def aggregate(self, *a, **k):
        return []

    def count_documents(self, *a, **k):
        return 0

    def estimated_document_count(self):
        return 0


class _DBFalsa:
    def __getattr__(self, _):
        return _ColeccionVacia()

    def __getitem__(self, _):
        return _ColeccionVacia()

    def command(self, *_):
        return {"ok": 1}


def _modelo_prueba():
    nf = 1 << 10
    W, b = np.zeros((3, nf)), np.zeros(3)
    for palabra, clase in [("terrible", 0), ("awful", 0), ("okay", 1), ("great", 2), ("delicious", 2)]:
        W[clase, textomodelo.indice(palabra, nf)] = 5.0
    return ModeloSentimiento(datos={"num_features": nf, "stopwords": {"the"}, "idf": np.ones(nf),
                                    "W": W, "b": b, "clases": ["negativo", "neutral", "positivo"]})


@pytest.fixture
def client():
    return create_app(db=_DBFalsa(), modelo=_modelo_prueba()).test_client()


@pytest.mark.parametrize("url", [
    "/businesses/near?lon=-82.4",
    "/businesses/near?lat=100&lon=-82.4",
    "/businesses/near?lat=27.9&lon=-82.4&radio=-1",
    "/zones/summary?lat=27.9",
    "/stats/closure?por=planeta",
    "/stats/closure?min_dias=abc",
    "/businesses/within",
    "/predict?texto=",
])
def test_parametros_invalidos_dan_400(client, url):
    r = client.get(url)
    assert r.status_code == 400 and "error" in r.get_json()


def test_within_post_poligono_abierto_da_400(client):
    r = client.post("/businesses/within", json={"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 2]]]})
    assert r.status_code == 400


def test_predict_positivo_y_negativo(client):
    r = client.post("/predict", json={"texto": "The food was great and delicious"})
    assert r.status_code == 200 and r.get_json()["sentimiento"] == "positivo"
    r = client.get("/predict?texto=terrible awful service")
    assert r.get_json()["sentimiento"] == "negativo"
    probs = r.get_json()["probabilidades"]
    assert abs(sum(probs.values()) - 1) < 1e-3


def test_consultas_validas_responden_200(client):
    assert client.get("/businesses/near?lat=27.95&lon=-82.45&radio=500&categoria=Pizza").status_code == 200
    assert client.get("/zones/summary?lat=27.95&lon=-82.45&radio=2000").status_code == 200
    assert client.get("/stats/closure?por=celda&min_dias=1000").status_code == 200


def test_health_incluye_commit_y_servicios(client):
    j = client.get("/health").get_json()
    assert "commit" in j and {"api", "mongodb", "spark", "dask"} <= set(j["servicios"])
