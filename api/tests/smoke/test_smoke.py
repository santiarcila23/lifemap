"""Pruebas de humo contra la API levantada (staging en Jenkins o la URL pública).

  API_URL=http://api-staging:5000 pytest tests/smoke
"""
import os

import pytest
import requests

API = os.environ.get("API_URL", "http://localhost:5000").rstrip("/")


def get(path, **params):
    r = requests.get(f"{API}{path}", params=params, timeout=30)
    return r


@pytest.fixture(scope="module")
def centro():
    m = get("/meta").json()
    area = m.get("area") or {}
    return area.get("lat", 27.9506), area.get("lon", -82.4572)


def test_health_con_todos_los_servicios():
    j = get("/health").json()
    assert j["commit"]
    for s in ("api", "mongodb", "spark", "dask"):
        assert j["servicios"][s]["estado"] == "up", f"{s} no está arriba: {j['servicios'][s]}"
    assert j["servicios"]["mongodb"]["negocios"] > 0


def test_near_devuelve_negocios_cercanos(centro):
    j = get("/businesses/near", lat=centro[0], lon=centro[1], radio=5000, limit=20).json()
    assert j["total"] > 0 and all("vida" in n for n in j["negocios"])


def test_within_poligono(centro):
    lat, lon = centro
    d = 0.05
    poly = {"type": "Polygon", "coordinates": [[[lon - d, lat - d], [lon + d, lat - d], [lon + d, lat + d],
                                                [lon - d, lat + d], [lon - d, lat - d]]]}
    r = requests.post(f"{API}/businesses/within", json=poly, timeout=30)
    assert r.status_code == 200 and r.json()["total"] > 0


def test_zones_summary_tasa_y_sentimiento(centro):
    res = get("/zones/summary", lat=centro[0], lon=centro[1], radio=3000).json()["resumen"]
    assert res["negocios"] > 0 and res["tasa_cierre_anual"] is not None and "sentimiento_medio" in res


@pytest.mark.parametrize("por", ["categoria", "celda"])
def test_stats_closure_respeta_minimo(por):
    j = get("/stats/closure", por=por, min_dias=36500).json()
    assert all(x["dias_negocio"] >= 36500 for x in j["resultados"])


def test_predict():
    r = requests.post(f"{API}/predict", json={"texto": "Amazing food, friendly staff, we loved it!"}, timeout=30)
    assert r.status_code == 200 and r.json()["sentimiento"] == "positivo"


def test_validacion_400():
    assert get("/businesses/near", lat=200, lon=0).status_code == 400


# ------------------------------------------------------------------ Entrega 2
def test_health_incluye_kafka_y_consumidor():
    k = get("/health").json()["servicios"]["kafka"]
    assert k["estado"] == "up", k
    assert "consumidor" in k


def test_hotspots_y_evolucion(centro):
    j = get("/live/hotspots", lat=centro[0], lon=centro[1], radio=30000).json()
    assert "ventanas" in j
    assert "evolucion" in get("/live/evolucion").json()


def test_alertas_por_radio(centro):
    j = get("/alerts", lat=centro[0], lon=centro[1], radio=30000).json()
    assert "alertas" in j and all("probabilidad" in a for a in j["alertas"])


def test_supervivencia_por_categoria_y_zona():
    j = get("/survival/todas").json()
    assert j["negocios"] > 0 and j["curva"][0]["supervivencia"] == 1.0
    z = get("/survival/todas", comparar="zona").json()
    assert z["grupos"]


def test_riesgo_cerca(centro):
    j = get("/risk/near", lat=centro[0], lon=centro[1], radio=3000).json()
    assert j["total"] > 0 and 0 <= j["negocios"][0]["probabilidad"] <= 1
