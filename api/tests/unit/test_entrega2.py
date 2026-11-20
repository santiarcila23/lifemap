"""Pruebas de la Entrega 2 que no necesitan servicios: Kaplan-Meier, variables y etiqueta del
modelo de riesgo, y validación de los endpoints nuevos (400)."""
from datetime import datetime

import pytest

import km
import riesgo_vars as rv
from tests.unit.test_api import client  # noqa: F401  (fixture con MongoDB falso)


# ------------------------------------------------------------------ Kaplan-Meier
def test_km_sin_entrada_tardia_coincide_con_el_calculo_a_mano():
    r = km.kaplan_meier([1, 2, 2, 3, 4], [1, 1, 0, 1, 0])
    s = {p["anios"]: p["supervivencia"] for p in r["curva"]}
    assert s[1.0] == pytest.approx(0.8) and s[2.0] == pytest.approx(0.6) and s[3.0] == pytest.approx(0.3)
    assert r["mediana_anios"] == 3.0 and r["cierres"] == 3 and r["censurados"] == 2


def test_km_con_entrada_tardia_no_cuenta_en_riesgo_antes_de_entrar():
    r = km.kaplan_meier([1, 2, 2, 3, 4], [1, 1, 0, 1, 0], entradas=[0, 0, 0, 0, 2.5])
    p = {x["anios"]: x for x in r["curva"]}
    assert p[1.0]["en_riesgo"] == 4            # el que entra a los 2,5 años no está en riesgo en t = 1
    assert p[3.0]["en_riesgo"] == 2 and p[3.0]["supervivencia"] == pytest.approx(0.25)


def test_km_los_censurados_no_cuentan_como_cierre():
    r = km.kaplan_meier([5, 5, 5], [0, 0, 0])
    assert r["cierres"] == 0 and r["mediana_anios"] is None and r["curva"][-1]["supervivencia"] == 1.0


def test_km_excluye_negocios_que_terminaron_antes_de_la_ventana():
    docs = [{"vida": {"primera_resena": datetime(2010, 1, 1), "ultima_resena": datetime(2014, 1, 1), "dias_negocio": 1462, "evento": 1}},
            {"vida": {"primera_resena": datetime(2014, 1, 1), "ultima_resena": datetime(2020, 1, 1), "dias_negocio": 2192, "evento": 0}}]
    dur, ev, ent = km.datos_negocios(docs, datetime(2016, 1, 1))
    assert len(dur) == 1 and ent[0] == pytest.approx(730 / 365.25)
    assert len(km.datos_negocios(docs, None)[0]) == 2   # la versión ingenua los cuenta a todos


# ------------------------------------------------------------------ modelo de riesgo
def _estado():
    e = rv.estado_vacio()
    for mes, estrellas in [(1, 5), (2, 5), (3, 4), (4, 5), (10, 1), (11, 2), (12, 1)]:
        rv.acumular(e, datetime(2020, mes, 15), estrellas, 0 if estrellas <= 2 else 2)
    return e


def test_tendencia_compara_ultimos_6_meses_con_lo_anterior():
    assert rv.tendencia_negativas(_estado()["meses"], datetime(2020, 12, 31)) == pytest.approx(100.0)


def test_variables_no_miran_despues_de_la_fecha_de_referencia():
    e = _estado()
    v = rv.variables(e, datetime(2020, 12, 31), competidores=3)
    rv.acumular(e, datetime(2021, 6, 1), 1, 0)          # reseña posterior a la referencia
    assert rv.tendencia_negativas(e["meses"], datetime(2020, 12, 31)) == v["tendencia_negativas"]
    assert set(v) == set(rv.VARIABLES) and v["competidores_500m"] == 3


@pytest.mark.parametrize("is_open,ultima,esperado", [
    (0, datetime(2021, 6, 1), 1),       # cerró dentro de los 12 meses
    (0, datetime(2022, 1, 10), 0),      # cerró, pero tuvo actividad después de T + 12 meses
    (1, datetime(2021, 3, 1), 0),       # sigue abierto
    (0, datetime(2020, 11, 1), None),   # sin actividad después de T: se excluye
])
def test_etiqueta_de_riesgo(is_open, ultima, esperado):
    assert rv.etiqueta(is_open, ultima, datetime(2021, 1, 1)) == esperado


# ------------------------------------------------------------------ validación de los endpoints nuevos
@pytest.mark.parametrize("url", [
    "/live/hotspots?fecha=2021-13-40", "/live/hotspots?lat=27.9", "/live/hotspots?solo_hotspots=quizas",
    "/alerts?lat=27.9&lon=-82.4&radio=999999", "/alerts?poligono=nojson",
    "/survival/todas?zona=narnia", "/survival/todas?comparar=ciudad", "/survival/todas?min_negocios=0",
    "/risk/near?lat=27.9", "/risk/near?lat=27.9&lon=-82.4&radio=-5",
])
def test_endpoints_entrega2_validan_parametros(client, url):  # noqa: F811
    r = client.get(url)
    assert r.status_code == 400, r.get_json()
    assert "error" in r.get_json()


def test_health_incluye_kafka(client):  # noqa: F811
    j = client.get("/health").get_json()
    assert "kafka" in j["servicios"] and "estado" in j["servicios"]["kafka"]
