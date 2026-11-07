import pytest

import geohash
import queries as q


def test_punto_valido_y_rangos():
    assert q.punto({"lat": "27.95", "lon": "-82.45"}) == (27.95, -82.45)
    for args in ({"lat": "95", "lon": "0"}, {"lat": "0", "lon": "-200"}, {"lat": "x", "lon": "0"}, {"lon": "0"}):
        with pytest.raises(q.ValidationError):
            q.punto(args)


def test_radio_y_limite():
    assert q.radio({}) == 1000 and q.radio({"radio": "500"}) == 500
    with pytest.raises(q.ValidationError):
        q.radio({"radio": "0"})
    with pytest.raises(q.ValidationError):
        q.radio({"radio": str(q.RADIO_MAX_M + 1)})
    with pytest.raises(q.ValidationError):
        q.limite({"limit": "9999"})


ANILLO = [[-82.5, 27.9], [-82.4, 27.9], [-82.4, 28.0], [-82.5, 28.0], [-82.5, 27.9]]


def test_poligono_valido_desde_texto_y_feature():
    p = q.poligono({"type": "Polygon", "coordinates": [ANILLO]})
    assert p["type"] == "Polygon" and len(p["coordinates"][0]) == 5
    assert q.poligono('{"type":"Feature","geometry":{"type":"Polygon","coordinates":[%s]}}'
                      % str(ANILLO).replace(" ", ""))["type"] == "Polygon"


@pytest.mark.parametrize("malo", [
    {"type": "Point", "coordinates": [0, 0]},
    {"type": "Polygon", "coordinates": [ANILLO[:3]]},
    {"type": "Polygon", "coordinates": [ANILLO[:-1] + [[-82.45, 27.95]]]},   # sin cerrar
    {"type": "Polygon", "coordinates": [[[0, 0], [300, 0], [0, 1], [0, 0]]]},
    "no es json",
])
def test_poligono_invalido(malo):
    with pytest.raises(q.ValidationError):
        q.poligono(malo)


def test_por_y_orden():
    assert q.por({}) == "categoria" and q.por({"por": "celda"}) == "celda"
    with pytest.raises(q.ValidationError):
        q.por({"por": "ciudad"})
    with pytest.raises(q.ValidationError):
        q.orden({"orden": "x"})


def test_consultas_geoespaciales():
    near = q.near_query(27.9, -82.4, 800, "Pizza")
    assert near["location"]["$near"]["$maxDistance"] == 800
    assert near["location"]["$near"]["$geometry"]["coordinates"] == [-82.4, 27.9]   # [lon, lat]
    assert near["categories"] == "Pizza"
    pipe = q.zone_pipeline(27.9, -82.4, 2000, None)
    assert list(pipe[0]) == ["$geoNear"]                                           # $geoNear va primero
    assert "query" not in pipe[0]["$geoNear"]


def test_resumen_zona_usa_tasa_anualizada():
    doc = {"_id": None, "negocios": 3, "cierres": 1, "dias_negocio": 730, "resenas": 10,
           "resenas_negativas": 2, "sentimiento_medio": 0.5, "estrellas_medias": 4, "distancia_media_m": 100}
    r = q.resumen_zona(doc, 365)
    assert r["tasa_cierre_anual"] == 50.0 and r["pct_resenas_negativas"] == 20.0 and r["confiable"]


def test_texto():
    assert q.texto({"texto": "  great food "}) == "great food"
    with pytest.raises(q.ValidationError):
        q.texto({"texto": "   "})


def test_geohash():
    assert geohash.encode(57.64911, 10.40744, 11) == "u4pruydqqvj"
    gh = geohash.encode(27.9506, -82.4572)
    s, n, w, e = geohash.bbox(gh)
    assert s <= 27.9506 <= n and w <= -82.4572 <= e
    poly = geohash.polygon(gh)["coordinates"][0]
    assert poly[0] == poly[-1] and len(poly) == 5
