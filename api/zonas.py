"""Zonas del área de Tampa para comparar curvas de supervivencia (polígonos GeoJSON).

Son rectángulos aproximados de sectores conocidos del área metropolitana. Los negocios de cada
zona se consultan en MongoDB con $geoWithin sobre estos polígonos.
"""


def _rect(lon1, lat1, lon2, lat2) -> dict:
    return {"type": "Polygon", "coordinates": [[[lon1, lat1], [lon2, lat1], [lon2, lat2], [lon1, lat2], [lon1, lat1]]]}


ZONAS = {
    "tampa_centro": {"nombre": "Tampa centro y Ybor", "poligono": _rect(-82.48, 27.93, -82.43, 27.98)},
    "south_tampa": {"nombre": "South Tampa y Westshore", "poligono": _rect(-82.56, 27.84, -82.48, 27.97)},
    "tampa_norte": {"nombre": "Tampa norte (USF, Carrollwood)", "poligono": _rect(-82.56, 28.00, -82.38, 28.15)},
    "brandon": {"nombre": "Brandon y Riverview", "poligono": _rect(-82.36, 27.82, -82.24, 27.97)},
    "st_petersburg": {"nombre": "St. Petersburg", "poligono": _rect(-82.76, 27.70, -82.60, 27.85)},
    "clearwater": {"nombre": "Clearwater y Largo", "poligono": _rect(-82.85, 27.87, -82.70, 28.05)},
}
