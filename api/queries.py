"""Validación de parámetros y construcción de consultas MongoDB de la API de LifeMap.

Todo lo que no depende de una conexión a MongoDB vive aquí para poder probarlo con pytest.
"""
from __future__ import annotations

import json
import math


class ValidationError(ValueError):
    """Parámetro inválido: la API responde 400 con el mensaje."""


RADIO_MAX_M = 50_000
LIMITE_MAX = 500
MIN_DIAS_DEFECTO = 36_500


# ------------------------------------------------------------------ fórmula
def tasa_cierre(cierres: float, dias_negocio: float) -> float | None:
    """Tasa de cierre anualizada (%) = cierres / (días-negocio / 365) * 100.

    Es la misma fórmula de la AFR del ejercicio de discos, con días-negocio en lugar de
    días-disco. No es la proporción de negocios cerrados.
    """
    if dias_negocio is None or dias_negocio <= 0:
        return None
    return round(cierres / (dias_negocio / 365.0) * 100.0, 4)


# ------------------------------------------------------------------ validadores
def _num(args, name, default=None, lo=None, hi=None, cast=float):
    raw = args.get(name, default)
    if raw is None or raw == "":
        raise ValidationError(f"Falta el parámetro '{name}'")
    try:
        v = cast(raw)
    except (TypeError, ValueError):
        raise ValidationError(f"'{name}' debe ser numérico (recibido: {raw!r})") from None
    if isinstance(v, float) and not math.isfinite(v):
        raise ValidationError(f"'{name}' debe ser un número finito")
    if lo is not None and v < lo:
        raise ValidationError(f"'{name}' debe ser >= {lo}")
    if hi is not None and v > hi:
        raise ValidationError(f"'{name}' debe ser <= {hi}")
    return v


def punto(args) -> tuple[float, float]:
    return _num(args, "lat", lo=-90, hi=90), _num(args, "lon", lo=-180, hi=180)


def radio(args, default=1000) -> float:
    return _num(args, "radio", default, lo=1, hi=RADIO_MAX_M)


def limite(args, default=50) -> int:
    return _num(args, "limit", default, lo=1, hi=LIMITE_MAX, cast=int)


def min_dias(args) -> int:
    return _num(args, "min_dias", MIN_DIAS_DEFECTO, lo=0, cast=int)


def categoria(args) -> str | None:
    c = (args.get("categoria") or "").strip()
    if len(c) > 80:
        raise ValidationError("'categoria' es demasiado larga")
    return c or None


def poligono(obj) -> dict:
    """Valida un polígono GeoJSON: tipo Polygon, anillo cerrado de al menos 4 puntos en rango."""
    if isinstance(obj, str):
        try:
            obj = json.loads(obj)
        except json.JSONDecodeError:
            raise ValidationError("El polígono no es JSON válido") from None
    if isinstance(obj, dict) and obj.get("type") == "Feature":
        obj = obj.get("geometry")
    if not isinstance(obj, dict) or obj.get("type") != "Polygon":
        raise ValidationError("Se espera un GeoJSON de tipo 'Polygon'")
    coords = obj.get("coordinates")
    if not isinstance(coords, list) or not coords or not isinstance(coords[0], list):
        raise ValidationError("'coordinates' debe ser una lista de anillos [[lon, lat], ...]")
    anillo = coords[0]
    if len(anillo) < 4:
        raise ValidationError("El anillo necesita al menos 4 puntos (el primero repetido al final)")
    for p in anillo:
        if not (isinstance(p, (list, tuple)) and len(p) >= 2 and all(isinstance(x, (int, float)) for x in p[:2])):
            raise ValidationError("Cada punto debe ser [lon, lat] numérico")
        if not (-180 <= p[0] <= 180 and -90 <= p[1] <= 90):
            raise ValidationError(f"Punto fuera de rango: {p}")
    if list(anillo[0][:2]) != list(anillo[-1][:2]):
        raise ValidationError("El anillo debe cerrarse: el último punto igual al primero")
    return {"type": "Polygon", "coordinates": [[[float(p[0]), float(p[1])] for p in anillo]]}


def por(args) -> str:
    v = (args.get("por") or "categoria").strip().lower()
    if v not in ("categoria", "celda"):
        raise ValidationError("'por' debe ser 'categoria' o 'celda'")
    return v


def orden(args) -> str:
    v = (args.get("orden") or "tasa").strip().lower()
    if v not in ("tasa", "negocios", "cierres"):
        raise ValidationError("'orden' debe ser 'tasa', 'negocios' o 'cierres'")
    return v


def texto(payload) -> str:
    t = (payload or {}).get("texto") or (payload or {}).get("text") or ""
    t = str(t).strip()
    if not t:
        raise ValidationError("Envíe el texto en el campo 'texto'")
    if len(t) > 5000:
        raise ValidationError("El texto no puede pasar de 5000 caracteres")
    return t


# ------------------------------------------------------------------ consultas
PROYECCION_NEGOCIO = {"name": 1, "city": 1, "categoria_principal": 1, "categories": 1, "stars": 1, "is_open": 1,
                      "location": 1, "geohash": 1, "vida": 1, "sentimiento": 1}


def filtro_categoria(cat: str | None) -> dict:
    return {"categories": cat} if cat else {}


def near_query(lat, lon, radio_m, cat) -> dict:
    return {"location": {"$near": {"$geometry": {"type": "Point", "coordinates": [lon, lat]},
                                   "$maxDistance": radio_m}}, **filtro_categoria(cat)}


def within_query(poly: dict, cat) -> dict:
    return {"location": {"$geoWithin": {"$geometry": poly}}, **filtro_categoria(cat)}


def zone_pipeline(lat, lon, radio_m, cat) -> list[dict]:
    geo_near = {"near": {"type": "Point", "coordinates": [lon, lat]}, "distanceField": "distancia_m",
                "maxDistance": radio_m, "spherical": True}
    if cat:
        geo_near["query"] = filtro_categoria(cat)
    return [
        {"$geoNear": geo_near},
        {"$group": {"_id": None,
                    "negocios": {"$sum": 1},
                    "abiertos": {"$sum": "$is_open"},
                    "cierres": {"$sum": "$vida.evento"},
                    "dias_negocio": {"$sum": "$vida.dias_negocio"},
                    "resenas": {"$sum": "$vida.n_resenas"},
                    "resenas_negativas": {"$sum": "$sentimiento.negativas"},
                    "sentimiento_medio": {"$avg": "$sentimiento.medio"},
                    "estrellas_medias": {"$avg": "$stars"},
                    "distancia_media_m": {"$avg": "$distancia_m"}}},
    ]


def resumen_zona(doc: dict | None, min_d: int) -> dict:
    if not doc:
        return {"negocios": 0, "mensaje": "No hay negocios en ese radio"}
    doc.pop("_id", None)
    d = doc["dias_negocio"]
    doc["anios_negocio"] = round(d / 365, 1)
    doc["tasa_cierre_anual"] = tasa_cierre(doc["cierres"], d)
    doc["pct_resenas_negativas"] = round(doc["resenas_negativas"] / doc["resenas"] * 100, 2) if doc["resenas"] else None
    doc["confiable"] = d >= min_d
    for k in ("sentimiento_medio", "estrellas_medias", "distancia_media_m"):
        if doc.get(k) is not None:
            doc[k] = round(doc[k], 3)
    return doc


def negocio_salida(d: dict) -> dict:
    lon, lat = d["location"]["coordinates"]
    vida = d.get("vida", {})
    return {"business_id": d["_id"], "nombre": d.get("name"), "ciudad": d.get("city"),
            "categoria_principal": d.get("categoria_principal"), "estrellas": d.get("stars"),
            "abierto": bool(d.get("is_open")), "lat": lat, "lon": lon, "geohash": d.get("geohash"),
            "vida": {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in vida.items()},
            "sentimiento": d.get("sentimiento")}
