"""Variables del modelo de riesgo de cierre (las mismas en el entrenamiento y en el streaming).

Cada negocio tiene un "estado" acumulado con sus reseñas:
  {"n": reseñas, "suma_sent": suma de (-1, 0, 1), "suma_estrellas": ..., "primera": fecha,
   "ultima": fecha, "meses": {"2020-11": [reseñas, negativas], ...}}
El entrenamiento lo arma con las reseñas hasta la fecha de corte T; el job de streaming lo
actualiza con cada micro-lote y vuelve a calcular las variables con la fecha del último evento.
"""
from __future__ import annotations

import math
from datetime import datetime

VARIABLES = ["sentimiento_medio", "tendencia_negativas", "estrellas_medias", "log_resenas",
             "log_resenas_6m", "edad_anios", "dias_sin_resenas", "competidores_500m"]
MESES_RECIENTES = 6


def _fecha(x) -> datetime:
    if isinstance(x, datetime):
        return x.replace(tzinfo=None)
    return datetime.fromisoformat(str(x)[:19])


def clave_mes(fecha) -> str:
    f = _fecha(fecha)
    return f"{f.year:04d}-{f.month:02d}"


def _indice_mes(clave: str) -> int:
    y, m = clave.split("-")
    return int(y) * 12 + int(m) - 1


def estado_vacio() -> dict:
    return {"n": 0, "suma_sent": 0.0, "suma_estrellas": 0.0, "primera": None, "ultima": None, "meses": {}}


def acumular(estado: dict, fecha, estrellas: float, sentimiento: int, n: int = 1, negativas: int | None = None,
             suma_sent: float | None = None, suma_estrellas: float | None = None) -> dict:
    """Suma una reseña (o un grupo de n reseñas del mismo mes) al estado.

    sentimiento: 0 negativo, 1 neutral, 2 positivo (para una sola reseña).
    """
    f = _fecha(fecha)
    neg = negativas if negativas is not None else (1 if sentimiento == 0 else 0)
    estado["n"] += n
    estado["suma_sent"] += suma_sent if suma_sent is not None else (sentimiento - 1)
    estado["suma_estrellas"] += suma_estrellas if suma_estrellas is not None else estrellas
    estado["primera"] = f if estado["primera"] is None or f < _fecha(estado["primera"]) else _fecha(estado["primera"])
    estado["ultima"] = f if estado["ultima"] is None or f > _fecha(estado["ultima"]) else _fecha(estado["ultima"])
    mes = estado["meses"].setdefault(clave_mes(f), [0, 0])
    mes[0] += n
    mes[1] += neg
    return estado


def tendencia_negativas(meses: dict, fecha_ref) -> float:
    """% de reseñas negativas de los últimos 6 meses menos el % de antes (en puntos).

    Positivo = el negocio está recibiendo más reseñas negativas que en su historia.
    Si falta alguno de los dos periodos, vale 0 (no hay con qué comparar).
    """
    ref = _indice_mes(clave_mes(fecha_ref))
    n_r = neg_r = n_a = neg_a = 0
    for clave, (n, neg) in meses.items():
        i = _indice_mes(clave)
        if i > ref:
            continue                       # nunca se mira después de la fecha de referencia
        if i > ref - MESES_RECIENTES:
            n_r, neg_r = n_r + n, neg_r + neg
        else:
            n_a, neg_a = n_a + n, neg_a + neg
    if not n_r or not n_a:
        return 0.0
    return round((neg_r / n_r - neg_a / n_a) * 100.0, 4)


def resenas_recientes(meses: dict, fecha_ref) -> int:
    """Reseñas de los últimos 6 meses hasta la fecha de referencia (actividad reciente)."""
    ref = _indice_mes(clave_mes(fecha_ref))
    return sum(n for clave, (n, _) in meses.items() if ref - MESES_RECIENTES < _indice_mes(clave) <= ref)


def variables(estado: dict, fecha_ref, competidores: int) -> dict:
    """Las 8 variables del modelo a una fecha de referencia (T en el entrenamiento)."""
    n = max(estado["n"], 1)
    ref = _fecha(fecha_ref)
    primera, ultima = _fecha(estado["primera"]), _fecha(estado["ultima"])
    return {
        "sentimiento_medio": round(estado["suma_sent"] / n, 4),
        "tendencia_negativas": tendencia_negativas(estado["meses"], ref),
        "estrellas_medias": round(estado["suma_estrellas"] / n, 4),
        "log_resenas": round(math.log1p(estado["n"]), 4),
        "log_resenas_6m": round(math.log1p(resenas_recientes(estado["meses"], ref)), 4),
        "edad_anios": round(max((ref - primera).days, 0) / 365.25, 4),
        "dias_sin_resenas": float(max((ref - ultima).days, 0)),
        "competidores_500m": float(competidores or 0),
    }


def etiqueta(is_open: int, ultima_total, corte, meses: int = 12) -> int | None:
    """1 = cerró y su última reseña cae entre T y T + 12 meses; 0 = siguió activo o abierto;
    None = sin actividad después de T (se excluye)."""
    ult, t = _fecha(ultima_total), _fecha(corte)
    if ult < t:
        return None
    fin = t.replace(year=t.year + meses // 12)
    return 1 if (is_open == 0 and ult < fin) else 0
