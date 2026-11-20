"""Kaplan-Meier con censura por la derecha y entrada tardía (truncamiento por la izquierda).

Igual que en el ejercicio de discos, pero sin lifelines para poder usarlo en la API:
  - duración: años desde la primera reseña hasta la última (cierre o fecha de corte)
  - evento: 1 si el negocio cerró (is_open = 0), 0 si sigue abierto (censurado)
  - entrada: edad a la que el negocio empieza a estar en riesgo. Si el estudio arranca en
    VENTANA_INICIO, un negocio cuya primera reseña es anterior entra con esa edad; antes no lo
    estábamos observando, igual que un disco que ya venía funcionando.

En riesgo en t = negocios con entrada < t <= duración.  S(t) = prod(1 - d_i / n_i).
"""
from __future__ import annotations

from datetime import datetime

import numpy as np

TIEMPOS_TABLA = [0, 1, 2, 3, 5, 10]


def kaplan_meier(duraciones, eventos, entradas=None) -> dict:
    d = np.asarray(duraciones, dtype=float)
    e = np.asarray(eventos, dtype=int)
    a = np.zeros_like(d) if entradas is None else np.asarray(entradas, dtype=float)
    ok = d > a
    d, e, a = d[ok], e[ok], a[ok]
    tiempos = np.unique(d[e == 1])
    s, curva = 1.0, [{"anios": 0.0, "supervivencia": 1.0, "en_riesgo": int(((a <= 0) & (d > 0)).sum()), "cierres": 0}]
    for t in tiempos:
        n = int(((a < t) & (d >= t)).sum())
        k = int(((d == t) & (e == 1)).sum())
        if n == 0:
            continue
        s *= 1.0 - k / n
        curva.append({"anios": round(float(t), 4), "supervivencia": round(s, 6), "en_riesgo": n, "cierres": k})
    mediana = next((p["anios"] for p in curva if p["supervivencia"] <= 0.5), None)
    tabla = [{"anios": t, "en_riesgo": int(((a <= t) & (d > t)).sum()),
              "supervivencia": _s_en(curva, t)} for t in TIEMPOS_TABLA]
    return {"negocios": int(len(d)), "cierres": int(e.sum()), "censurados": int(len(d) - e.sum()),
            "mediana_anios": mediana, "en_riesgo": tabla, "curva": curva}


def _s_en(curva: list[dict], t: float) -> float:
    s = 1.0
    for p in curva:
        if p["anios"] > t:
            break
        s = p["supervivencia"]
    return round(s, 4)


def reducir(curva: list[dict], max_puntos: int = 150) -> list[dict]:
    """Deja como mucho max_puntos (para gráficas y Power BI), conservando el primero y el último."""
    if len(curva) <= max_puntos:
        return curva
    idx = np.unique(np.linspace(0, len(curva) - 1, max_puntos).astype(int))
    return [curva[i] for i in idx]


def datos_negocios(docs, inicio: datetime | None):
    """Convierte documentos de MongoDB (campo vida) en duraciones, eventos y entradas en años.

    Con inicio (ventana de observación): se excluyen los negocios cuya última reseña es anterior
    a la ventana (ya habían terminado) y los demás entran con su edad al inicio de la ventana.
    Sin inicio: todos desde la edad 0 (versión ingenua, ignora la entrada tardía).
    """
    dur, ev, ent = [], [], []
    for x in docs:
        v = x["vida"]
        primera, ultima = v["primera_resena"], v["ultima_resena"]
        if inicio is not None and ultima < inicio:
            continue
        dur.append(v["dias_negocio"] / 365.25)
        ev.append(int(v["evento"]))
        ent.append(max((inicio - primera).days, 0) / 365.25 if inicio is not None else 0.0)
    return dur, ev, ent
