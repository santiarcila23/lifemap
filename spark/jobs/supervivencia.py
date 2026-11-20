"""
Curvas de Kaplan-Meier por categoría principal y por zona (Entrega 2).

  - Censura: los negocios abiertos (is_open = 1) cuentan hasta su última reseña, sin cierre.
  - Entrada tardía: la observación empieza en VENTANA_INICIO; quien tiene reseñas antes entra
    al riesgo con la edad que ya tenía, y quien terminó antes de la ventana no se cuenta.
  - Grupos pequeños: solo se comparan grupos con al menos MIN_NEGOCIOS negocios.
  - Zonas: los negocios de cada polígono (zonas.py) se consultan en MongoDB con $geoWithin.

Se ejecuta con Python (no necesita Spark): python3 supervivencia.py
Salidas: /data/reports/supervivencia.json y MongoDB supervivencia
"""
import json
import os
import sys
from datetime import datetime, timezone

from pymongo import MongoClient

import km
from zonas import ZONAS

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "lifemap")
REPORTS = os.environ.get("REPORT_DIR", "/data/reports")
INICIO = datetime.fromisoformat(os.environ.get("VENTANA_INICIO", "2016-01-01"))
MIN_NEGOCIOS = int(os.environ.get("MIN_NEGOCIOS_KM", "100"))
TOP_CATEGORIAS = int(os.environ.get("TOP_CATEGORIAS_KM", "12"))
FORZAR = os.environ.get("FORCE_SPARK", "false").lower() in ("1", "true", "yes")
PROY = {"vida": 1, "categoria_principal": 1}


def log(m):
    print(f"[KM {datetime.now():%H:%M:%S}] {m}", flush=True)


def resumen(nombre, tipo, docs, inicio=INICIO):
    r = km.kaplan_meier(*km.datos_negocios(docs, inicio))
    r["curva"] = km.reducir(r["curva"])
    r.update({"_id": f"{tipo}:{nombre}", "tipo": tipo, "grupo": nombre, "confiable": r["negocios"] >= MIN_NEGOCIOS,
              "entrada_tardia": inicio is not None})
    return r


def main():
    ruta = os.path.join(REPORTS, "supervivencia.json")
    if not FORZAR and os.path.exists(ruta):
        with open(ruta, encoding="utf-8") as f:
            if json.load(f).get("ventana_inicio") == INICIO.date().isoformat():
                log("supervivencia.json ya existe; FORCE_SPARK=true para recalcular")
                return
    db = MongoClient(MONGO_URI)[MONGO_DB]
    todos = list(db.businesses.find({}, PROY))
    if not todos:
        sys.exit("No hay negocios en MongoDB: corra primero la ingesta")
    log(f"{len(todos):,} negocios. Ventana desde {INICIO.date()} | mínimo {MIN_NEGOCIOS} negocios por grupo")

    grupos = [resumen("todas", "general", todos)]
    ingenua = resumen("todas_ingenua", "general", todos, inicio=None)
    grupos.append(ingenua)
    g = grupos[0]
    log(f"Todas: {g['negocios']:,} negocios, {g['cierres']:,} cierres, mediana {g['mediana_anios']} años "
        f"| ingenua (sin entrada tardía): mediana {ingenua['mediana_anios']}")

    # por categoría: las más frecuentes dentro de la ventana
    conteo = {}
    for d in todos:
        if d["vida"]["ultima_resena"] >= INICIO:
            conteo[d["categoria_principal"]] = conteo.get(d["categoria_principal"], 0) + 1
    for cat, n in sorted(conteo.items(), key=lambda x: -x[1])[:TOP_CATEGORIAS]:
        r = resumen(cat, "categoria", [d for d in todos if d["categoria_principal"] == cat])
        grupos.append(r)
        log(f"  categoría {cat:<28} n={r['negocios']:>5} cierres={r['cierres']:>4} mediana={r['mediana_anios']} "
            f"S(5)={r['en_riesgo'][4]['supervivencia']} {'' if r['confiable'] else '(pocos negocios)'}")

    # por zona: $geoWithin sobre cada polígono
    for clave, z in ZONAS.items():
        docs = list(db.businesses.find({"location": {"$geoWithin": {"$geometry": z["poligono"]}}}, PROY))
        r = resumen(clave, "zona", docs)
        r.update({"nombre": z["nombre"], "poligono": z["poligono"]})
        grupos.append(r)
        log(f"  zona {z['nombre']:<32} n={r['negocios']:>5} cierres={r['cierres']:>4} mediana={r['mediana_anios']} "
            f"S(5)={r['en_riesgo'][4]['supervivencia']} {'' if r['confiable'] else '(pocos negocios)'}")

    db.supervivencia.delete_many({})
    db.supervivencia.insert_many([dict(x) for x in grupos])
    os.makedirs(REPORTS, exist_ok=True)
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump({"generado": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                   "ventana_inicio": INICIO.date().isoformat(), "min_negocios": MIN_NEGOCIOS,
                   "definicion": "duración = primera a última reseña; evento = is_open 0; censura = is_open 1; "
                                 "entrada = edad al inicio de la ventana",
                   "grupos": [{k: v for k, v in x.items() if k not in ("curva", "poligono")} for x in grupos],
                   "curvas": {x["_id"]: x["curva"] for x in grupos}},
                  f, ensure_ascii=False, indent=1, default=str)
    log(f"Listo: {len(grupos)} curvas en MongoDB (supervivencia) y {ruta}")


if __name__ == "__main__":
    main()
