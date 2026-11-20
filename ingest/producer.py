"""
Productor de LifeMap (Entrega 2): publica en Kafka las reseñas POSTERIORES a la fecha de corte T,
en orden de fecha y con reloj acelerado.

  - Solo reseñas con date >= FECHA_CORTE: ninguna se usó para entrenar los modelos.
  - Topic `reviews` con 3 particiones; la clave del mensaje es el business_id, así todas las
    reseñas de un negocio caen en la misma partición y conservan su orden.
  - Reloj: un mes de reseñas cada SEGUNDOS_POR_MES segundos (por defecto 10). Dentro del mes se
    reparten en PASOS envíos para que el flujo se vea continuo.
  - MAX_MENSAJES limita el total (0 = todas). PRUEBA=true marca los mensajes como prueba de humo
    (el streaming los clasifica pero no los usa para alertas ni ventanas); CORRIDA identifica el
    envío para poder contarlos en MongoDB.

  docker compose --profile demo run --rm producer
  docker compose --profile demo run --rm -e SEGUNDOS_POR_MES=5 producer
"""
import json
import os
import time

import pandas as pd
import pyarrow.dataset as ds
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA = os.environ.get("KAFKA", "kafka:9092")
TOPIC = os.environ.get("TOPIC", "reviews")
PARTICIONES = int(os.environ.get("PARTICIONES_TOPIC", "3"))
CORTE = os.environ.get("FECHA_CORTE", "2021-01-01")
HASTA = os.environ.get("FECHA_FIN", "")                  # opcional: publicar solo hasta esta fecha
SEG_MES = float(os.environ.get("SEGUNDOS_POR_MES", "10"))
PASOS = int(os.environ.get("PASOS_POR_MES", "10"))
MAX = int(os.environ.get("MAX_MENSAJES", "0"))
PRUEBA = os.environ.get("PRUEBA", "false").lower() in ("1", "true", "yes")
CORRIDA = os.environ.get("CORRIDA", f"demo-{int(time.time())}")
REVIEWS = os.path.join(os.environ.get("CLEAN_DIR", "/data/clean"), "reviews")
COLS = ["review_id", "business_id", "stars", "text", "date", "geohash", "categoria_principal"]


def log(m):
    print(f"[producer {time.strftime('%H:%M:%S')}] {m}", flush=True)


def crear_topic():
    admin = AdminClient({"bootstrap.servers": KAFKA})
    for intento in range(40):
        try:
            existentes = admin.list_topics(timeout=5).topics
            if TOPIC not in existentes:
                admin.create_topics([NewTopic(TOPIC, num_partitions=PARTICIONES, replication_factor=1)])[TOPIC].result()
                log(f"Topic '{TOPIC}' creado ({PARTICIONES} particiones)")
            else:
                log(f"Topic '{TOPIC}' ya existe ({len(existentes[TOPIC].partitions)} particiones)")
            return
        except Exception as e:  # noqa: BLE001  Kafka puede tardar en arrancar
            log(f"Esperando a Kafka ({type(e).__name__})... {intento + 1}/40")
            time.sleep(3)
    raise SystemExit("Kafka no respondió")


def cargar() -> pd.DataFrame:
    filtro = ds.field("date") >= pd.Timestamp(CORTE)
    if HASTA:
        filtro = filtro & (ds.field("date") < pd.Timestamp(HASTA))
    d = ds.dataset(REVIEWS, format="parquet").to_table(columns=COLS, filter=filtro).to_pandas()
    d = d.sort_values(["date", "review_id"], kind="stable").reset_index(drop=True)
    return d.head(MAX) if MAX else d


def main():
    d = cargar()
    if d.empty:
        raise SystemExit(f"No hay reseñas posteriores a {CORTE} en {REVIEWS}: ¿corrió la ingesta?")
    d["mes"] = d["date"].dt.strftime("%Y-%m")
    log(f"{len(d):,} reseñas desde {d['date'].min():%Y-%m-%d} hasta {d['date'].max():%Y-%m-%d} "
        f"(corte T = {CORTE}) | reloj: 1 mes cada {SEG_MES:g} s | prueba={PRUEBA} corrida={CORRIDA}")
    crear_topic()
    p = Producer({"bootstrap.servers": KAFKA, "linger.ms": 20, "compression.type": "lz4",
                  "queue.buffering.max.messages": 200000})
    total = 0
    for mes, g in d.groupby("mes", sort=True):
        t_mes = time.time()
        # trozos consecutivos: se conserva el orden de fecha dentro del mes
        n = 1 if MAX else min(PASOS, len(g))
        cortes = [round(i * len(g) / n) for i in range(n + 1)]
        trozos = [g.iloc[cortes[i]:cortes[i + 1]] for i in range(n)]
        for k, t in enumerate(trozos):
            t0 = time.time()
            for r in t.itertuples(index=False):
                msg = {"review_id": r.review_id, "business_id": r.business_id, "stars": int(r.stars), "text": r.text,
                       "date": r.date.strftime("%Y-%m-%dT%H:%M:%S"), "geohash": r.geohash,
                       "categoria_principal": r.categoria_principal, "prueba": PRUEBA, "corrida": CORRIDA}
                p.produce(TOPIC, key=r.business_id.encode(), value=json.dumps(msg, ensure_ascii=False).encode())
                p.poll(0)
            p.flush()
            total += len(t)
            if not MAX and k < len(trozos) - 1:
                time.sleep(max(0.0, SEG_MES / PASOS - (time.time() - t0)))
        neg = int((g["stars"] <= 2).sum())
        log(f"Mes {mes}: {len(g):,} reseñas publicadas ({neg / len(g):.0%} con 1-2 estrellas) en "
            f"{time.time() - t_mes:.1f} s | acumulado {total:,}")
        if not MAX:
            time.sleep(max(0.0, SEG_MES - (time.time() - t_mes)))
    log(f"Fin: {total:,} mensajes publicados en '{TOPIC}' (corrida {CORRIDA})")


if __name__ == "__main__":
    main()
