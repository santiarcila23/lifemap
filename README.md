# LifeMap: reputación, ubicación y supervivencia de negocios

Proyecto final de Big Data (IUE, 2026-2), en dos entregas sobre el mismo sistema:

- **Entrega 1:** plataforma batch con el texto de las reseñas clasificado, la vida de cada negocio construida y las tasas de cierre calculadas con MapReduce, desplegada en una URL pública con Jenkins.
- **Entrega 2:** Kafka conectado a la plataforma; las reseñas posteriores a la fecha de corte se clasifican en tiempo real, se agregan en ventanas por celda para detectar *hotspots*, se estiman curvas de supervivencia y se emiten alertas de riesgo de cierre por ubicación.

**Equipo:** Santiago Arcila Gutiérrez, Alejandro Restrepo Uribe, Natalia Florez Pérez
**Docente:** Andrés Felipe Hernández Marulanda
**Área y familia:** Tampa (Florida), radio 45 km · Restaurantes

## URL pública

**https://strategy-chowtime-unified.ngrok-free.dev**

| Ruta | Qué muestra |
|---|---|
| `/` | Mapa: clic en un punto para ver negocios a 1 km y el resumen de la zona a 2 km |
| `/health` | Estado de MongoDB, Spark, Dask, la API y el modelo, con el **hash del commit desplegado** |
| `/reports` | Reportes de la ingesta, el MapReduce, el modelo, el balanceo, el riesgo y la supervivencia (JSON) |
| `/` → *En vivo* | Capas de alertas de riesgo y de celdas (hotspots) que se actualizan cada 10 s durante la demo |

## Arquitectura

```mermaid
flowchart LR
  K[(Kaggle<br/>Yelp Open Dataset)] -->|API de Kaggle| I[ingest<br/>cliente Dask]
  subgraph Dask
    DS[dask-scheduler] --- DW1[dask-worker 1] & DW2[dask-worker 2]
  end
  I --- DS
  I -->|GeoJSON + 2dsphere<br/>carga por lotes| M[(MongoDB 7)]
  I -->|Parquet| V[(volumen /data)]
  subgraph Spark
    SM[spark-master] --- SW[spark-worker]
  end
  V --> J1[MapReduce de tasas] & J2[Sentimiento<br/>NB y regresión logística] & J3[Balanceo]
  J1 & J2 & J3 --- SM
  J1 & J3 -->|conector MongoDB| M
  J2 -->|modelo| MO[(volumen /models)]
  M & MO --> A[API Flask]
  V -->|reseñas posteriores a T| P[producer<br/>reloj acelerado] -->|topic reviews<br/>3 particiones, clave business_id| KF[(Kafka KRaft)]
  KF --> ST[Spark Structured Streaming<br/>sentimiento + ventanas + riesgo] -->|reviews_stream, ventanas_celda,<br/>riesgo_negocios, alerts| M
  MO --> ST
  A --> PBI[Power BI]
  A --> T[ngrok<br/>URL pública]
  G[GitHub main] -->|pollSCM 2 min| JK[Jenkins] -->|build, pytest, deploy| A
```

Todo corre en un solo `docker-compose.yml` (proyecto `lifemap`):

| Servicio | Función | Puerto local |
|---|---|---|
| `mongo` | MongoDB 7: `businesses` (GeoJSON, 2dsphere), `reviews`, tasas y métricas | 127.0.0.1:27020 |
| `spark-master`, `spark-worker` | Spark 3.5.3 con MongoDB Spark Connector 10.4 | 8082 (UI) |
| `dask-scheduler`, `dask-worker` ×2 | Ingesta y limpieza distribuida | 8788 (UI) |
| `ingest` | Descarga de Kaggle, limpieza con Dask, vida de los negocios, carga a MongoDB | — |
| `spark-mapreduce`, `spark-sentimiento`, `spark-balanceo` | Trabajos de Spark | — |
| `kafka`, `kafka-init` | Kafka 3.8 en modo KRaft; `kafka-init` crea el topic `reviews` con 3 particiones | — |
| `spark-riesgo`, `spark-supervivencia` | Modelo de riesgo de cierre y curvas de Kaplan-Meier (Entrega 2) | — |
| `spark-streaming` | Structured Streaming: clasifica, agrega ventanas y emite alertas (siempre arriba) | — |
| `producer` | Publica en Kafka las reseñas posteriores a T con reloj acelerado (perfil `demo`) | — |
| `api` | API Flask (gunicorn) | 5050 |
| `tunnel` | ngrok: publica la API en la URL fija del equipo | 127.0.0.1:4040 |
| `jenkins` | CI/CD: construye, prueba y despliega al integrar a main | 8083 |

Los trabajos de ingesta y de Spark se ejecutan una vez y terminan. Si sus resultados ya existen, no se repiten (salvo `FORCE_INGEST=true` / `FORCE_SPARK=true`).

## Levantar el sistema desde cero

Requisitos del servidor: Docker Desktop con **al menos 10 GB de RAM** asignados y **25 GB libres** en disco (Yelp descomprimido pesa unos 9 GB), y Git.

```powershell
# 1. Clonar
git clone https://github.com/USUARIO/lifemap.git
cd lifemap

# 2. Variables y secretos (este archivo NO se sube a GitHub)
copy .env.example .env      # en Linux/Mac: cp .env.example .env
#    Editar .env: JENKINS_ADMIN_PASSWORD, GIT_REPO_URL, KAGGLE_USERNAME, KAGGLE_KEY,
#    NGROK_AUTHTOKEN y NGROK_DOMAIN

# 3. Levantar todo. La primera vez descarga Yelp y corre la ingesta y Spark (30 a 60 min)
docker compose up -d --build

# 4. Seguir el avance
docker compose logs -f ingest spark-mapreduce spark-sentimiento

# 5. Medición de balanceo (unos minutos)
docker compose --profile benchmark run --rm spark-balanceo

# 6. Publicar la API en la URL pública
docker compose --profile tunnel up -d tunnel

# 7. Demo de la Entrega 2: publicar en Kafka las reseñas posteriores a la fecha de corte
docker compose --profile demo run --rm producer
```

Comprobar: http://localhost:5050/health en el servidor y `https://strategy-chowtime-unified.ngrok-free.dev/health` desde cualquier lugar.

- **Kaggle:** kaggle.com → Settings → API → *Create New Token*. El archivo `kaggle.json` trae `username` y `key`.
- **ngrok:** cuenta gratis en dashboard.ngrok.com. *Your Authtoken* va en `NGROK_AUTHTOKEN` y el dominio gratis de *Domains* en `NGROK_DOMAIN`. El dominio es fijo: no cambia aunque se reinicie el servidor.
- **Área y categoría:** están en `docker-compose.yml` (`AREA_LAT`, `AREA_LON`, `AREA_RADIUS_KM`, `CATEGORY_FAMILY`) para que Jenkins use los mismos valores. Si se cambian, hay que correr de nuevo con `FORCE_INGEST=true` y `FORCE_SPARK=true`.

## Despliegue continuo (Jenkins)

Jenkins queda configurado como código (`jenkins/casc.yaml`): al arrancar ya tiene el usuario administrador, la credencial `kaggle-api` (tomada del `.env`, nunca del repositorio) y el job `lifemap`.

- **Disparo:** `pollSCM` cada 2 minutos sobre `main`. Jenkins corre en el servidor del equipo sin URL pública, así que GitHub no puede llamarlo por webhook; el enunciado acepta pollSCM documentado.
- **Etapas:** checkout → build de imágenes → **pytest unitarias** (incluye la fórmula de la tasa de cierre) → servicios → ingesta (solo si falta) → Spark (MapReduce y sentimiento, solo si faltan) → staging → **pruebas de humo contra la API** → despliegue → verificación de que `/health` muestra el commit de `main`.
- **Si una prueba falla, no se despliega:** la API pública sigue con la versión anterior.
- Jenkins usa el socket de Docker del servidor (`/var/run/docker.sock`) para construir imágenes y manejar los mismos contenedores.

Panel de Jenkins: http://localhost:8083 (usuario `admin` y la clave del `.env`).

## Endpoints

| Endpoint | Parámetros | Consulta |
|---|---|---|
| `GET /health` | — | Estado de cada servicio + commit |
| `GET /businesses/near` | `lat`, `lon`, `radio` (m, máx. 50.000), `categoria`?, `limit`? | `$near` |
| `GET\|POST /businesses/within` | Polígono GeoJSON en el cuerpo o `?poligono=`; `categoria`? | `$geoWithin` |
| `GET /zones/summary` | `lat`, `lon`, `radio`, `categoria`?, `min_dias`? | `$geoNear` + `$group`: sentimiento medio y tasa de cierre |
| `GET /stats/closure` | `por=categoria\|celda`, `min_dias` (por defecto 36.500), `orden`, `limit` | Tasas del MapReduce |
| `GET\|POST /predict` | `texto` | Sentimiento con el modelo entrenado en Spark |

Todos validan sus parámetros y responden **400** con un mensaje si algo no es válido.

```bash
curl "https://strategy-chowtime-unified.ngrok-free.dev/businesses/near?lat=27.9506&lon=-82.4572&radio=1000&categoria=Mexican"
curl -X POST "https://strategy-chowtime-unified.ngrok-free.dev/businesses/within" -H "Content-Type: application/json" \
     -d '{"type":"Polygon","coordinates":[[[-82.47,27.94],[-82.44,27.94],[-82.44,27.96],[-82.47,27.96],[-82.47,27.94]]]}'
curl "https://strategy-chowtime-unified.ngrok-free.dev/zones/summary?lat=27.9506&lon=-82.4572&radio=2000"
curl "https://strategy-chowtime-unified.ngrok-free.dev/stats/closure?por=celda&min_dias=36500"
curl -X POST "https://strategy-chowtime-unified.ngrok-free.dev/predict" -H "Content-Type: application/json" -d '{"texto":"Great tacos, friendly staff"}'
```

## Datos y definiciones

- **Dataset:** Yelp Open Dataset (Kaggle `yelp-dataset/yelp-dataset`), archivos `business.json` y `review.json`, descargados por el pipeline con la API de Kaggle.
- **Limpieza** (los conteos antes y después de cada regla están en `/reports/ingesta`):
  - Negocios: se descartan los que tienen coordenadas nulas, fuera de rango o fuera del área; los que no son de la familia; y los duplicados.
  - Reseñas: se descartan las de negocios fuera del área, sin texto, con estrellas o fechas inválidas, y las duplicadas por `review_id`.
- **Sentimiento:** 1–2 estrellas es negativo, 3 neutral y 4–5 positivo.
- **Vida de un negocio:** de su primera a su última reseña, en días-negocio.
  - **Evento:** `is_open = 0` (cierre observado).
  - **Censura:** `is_open = 1` (seguía abierto en la fecha de corte, la última reseña del área).
  - Todos tienen **entrada tardía**, porque el negocio ya existía antes de su primera reseña.
  - Es una aproximación: Yelp no trae fechas de apertura ni de cierre.
- **Tasa de cierre anualizada:** `cierres ÷ (días-negocio ÷ 365) × 100`, la misma fórmula de la AFR del ejercicio de discos.
  - **Mínimo para comparar grupos:** 36.500 días-negocio (100 años-negocio). Con esa exposición, un cierre más o menos mueve la tasa a lo sumo 1 punto.
- **Celdas:** geohash de 5 caracteres (unos 4,9 × 4,9 km).

## Modelo de sentimiento

- **Pipeline de SentimentStream:** limpieza → `RegexTokenizer` → `StopWordsRemover` (inglés) → `HashingTF` (65.536) → `IDF`.
- **Clasificadores:** Naive Bayes multinomial y regresión logística multinomial, ambos con pesos por clase para compensar el desbalance.
- **Partición por negocio:** `abs(hash(business_id)) % 10 < 8` es entrenamiento. Ningún negocio queda en los dos lados.
- **Modelos guardados:** en `/models/sentiment_nb`, `/models/sentiment_lr` y `/models/sentiment_v1` (el mejor por F1 macro, para la Entrega 2).
- **`/predict`:** no carga Spark. Replica en Python el mismo `HashingTF` (MurmurHash3, semilla 42) con los pesos exportados, y el trabajo de Spark verifica que ambas predicciones coinciden.

## Entrega 2: streaming, supervivencia y alerta temprana

### Fecha de corte y productor

- **T = 2021-01-01** (`FECHA_CORTE`). Yelp termina el 19 de enero de 2022, así que de T a T + 12 meses queda completo dentro de los datos (lo necesita la etiqueta de riesgo).
- El modelo de sentimiento (`sentiment_v1`) y el de riesgo se entrenan **solo con reseñas anteriores a T**. Si `FECHA_CORTE` cambia, los trabajos se repiten solos.
- `producer` publica las reseñas con fecha ≥ T **en orden de fecha**, con clave `business_id`, en el topic `reviews` (3 particiones). Velocidad: un mes de reseñas cada `SEGUNDOS_POR_MES` segundos (10 por defecto).

```powershell
docker compose --profile demo run --rm producer                              # todo el año, 1 mes cada 10 s
docker compose --profile demo run --rm -e SEGUNDOS_POR_MES=5 producer        # más rápido
docker compose --profile demo run --rm -e MAX_MENSAJES=500 producer          # solo 500 mensajes
```

### Streaming (`spark-streaming`)

- Lee `reviews` con Structured Streaming y **carga** `sentiment_v1` (no lo reentrena). Cada reseña queda en `reviews_stream` con su predicción, su probabilidad y la marca `procesado_en`.
- **Ventanas:** 30 días por celda de geohash sobre `date` (tiempo del evento), con **watermark de 7 días**. Cada ventana se guarda en `ventanas_celda` con el polígono de su celda (índice 2dsphere).
- **Hotspot:** el % de reseñas negativas sube **más de 15 puntos** frente a la ventana anterior de la misma celda, con **al menos 30 reseñas**. Con 30 reseñas y un 20 % negativo típico, el error estándar del porcentaje es √(0,2·0,8/30) ≈ 7,3 puntos; 15 puntos son unos 2 errores estándar, así que un salto así difícilmente es casualidad.
- **Alertas:** en cada micro-lote (`foreachBatch`) se actualizan las variables de los negocios que recibieron reseñas, se puntúan con `riesgo_v1` y los que superan el umbral quedan en `alerts` con geometría, probabilidad y fecha.
- Cada 5 s guarda en `stream_estado` los offsets procesados frente a los de Kafka: así `/health` muestra el **retraso del consumidor**.
- Repetir la demo desde T: `docker compose stop spark-streaming`, `docker compose --profile herramientas run --rm spark-reinicio`, `docker compose up -d spark-streaming`.

### Supervivencia (`spark-supervivencia`)

- Kaplan-Meier por **categoría principal** y por **zona** (polígonos de `zonas.py` consultados con `$geoWithin`).
- **Censura:** los negocios abiertos cuentan hasta su última reseña sin sumar un cierre. **Entrada tardía:** la observación empieza en `VENTANA_INICIO` (2016-01-01); quien tiene reseñas antes entra al riesgo con la edad que ya tenía, y quien terminó antes no se cuenta. Se incluye la curva ingenua (sin entrada tardía) para comparar.
- Se reportan la mediana de vida (si la curva baja de 0,5) y el número en riesgo a 0, 1, 2, 3, 5 y 10 años. Solo se comparan grupos con **al menos 100 negocios**.

### Modelo de riesgo de cierre (`spark-riesgo`)

- Variables por negocio con datos hasta T: sentimiento medio, tendencia del % negativo (últimos 6 meses frente a lo anterior), estrellas medias, log(reseñas), edad, días sin reseñas y **competidores de la misma categoría a 500 m** (`$geoNear`, solo los activos en T).
- **Etiqueta:** 1 si cerró y su última reseña cae entre T y T + 12 meses; 0 si siguió activo después o sigue abierto; se excluyen los negocios sin actividad después de T.
- Partición por negocio (80/20), pesos por clase, regresión logística frente a random forest. Se elige por **AUC-PR** y se reportan AUC-ROC, AUC-PR (frente a la tasa de cierres, que es su línea base) y recall/precisión en el umbral.
- Umbral: el mayor que alcanza un **recall de 0,70** en entrenamiento. Se prefiere avisar de más que dejar pasar cierres.

### Endpoints nuevos

| Endpoint | Parámetros | Consulta |
|---|---|---|
| `GET /live/hotspots` | `fecha`?, `lat`/`lon`/`radio`?, `solo_hotspots`?, `limit`? | Ventanas que contienen la fecha (por defecto la última) |
| `GET /live/evolucion` | `geohash`? | % negativo por ventana (para la gráfica de evolución) |
| `GET\|POST /alerts` | `lat`/`lon`/`radio` o polígono GeoJSON; `activas`?, `categoria`? | Alertas del streaming |
| `GET /survival/<categoria>` | `zona`? o `poligono`?, `entrada_tardia`?, `min_negocios`?, `comparar=categoria\|zona`? | Kaplan-Meier (`todas` para el total) |
| `GET /risk/near` | `lat`, `lon`, `radio`, `categoria`?, `limit`? | `$near` sobre `riesgo_negocios` |
| `GET /zonas` | — | Polígonos de las zonas |

`/health` incluye `kafka` (broker y consumidor: retraso, último lote y modelo cargado).

### Dashboard (Power BI)

Power BI Desktop, *Obtener datos → Web*, con las URL públicas:

- **Mapa de hotspots:** `/live/hotspots?limit=500`
- **Tabla de alertas activas:** `/alerts?limit=500`
- **Curvas de supervivencia:** `/survival/todas?comparar=zona` y `/survival/todas?comparar=categoria`
- **Evolución del % negativo:** `/live/evolucion`

Durante la demo, con el productor publicando, se pulsa **Actualizar**.

### Jenkins (Entrega 2)

Se agregan las etapas de riesgo, supervivencia y streaming, y dos pruebas que bloquean el despliegue si fallan:

- **Prueba del modelo versionado** (`api/tests/modelo`): el modelo guardado y el que cargó el streaming son los de `api/modelos_esperados.json` (versión y fecha de corte).
- **Humo del streaming:** publica 100 mensajes marcados como prueba y espera a que aparezcan clasificados en `reviews_stream`. Estos mensajes no se usan para alertas ni ventanas.

## Forma de trabajo en GitHub

Cada integrante trabaja en su rama y abre un pull request hacia `main`; al hacer merge, Jenkins despliega. Para mostrar que una prueba fallida bloquea el despliegue, se abrió un PR que rompe la prueba de la fórmula y quedó un build en rojo en el historial de Jenkins.

## Estructura

```
├── docker-compose.yml   Jenkinsfile   .env.example
├── ingest/              Dask: descarga, limpieza, vida, carga a MongoDB · producer.py (Kafka)
├── spark/jobs/          mapreduce.py · sentimiento.py · balanceo.py · riesgo.py · supervivencia.py
│                        streaming.py · reinicio.py · comun.py · riesgo_vars.py · km.py · zonas.py
├── api/                 Flask: app.py · queries.py · km.py · tests/unit · tests/modelo · tests/smoke · static/
└── jenkins/             imagen con Docker CLI + configuración como código
```

## Fuentes

- Yelp Open Dataset, vía Kaggle (`yelp-dataset/yelp-dataset`). Se usa según los términos de uso de Yelp.
- Se reutilizan piezas de trabajos anteriores del equipo: SentimentStream (pipeline de texto), KafkaMed (productor, inferencia en streaming y alerta temprana), el sistema geoespacial y el ejercicio de discos (MapReduce, ventanas con watermark y Kaplan-Meier con entrada tardía).
- Parte del código se desarrolló con apoyo de un asistente de IA (Claude, de Anthropic) y fue revisado, probado y adaptado por el equipo.
