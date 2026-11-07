#!/usr/bin/env bash
# Envía un trabajo PySpark al clúster standalone (modo client): el driver corre en este
# contenedor y los executors del worker se conectan a él por su IP en la red de Compose.
set -euo pipefail
JOB="$1"; shift
DRIVER_IP="$(hostname -i | awk '{print $1}')"

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-spark://spark-master:7077}" \
  --conf spark.driver.host="${DRIVER_IP}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.memory="${SPARK_DRIVER_MEMORY:-1500m}" \
  --conf spark.driver.maxResultSize=512m \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-1500m}" \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.shuffle.partitions="${SPARK_SHUFFLE_PARTITIONS:-16}" \
  --conf spark.jars.ivy=/tmp/ivy \
  --py-files /opt/jobs/comun.py,/opt/jobs/geohash.py,/opt/jobs/textomodelo.py \
  "/opt/jobs/${JOB}.py" "$@"
