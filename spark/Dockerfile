# Spark 3.5 + MongoDB Spark Connector 10.x + conector de Kafka (Structured Streaming)
# + numpy y pandas (los usa Spark MLlib en Python) + pymongo (streaming, riesgo y supervivencia)
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# El conector (y sus dependencias: driver de MongoDB y BSON) se resuelve al construir la
# imagen y se copia a /opt/spark/jars: master, worker y driver tienen los mismos jars.
ARG PAQUETES=org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3
RUN echo "" > /tmp/noop.py \
 && /opt/spark/bin/spark-submit --master "local[1]" \
      --conf spark.jars.ivy=/tmp/ivy \
      --packages "${PAQUETES}" /tmp/noop.py \
 && cp /tmp/ivy/jars/*.jar /opt/spark/jars/ \
 && ls /opt/spark/jars | grep -i -E "mongo|bson|kafka" \
 && rm -rf /tmp/ivy /tmp/noop.py

RUN (command -v pip3 >/dev/null || (apt-get update && apt-get install -y --no-install-recommends python3-pip \
      && rm -rf /var/lib/apt/lists/*)) \
 && (pip3 install --no-cache-dir numpy==1.26.4 pandas==2.0.3 pymongo==4.10.1 \
     || pip3 install --no-cache-dir --break-system-packages numpy==1.26.4 pandas==2.0.3 pymongo==4.10.1)

COPY jobs /opt/jobs
RUN chmod +x /opt/jobs/*.sh && mkdir -p /data /models /data/checkpoints
WORKDIR /opt/jobs
