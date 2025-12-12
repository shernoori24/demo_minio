FROM apache/spark-py:v3.4.0

USER root
WORKDIR /app

# ---------------------------------------------------------
# Copie du script Spark Streaming
# ---------------------------------------------------------
COPY spark-streaming.py .
COPY requirements.txt .

# ---------------------------------------------------------
# Copie du dossier entrypoint-initdb.d
# ---------------------------------------------------------
# COPY docker-entrypoint-initdb.d /docker-entrypoint-initdb.d

# ---------------------------------------------------------
# Copie du JAR Kafka
# ---------------------------------------------------------
COPY spark-sql-kafka-0-10_2.12-3.4.0.jar /opt/spark/jars/
COPY kafka-clients-3.4.0.jar /opt/spark/jars/

# ---------------------------------------------------------
# Dépendances Python
# ---------------------------------------------------------
RUN pip install kafka-python

# ---------------------------------------------------------
# MinIO credentials
# ---------------------------------------------------------
ENV AWS_ACCESS_KEY_ID=admin
ENV AWS_SECRET_ACCESS_KEY=admin123

# ---------------------------------------------------------
# Spark-submit
# ---------------------------------------------------------
CMD ["/opt/spark/bin/spark-submit", "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000", "--conf", "spark.hadoop.fs.s3a.access.key=admin", "--conf", "spark.hadoop.fs.s3a.secret.key=admin123", "--conf", "spark.hadoop.fs.s3a.path.style.access=true", "/app/spark-streaming.py"]
