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
# Kafka package will be downloaded by Spark (let Spark handle versions)
# ---------------------------------------------------------

# ---------------------------------------------------------
# Hadoop S3A dependencies
# ---------------------------------------------------------
# Add Hadoop AWS and AWS SDK to enable s3a:// filesystem (S3/MinIO)
RUN apt-get update && apt-get install -y curl ca-certificates && \
    curl -L -o /opt/spark/jars/hadoop-aws-3.3.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.375.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.375/aws-java-sdk-bundle-1.12.375.jar

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
CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0", "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000", "--conf", "spark.hadoop.fs.s3a.access.key=admin", "--conf", "spark.hadoop.fs.s3a.secret.key=admin123", "--conf", "spark.hadoop.fs.s3a.path.style.access=true", "/app/spark-streaming.py"]
