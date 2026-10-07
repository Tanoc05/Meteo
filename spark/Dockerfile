FROM python:3.11-slim

# Installazione di OpenJDK e utilità necessarie per Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
        netcat-traditional && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configurazione JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Fallback per architetture x86/amd64 se non arm64
RUN if [ ! -d "$JAVA_HOME" ]; then export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))); fi

WORKDIR /app

# Installazione dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-scaricamento del package Spark-Kafka Maven cache per avvio immediato
RUN python3 -c "from pyspark.sql import SparkSession; SparkSession.builder.appName('warmup').config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1').getOrCreate().stop()" || true

# Copia del codice sorgente
COPY spark_stream_processor.py .

CMD ["python3", "spark_stream_processor.py"]
