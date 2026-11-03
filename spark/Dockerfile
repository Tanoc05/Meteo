FROM python:3.11-slim

# Installazione di JRE e utilità di sistema per Apache Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre-headless \
        curl \
        procps \
        netcat-traditional && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configurazione automatica JAVA_HOME dinamica
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Installazione dipendenze Python (pyspark, requests, numpy)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia del codice sorgente (sia stream processor che ML anomaly detector)
COPY . .

CMD ["python3", "spark_stream_processor.py"]
