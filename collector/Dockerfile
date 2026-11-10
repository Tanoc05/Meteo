# ============================================================================
# DOCKERFILE: Collector Node.js (stdout JSON) + Logstash (pipe -> Kafka)
# ============================================================================

# 1. Preleva il binario Node.js 18 (compatibile con glibc 2.31 di Ubuntu 20.04)
FROM node:18-bullseye-slim AS nodebin

# 2. Immagine ufficiale Logstash 8.12.0 (con plugin pipe e kafka già inclusi)
FROM docker.elastic.co/logstash/logstash:8.12.0

USER root

# Copia l'eseguibile 'node' dentro Logstash così la pipe può eseguire collector.js
COPY --from=nodebin /usr/local/bin/node /usr/local/bin/node

# Copia gli script del Collector in /app e la configurazione pipeline in Logstash
WORKDIR /app
COPY package.json cities.json collector.js ./
COPY logstash.conf /usr/share/logstash/pipeline/logstash.conf

RUN chown -R logstash:logstash /app /usr/share/logstash/pipeline

USER logstash
