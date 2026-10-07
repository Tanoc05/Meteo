# ============================================================================
# MODULO: PySpark Structured Streaming Processor
# TECNOLOGIA: Apache Spark 3.5+ (PySpark) + Spark-Kafka + Elasticsearch
# SCOPO: Stream Processing in tempo reale per aggregazioni temporali, 
#        calcolo di allarmi ambientali OMS/UE e indicizzazione su Elasticsearch.
# ============================================================================

import os
import sys
import json
import logging
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, window, avg, max, min, count,
    when, current_timestamp, date_format, expr, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# Configurazione del Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SparkStreamProcessor")

# ============================================================================
# 1. LETTURA DELLE VARIABILI D'AMBIENTE (con valori di default)
# ============================================================================
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_TOPIC_TELEMETRY", "weather_pollution.telemetry")
KAFKA_ALERTS_TOPIC = os.getenv("KAFKA_TOPIC_ALERTS", "weather_pollution.alerts")
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ELASTICSEARCH_URL = f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"

# Soglie normative OMS (Organizzazione Mondiale della Sanità) ed Unione Europea
THRESHOLD_PM25_WHO = 25.0       # Limite 24h OMS per PM2.5 in µg/m³
THRESHOLD_PM10_WHO = 50.0       # Limite 24h OMS per PM10 in µg/m³
THRESHOLD_AQI_WARNING = 50.0    # Soglia indice europeo AQI (qualità moderata/scadente)

# ============================================================================
# 2. INIZIALIZZAZIONE DEGLI INDICI E MAPPING SU ELASTICSEARCH
# ============================================================================
def init_elasticsearch_indices():
    """
    Crea i template di mapping su Elasticsearch per garantire che il campo
    'location' venga interpretato come 'geo_point' per le mappe di Kibana.
    """
    logger.info(f"Verifica/Configurazione indici su Elasticsearch ({ELASTICSEARCH_URL})...")
    
    mapping_payload = {
        "mappings": {
            "properties": {
                "@timestamp": { "type": "date" },
                "city": { "type": "keyword" },
                "country": { "type": "keyword" },
                "location": { "type": "geo_point" },
                "temperature_c": { "type": "float" },
                "humidity_pct": { "type": "float" },
                "pressure_hpa": { "type": "float" },
                "wind_speed_kmh": { "type": "float" },
                "pm2_5": { "type": "float" },
                "pm10": { "type": "float" },
                "no2": { "type": "float" },
                "so2": { "type": "float" },
                "co": { "type": "float" },
                "o3": { "type": "float" },
                "european_aqi": { "type": "float" },
                "alert_level": { "type": "keyword" },
                "alert_reason": { "type": "text" }
            }
        }
    }

    indices = [
        "weather-pollution-telemetry",
        "weather-pollution-aggregations",
        "weather-pollution-alerts"
    ]

    for index_name in indices:
        url = f"{ELASTICSEARCH_URL}/{index_name}"
        try:
            res = requests.head(url, timeout=3)
            if res.status_code == 404:
                create_res = requests.put(url, json=mapping_payload, timeout=5)
                logger.info(f"Indice '{index_name}' creato con successo: {create_res.status_code}")
            else:
                logger.info(f"Indice '{index_name}' già presente su Elasticsearch.")
        except Exception as e:
            logger.warning(f"Impossibile pre-configurare l'indice '{index_name}': {e}")


# ============================================================================
# 3. CREAZIONE DELLA SPARK SESSION
# ============================================================================
def create_spark_session():
    """
    Inizializza la sessione PySpark con i connettori Maven necessari:
    - spark-sql-kafka-0-10 (per leggere e scrivere su Kafka in streaming)
    """
    logger.info("Inizializzazione SparkSession...")
    
    # Pacchetti Maven per l'integrazione di Kafka e formati binari in Spark
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ]

    spark = (
        SparkSession.builder
        .appName("WeatherPollutionStreamProcessor")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.shuffle.partitions", "3") # Partizioni ridotte per elaborazione locale veloce
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession creata con successo!")
    return spark


# ============================================================================
# 4. DEFINIZIONE DELLO SCHEMA DEI DATI IN INGRESSO (TELEMETRIA JSON)
# ============================================================================
telemetry_schema = StructType([
    StructField("@timestamp", TimestampType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("location", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ]), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("humidity_pct", DoubleType(), True),
    StructField("pressure_hpa", DoubleType(), True),
    StructField("wind_speed_kmh", DoubleType(), True),
    StructField("pm2_5", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("so2", DoubleType(), True),
    StructField("co", DoubleType(), True),
    StructField("o3", DoubleType(), True),
    StructField("european_aqi", DoubleType(), True)
])


# ============================================================================
# 5. SCRITTURA SU ELASTICSEARCH TRAMITE FOREACHBATCH (REST API)
# ============================================================================
def write_to_elasticsearch(batch_df, batch_id, index_name):
    """
    Funzione helper eseguita su ciascun micro-batch di Spark per inviare i dati
    a Elasticsearch tramite la Bulk API REST in modo resiliente e leggero.
    """
    if batch_df.isEmpty():
        return

    records = batch_df.toJSON().collect()
    if not records:
        return

    # Costruzione del payload per la Bulk API di Elasticsearch (formato NDJSON)
    bulk_payload = ""
    for record_json in records:
        bulk_payload += json.dumps({ "index": { "_index": index_name } }) + "\n"
        bulk_payload += record_json + "\n"

    try:
        url = f"{ELASTICSEARCH_URL}/_bulk"
        headers = { "Content-Type": "application/x-ndjson" }
        res = requests.post(url, data=bulk_payload, headers=headers, timeout=5)
        if res.status_code in [200, 201]:
            logger.info(f"[Batch {batch_id}] Inviati {len(records)} record a ES '{index_name}' (Status {res.status_code})")
        else:
            logger.error(f"[Batch {batch_id}] Errore invio a ES '{index_name}': {res.text}")
    except Exception as e:
        logger.error(f"[Batch {batch_id}] Eccezione durante l'invio a ES: {e}")


# ============================================================================
# 6. LOGICA PRINCIPALE DELLO STREAM PROCESSING
# ============================================================================
def main():
    # 1. Configurazione indici
    init_elasticsearch_indices()

    # 2. Creazione sessione Spark
    spark = create_spark_session()

    logger.info(f"Connessione allo stream Kafka: {KAFKA_BROKERS}, Topic: {KAFKA_INPUT_TOPIC}")

    # 3. Lettura dello Stream da Kafka
    raw_kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 4. Deserializzazione del valore del messaggio (da binario a stringa JSON)
    #    e parsing con lo schema tipizzato 'telemetry_schema'
    telemetry_stream = (
        raw_kafka_stream
        .selectExpr("CAST(value AS STRING) as json_payload")
        .select(from_json(col("json_payload"), telemetry_schema).alias("data"))
        .select("data.*")
        .filter(col("city").isNotNull())
    )

    # ------------------------------------------------------------------------
    # STREAM 1: Scrittura Telemetria Grezza Normalizzata su Elasticsearch
    # ------------------------------------------------------------------------
    telemetry_sink = (
        telemetry_stream.writeStream
        .foreachBatch(lambda df, epoch_id: write_to_elasticsearch(df, epoch_id, "weather-pollution-telemetry"))
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ------------------------------------------------------------------------
    # STREAM 2: Aggregazioni Temporali & Medie Mobili su Finestre (Windowing)
    # - Watermark di 2 minuti per gestire dati in ritardo
    # - Sliding Window di 5 minuti con avanzamento ogni 1 minuto
    # ------------------------------------------------------------------------
    aggregated_stream = (
        telemetry_stream
        .withWatermark("@timestamp", "2 minutes")
        .groupBy(
            window(col("@timestamp"), "5 minutes", "1 minute"),
            col("city"),
            col("country")
        )
        .agg(
            avg("pm2_5").alias("avg_pm2_5"),
            avg("pm10").alias("avg_pm10"),
            avg("temperature_c").alias("avg_temperature_c"),
            avg("humidity_pct").alias("avg_humidity_pct"),
            max("european_aqi").alias("max_european_aqi"),
            count(lit(1)).alias("sample_count")
        )
        .select(
            col("window.end").alias("@timestamp"),
            col("city"),
            col("country"),
            col("avg_pm2_5"),
            col("avg_pm10"),
            col("avg_temperature_c"),
            col("avg_humidity_pct"),
            col("max_european_aqi"),
            col("sample_count")
        )
    )

    aggregations_sink = (
        aggregated_stream.writeStream
        .foreachBatch(lambda df, epoch_id: write_to_elasticsearch(df, epoch_id, "weather-pollution-aggregations"))
        .outputMode("update")
        .trigger(processingTime="15 seconds")
        .start()
    )

    # ------------------------------------------------------------------------
    # STREAM 3: Rule-Based Environmental Alerts (Soglie OMS ed UE)
    # - Filtra eventi dove PM2.5 > 25 µg/m³, PM10 > 50 µg/m³ o AQI > 50
    # - Genera un nuovo allarme e lo invia a:
    #   1. Topic Kafka: weather_pollution.alerts
    #   2. Indice Elasticsearch: weather-pollution-alerts
    # ------------------------------------------------------------------------
    alerts_stream = (
        telemetry_stream
        .filter(
            (col("pm2_5") > THRESHOLD_PM25_WHO) |
            (col("pm10") > THRESHOLD_PM10_WHO) |
            (col("european_aqi") > THRESHOLD_AQI_WARNING)
        )
        .withColumn("alert_level", 
            when((col("pm2_5") > 50.0) | (col("pm10") > 100.0) | (col("european_aqi") > 80.0), "CRITICAL")
            .otherwise("WARNING")
        )
        .withColumn("alert_reason",
            when(col("pm2_5") > THRESHOLD_PM25_WHO, f"PM2.5 ha superato la soglia OMS di {THRESHOLD_PM25_WHO} µg/m³")
            .when(col("pm10") > THRESHOLD_PM10_WHO, f"PM10 ha superato la soglia OMS di {THRESHOLD_PM10_WHO} µg/m³")
            .otherwise("Indice Europeo AQI elevato")
        )
    )

    # 3a. Inoltro allarmi verso Elasticsearch
    alerts_es_sink = (
        alerts_stream.writeStream
        .foreachBatch(lambda df, epoch_id: write_to_elasticsearch(df, epoch_id, "weather-pollution-alerts"))
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # 3b. Inoltro allarmi verso il topic Kafka 'weather_pollution.alerts'
    kafka_alerts_output = (
        alerts_stream
        .select(
            col("city").alias("key"),
            to_json(struct(
                col("@timestamp"),
                col("city"),
                col("country"),
                col("location"),
                col("pm2_5"),
                col("pm10"),
                col("european_aqi"),
                col("alert_level"),
                col("alert_reason")
            )).alias("value")
        )
    )

    kafka_alerts_sink = (
        kafka_alerts_output.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("topic", KAFKA_ALERTS_TOPIC)
        .option("checkpointLocation", "/tmp/spark-checkpoints/alerts-kafka")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # ------------------------------------------------------------------------
    # STREAM 4: Console Output per debug immediato a terminale
    # ------------------------------------------------------------------------
    console_debug_sink = (
        alerts_stream.select(
            col("@timestamp"), col("city"), col("alert_level"), col("alert_reason"), col("pm2_5"), col("pm10")
        )
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info("Tutti gli stream PySpark sono avviati e attivi. In attesa di dati da Kafka...")
    
    # Mantiene l'applicazione attiva in ascolto di tutti gli stream
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

