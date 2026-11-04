# ============================================================================
# FASE 3: PYSPARK STREAM PROCESSOR
# SCOPO: 1. Legge i messaggi JSON da Kafka
#        2. Calcola le medie per città e controlla gli allarmi OMS
#        3. Salva tutto nel database Elasticsearch
# ============================================================================

import os
import json
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, avg, max, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ============================================================================
# 1. CONFIGURAZIONE (Indirizzi di Kafka ed Elasticsearch)
# ============================================================================
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TELEMETRY", "weather_pollution.telemetry")
KAFKA_ALERTS_TOPIC = os.getenv("KAFKA_TOPIC_ALERTS", "weather_pollution.alerts")
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "localhost")
ES_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ES_URL = f"http://{ES_HOST}:{ES_PORT}"

# ============================================================================
# 2. AVVIO DI SPARK E PREPARAZIONE MAPPA ELASTICSEARCH
# ============================================================================
# Dice a Elasticsearch che "location" è una coordinata GPS (geo_point) per la Mappa di Kibana
mapping = {"mappings": {"properties": {"@timestamp": {"type": "date"}, "location": {"type": "geo_point"}}}}
for idx in ["weather-pollution-telemetry", "weather-pollution-aggregations", "weather-pollution-alerts"]:
    try:
        requests.put(f"{ES_URL}/{idx}", json=mapping, timeout=3)
    except Exception:
        pass

# Accende il motore Apache Spark con il plugin per leggere da Kafka
spark = (
    SparkSession.builder
    .appName("SimpleSparkProcessor")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ============================================================================
# 3. LETTURA DEI MESSAGGI DA KAFKA (E TRASFORMAZIONE IN TABELLA)
# ============================================================================
# Elenco delle colonne che ci aspettiamo dentro il JSON inviato dal Collector
schema = StructType([
    StructField("@timestamp", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("location", StructType([
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType())
    ])),
    StructField("temperature_c", DoubleType()),
    StructField("humidity_pct", DoubleType()),
    StructField("pressure_hpa", DoubleType()),
    StructField("wind_speed_kmh", DoubleType()),
    StructField("pm2_5", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("no2", DoubleType()),
    StructField("so2", DoubleType()),
    StructField("co", DoubleType()),
    StructField("o3", DoubleType()),
    StructField("european_aqi", DoubleType())
])

# Spark si collega a Kafka, legge i JSON dal topic e li trasforma in una Tabella (DataFrame)
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", KAFKA_TOPIC)
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .filter("city IS NOT NULL")
)

# ============================================================================
# 4. FUNZIONE PER SALVARE UNA TABELLA SPARK SU ELASTICSEARCH
# ============================================================================
def send_to_es(df, index_name):
    records = df.toJSON().collect() # Trasforma le righe della tabella in una lista di JSON
    if not records:
        return

    # Invia ogni riga al database Elasticsearch tramite chiamata HTTP POST
    for r in records:
        url = f"{ES_URL}/{index_name}/_doc"
        documento = json.loads(r)
        requests.post(url, json=documento)

    print(f"✅ Inviati {len(records)} dati all'indice '{index_name}'")

# ============================================================================
# 5. ELABORAZIONE DEI DATI (LE 3 FASI ESEGUITE OGNI 10 SECONDI)
# ============================================================================
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    print(f"\n--- Elaborazione Blocco #{batch_id} ---")

    # FASE A: Salva i dati grezzi così come sono arrivati da Kafka
    send_to_es(batch_df, "weather-pollution-telemetry")

    # FASE B: Raggruppa per città e calcola le medie (PM2.5, PM10, Temperatura, AQI Max)
    medie_df = (
        batch_df.groupBy("city", "country")
        .agg(
            avg("pm2_5").alias("avg_pm2_5"),
            avg("pm10").alias("avg_pm10"),
            avg("temperature_c").alias("avg_temperature_c"),
            max("european_aqi").alias("max_european_aqi")
        )
        .withColumn("@timestamp", current_timestamp())
    )
    send_to_es(medie_df, "weather-pollution-aggregations")

    # FASE C: Filtra solo le città che superano i limiti OMS e crea gli allarmi
    allarmi_df = (
        batch_df.filter("pm2_5 > 25 OR pm10 > 50 OR european_aqi > 50")
        .withColumn("alert_level", when(col("pm2_5") > 50, "CRITICAL").otherwise("WARNING"))
        .withColumn("alert_reason", when(col("pm2_5") > 25, "Superata soglia OMS PM2.5 (>25)").otherwise("AQI o PM10 elevato"))
    )
    # Salva gli allarmi su Elasticsearch
    send_to_es(allarmi_df, "weather-pollution-alerts")

    # Invia gli allarmi anche al secondo topic di Kafka ('weather_pollution.alerts')
    if not allarmi_df.isEmpty():
        (
            allarmi_df.select(col("city").alias("key"), to_json(struct("*")).alias("value"))
            .write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKERS)
            .option("topic", KAFKA_ALERTS_TOPIC)
            .save()
        )

# ============================================================================
# 6. AVVIO DEL SERVIZIO (Come 'start()' nel Collector)
# ============================================================================
print("🚀 Spark Processor avviato! In ascolto di Kafka...")
query = kafka_df.writeStream.foreachBatch(process_batch).trigger(processingTime="10 seconds").start()
query.awaitTermination()
