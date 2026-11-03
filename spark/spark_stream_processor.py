# ============================================================================
# FASE 3: PYSPARK STREAM PROCESSOR (Versione Semplificata Unificata)
# SCOPO: Legge in tempo reale i messaggi JSON da Kafka e in un'unica funzione
#        (process_batch) esegue 3 operazioni:
#        - FASE A: Salva i dati grezzi (Telemetria) su Elasticsearch
#        - FASE B: Calcola le medie per ogni città (Aggregazioni)
#        - FASE C: Controlla le soglie OMS (PM2.5 > 25, PM10 > 50, AQI > 50)
#                  e invia gli allarmi sia su Elasticsearch sia su Kafka
# ============================================================================

import os       # Per leggere le variabili d'ambiente del container Docker
import json     # Per convertire le stringhe JSON in dizionari Python
import requests # Per inviare i dati a Elasticsearch tramite chiamate HTTP REST
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, avg, max, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ============================================================================
# 1. CONFIGURAZIONE DEGLI INDIRIZZI (KAFKA ED ELASTICSEARCH)
# ============================================================================
# Se gira in Docker usa gli indirizzi della rete interna (kafka:9092, elasticsearch:9200),
# altrimenti usa localhost per i test diretti sul Mac
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TELEMETRY", "weather_pollution.telemetry")
KAFKA_ALERTS_TOPIC = os.getenv("KAFKA_TOPIC_ALERTS", "weather_pollution.alerts")
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "localhost")
ES_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ES_URL = f"http://{ES_HOST}:{ES_PORT}"

# ============================================================================
# 2. SCHEMA DEL JSON INVIATO DAL COLLECTOR
# ============================================================================
# Kafka invia i messaggi come semplice testo (stringa JSON).
# Questo schema dice a Spark quali colonne creare nel DataFrame e di che tipo sono
# (StringType = testo, DoubleType = numero con la virgola).
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

# ============================================================================
# 3. FUNZIONE HELPER PER SALVARE UN DATAFRAME SPARK SU ELASTICSEARCH
# ============================================================================
def send_to_es(df, index_name):
    # Converte ogni riga della tabella Spark (DataFrame) in una stringa JSON
    # e le raccoglie tutte in una lista Python chiamata 'records'
    records = df.toJSON().collect()

    # Se la tabella è vuota (es. 0 allarmi), non fa nulla ed esce subito
    if not records:
        return

    # Scorre ogni riga JSON e la invia come documento a Elasticsearch
    for r in records:
        url = f"{ES_URL}/{index_name}/_doc" # Endpoint di Elasticsearch per inserire un documento
        documento = json.loads(r)           # Converte la stringa JSON in un dizionario Python
        requests.post(url, json=documento)  # Invia il documento con una chiamata HTTP POST

    print(f"✅ Inviati {len(records)} dati all'indice '{index_name}'")

# ============================================================================
# 4. CONFIGURAZIONE INIZIALE DI ELASTICSEARCH (MAPPING GEOSPAZIALE)
# ============================================================================
def init_es():
    # Dice a Elasticsearch che il campo 'location' contiene coordinate geografiche (geo_point)
    # così Kibana può posizionare automaticamente le città sulla mappa
    mapping = {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "location": {"type": "geo_point"}
            }
        }
    }
    # Applica questa configurazione a tutti e 3 gli indici prima di iniziare a scrivere i dati
    for idx in ["weather-pollution-telemetry", "weather-pollution-aggregations", "weather-pollution-alerts"]:
        try:
            requests.put(f"{ES_URL}/{idx}", json=mapping, timeout=3)
        except Exception:
            pass

# ============================================================================
# 5. IL CUORE DEL PROCESSOR: ESEGUE TUTTE LE 3 FASI A OGNI MICRO-BATCH
# ============================================================================
def process_batch(batch_df, batch_id):
    # Se negli ultimi 10 secondi non sono arrivati nuovi messaggi da Kafka, salta il turno
    if batch_df.isEmpty():
        return

    print(f"\n--- Elaborazione Blocco #{batch_id} ---")

    # ------------------------------------------------------------------------
    # FASE A: Salva i dati grezzi di telemetria così come sono arrivati
    # ------------------------------------------------------------------------
    send_to_es(batch_df, "weather-pollution-telemetry")

    # ------------------------------------------------------------------------
    # FASE B: Raggruppa per città e calcola le medie (Aggregazioni)
    # ------------------------------------------------------------------------
    medie_df = (
        batch_df.groupBy("city", "country") # Raggruppa le righe per nome della città
        .agg(
            avg("pm2_5").alias("avg_pm2_5"),                 # Media PM2.5
            avg("pm10").alias("avg_pm10"),                   # Media PM10
            avg("temperature_c").alias("avg_temperature_c"), # Media Temperatura
            max("european_aqi").alias("max_european_aqi")    # Valore massimo di AQI registrato
        )
        .withColumn("@timestamp", current_timestamp())       # Aggiunge l'orario del calcolo
    )
    # Salva la tabella delle medie nell'indice dedicato su Elasticsearch
    send_to_es(medie_df, "weather-pollution-aggregations")

    # ------------------------------------------------------------------------
    # FASE C: Controlla i limiti OMS (WHO) e genera gli Allarmi
    # ------------------------------------------------------------------------
    allarmi_df = (
        # Tiene solo le città che superano almeno una soglia di pericolo OMS
        batch_df.filter("pm2_5 > 25 OR pm10 > 50 OR european_aqi > 50")
        # Aggiunge la colonna 'alert_level': CRITICAL se PM2.5 > 50, altrimenti WARNING
        .withColumn("alert_level", when(col("pm2_5") > 50, "CRITICAL").otherwise("WARNING"))
        # Aggiunge la colonna 'alert_reason' con la spiegazione del superamento
        .withColumn("alert_reason", when(col("pm2_5") > 25, "Superata soglia OMS PM2.5 (>25)").otherwise("AQI o PM10 elevato"))
    )
    # 1. Salva gli allarmi su Elasticsearch nell'indice 'weather-pollution-alerts'
    send_to_es(allarmi_df, "weather-pollution-alerts")

    # 2. Se c'è almeno un allarme, lo invia anche al secondo topic Kafka 'weather_pollution.alerts'
    if not allarmi_df.isEmpty():
        (
            # Prepara il messaggio Kafka con 'key' = nome città e 'value' = tutta la riga convertita in JSON
            allarmi_df.select(col("city").alias("key"), to_json(struct("*")).alias("value"))
            .write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKERS)
            .option("topic", KAFKA_ALERTS_TOPIC)
            .save()
        )

# ============================================================================
# 6. AVVIO DI SPARK E ASCOLTO CONTINUO DI KAFKA
# ============================================================================
if __name__ == "__main__":
    # Prepara gli indici su Elasticsearch con il supporto per le mappe
    init_es()

    # Crea la sessione Spark usando tutti i core disponibili ("local[*]")
    # e scarica automaticamente il connettore ufficiale Spark-Kafka
    spark = (
        SparkSession.builder
        .appName("SimpleSparkProcessor")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    # Riduce i messaggi di log interni di Spark per mantenere pulito il terminale
    spark.sparkContext.setLogLevel("WARN")

    # Si collega in modalità Streaming al topic Kafka 'weather_pollution.telemetry'
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .load()
        # Prende il contenuto binario del messaggio Kafka ('value'), lo converte in stringa
        # e applica lo 'schema' per trasformarlo in colonne strutturate
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")           # Espande tutte le colonne del JSON nella tabella principale
        .filter("city IS NOT NULL") # Ignora eventuali messaggi vuoti o malformati
    )

    # Avvia lo streaming: ogni 10 secondi raccoglie i dati arrivati e li passa a 'process_batch'
    print("🚀 Spark Processor avviato! In ascolto di Kafka...")
    query = kafka_df.writeStream.foreachBatch(process_batch).trigger(processingTime="10 seconds").start()
    query.awaitTermination() # Mantiene il programma sempre acceso in ascolto
