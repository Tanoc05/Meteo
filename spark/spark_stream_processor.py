# ============================================================================
# MODULO: PySpark Structured Streaming Processor
# TECNOLOGIA: Apache Spark 3.5+ (PySpark) + Spark-Kafka + Elasticsearch
# SCOPO: Stream Processing in tempo reale per aggregazioni temporali, 
#        calcolo di allarmi ambientali OMS/UE e indicizzazione su Elasticsearch.
# ============================================================================

# 1. IMPORTAZIONE DEI MODULI PYTHON E PYSPARK
# 'os' e 'sys' per interagire con il sistema operativo, variabili d'ambiente e percorsi
import os
import sys

# 'json' per la serializzazione e deserializzazione di stringhe e documenti JSON
import json

# 'logging' per tracciare le operazioni, i messaggi informativi e gli errori
import logging

# 'requests' per effettuare chiamate HTTP REST verso Elasticsearch (creazione indici, Bulk API)
import requests

# 'SparkSession' è il punto di ingresso principale per lavorare con i DataFrame e lo Structured Streaming in Spark
from pyspark.sql import SparkSession

# Funzioni SQL integrate di PySpark per manipolare colonne, aggregare dati e gestire finestre temporali:
from pyspark.sql.functions import (
    col,               # Seleziona una colonna del DataFrame per nome
    from_json,         # Converte una stringa JSON in una colonna strutturata basata su uno schema
    to_json,           # Converte una struttura o colonna Spark in una stringa JSON
    struct,            # Raggruppa più colonne in un unico oggetto composito (Struct)
    window,            # Crea finestre temporali (Tumbling o Sliding) su una colonna timestamp
    avg,               # Calcola la media aritmetica di una colonna numerica
    max,               # Trova il valore massimo di una colonna
    min,               # Trova il valore minimo di una colonna
    count,             # Conta il numero di record/campioni
    when,              # Costrutto condizionale IF-THEN-ELSE per creare nuove colonne basate su regole
    current_timestamp, # Restituisce il timestamp corrente del sistema
    date_format,       # Formatta una data/ora secondo un pattern specifico
    expr,              # Valuta un'espressione SQL personalizzata
    lit                # Crea un valore scalare letterale (costante) utilizzabile nelle query Spark
)

# Tipi di dato di Spark SQL per definire lo schema fortemente tipizzato dei dati
from pyspark.sql.types import (
    StructType,        # Rappresenta un oggetto complesso contenente più campi
    StructField,       # Rappresenta un singolo campo dello StructType (nome, tipo, nullable)
    StringType,        # Tipo stringa di testo
    DoubleType,        # Tipo numerico a virgola mobile a doppia precisione (64-bit float)
    TimestampType      # Tipo data e ora con fuso orario (formato ISO 8601)
)

# ============================================================================
# 2. CONFIGURAZIONE DEL LOGGING
# ============================================================================
# Imposta il formato dei log a video con timestamp, livello (INFO/WARN/ERROR) e messaggio
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SparkStreamProcessor")

# ============================================================================
# 3. LETTURA DELLE VARIABILI D'AMBIENTE (con valori di fallback di default)
# ============================================================================
# Indirizzo del broker Kafka (default: 'localhost:9092', dentro Docker: 'kafka:9092')
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")

# Topic Kafka di input da cui Spark legge i dati prodotti dal Collector Node.js
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_TOPIC_TELEMETRY", "weather_pollution.telemetry")

# Topic Kafka di output su cui Spark pubblica gli allarmi ambientali in tempo reale
KAFKA_ALERTS_TOPIC = os.getenv("KAFKA_TOPIC_ALERTS", "weather_pollution.alerts")

# Host e porta di Elasticsearch per l'archiviazione e indicizzazione dei dati
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ELASTICSEARCH_URL = f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"

# SOGLIE CRITICHE NORMATIVE OMS (Organizzazione Mondiale della Sanità) ed Unione Europea:
THRESHOLD_PM25_WHO = 25.0       # Limite 24h OMS per PM2.5 in µg/m³ (particolato fine)
THRESHOLD_PM10_WHO = 50.0       # Limite 24h OMS per PM10 in µg/m³ (particolato inalabile)
THRESHOLD_AQI_WARNING = 50.0    # Soglia indice europeo AQI (qualità moderata/scadente)

# ============================================================================
# 4. INIZIALIZZAZIONE DEGLI INDICI E MAPPING SU ELASTICSEARCH
# ============================================================================
def init_elasticsearch_indices():
    """
    Invia le configurazioni di Mapping ad Elasticsearch prima dell'avvio degli stream.
    Questo passaggio è indispensabile affinché Elasticsearch riconosca il campo
    'location' come 'geo_point' abilitando le mappe geospaziali su Kibana.
    """
    logger.info(f"Verifica e configurazione indici su Elasticsearch ({ELASTICSEARCH_URL})...")
    
    # Definizione dello schema di mapping per Elasticsearch
    mapping_payload = {
        "mappings": {
            "properties": {
                "@timestamp": { "type": "date" },         # Timestamp temporale per serie storiche
                "city": { "type": "keyword" },            # Stringa non analizzata per filtri esatti
                "country": { "type": "keyword" },         # Codice nazione per aggregazioni
                "location": { "type": "geo_point" },      # Coordinate {lat, lon} per Kibana Maps
                "temperature_c": { "type": "float" },     # Temperatura in °C
                "humidity_pct": { "type": "float" },      # Umidità %
                "pressure_hpa": { "type": "float" },      # Pressione atmosferica
                "wind_speed_kmh": { "type": "float" },    # Velocità vento
                "pm2_5": { "type": "float" },             # Polveri PM2.5
                "pm10": { "type": "float" },              # Polveri PM10
                "no2": { "type": "float" },               # Biossido di azoto
                "so2": { "type": "float" },               # Biossido di zolfo
                "co": { "type": "float" },                # Monossido di carbonio
                "o3": { "type": "float" },                # Ozono
                "european_aqi": { "type": "float" },      # Indice AQI
                "alert_level": { "type": "keyword" },     # Livello allarme ('WARNING' o 'CRITICAL')
                "alert_reason": { "type": "text" }        # Messaggio descrittivo della violazione
            }
        }
    }

    # Elenco dei tre indici principali usati nel progetto
    indices = [
        "weather-pollution-telemetry",     # Telemetria grezza normalizzata
        "weather-pollution-aggregations",  # Medie mobili e statistiche su finestre temporali
        "weather-pollution-alerts"         # Allarmi e superamento soglie OMS/UE
    ]

    for index_name in indices:
        url = f"{ELASTICSEARCH_URL}/{index_name}"
        try:
            # Effettua una richiesta HEAD per verificare se l'indice esiste già
            res = requests.head(url, timeout=3)
            if res.status_code == 404:
                # Se non esiste (404 Not Found), lo crea applicando il mapping geo_point
                create_res = requests.put(url, json=mapping_payload, timeout=5)
                logger.info(f"Indice '{index_name}' creato con successo (Status: {create_res.status_code})")
            else:
                logger.info(f"Indice '{index_name}' già esistente su Elasticsearch.")
        except Exception as e:
            # Se Elasticsearch non è ancora raggiungibile, prosegue senza crashare
            logger.warning(f"Attenzione durante la configurazione dell'indice '{index_name}': {e}")


# ============================================================================
# 5. CREAZIONE E CONFIGURAZIONE DELLA SPARK SESSION
# ============================================================================
def create_spark_session():
    """
    Inizializza la sessione Spark distribuita scaricando il connettore Maven
    ufficiale per Apache Kafka.
    """
    logger.info("Inizializzazione della SparkSession in corso...")
    
    # Pacchetto Maven per l'integrazione di Kafka con Spark SQL Structured Streaming
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ]

    spark = (
        SparkSession.builder
        # Nome dell'applicazione visualizzato nella Spark UI (porta 4040)
        .appName("WeatherPollutionStreamProcessor")
        # Esegue Spark in locale sfruttando tutti i core CPU del computer (* = all cores)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        # Scarica e carica i JAR Maven necessari per Kafka
        .config("spark.jars.packages", ",".join(packages))
        # Elimina automaticamente checkpoint temporanei orfani al riavvio
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        # Riduce le partizioni di shuffle da 200 (default) a 3 per ottimizzare i tempi di calcolo in locale
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )

    # Imposta il livello di log interno di Spark a WARN per evitare stampe eccessive a console
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession inizializzata con successo!")
    return spark


# ============================================================================
# 6. DEFINIZIONE DELLO SCHEMA TIPPO DEL DATO TELEMETRICO
# ============================================================================
# Definiamo rigorosamente i tipi di dato attesi dal JSON inviato dal Collector Node.js.
# Questo permette a Spark di validare i campi, scartare dati corrotti e ottimizzare la memoria.
telemetry_schema = StructType([
    StructField("@timestamp", TimestampType(), True),  # Data e ora evento ISO 8601
    StructField("city", StringType(), True),           # Nome città (es: "Catania")
    StructField("country", StringType(), True),        # Nazione (es: "IT")
    # Oggetto nidificato per le coordinate geospaziali
    StructField("location", StructType([
        StructField("lat", DoubleType(), True),        # Latitudine decimale
        StructField("lon", DoubleType(), True)         # Longitudine decimale
    ]), True),
    StructField("temperature_c", DoubleType(), True),  # Temperatura in gradi Celsius
    StructField("humidity_pct", DoubleType(), True),   # Percentuale umidità
    StructField("pressure_hpa", DoubleType(), True),   # Pressione atmosferica
    StructField("wind_speed_kmh", DoubleType(), True), # Velocità del vento
    StructField("pm2_5", DoubleType(), True),          # Polveri sottili PM2.5 (µg/m³)
    StructField("pm10", DoubleType(), True),           # Polveri PM10 (µg/m³)
    StructField("no2", DoubleType(), True),            # Biossido di azoto (µg/m³)
    StructField("so2", DoubleType(), True),            # Biossido di zolfo (µg/m³)
    StructField("co", DoubleType(), True),             # Monossido di carbonio (µg/m³)
    StructField("o3", DoubleType(), True),             # Ozono (µg/m³)
    StructField("european_aqi", DoubleType(), True)    # Indice Europeo Qualità Aria
])


# ============================================================================
# 7. SCRITTURA SU ELASTICSEARCH TRAMITE FOREACHBATCH (REST BULK API)
# ============================================================================
def write_to_elasticsearch(batch_df, batch_id, index_name):
    """
    Funzione invocata da Spark su ciascun micro-batch temporale per inviare
    i dati ad Elasticsearch tramite la Bulk API REST.
    
    :param batch_df: DataFrame contenente i soli record del micro-batch corrente
    :param batch_id: ID sequenziale del micro-batch
    :param index_name: Nome dell'indice di destinazione in Elasticsearch
    """
    # Se il batch corrente è vuoto, non fa nulla
    if batch_df.isEmpty():
        return

    # Converte il DataFrame in un array di stringhe JSON native
    records = batch_df.toJSON().collect()
    if not records:
        return

    # Costruzione del payload nel formato NDJSON richiesto dalla Bulk API di Elasticsearch:
    # Per ogni documento servono due righe:
    # 1. Metadati di azione: {"index": {"_index": "nome-indice"}}
    # 2. Documento effettivo: {"@timestamp": ..., "city": "Catania", ...}
    bulk_payload = ""
    for record_json in records:
        bulk_payload += json.dumps({ "index": { "_index": index_name } }) + "\n"
        bulk_payload += record_json + "\n"

    try:
        url = f"{ELASTICSEARCH_URL}/_bulk"
        headers = { "Content-Type": "application/x-ndjson" }
        # Invio HTTP POST con tutti i record del micro-batch in un'unica chiamata di rete
        res = requests.post(url, data=bulk_payload, headers=headers, timeout=5)
        if res.status_code in [200, 201]:
            logger.info(f"[Batch {batch_id}] Inviati {len(records)} record su Elasticsearch '{index_name}' (Status: {res.status_code})")
        else:
            logger.error(f"[Batch {batch_id}] Errore durante l'invio a ES '{index_name}': {res.text}")
    except Exception as e:
        logger.error(f"[Batch {batch_id}] Eccezione di connessione con Elasticsearch: {e}")


# ============================================================================
# 8. FUNZIONE PRINCIPALE: FLUSSI DI ELABORAZIONE STREAMING
# ============================================================================
def main():
    # 1. Pre-configura gli indici su Elasticsearch
    init_elasticsearch_indices()

    # 2. Avvia la SparkSession
    spark = create_spark_session()

    logger.info(f"Connessione allo stream Kafka: {KAFKA_BROKERS}, Topic di ascolto: {KAFKA_INPUT_TOPIC}")

    # 3. LETTURA STREAMING CONTINUA DA KAFKA
    # 'spark.readStream' crea un DataFrame in streaming non limitato
    raw_kafka_stream = (
        spark.readStream
        .format("kafka")                                     # Usa il connettore Kafka
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)   # Indirizzo dei broker
        .option("subscribe", KAFKA_INPUT_TOPIC)              # Topic da cui leggere
        .option("startingOffsets", "latest")                 # Legge a partire dai messaggi più recenti
        .option("failOnDataLoss", "false")                   # Non fallire se un offset scade
        .load()
    )

    # 4. DESERIALIZZAZIONE E PARSING DEI MESSAGGI JSON
    # Kafka fornisce i dati in formato binario (colonna 'value').
    # Noi facciamo il CAST a stringa e poi usiamo 'from_json' per estrarre le colonne secondo lo schema.
    telemetry_stream = (
        raw_kafka_stream
        .selectExpr("CAST(value AS STRING) as json_payload")
        .select(from_json(col("json_payload"), telemetry_schema).alias("data"))
        .select("data.*")
        .filter(col("city").isNotNull()) # Filtra eventuali payload malformati privi di città
    )

    # ------------------------------------------------------------------------
    # STREAM 1: Scrittura Telemetria Grezza Normalizzata su Elasticsearch
    # Ogni volta che arriva un dato, lo indicizza su 'weather-pollution-telemetry'
    # ------------------------------------------------------------------------
    telemetry_sink = (
        telemetry_stream.writeStream
        .foreachBatch(lambda df, epoch_id: write_to_elasticsearch(df, epoch_id, "weather-pollution-telemetry"))
        .outputMode("append")                      # Modalità append: inserisce i nuovi record in coda
        .trigger(processingTime="10 seconds")      # Raccoglie i micro-batch ogni 10 secondi
        .start()
    )

    # ------------------------------------------------------------------------
    # STREAM 2: Aggregazioni Temporali & Medie Mobili su Finestre (Windowing)
    # - Watermarking di 2 minuti: gestisce record che arrivano in ritardo
    # - Sliding Window di 5 minuti con avanzamento (slide) ogni 1 minuto
    # - Calcola: Medie di PM2.5, PM10, Temperatura, Umidità e AQI Massimo per città
    # ------------------------------------------------------------------------
    aggregated_stream = (
        telemetry_stream
        # Imposta una soglia di ritardo massimo accettabile per i dati
        .withWatermark("@timestamp", "2 minutes")
        # Raggruppa per finestra temporale scorrevole e per città/nazione
        .groupBy(
            window(col("@timestamp"), "5 minutes", "1 minute"),
            col("city"),
            col("country")
        )
        # Calcola le metriche statistiche
        .agg(
            avg("pm2_5").alias("avg_pm2_5"),
            avg("pm10").alias("avg_pm10"),
            avg("temperature_c").alias("avg_temperature_c"),
            avg("humidity_pct").alias("avg_humidity_pct"),
            max("european_aqi").alias("max_european_aqi"),
            count(lit(1)).alias("sample_count")
        )
        # Formatta le colonne finali
        .select(
            col("window.end").alias("@timestamp"),  # Usa l'orario di chiusura della finestra come timestamp
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

    # Scrittura delle aggregazioni su Elasticsearch nell'indice 'weather-pollution-aggregations'
    aggregations_sink = (
        aggregated_stream.writeStream
        .foreachBatch(lambda df, epoch_id: write_to_elasticsearch(df, epoch_id, "weather-pollution-aggregations"))
        .outputMode("update")                      # Modalità update: aggiorna il valore aggregato della finestra
        .trigger(processingTime="15 seconds")      # Esegue l'aggregazione ogni 15 secondi
        .start()
    )

    # ------------------------------------------------------------------------
    # STREAM 3: Rule-Based Environmental Alerts (Soglie OMS ed UE)
    # Rileva automaticamente se una città supera i limiti di inquinamento
    # e genera un allarme classificato come 'WARNING' o 'CRITICAL'.
    # ------------------------------------------------------------------------
    alerts_stream = (
        telemetry_stream
        # Filtra solo gli eventi che superano almeno una soglia OMS o AQI
        .filter(
            (col("pm2_5") > THRESHOLD_PM25_WHO) |
            (col("pm10") > THRESHOLD_PM10_WHO) |
            (col("european_aqi") > THRESHOLD_AQI_WARNING)
        )
        # Calcola il livello di severità: CRITICAL se i valori sono doppi rispetto ai limiti, altrimenti WARNING
        .withColumn("alert_level", 
            when((col("pm2_5") > 50.0) | (col("pm10") > 100.0) | (col("european_aqi") > 80.0), "CRITICAL")
            .otherwise("WARNING")
        )
        # Assegna una motivazione leggibile per l'allarme
        .withColumn("alert_reason",
            when(col("pm2_5") > THRESHOLD_PM25_WHO, f"PM2.5 ha superato la soglia OMS di {THRESHOLD_PM25_WHO} µg/m³")
            .when(col("pm10") > THRESHOLD_PM10_WHO, f"PM10 ha superato la soglia OMS di {THRESHOLD_PM10_WHO} µg/m³")
            .otherwise("Indice Europeo AQI elevato")
        )
    )

    # 3a. Inoltro allarmi verso Elasticsearch ('weather-pollution-alerts') per visualizzazione su Kibana
    alerts_es_sink = (
        alerts_stream.writeStream
        .foreachBatch(lambda df, epoch_id: write_to_elasticsearch(df, epoch_id, "weather-pollution-alerts"))
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # 3b. Inoltro allarmi verso il topic Kafka 'weather_pollution.alerts' (per eventuali notifiche o downstream microservices)
    kafka_alerts_output = (
        alerts_stream
        .select(
            col("city").alias("key"), # Usa la città come chiave del messaggio Kafka
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
            )).alias("value")         # Converte l'allarme in stringa JSON come payload Kafka
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
    # STREAM 4: Console Output per monitoraggio e debug immediato a terminale
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

    logger.info("Tutti gli stream PySpark sono avviati e attivi. In ascolto continuo dei dati da Kafka...")
    
    # Mantiene vivo il processo Python finché tutti gli stream sono attivi
    spark.streams.awaitAnyTermination()


# Entrypoint dello script Python
if __name__ == "__main__":
    main()
