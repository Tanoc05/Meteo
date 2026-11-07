# ============================================================================
# FASE 4: PYSPARK MLLIB ANOMALY DETECTOR (Machine Learning in Tempo Reale)
# SCOPO: 1. Legge gli stessi messaggi JSON da Kafka
#        2. Usa una Pipeline Spark MLlib (VectorAssembler -> StandardScaler -> KMeans)
#           per dividere le città in 3 Cluster (Aria Pulita, Moderata, Critica)
#        3. Calcola la distanza dal centro del cluster (anomaly_score) per trovare anomalie
#        4. Salva i risultati su Elasticsearch e Kafka
# ============================================================================

import os
import json
import requests
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, udf, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

# ============================================================================
# 1. CONFIGURAZIONE (Identica allo Stream Processor)
# ============================================================================
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TELEMETRY", "weather_pollution.telemetry")
KAFKA_ML_TOPIC = os.getenv("KAFKA_TOPIC_ML_ALERTS", "weather_pollution.ml-alerts")
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "localhost")
ES_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ES_URL = f"http://{ES_HOST}:{ES_PORT}"

# ============================================================================
# 2. AVVIO DI SPARK E PREPARAZIONE MAPPA ELASTICSEARCH (Identico allo Stream Processor)
# ============================================================================
mapping = {"mappings": {"properties": {"@timestamp": {"type": "date"}, "location": {"type": "geo_point"}}}}
try:
    requests.put(f"{ES_URL}/weather-pollution-ml-alerts", json=mapping, timeout=3)
except Exception:
    pass

spark = (
    SparkSession.builder
    .appName("SparkMLAnomalyDetector")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ============================================================================
# 3. LETTURA DEI MESSAGGI DA KAFKA (Identica allo Stream Processor)
# ============================================================================
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
    StructField("wind_speed_kmh", DoubleType()),
    StructField("pm2_5", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("no2", DoubleType()),
    StructField("european_aqi", DoubleType())
])

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
# 4. ADDESTRAMENTO DELLA PIPELINE DI MACHINE LEARNING (K-MEANS CLUSTERING)
# ============================================================================
# Le 7 colonne numeriche (meteo + smog) che il Machine Learning analizza insieme
FEATURE_COLS = [
    "temperature_c", "humidity_pct", "wind_speed_kmh",
    "pm2_5", "pm10", "no2", "european_aqi"
]

# Esempi di riferimento per insegnare a K-Means i 3 tipi di aria
training_data = [
    # Cluster 0: Aria Pulita (vento forte, smog basso)
    (22.0, 45.0, 18.0,  6.0, 11.0,  8.0, 20.0),
    (20.0, 50.0, 15.0,  8.0, 14.0, 10.0, 25.0),
    (19.0, 55.0, 14.0,  7.0, 12.0,  9.0, 22.0),
    # Cluster 1: Inquinamento Moderato (vento debole, smog medio)
    (17.0, 70.0,  5.0, 18.0, 28.0, 24.0, 42.0),
    (16.0, 75.0,  4.0, 21.0, 32.0, 28.0, 46.0),
    (18.0, 68.0,  6.0, 19.0, 30.0, 25.0, 44.0),
    # Cluster 2: Inquinamento Critico (aria ferma, smog molto alto)
    (12.0, 85.0,  1.5, 42.0, 65.0, 48.0, 75.0),
    (10.0, 90.0,  1.0, 55.0, 80.0, 55.0, 85.0),
    (11.0, 88.0,  1.2, 48.0, 70.0, 50.0, 80.0)
]
train_df = spark.createDataFrame(training_data, FEATURE_COLS)

# STADIO 1: Unisce le 7 colonne in una sola colonna vettore chiamata 'raw_features'
assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features", handleInvalid="skip")

# STADIO 2: Normalizza i numeri sulla stessa scala (così l'umidità 0-100 non schiaccia il vento 0-20)
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)

# STADIO 3: Algoritmo K-Means che divide le città in 3 gruppi (k=3) nella colonna 'ml_cluster'
kmeans = KMeans(k=3, seed=42, featuresCol="features", predictionCol="ml_cluster")

# Unisce i 3 stadi nella Pipeline e addestra il modello (.fit)
print("🧠 Addestramento Pipeline Spark MLlib (K-Means)...")
pipeline = Pipeline(stages=[assembler, scaler, kmeans])
trained_model = pipeline.fit(train_df)
print("✅ Modello K-Means addestrato con successo!")

# Salva le coordinate dei 3 centri (centroidi) calcolati dallo stadio 2 (kmeans)
centers = trained_model.stages[2].clusterCenters()

# Funzione che calcola la distanza (Anomaly Score) tra una città e il centro del suo cluster
def calcola_distanza(vec, cluster_id):
    if vec is None or cluster_id is None:
        return 0.0
    dist = np.linalg.norm(vec.toArray() - centers[int(cluster_id)])
    return round(float(dist), 3)

dist_udf = udf(calcola_distanza, DoubleType())

# ============================================================================
# 5. ELABORAZIONE MACHINE LEARNING SU OGNI BLOCCO DI DATI
# ============================================================================
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # FASE A: Passa le città dentro il modello ML per assegnare il Cluster (0, 1 o 2)
    predictions_df = trained_model.transform(batch_df)

    # FASE B: Calcola l'Anomaly Score, segna se è anomalia (> 2.2) e dà il nome al Cluster
    final_df = (
        predictions_df
        .withColumn("anomaly_score", dist_udf(col("features"), col("ml_cluster")))
        .withColumn("is_anomaly", col("anomaly_score") > 2.2)
        .withColumn("ml_cluster_label",
            when(col("ml_cluster") == 0, "Cluster 0: Aria Pulita")
            .when(col("ml_cluster") == 1, "Cluster 1: Inquinamento Moderato")
            .otherwise("Cluster 2: Inquinamento Critico")
        )
        .drop("raw_features", "features")
    )

    # Mostra la tabella ML nel terminale
    print(f"\n🤖 --- RISULTATI MACHINE LEARNING (Blocco #{batch_id}) ---")
    final_df.select("city", "pm2_5", "wind_speed_kmh", "ml_cluster_label", "anomaly_score", "is_anomaly").show(truncate=False)

    # FASE C: Salva i risultati su Elasticsearch ('weather-pollution-ml-alerts')
    records = final_df.toJSON().collect()
    for r in records:
        url = f"{ES_URL}/weather-pollution-ml-alerts/_doc"
        documento = json.loads(r)
        requests.post(url, json=documento)

    # Invia i risultati anche al topic Kafka del Machine Learning ('weather_pollution.ml-alerts')
    (
        final_df.select(col("city").alias("key"), to_json(struct("*")).alias("value"))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("topic", KAFKA_ML_TOPIC)
        .save()
    )

# ============================================================================
# 6. AVVIO DEL SERVIZIO (Identico allo Stream Processor)
# ============================================================================
print("🚀 Spark ML Anomaly Detector in ascolto su Kafka...")

query = (
    kafka_df.writeStream
    .foreachBatch(process_batch)          # Passa ogni nuovo blocco di dati alla funzione 'process_batch'
    .trigger(processingTime="10 seconds") # Esegue il controllo ogni 10 secondi
    .start()                              # Avvia lo streaming in background
)

# Mantiene il programma sempre acceso in ascolto infinito
query.awaitTermination()
