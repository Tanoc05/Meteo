# ============================================================================
# FASE 4: PYSPARK MLLIB ANOMALY DETECTOR & AIR QUALITY CLUSTERING
# TECNOLOGIA: Apache Spark 3.5 (Spark MLlib Pipeline: Assembler -> Scaler -> KMeans)
# SCOPO: 1. Raggruppa in tempo reale le città in 3 Cluster con l'algoritmo K-Means
#        2. Calcola l'Anomaly Score (distanza euclidea dal centroide del cluster)
#        3. Individua anomalie meteo-inquinamento e salva su Elasticsearch e Kafka
# ============================================================================

import os
import json
import requests
import numpy as np # Libreria matematica usata per calcolare la distanza euclidea tra vettori
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, udf, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# Import dei moduli ufficiali di Machine Learning di Spark (pyspark.ml)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

# ============================================================================
# 1. CONFIGURAZIONE DEGLI INDIRIZZI (KAFKA ED ELASTICSEARCH)
# ============================================================================
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:29092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_TOPIC_TELEMETRY", "weather_pollution.telemetry")
KAFKA_ML_TOPIC = os.getenv("KAFKA_TOPIC_ML_ALERTS", "weather_pollution.ml-alerts")
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "localhost")
ES_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ES_URL = f"http://{ES_HOST}:{ES_PORT}"

# ============================================================================
# 2. SCHEMA DEI DATI IN INGRESSO E DEFINIZIONE DELLE 7 FEATURE ML
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

# Le 7 variabili fisiche (meteo + inquinanti) che il modello ML analizza insieme
FEATURE_COLS = [
    "temperature_c", "humidity_pct", "wind_speed_kmh",
    "pm2_5", "pm10", "no2", "european_aqi"
]

# ============================================================================
# 3. ADDESTRAMENTO DELLA PIPELINE SPARK MLLIB (K-MEANS CLUSTERING)
# ============================================================================
def train_kmeans_pipeline(spark):
    """
    Costruisce e addestra una Pipeline Spark MLlib composta da 3 stadi:
    1. VectorAssembler: unisce le 7 colonne numeriche in un unico vettore 'raw_features'
    2. StandardScaler: normalizza i valori (media=0, deviazione standard=1)
    3. KMeans (k=3): raggruppa le condizioni atmosferiche in 3 Cluster
    """
    # Dataset di calibrazione iniziale rappresentativo dei 3 regimi atmosferici
    # Ordine colonne: (temperature_c, humidity_pct, wind_speed_kmh, pm2_5, pm10, no2, european_aqi)
    training_data = [
        # Cluster 0: Aria Pulita (vento sostenuto che disperde gli inquinanti)
        (22.0, 45.0, 18.0,  6.0, 11.0,  8.0, 20.0),
        (20.0, 50.0, 15.0,  8.0, 14.0, 10.0, 25.0),
        (19.0, 55.0, 14.0,  7.0, 12.0,  9.0, 22.0),
        # Cluster 1: Inquinamento Urbano Moderato (vento debole, polveri medie)
        (17.0, 70.0,  5.0, 18.0, 28.0, 24.0, 42.0),
        (16.0, 75.0,  4.0, 21.0, 32.0, 28.0, 46.0),
        (18.0, 68.0,  6.0, 19.0, 30.0, 25.0, 44.0),
        # Cluster 2: Inquinamento Elevato / Critico (aria ferma, alta umidità, alte polveri)
        (12.0, 85.0,  1.5, 42.0, 65.0, 48.0, 75.0),
        (10.0, 90.0,  1.0, 55.0, 80.0, 55.0, 85.0),
        (11.0, 88.0,  1.2, 48.0, 70.0, 50.0, 80.0)
    ]
    # Trasforma la lista Python in un DataFrame Spark per l'addestramento
    train_df = spark.createDataFrame(training_data, FEATURE_COLS)

    # STADIO 1: VectorAssembler
    # Gli algoritmi di Spark MLlib vogliono tutte le feature dentro una sola colonna di tipo Vettore.
    # handleInvalid="skip" salta automaticamente la riga se dovesse mancare un dato (null).
    assembler = VectorAssembler(
        inputCols=FEATURE_COLS,
        outputCol="raw_features",
        handleInvalid="skip"
    )

    # STADIO 2: StandardScaler
    # Poiché umidità (0-100) e vento (0-20) hanno scale diverse, normalizza tutte le variabili
    # con lo Z-score (media = 0, deviazione standard = 1) così nessuna variabile domina sulle altre.
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=True
    )

    # STADIO 3: K-Means Clustering (k=3)
    # Divide i dati in 3 gruppi (cluster 0, 1 o 2) e scrive il risultato nella colonna 'ml_cluster'
    kmeans = KMeans(
        k=3,
        seed=42,
        featuresCol="features",
        predictionCol="ml_cluster"
    )

    # Concatena i 3 stadi in una Pipeline unica e la addestra (.fit) sul dataset di calibrazione
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    return pipeline.fit(train_df)

# ============================================================================
# 4. INFERENZA ML IN TEMPO REALE SU OGNI BLOCCO DI DATI (FOREACHBATCH)
# ============================================================================
def process_ml_batch(batch_df, batch_id, model):
    # Se non ci sono dati nel micro-batch, esce subito
    if batch_df.isEmpty():
        return

    # 1. Passa i nuovi dati arrivati da Kafka dentro la Pipeline ML addestrata (.transform)
    # Questo aggiunge automaticamente le colonne 'raw_features', 'features' e 'ml_cluster'
    predictions_df = model.transform(batch_df)

    # 2. Estrae dal modello K-Means (ultimo stadio della pipeline) le coordinate dei 3 centroidi
    kmeans_stage = model.stages[-1]
    centers = [c.tolist() for c in kmeans_stage.clusterCenters()]

    # Funzione che calcola la distanza geometrica (Euclidea) tra i dati della città (vec)
    # e il centro del suo cluster di appartenenza (centers[cluster_id])
    def euclidean_distance(vec, cluster_id):
        if vec is None or cluster_id is None:
            return 0.0
        # Calcola la norma (distanza) tra il vettore della città e il centroide
        dist = np.linalg.norm(vec.toArray() - centers[int(cluster_id)])
        return round(float(dist), 3)

    # Registra la funzione Python come UDF (User Defined Function) utilizzabile sulle colonne Spark
    dist_udf = udf(euclidean_distance, DoubleType())

    # 3. Arricchisce la tabella con:
    #    - 'anomaly_score': quanto la città è distante dal comportamento normale del suo cluster
    #    - 'is_anomaly': True se lo score supera la soglia di anomalia (2.2)
    #    - 'ml_cluster_label': nome leggibile del cluster assegnato
    final_df = (
        predictions_df
        .withColumn("anomaly_score", dist_udf(col("features"), col("ml_cluster")))
        .withColumn("is_anomaly", col("anomaly_score") > 2.2)
        .withColumn("ml_cluster_label",
            when(col("ml_cluster") == 0, "Cluster 0: Aria Pulita")
            .when(col("ml_cluster") == 1, "Cluster 1: Inquinamento Moderato")
            .otherwise("Cluster 2: Inquinamento Critico")
        )
        # Rimuove i vettori matematici interni prima di salvare il JSON su Elasticsearch
        .drop("raw_features", "features")
    )

    # 4. Stampa nei log la tabella riassuntiva del Machine Learning (perfetta da mostrare all'esame!)
    print(f"\n🤖 --- RISULTATI MACHINE LEARNING (Batch #{batch_id}) ---")
    final_df.select("city", "pm2_5", "wind_speed_kmh", "ml_cluster_label", "anomaly_score", "is_anomaly").show(truncate=False)

    # 5. Salva ogni riga con le predizioni ML su Elasticsearch nell'indice 'weather-pollution-ml-alerts'
    records = final_df.toJSON().collect()
    for r in records:
        url = f"{ES_URL}/weather-pollution-ml-alerts/_doc"
        documento = json.loads(r)
        requests.post(url, json=documento)

    # 6. Invia i risultati anche al topic Kafka dedicato al Machine Learning ('weather_pollution.ml-alerts')
    try:
        (
            final_df
            .select(col("city").alias("key"), to_json(struct("*")).alias("value"))
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKERS)
            .option("topic", KAFKA_ML_TOPIC)
            .save()
        )
    except Exception as e:
        print(f"Errore invio a Kafka ML topic: {e}")

# ============================================================================
# 5. AVVIO DEL SERVIZIO DI MACHINE LEARNING
# ============================================================================
if __name__ == "__main__":
    # Configura l'indice Elasticsearch 'weather-pollution-ml-alerts' con il campo geo_point per la mappa
    try:
        requests.put(
            f"{ES_URL}/weather-pollution-ml-alerts",
            json={"mappings": {"properties": {"@timestamp": {"type": "date"}, "location": {"type": "geo_point"}}}},
            timeout=3
        )
    except Exception:
        pass

    # Avvia la sessione Spark
    spark = (
        SparkSession.builder
        .appName("SparkMLAnomalyDetector")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Addestra la Pipeline K-Means all'avvio del container
    print("🧠 Addestramento Pipeline Spark MLlib (K-Means)...")
    trained_model = train_kmeans_pipeline(spark)
    print("✅ Modello K-Means addestrato con successo!")

    # Si mette in ascolto del topic Kafka 'weather_pollution.telemetry'
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_INPUT_TOPIC)
        .load()
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .filter("city IS NOT NULL")
    )

    # Ogni 10 secondi esegue l'inferenza ML sui nuovi dati tramite 'process_ml_batch'
    print("🚀 Spark ML Anomaly Detector in ascolto su Kafka...")
    query = (
        kafka_stream.writeStream
        .foreachBatch(lambda df, bid: process_ml_batch(df, bid, trained_model))
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()
