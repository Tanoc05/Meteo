# 📘 GUIDA ALL'ORALE D'ESAME: SPIEGAZIONE COMPLETA DELLA PIPELINE TAP

Questa guida raccoglie l'esposizione completa e corretta del progetto per l'esame di **Technologies for Advanced Programming (TAP)**.  
È pensata per essere ripetuta a voce in modo fluido, chiaro e tecnicamente ineccepibile davanti al Professore, coprendo l'intero flusso dati e i tre script centrali del progetto:
1. **Collector Node.js + Logstash** (`collector/collector.js` + `collector/logstash.conf`)
2. **PySpark Stream Processor** (`spark/spark_stream_processor.py`)
3. **PySpark MLlib Anomaly Detector** (`spark/spark_ml_anomaly_detector.py`)

---

## 🧭 Panoramica di Apertura (30-45 secondi)

> *"Il nostro progetto realizza una pipeline Big Data in tempo reale per il monitoraggio della qualità dell'aria e delle condizioni meteo su 12 importanti città italiane ed europee.*  
> *L'architettura è suddivisa in cinque macro-fasi:*
> 1. ***Data Ingestion***: uno script Node.js interroga le API REST di Open-Meteo e stampa i JSON su `stdout`; **Logstash** li raccoglie tramite una `pipe` e li inoltra a Kafka.
> 2. ***Message Brokering***: Apache Kafka disaccoppia chi produce i dati da chi li consuma, canalizzandoli in topic dedicati.
> 3. ***Stream Processing***: un primo consumer Apache Spark Structured Streaming calcola aggregazioni e allarmi basati sulle soglie OMS.
> 4. ***Machine Learning in Streaming***: un secondo consumer Spark MLlib esegue una pipeline K-Means (k=3) per classificare lo stato dell'aria e calcola una distanza euclidea rispetto al centroide per identificare anomalie complesse.
> 5. ***Storage & Visualizzazione***: Elasticsearch memorizza i documenti arricchiti e Kibana ne permette l'analisi su cruscotti grafici e mappe geospaziali.*  
> *Tutta l'infrastruttura è containerizzata e orchestrata tramite Docker Compose."*

---

## 1️⃣ FASE 1: Data Ingestion (`collector/collector.js` + `collector/logstash.conf`)

### 🎯 Ruolo del modulo
Separare in modo pulito il **recupero dati** dal **trasporto verso Kafka**:
- **`collector.js` (Node.js)**: scarica in parallelo meteo e qualità dell'aria da Open-Meteo ogni 30/60 secondi e stampa una riga JSON su `stdout`.
- **`logstash.conf` (Logstash)**: esegue `node /app/collector.js` tramite il plugin `input { pipe }`, pulisce i metadati tecnici e spedisce gli eventi al topic Kafka `weather_pollution.telemetry`.

```
[Open-Meteo APIs] ---> [collector.js (stdout JSON)] === pipe ===> [Logstash] ---> [Kafka: telemetry]
```

### 🗣️ Discorso da ripetere:
1. **Perché Node.js + Logstash con `pipe`?**  
   *"Abbiamo combinato il meglio dei due mondi: `collector.js` in Node.js è leggerissimo e ideale per eseguire in parallelo con `Promise.all` le chiamate HTTP verso le due API di Open-Meteo, ma **non contiene nessun codice legato a Kafka**. Si limita a stampare un JSON pulito per ogni città su `stdout`. È poi **Logstash** (parte ufficiale dello stack ELK) che legge lo streaming dalla `pipe`, lo normalizza e si occupa della consegna affidabile verso Apache Kafka."*

2. **Configurazione e Caricamento Città (`collector.js`, Righe 7-22):**  
   *"Importiamo solo `fs` per leggere dal disco `cities.json` (con `new URL("./cities.json", import.meta.url)`), impostiamo l'intervallo `INTERVAL_MS` tramite la variabile d'ambiente `POLL_INTERVAL_SEC` e trasformiamo l'elenco delle 12 città in un array JavaScript con `JSON.parse`."*

3. **Funzione `fetchCityData({ city, country, lat, lon })` (Righe 27-95):**  
   *"Per ciascuna città interroghiamo in parallelo con **`Promise.all([fetch(weatherUrl), fetch(airUrl)])`** i due endpoint Open-Meteo (Meteo + Qualità dell'Aria). Estraiamo i dati attuali dentro `.current` (`w = weatherJson.current || {}` e `a = airJson.current || {}`).  
   **Il blocco di Catch (Salvagente):** se Open-Meteo restituisce un errore HTTP 429 (rate limiting) o cade internet, il `catch` genera valori plausibili casuali così la pipeline non si blocca mai durante la demo.  
   La funzione restituisce un JSON piatto con `@timestamp`, città, nazione, `location: { lat, lon }` e tutti i parametri ambientali."*

4. **Funzione `collectAll()` e separazione `stdout` / `stderr` (Righe 100-115):**  
   *"Con `cities.map` e `Promise.all` raccogliamo tutte e 12 le città insieme.  
   Un dettaglio tecnico importante: i messaggi di log umani usano `console.error(...)` (canale `stderr`), mentre **`console.log(JSON.stringify(data))`** stampa **solo righe JSON pure** su `stdout`, così la pipe di Logstash riceve esclusivamente dati validi."*

5. **La configurazione di Logstash (`collector/logstash.conf`):**  
   *"Il file di configurazione Logstash è diviso nei 3 blocchi classici:*  
   - ***`input { pipe }`***: *avvia `node /app/collector.js` con `codec => json` (perché il plugin `pipe` divide già le righe ad ogni `\n`).*  
   - ***`filter { mutate }`***: *rimuove i campi di servizio aggiunti da Logstash (`@version`, `host`, `process`, `event`) lasciando intatto il nostro schema JSON.*  
   - ***`output { kafka }`***: *invia l'evento al broker `kafka:9092` sul topic `weather_pollution.telemetry`, usando `message_key => "%{city}"` per garantire il partizionamento ordinato per città."*

---

## 2️⃣ FASE 3: PySpark Stream Processor (`spark/spark_stream_processor.py`)

### 🎯 Ruolo dello script
Consumare i messaggi grezzi da Kafka, validarli tramite uno schema tabellare (`StructType`), calcolare aggregazioni aggregate per città (`groupBy`) e filtrare gli allarmi sanitari secondo le linee guida OMS, salvando i record su Elasticsearch e rilanciando gli alert su Kafka.

```
                    ┌---> [Elasticsearch: telemetry]
                    │
[Kafka: telemetry] -┴-> [PySpark Processor] 
                               │
                               ├---> groupBy() ---> [Elasticsearch: aggregations]
                               │
                               └---> filter(OMS) -> [Elasticsearch: alerts] & [Kafka: alerts]
```

### 🗣️ Discorso da ripetere:
1. **Configurazione e Inizializzazione (Righe 15-45):**  
   *"Importiamo i moduli di PySpark SQL e requests.  
   Configuriamo gli indirizzi e inviamo a Elasticsearch tramite `requests.put` una configurazione di **Mapping** per dire che il campo `location` è di tipo **`geo_point`**: questo passaggio è vitale, altrimenti Elasticsearch vedrebbe latitudine e longitudine come numeri scalari e Kibana non potrebbe disegnare la mappa.  
   Inizializziamo poi la **`SparkSession`** col driver master locale `local[*]` (usando tutti i core della macchina) e carichiamo il connettore Maven ufficiale `spark-sql-kafka-0-10`."*

2. **Schema e Lettura dello Stream (`kafka_df`, Righe 47-82):**  
   *"Poiché Kafka consegna a Spark un semplice flusso di byte/stringhe JSON, definiamo un **`StructType`** con i relativi `StructField` (`StringType` e `DoubleType`).  
   Tramite `spark.readStream.format("kafka")` apriamo il canale in lettura sul topic `weather_pollution.telemetry`.  
   Effettuiamo una `select` con `from_json(col("value").cast("string"), schema)` che deserializza la stringa nella tabella Spark (`DataFrame`), estraiamo tutte le colonne con `.select("data.*")` e scartiamo eventuali record nulli."*

3. **Funzione helper `send_to_es(df, index_name)` (Righe 84-98):**  
   *"Spark ed Elasticsearch parlano linguaggi diversi: Spark manipola tabelle (DataFrame), mentre Elasticsearch memorizza documenti JSON via API REST.  
   Questa funzione helper converte ogni riga della tabella Spark in una stringa JSON (`df.toJSON().collect()`), itera i record e con `requests.post(f"{ES_URL}/{index_name}/_doc", json=documento)` inserisce i singoli documenti nell'indice di destinazione."*

4. **Il cuore dello Streaming: `process_batch(batch_df, batch_id)` (Righe 100-142):**  
   *"Spark raccoglie i messaggi in micro-batch. All'interno di questa funzione eseguiamo in parallelo logico tre elaborazioni:*  
   - ***Fase A (Telemetria Grezza):*** *salviamo la tabella tale e quale nell'indice `weather-pollution-telemetry`.*  
   - ***Fase B (Aggregazioni per Città):*** *eseguiamo `batch_df.groupBy("city", "country").agg(...)` calcolando le medie di PM2.5, PM10, temperatura e il picco massimo di AQI (`max_european_aqi`). Aggiungiamo il timestamp di calcolo e salviamo in `weather-pollution-aggregations`.*  
   - ***Fase C (Allarmi Sanitari OMS):*** *applichiamo un filtro: `batch_df.filter("pm2_5 > 25 OR pm10 > 50 OR european_aqi > 50")`. Aggiungiamo la colonna `alert_level` ('CRITICAL' se PM2.5 supera 50, altrimenti 'WARNING') e la motivazione `alert_reason`. Salviamo questi allarmi su Elasticsearch (`weather-pollution-alerts`) e, se la tabella non è vuota, inoltriamo il JSON anche al topic Kafka `weather_pollution.alerts` per alimentare eventuali sistemi di notifica esterni."*

5. **Avvio dello Streaming (Righe 147-157):**  
   *"Colleghiamo l'uscita con `kafka_df.writeStream.foreachBatch(process_batch)`, impostiamo un trigger temporale a 10 secondi e blocchiamo il processo principale con `query.awaitTermination()` per mantenerlo in ascolto continuo."*

---

## 3️⃣ FASE 4: PySpark MLlib Anomaly Detector (`spark/spark_ml_anomaly_detector.py`)

### 🎯 Ruolo dello script
Andare oltre le soglie statiche OMS: analizzare contemporaneamente 7 variabili fisiche correlate (meteo + smog) usando un modello non supervisionato di Clustering (**K-Means**) e rilevare anomalie ambientali calcolando la distanza euclidea tra ogni osservazione e il proprio centroide.

```
                                  [Pipeline K-Means (k=3)]
                                             │
                                   .fit(train_df) all'avvio
                                             │
[Kafka: telemetry] ---> .transform(batch_df) ┼---> Assegna Cluster (0, 1, 2)
                                             │
                                             ├---> Calcola Anomaly Score (Distanza L2)
                                             │
                                             ├---> anomaly_score > 2.2? ---> is_anomaly = True
                                             │
                                             └---> [Elasticsearch: ml-alerts] & [Kafka: ml-alerts]
```

### 🗣️ Discorso da ripetere:
1. **Perché usare il Machine Learning e non solo regole IF/ELSE?**  
   *"Le soglie OMS guardano i singoli inquinanti isolati (es. PM2.5 > 25). Nel mondo reale, però, i fenomeni atmosferici sono correlati: ad esempio, con vento a 40 km/h è fisiologico che il particolato sia basso; se invece registriamo contemporaneamente vento fortissimo e particolato alle stelle (es. tempesta di sabbia sahariana o grandi incendi), le normali regole potrebbero non cogliere la stranezza del quadro complessivo. Il Machine Learning analizza **tutte e 7 le feature insieme**."*

2. **Le 7 Feature e il Dataset di Calibrazione (Righe 84-105):**  
   *"Definiamo la lista `FEATURE_COLS`: `temperature_c`, `humidity_pct`, `wind_speed_kmh`, `pm2_5`, `pm10`, `no2`, `european_aqi`.  
   All'avvio non abbiamo a disposizione un dataset storico su disco, quindi definiamo `training_data` con 9 profili di riferimento realistici suddivisi nei tre pattern meteorologici noti:  
   - Aria pulita con vento sostenuto;  
   - Inquinamento moderato urbano;  
   - Condizioni critiche di ristagno con alta umidità e assenza di vento.  
   Convertiamo la lista in un DataFrame Spark con `spark.createDataFrame(training_data, FEATURE_COLS)` ottenendo `train_df`."*

3. **I Tre Stadi della Pipeline Spark MLlib (Righe 108-120):**  
   *"Incateniamo tre passaggi fondamentali:*  
   - ***Stadio 1 (`VectorAssembler`):*** *prende le 7 colonne numeriche e le impacchetta in un singolo vettore di feature (`raw_features`), formato obbligatorio per gli algoritmi MLlib.*  
   - ***Stadio 2 (`StandardScaler`):*** *normalizza i dati impostando media zero e deviazione standard 1 (`withStd=True, withMean=True`). Senza questo passaggio, variabili con valori elevati come l'umidità (fino a 100) dominerebbero matematicamente su variabili a valori bassi come il vento (da 1 a 20).*  
   - ***Stadio 3 (`KMeans`):*** *imposta $k=3$ cluster, con un `seed=42` fisso per garantire la riproducibilità dei risultati, generando la colonna `ml_cluster`.*  
   
   *Includiamo gli stadi in `Pipeline(stages=[assembler, scaler, kmeans])` e chiamiamo **`pipeline.fit(train_df)`**. Questa operazione calcola i pesi dello scaler e fissa le coordinate dei 3 centri ottimali, restituendo il modello addestrato `trained_model`."*

4. **I Centroidi e la Distanza Euclidea (Righe 122-132):**  
   *"Estraiamo le coordinate dei 3 centroidi con `trained_model.stages[2].clusterCenters()`.  
   Definiamo una funzione Python `calcola_distanza(vec, cluster_id)` che, dato il vettore scalato della città e il cluster assegnato, calcola la norma vettoriale L2 (`np.linalg.norm`) rispetto al centroide di quel cluster. Registriamo questa funzione come `udf` (User Defined Function) in modo che PySpark possa applicarla come operazione colonnare distribuita."*

5. **Inferenza in Streaming: `process_batch` (Righe 137-176):**  
   *"Ad ogni micro-batch di 10 secondi:*  
   - *Eseguiamo `predictions_df = trained_model.transform(batch_df)`: il modello applica l'intera pipeline e aggiunge la colonna `ml_cluster` (0, 1 o 2).*  
   - *Calcoliamo l'**`anomaly_score`** applicando la nostra UDF di distanza euclidea.*  
   - *Impostiamo il flag **`is_anomaly = anomaly_score > 2.2`**: se la distanza geometrica dal centro del proprio cluster è superiore a 2.2 deviazioni standard, la città viene marcata come anomalia.*  
   - *Assegniamo l'etichetta leggibile `ml_cluster_label` ('Cluster 0: Aria Pulita', 'Cluster 1: Inquinamento Moderato', 'Cluster 2: Inquinamento Critico').*  
   - *Rimuoviamo le colonne vettoriali temporanee (`raw_features`, `features`) e salviamo su Elasticsearch nell'indice `weather-pollution-ml-alerts` e sul topic Kafka `weather_pollution.ml-alerts`."*

---

## 4️⃣ FASE 5: Storage & Dashboard Kibana (Sintesi per la Demo)

Quando apri il browser su `http://localhost:5601`:
1. **Mappa Geospaziale (in alto a sinistra):** visualizza i pallini delle 12 città colorati in base alle coordinate del campo `location` (`geo_point`), con tooltip interattivi contenenti temperatura, PM2.5 e AQI.
2. **KPI Live e Grafico a Colonne (in alto):** mostra i valori aggregati in diretta e il confronto dell'inquinamento per città.
3. **Ciambella K-Means (al centro):** visualizza in tempo reale la percentuale di città che ricadono nei 3 Cluster ambientali calcolati da Spark MLlib.
4. **Tabelle di Dettaglio (in basso):**
   - **Tabella Machine Learning:** mostra città per città il cluster assegnato, il valore numerico dell'`anomaly_score` e il flag booleano `is_anomaly`.
   - **Tabella Allarmi OMS:** elenca gli sforamenti dei limiti sanitari con il rispettivo livello di allerta (`WARNING` o `CRITICAL`).

---

## 💡 Tabella Riassuntiva "A colpo d'occhio"

| Componente | Linguaggio / Tech | Input | Output / Destinazione | Ruolo Principale |
| :--- | :--- | :--- | :--- | :--- |
| **Collector + Logstash** | Node.js + Logstash 8.12 | Open-Meteo REST API (`stdout` -> `pipe`) | Kafka (`telemetry`) | Ingestion asincrona, pulizia campi (`mutate`), invio a Kafka |
| **Kafka Broker** | Confluent Kafka 7.5 | Collector & Spark | Spark & Kafka-UI | Disaccoppiamento, buffering e persistenza a partizioni |
| **Stream Processor** | PySpark (Structured Streaming) | Kafka (`telemetry`) | Elasticsearch (`telemetry`, `aggregations`, `alerts`) + Kafka (`alerts`) | Schema mapping, medie aggregate per città, regole sanitarie OMS |
| **ML Anomaly Detector** | PySpark MLlib | Kafka (`telemetry`) | Elasticsearch (`ml-alerts`) + Kafka (`ml-alerts`) | Pipeline K-Means (k=3), scaling Z-score, Anomaly Score L2 |
| **Elasticsearch** | Elasticsearch 8.12 | PySpark HTTP POST | Kibana (REST Queries) | Storage distribuito NoSQL, indici con mapping geospaziale `geo_point` |
| **Kibana** | Kibana 8.12 | Elasticsearch Indici | Browser Web (`:5601`) | Dashboard unificata: mappa interattiva, KPI, grafici temporali e clustering |

