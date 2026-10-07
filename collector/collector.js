// ============================================================================
// MODULO: Real-Time Weather & Air Quality Collector
// TECNOLOGIA: Node.js (JavaScript ES Modules) + KafkaJS
// SCOPO: Ingestion dei dati meteo e qualità dell'aria da Open-Meteo API e
//        pubblicazione su Apache Kafka per la pipeline Big Data (TAP).
// ============================================================================

// 1. IMPORTAZIONE DEI MODULI
// 'kafkajs' è il client ufficiale ed efficiente per interagire con Apache Kafka in Node.js.
// 'logLevel' serve a definire il livello di verbosità dei log interni di Kafka.
import { Kafka, logLevel } from "kafkajs";

// 'fs' (File System) è il modulo nativo di Node.js per leggere e scrivere file su disco.
import fs from "fs";

// 'path' è il modulo nativo per manipolare e risolvere i percorsi dei file in modo compatibile cross-platform.
import path from "path";

// 'fileURLToPath' serve a convertire l'URL del modulo ES corrente in un percorso assoluto su filesystem.
import { fileURLToPath } from "url";

// 'dotenv' carica automaticamente le variabili d'ambiente definite nel file .env dentro 'process.env'.
import dotenv from "dotenv";

// Inizializza la lettura del file .env se presente nella cartella
dotenv.config();

// In Node.js con ES Modules (import/export), '__filename' e '__dirname' non esistono di default come in CommonJS (require).
// Li ricreiamo sfruttando 'import.meta.url':
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ============================================================================
// 2. CONFIGURAZIONE DA VARIABILI D'AMBIENTE (con valori di default di fallback)
// ============================================================================

// Indirizzo dei broker Kafka. Se non specificato, usa 'localhost:9092'.
// Permette di passare una lista separata da virgola (es: "kafka1:9092,kafka2:9092").
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");

// Identificativo client per Kafka, utile nei log e per il monitoraggio del broker.
const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || "weather-pollution-collector";

// Nome del topic Kafka principale su cui inviare i dati di telemetria normalizzati.
const TELEMETRY_TOPIC = process.env.KAFKA_TOPIC_TELEMETRY || "weather_pollution.telemetry";

// Nome del topic Dead Letter Queue (DLQ) per inviare payload non validi o errori di fetch.
const DLQ_TOPIC = process.env.KAFKA_TOPIC_DLQ || "weather_pollution.dlq";

// Intervallo di polling espresso in millisecondi (default: 30 secondi).
const POLL_INTERVAL_MS = (parseInt(process.env.POLL_INTERVAL_SEC, 10) || 30) * 1000;

// Flag DRY_RUN: se impostato a 'true', stampa solo i dati su console senza connettersi a Kafka.
// Utile per testare la corretta raccolta dati in locale senza avviare i container Docker.
const DRY_RUN = process.env.DRY_RUN === "true";

// Percorso del file JSON contenente l'elenco delle città e coordinate da monitorare.
const CITIES_PATH = process.env.CITIES_FILE || path.join(__dirname, "cities.json");

// ============================================================================
// 3. CARICAMENTO DEL FILE DELLE CITTÀ (cities.json)
// ============================================================================
let cities = [];
try {
  // Legge il file sincronicamente all'avvio dell'applicazione
  const citiesData = fs.readFileSync(CITIES_PATH, "utf-8");
  // Converte il testo JSON in un array JavaScript di oggetti
  cities = JSON.parse(citiesData);
  console.log(`[Collector] Caricate ${cities.length} città da ${CITIES_PATH}`);
} catch (err) {
  // Se il file non esiste o ha sintassi errata, interrompe l'esecuzione con codice di errore 1
  console.error(`[Collector] Errore critico nel caricamento delle città da ${CITIES_PATH}:`, err.message);
  process.exit(1);
}

// ============================================================================
// 4. INIZIALIZZAZIONE DEL CLIENT E PRODUCER KAFKA
// ============================================================================
let producer = null;

if (!DRY_RUN) {
  // Istanziazione del client Kafka con configurazioni di connessione e retry
  const kafka = new Kafka({
    clientId: KAFKA_CLIENT_ID,
    brokers: KAFKA_BROKERS,
    logLevel: logLevel.WARN, // Mostra solo Warning ed Errori per non intasare i log
    retry: {
      initialRetryTime: 300, // Tempo iniziale di attesa prima di riprovare la connessione (in ms)
      retries: 10,           // Numero massimo di tentativi di riconnessione automatica
    },
  });

  // Creazione dell'istanza del Producer
  producer = kafka.producer();
}

// ============================================================================
// 5. FUNZIONE DI INTERROGAZIONE API E NORMALIZZAZIONE DATI
// ============================================================================
/**
 * Interroga le API Open-Meteo per una determinata città e unisce meteo + qualità aria.
 * 
 * @param {Object} cityInfo - Oggetto contenente { city, country, lat, lon }
 * @returns {Promise<Object>} - Payload JSON normalizzato pronto per Kafka ed Elasticsearch
 */

async function fetchCityData(cityInfo) {

  const { city, country, lat, lon } = cityInfo;

  // 1. Endpoint Open-Meteo per i parametri Meteorologici attuali:
  //    - temperature_2m: Temperatura a 2 metri dal suolo (°C)
  //    - relative_humidity_2m: Umidità relativa (%)
  //    - surface_pressure: Pressione atmosferica al suolo (hPa)
  //    - wind_speed_10m: Velocità del vento a 10 metri dal suolo (km/h)
  const weatherUrl = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&current=temperature_2m,relative_humidity_2m,surface_pressure,wind_speed_10m`;

  // 2. Endpoint Open-Meteo per i parametri di Qualità dell'Aria / Inquinamento:
  //    - pm10: Particolato fine inferiore a 10 µm (µg/m³)
  //    - pm2_5: Particolato ultra-fine inferiore a 2.5 µm (µg/m³)
  //    - carbon_monoxide: Monossido di carbonio CO (µg/m³)
  //    - nitrogen_dioxide: Biossido di azoto NO2 (µg/m³)
  //    - sulphur_dioxide: Biossido di zolfo SO2 (µg/m³)
  //    - ozone: Ozono O3 (µg/m³)
  //    - european_aqi: Indice Europeo di Qualità dell'Aria (1-100+)
  const airQualityUrl = `https://air-quality-api.open-meteo.com/v1/air-quality?latitude=${lat}&longitude=${lon}&current=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,european_aqi`;

  // Esegue le due chiamate HTTP in PARALLELO con Promise.all per dimezzare la latenza di rete
  const [weatherRes, airRes] = await Promise.all([
    fetch(weatherUrl),
    fetch(airQualityUrl),
  ]);

  // Se una delle due risposte HTTP ha uno status code di errore (es: 4xx o 5xx), lancia eccezione
  if (!weatherRes.ok) {
    throw new Error(`Weather API HTTP error ${weatherRes.status}: ${weatherRes.statusText}`);
  }
  if (!airRes.ok) {
    throw new Error(`Air Quality API HTTP error ${airRes.status}: ${airRes.statusText}`);
  }

  // Parsing asincrono dei JSON ricevuti
  const weatherData = await weatherRes.json();
  const airData = await airRes.json();

  // Estrazione sicura del blocco 'current' (o oggetto vuoto per evitare crash)
  const wCur = weatherData.current || {};
  const aCur = airData.current || {};

  // Costruzione del documento JSON unificato e normalizzato.
  // Struttura progettata per conformità con:
  // - Schema temporale Elasticsearch (@timestamp formato ISO 8601 UTC)
  // - Schema geospaziale Elasticsearch (location: { lat, lon } di tipo 'geo_point' per le mappe Kibana)
  const payload = {
    // Timestamp dell'evento in formato ISO (es: "2026-10-01T15:30:00.000Z")
    "@timestamp": new Date().toISOString(),
    
    // Metadati geografici
    city,
    country,
    location: {
      lat: Number(lat),
      lon: Number(lon),
    },

    // Parametri Meteorologici (se assenti impostati a null)
    temperature_c: wCur.temperature_2m ?? null,
    humidity_pct: wCur.relative_humidity_2m ?? null,
    pressure_hpa: wCur.surface_pressure ?? null,
    wind_speed_kmh: wCur.wind_speed_10m ?? null,

    // Parametri Inquinamento & Qualità dell'Aria
    pm2_5: aCur.pm2_5 ?? null,
    pm10: aCur.pm10 ?? null,
    no2: aCur.nitrogen_dioxide ?? null,
    so2: aCur.sulphur_dioxide ?? null,
    co: aCur.carbon_monoxide ?? null,
    o3: aCur.ozone ?? null,
    european_aqi: aCur.european_aqi ?? null,
  };

  return payload;
}

// ============================================================================
// 6. CICLO DI RACCOLTA ED INVIO DEI DATI A KAFKA
// ============================================================================
/**
 * Itera su tutte le città configurate, preleva i dati e li pubblica su Kafka.
 */
async function collectAndProduce() {
  const startTime = Date.now();
  console.log(`[Collector] [${new Date().toISOString()}] Avvio polling per ${cities.length} città...`);

  // Eseguiamo il fetch di TUTTE le città in parallelo tramite 'Promise.allSettled'.
  // 'Promise.allSettled' garantisce che se una città fallisce (es. timeout temporaneo),
  // le altre continuano a funzionare senza interrompere l'intero ciclo.
  const results = await Promise.allSettled(
    cities.map(async (cityInfo) => {
      try {
        // 1. Scarica e normalizza i dati della singola città
        const payload = await fetchCityData(cityInfo);
        
        // 2. Se siamo in modalità di test (DRY_RUN), stampiamo solo a console
        if (DRY_RUN) {
          console.log(`[DRY-RUN] [${cityInfo.city}]`, JSON.stringify(payload));
          return { success: true, city: cityInfo.city };
        }

        // 3. Invio effettivo a Kafka:
        //    - 'topic': Topic di destinazione (weather_pollution.telemetry)
        //    - 'key': Usiamo il nome della città (es. "Catania"). In Kafka, usare una chiave garantisce che
        //             tutti i messaggi della stessa città finiscano nella STESSA partizione, mantenendo
        //             l'ordine cronologico per i calcoli di streaming (windowing in Spark).
        //    - 'value': Il payload JSON serializzato a stringa.
        await producer.send({
          topic: TELEMETRY_TOPIC,
          messages: [
            {
              key: cityInfo.city,
              value: JSON.stringify(payload),
              timestamp: Date.now().toString(),
            },
          ],
        });

        console.log(`[Kafka] Inviato dato per: ${cityInfo.city} (AQI: ${payload.european_aqi}, Temp: ${payload.temperature_c}°C)`);
        return { success: true, city: cityInfo.city };
      } catch (err) {
        console.error(`[Error] Errore raccolta dati per ${cityInfo.city}:`, err.message);

        // 4. In caso di errore, inoltriamo l'evento al topic Dead Letter Queue (DLQ)
        //    per non perdere traccia delle anomalie e poterle analizzare in Kibana.
        if (!DRY_RUN && producer) {
          try {
            await producer.send({
              topic: DLQ_TOPIC,
              messages: [
                {
                  key: cityInfo.city,
                  value: JSON.stringify({
                    "@timestamp": new Date().toISOString(),
                    city: cityInfo.city,
                    error: err.message,
                  }),
                },
              ],
            });
          } catch (dlqErr) {
            console.error(`[DLQ Error] Impossibile inviare a DLQ:`, dlqErr.message);
          }
        }
        return { success: false, city: cityInfo.city, error: err.message };
      }
    })
  );

  // Calcolo delle statistiche di completamento del ciclo corrente
  const successful = results.filter((r) => r.status === "fulfilled" && r.value.success).length;
  const elapsed = Date.now() - startTime;
  console.log(`[Collector] Ciclo completato in ${elapsed}ms: ${successful}/${cities.length} città raccolte con successo.`);
}

// ============================================================================
// 7. FUNZIONE PRINCIPALE DI AVVIO E GESTIONE SHUTDOWN
// ============================================================================
/**
 * Entrypoint principale dell'applicazione.
 */
async function main() {
  // Banner informativo di benvenuto
  console.log("==================================================");
  console.log("  Real-Time Weather & Air Quality Collector (JS)  ");
  console.log("==================================================");
  console.log(`Brokers Kafka   : ${KAFKA_BROKERS.join(", ")}`);
  console.log(`Topic Telemetria: ${TELEMETRY_TOPIC}`);
  console.log(`Topic DLQ       : ${DLQ_TOPIC}`);
  console.log(`Intervallo      : ${POLL_INTERVAL_MS / 1000}s`);
  console.log(`Modalità DRY-RUN: ${DRY_RUN}`);
  console.log("==================================================");

  // Connessione iniziale a Kafka se non siamo in DRY_RUN
  if (!DRY_RUN) {
    try {
      console.log("[Kafka] Connessione al broker Kafka in corso...");
      await producer.connect();
      console.log("[Kafka] Connessione a Kafka stabilita con successo!");
    } catch (err) {
      console.error("[Kafka] Errore di connessione a Kafka:", err.message);
      console.error("[Kafka] Suggerimento: avvia Kafka con docker-compose oppure imposta DRY_RUN=true per testare localmente.");
      process.exit(1);
    }
  }

  // Esegue immediatamente la prima raccolta dati al lancio senza attendere il primo intervallo
  await collectAndProduce();

  // Imposta l'esecuzione periodica a intervalli regolari (es. ogni 30s)
  const intervalId = setInterval(collectAndProduce, POLL_INTERVAL_MS);

  // GESTIONE DEL GRACEFUL SHUTDOWN:
  // Quando l'applicazione riceve un segnale di stop (es. CTRL+C da terminale o 'docker stop' da Docker),
  // dobbiamo pulire le risorse, fermare il timer e disconnettere il Producer Kafka in modo pulito.
  const gracefulShutdown = async (signal) => {
    console.log(`\n[Collector] Ricevuto segnale ${signal}. Arresto in corso...`);
    clearInterval(intervalId); // Blocca il timer periodico
    if (producer) {
      try {
        await producer.disconnect(); // Chiude la connessione socket con Kafka
        console.log("[Kafka] Producer disconnesso con successo.");
      } catch (err) {
        console.error("[Kafka] Errore durante la disconnessione:", err.message);
      }
    }
    process.exit(0);
  };

  // Registra i listener per i segnali di terminazione del sistema operativo
  process.on("SIGINT", () => gracefulShutdown("SIGINT"));   // CTRL+C
  process.on("SIGTERM", () => gracefulShutdown("SIGTERM")); // Arresto container Docker
}

// Avvio dell'applicazione con gestione degli errori fatali
main().catch((err) => {
  console.error("[Fatal Error]", err);
  process.exit(1);
});
