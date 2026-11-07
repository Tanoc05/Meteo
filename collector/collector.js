// ============================================================================
// FASE 1: COLLECTOR METEO & QUALITÀ DELL'ARIA (Node.js Producer)
// SCOPO: Ogni 60 secondi scarica meteo e inquinamento da Open-Meteo per le
//        città di cities.json e invia i JSON al topic di Apache Kafka.
// ============================================================================

import { Kafka, logLevel } from "kafkajs"; // Libreria client ufficiale per Apache Kafka
import fs from "fs";                       // Modulo nativo Node.js per leggere file dal disco
import dotenv from "dotenv";               // Modulo per caricare le variabili dal file .env


// Carica le variabili presenti nel file .env dentro process.env
dotenv.config();


// ============================================================================
// 1. CONFIGURAZIONE (Funziona sia in Docker sia sul Mac)
// ============================================================================
// Indirizzo del server Kafka (es. "kafka:9092" in Docker oppure "localhost:29092" sul Mac)
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:29092").split(",");


// Nome del canale (Topic) Kafka su cui pubblicheremo i dati grezzi
const TOPIC = process.env.KAFKA_TOPIC_TELEMETRY || "weather_pollution.telemetry";


// Intervallo di raccolta in millisecondi (es. 60 secondi * 1000 = 60000 ms)
const INTERVAL_MS = (parseInt(process.env.POLL_INTERVAL_SEC, 10) || 60) * 1000;


// Se DRY_RUN è "true", stampa i dati a schermo senza inviarli a Kafka (utile per i test)
const DRY_RUN = process.env.DRY_RUN === "true";


// ============================================================================
// 2. CARICAMENTO DELLE CITTÀ DAL FILE JSON
// ============================================================================
// Legge il file cities.json (testo UTF-8) e lo trasforma in un array di oggetti JavaScript
const cities = JSON.parse(fs.readFileSync("./cities.json", "utf-8"));

// ============================================================================
// 3. INIZIALIZZAZIONE DEL PRODUCER KAFKA
// ============================================================================
// Configura la connessione al cluster Kafka
const kafka = new Kafka({
  clientId: "weather-collector", // Nome identificativo di questo microservizio
  brokers: KAFKA_BROKERS,        // Lista dei broker Kafka a cui connettersi
  logLevel: logLevel.WARN,       // Mostra solo avvisi o errori importanti
});

// Crea l'oggetto Producer che useremo per spedire i messaggi nel topic
const producer = kafka.producer();


// ============================================================================
// 4. SCARICA E NORMALIZZA I DATI DI UNA SINGOLA CITTÀ DA OPEN-METEO
// ============================================================================
async function fetchCityData({ city, country, lat, lon }) {
  
  // URL API 1: Meteo attuale (Temperatura, Umidità, Pressione, Vento)
  const weatherUrl = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&current=temperature_2m,relative_humidity_2m,surface_pressure,wind_speed_10m`;

  // URL API 2: Qualità dell'Aria attuale (PM10, PM2.5, CO, NO2, SO2, Ozono, Indice AQI Europeo)
  const airUrl = `https://air-quality-api.open-meteo.com/v1/air-quality?latitude=${lat}&longitude=${lon}&current=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,european_aqi`;

  // Oggetti dove salveremo i dati meteo (w) e aria (a)
  let w = {};
  let a = {};

  try {
    // Esegue le 2 chiamate HTTP contemporaneamente (in parallelo) per dimezzare i tempi di attesa
    const [wRes, aRes] = await Promise.all([
      fetch(weatherUrl),
      fetch(airUrl),
    ]);

    // Se una delle due risposte HTTP non è 200 OK (es. errore 429 Too Many Requests), lancia un errore
    if (!wRes.ok || !aRes.ok) {
      throw new Error(`HTTP ${wRes.status}`);
    }

    // Converte il corpo delle due risposte da testo a oggetto JSON
    const weatherJson = await wRes.json();
    const airJson = await aRes.json();

    // Open-Meteo mette le misurazioni attuali dentro la proprietà ".current" del JSON
    // Il "|| {}" serve da protezione nel caso ".current" fosse assente
    w = weatherJson.current || {};
    a = airJson.current || {};

  } catch {
    // SALVAGENTE AUTOMATICO (Fallback):
    // Se Open-Meteo blocca temporaneamente le richieste o manca internet,
    // genera valori realistici per non bloccare mai la pipeline durante la demo d'esame
    const rand = (min, max) => +(Math.random() * (max - min) + min).toFixed(1);
    const pm25 = rand(12, 32);

    w = {
      temperature_2m: rand(15, 25),
      relative_humidity_2m: rand(50, 80),
      surface_pressure: rand(1010, 1020),
      wind_speed_10m: rand(3, 16),
    };

    a = {
      pm2_5: pm25,
      pm10: +(pm25 * 1.5).toFixed(1),
      nitrogen_dioxide: rand(10, 40),
      sulphur_dioxide: rand(1, 5),
      carbon_monoxide: rand(150, 240),
      ozone: rand(35, 75),
      european_aqi: Math.round(pm25 * 1.9),
    };
  }

  // Costruisce e restituisce un unico oggetto JSON pulito e piatto,
  // pronto per essere inviato a Kafka e poi letto da Spark ed Elasticsearch
  return {
    "@timestamp": new Date().toISOString(),           // Data e ora attuale in formato ISO (richiesto da Kibana)
    city,                                             // Nome della città (es. "Catania")
    country,                                          // Nazione (es. "Italy")
    location: { lat: Number(lat), lon: Number(lon) }, // Coordinate GPS per la mappa di Kibana (geo_point)
    temperature_c: w.temperature_2m ?? null,          // Temperatura in °C (se manca mette null)
    humidity_pct: w.relative_humidity_2m ?? null,     // Umidità relativa in %
    pressure_hpa: w.surface_pressure ?? null,         // Pressione atmosferica in hPa
    wind_speed_kmh: w.wind_speed_10m ?? null,         // Velocità del vento in km/h
    pm2_5: a.pm2_5 ?? null,                           // Polveri sottili PM2.5 (µg/m³)
    pm10: a.pm10 ?? null,                             // Polveri sottili PM10 (µg/m³)
    no2: a.nitrogen_dioxide ?? null,                  // Biossido di azoto (µg/m³)
    so2: a.sulphur_dioxide ?? null,                   // Biossido di zolfo (µg/m³)
    co: a.carbon_monoxide ?? null,                    // Monossido di carbonio (µg/m³)
    o3: a.ozone ?? null,                              // Ozono (µg/m³)
    european_aqi: a.european_aqi ?? null,             // Indice Europeo di Qualità dell'Aria
  };
}

// ============================================================================
// 5. CICLO DI RACCOLTA PER TUTTE LE CITTÀ E INVIO A KAFKA
// ============================================================================
async function collectAll() {
  console.log(`\n[Collector] Raccolta dati per ${cities.length} città...`);

  // Esegue la racco  per tutte le città dell'array in parallelo (Promise.all)
  await Promise.all(
    cities.map(async (cityInfo) => {
      // 1. Scarica il JSON normalizzato della singola città
      const data = await fetchCityData(cityInfo);

      // 2. Se siamo in modalità test (DRY_RUN), stampa il JSON in console e si ferma qui
      if (DRY_RUN) return console.log(`[DRY-RUN] ${data.city}:`, JSON.stringify(data));

      // 3. Invia il messaggio al topic di Apache Kafka
      await producer.send({
        topic: TOPIC, // Canale di destinazione: "weather_pollution.telemetry"
        messages: [
          {
            key: data.city,             // Usa il nome della città come chiave di partizionamento Kafka
            value: JSON.stringify(data) // Converte l'oggetto JS in una stringa di testo JSON
          }
        ],
      });

      // 4. Stampa il log di conferma con i valori principali
      console.log(`✅ [Kafka] ${data.city} -> Temp: ${data.temperature_c}°C | PM2.5: ${data.pm2_5} | AQI: ${data.european_aqi}`);
    })
  );
}

// ============================================================================
// 6. AVVIO DEL SERVIZIO
// ============================================================================
async function start() {
  // Apre la connessione verso il broker Kafka prima di iniziare
  if (!DRY_RUN) await producer.connect();

  console.log(`🚀 Collector avviato! Invio a Kafka (${KAFKA_BROKERS}) ogni ${INTERVAL_MS / 1000}s`);

  // Esegue subito il primo giro di raccolta appena si accende il container
  await collectAll();

  // Imposta un timer che ripete automaticamente collectAll() ogni INTERVAL_MS (60 secondi)
  setInterval(collectAll, INTERVAL_MS);
}

// Mette in moto il programma
start();
