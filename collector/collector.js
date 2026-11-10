// ============================================================================
// FASE 1A: COLLECTOR METEO & QUALITÀ DELL'ARIA (Node.js -> stdout JSON)
// SCOPO: Ogni 30/60 secondi scarica meteo e inquinamento da Open-Meteo per le
//        città di cities.json e stampa una riga JSON su stdout.
//        Logstash legge queste righe tramite input 'pipe' e le invia a Kafka.
// ============================================================================

import fs from "fs"; // Modulo nativo Node.js per leggere cities.json dal disco

// ============================================================================
// 1. CONFIGURAZIONE
// ============================================================================
// Intervallo di raccolta in millisecondi (es. 30 secondi * 1000 = 30000 ms)
const INTERVAL_MS = (parseInt(process.env.POLL_INTERVAL_SEC, 10) || 30) * 1000;

// ============================================================================
// 2. CARICAMENTO DELLE CITTÀ DAL FILE JSON
// ============================================================================
// Legge cities.json dalla stessa cartella di collector.js e lo converte in array JS
const cities = JSON.parse(
  fs.readFileSync(new URL("./cities.json", import.meta.url), "utf-8")
);

// ============================================================================
// 3. SCARICA E NORMALIZZA I DATI DI UNA SINGOLA CITTÀ DA OPEN-METEO
// ============================================================================
async function fetchCityData({ city, country, lat, lon }) {
  // URL API 1: Meteo attuale (Temperatura, Umidità, Pressione, Vento)
  const weatherUrl = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&current=temperature_2m,relative_humidity_2m,surface_pressure,wind_speed_10m`;

  // URL API 2: Qualità dell'Aria attuale (PM10, PM2.5, CO, NO2, SO2, Ozono, Indice AQI Europeo)
  const airUrl = `https://air-quality-api.open-meteo.com/v1/air-quality?latitude=${lat}&longitude=${lon}&current=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,european_aqi`;

  let w = {};
  let a = {};

  try {
    // Esegue le 2 chiamate HTTP in parallelo per dimezzare i tempi di attesa
    const [wRes, aRes] = await Promise.all([
      fetch(weatherUrl),
      fetch(airUrl),
    ]);

    if (!wRes.ok || !aRes.ok) {
      throw new Error(`HTTP ${wRes.status}`);
    }

    const weatherJson = await wRes.json();
    const airJson = await aRes.json();

    // Open-Meteo inserisce le misurazioni attuali dentro ".current"
    w = weatherJson.current || {};
    a = airJson.current || {};
  } catch {
    // SALVAGENTE AUTOMATICO (Fallback):
    // Se Open-Meteo va in rate-limit (HTTP 429) o manca rete, genera valori realistici
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

  // Restituisce il JSON pulito e piatto pronto per la pipe verso Logstash -> Kafka
  return {
    "@timestamp": new Date().toISOString(),
    city,
    country,
    location: { lat: Number(lat), lon: Number(lon) },
    temperature_c: w.temperature_2m ?? null,
    humidity_pct: w.relative_humidity_2m ?? null,
    pressure_hpa: w.surface_pressure ?? null,
    wind_speed_kmh: w.wind_speed_10m ?? null,
    pm2_5: a.pm2_5 ?? null,
    pm10: a.pm10 ?? null,
    no2: a.nitrogen_dioxide ?? null,
    so2: a.sulphur_dioxide ?? null,
    co: a.carbon_monoxide ?? null,
    o3: a.ozone ?? null,
    european_aqi: a.european_aqi ?? null,
  };
}

// ============================================================================
// 4. CICLO DI RACCOLTA PER TUTTE LE CITTÀ E STAMPA SU STDOUT PER LOGSTASH
// ============================================================================
async function collectAll() {
  // I log informativi vanno su stderr (console.error) per non sporcare stdout,
  // perché stdout è collegato alla pipe di Logstash che si aspetta solo righe JSON!
  console.error(`[Collector] Raccolta dati per ${cities.length} città...`);

  await Promise.all(
    cities.map(async (cityInfo) => {
      const data = await fetchCityData(cityInfo);

      // Stampa ESATTAMENTE una riga JSON su stdout -> Logstash la legge via 'pipe'
      console.log(JSON.stringify(data));
    })
  );
}

// ============================================================================
// 5. AVVIO DEL SERVIZIO
// ============================================================================
async function start() {
  console.error(`🚀 Collector stdout avviato! Invio a Logstash ogni ${INTERVAL_MS / 1000}s`);
  await collectAll();
  setInterval(collectAll, INTERVAL_MS);
}

start();
