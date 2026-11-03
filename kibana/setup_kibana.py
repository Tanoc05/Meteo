#!/usr/bin/env python3
# ============================================================================
# FASE 5: KIBANA AUTOMATED SETUP & DASHBOARD IMPORTER
# TECNOLOGIA: Python 3 + Kibana Saved Objects API & Data Views API
# SCOPO: 1. Aspetta che Kibana sia acceso e pronto
#        2. Crea automaticamente le 4 Data Views (viste sugli indici di Elasticsearch)
#        3. Se trova il file di backup 'saved_dashboard.ndjson', importa tutti i grafici
#        4. Altrimenti crea la Dashboard base SENZA mai sovrascrivere i grafici dell'utente
# ============================================================================

import time
import json
import logging
import requests
import os

# Configura il formato dei messaggi di log nel terminale
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("KibanaSetup")

# Indirizzo di Kibana (in Docker viene passato come http://kibana:5601, sul Mac è http://localhost:5601)
KIBANA_URL = os.getenv("KIBANA_URL", "http://localhost:5601")

# Percorso del file di backup dove vengono salvati grafici, mappe e dashboard
BACKUP_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "saved_dashboard.ndjson")

# Header obbligatorio richiesto da tutte le API di Kibana per sicurezza (protezione CSRF)
HEADERS = {
    "kbn-xsrf": "true",
    "Content-Type": "application/json"
}

# ============================================================================
# 1. ATTESA AVVIO DI KIBANA
# ============================================================================
def wait_for_kibana():
    """
    Controlla ogni 5 secondi se Kibana ha finito di avviarsi chiamando /api/status.
    """
    logger.info(f"Connessione al server Kibana ({KIBANA_URL})...")
    for attempt in range(40):
        try:
            res = requests.get(f"{KIBANA_URL}/api/status", headers=HEADERS, timeout=5)
            if res.status_code == 200:
                logger.info("Kibana è pronto e operativo!")
                return True
        except Exception:
            pass
        logger.info(f"In attesa che Kibana completi l'avvio... (tentativo {attempt+1}/40)")
        time.sleep(5)
    logger.error("Timeout: Kibana non raggiungibile.")
    return False

# ============================================================================
# 2. CREAZIONE AUTOMATICA DELLE 4 DATA VIEWS
# ============================================================================
def create_data_views():
    """
    Crea le 4 Data Views su Kibana collegate ai 4 indici di Elasticsearch:
    - weather-pollution-telemetry*    (dati grezzi)
    - weather-pollution-aggregations* (medie per città)
    - weather-pollution-alerts*       (allarmi soglie OMS)
    - weather-pollution-ml-alerts*    (cluster K-Means e anomalie ML)
    """
    data_views = [
        {
            "id": "dv-telemetry",
            "name": "weather-pollution-telemetry*",
            "title": "weather-pollution-telemetry*",
            "timeFieldName": "@timestamp"
        },
        {
            "id": "dv-aggregations",
            "name": "weather-pollution-aggregations*",
            "title": "weather-pollution-aggregations*",
            "timeFieldName": "@timestamp"
        },
        {
            "id": "dv-alerts",
            "name": "weather-pollution-alerts*",
            "title": "weather-pollution-alerts*",
            "timeFieldName": "@timestamp"
        },
        {
            "id": "dv-ml-alerts",
            "name": "weather-pollution-ml-alerts*",
            "title": "weather-pollution-ml-alerts*",
            "timeFieldName": "@timestamp"
        }
    ]

    # Invia a Kibana la richiesta di creazione per ciascuna Data View
    for dv in data_views:
        url = f"{KIBANA_URL}/api/data_views/data_view"
        payload = {
            "data_view": {
                "id": dv["id"],
                "title": dv["title"],
                "name": dv["name"],
                "timeFieldName": dv["timeFieldName"]
            }
        }
        try:
            res = requests.post(url, json=payload, headers=HEADERS, timeout=10)
            if res.status_code in [200, 201]:
                logger.info(f"Data View creata con successo: {dv['title']}")
            elif res.status_code in [400, 409] or "already exists" in res.text or "Duplicate" in res.text:
                logger.info(f"Data View già esistente (mantenuta): {dv['title']}")
            else:
                logger.warning(f"Risposta creazione Data View ({dv['title']}): {res.status_code} - {res.text}")
        except Exception as e:
            logger.error(f"Errore creazione Data View {dv['title']}: {e}")

# ============================================================================
# 3. IMPORTAZIONE AUTOMATICA DEL BACKUP (SE PRESENTE)
# ============================================================================
def import_backup_if_present():
    """
    Se nella cartella kibana/ esiste il file 'saved_dashboard.ndjson' (creato con backup_kibana.py),
    ricarica automaticamente tutte le dashboard, i grafici e le mappe salvate!
    """
    if not os.path.exists(BACKUP_FILE) or os.path.getsize(BACKUP_FILE) == 0:
        return False

    logger.info(f"Trovato file di backup '{BACKUP_FILE}'. Importazione in corso...")
    url = f"{KIBANA_URL}/api/saved_objects/_import?overwrite=true"
    try:
        with open(BACKUP_FILE, "rb") as f:
            files = {"file": ("saved_dashboard.ndjson", f, "application/ndjson")}
            res = requests.post(url, headers={"kbn-xsrf": "true"}, files=files, timeout=15)
            if res.status_code == 200:
                logger.info("🎉 Backup di Dashboard, Grafici e Mappe importato con successo!")
                return True
            else:
                logger.warning(f"Attenzione durante importazione backup: {res.status_code} - {res.text}")
    except Exception as e:
        logger.error(f"Errore importazione backup: {e}")
    return False

# ============================================================================
# 4. CREAZIONE DELLA DASHBOARD INIZIALE (SENZA SOVRASCRITTURA)
# ============================================================================
def create_main_dashboard():
    """
    Crea la Dashboard principale SOLO se non esiste già su Kibana.
    Così se riavvii i container Docker, i grafici che hai creato a mano non vengono mai cancellati!
    """
    check_url = f"{KIBANA_URL}/api/saved_objects/dashboard/weather-pollution-overview"
    
    # 1. Controlla con una chiamata GET se la dashboard esiste già
    try:
        check_res = requests.get(check_url, headers=HEADERS, timeout=5)
        if check_res.status_code == 200:
            logger.info("✅ La Dashboard esiste già su Kibana: mantengo tutti i tuoi grafici salvati senza sovrascrivere!")
            return
    except Exception:
        pass

    # 2. Se non esiste ancora (es. primo avvio assoluto), la crea vuota pronta all'uso
    create_url = f"{KIBANA_URL}/api/saved_objects/dashboard/weather-pollution-overview"
    dashboard_payload = {
        "attributes": {
            "title": "TAP: Real-Time Weather & Pollution Live Monitor",
            "description": "Dashboard Big Data per il monitoraggio in streaming di parametri meteo, particolato PM2.5/PM10, allarmi sanitari OMS e rilevamento anomalie Machine Learning.",
            "hits": 0,
            "timeRestore": True,
            "timeFrom": "now-1h",
            "timeTo": "now",
            "optionsJSON": json.dumps({
                "useMargins": True,
                "hidePanelTitles": False,
                "syncColors": False,
                "syncCursor": True,
                "syncTooltips": False
            }),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {
                        "query": "",
                        "language": "kuery"
                    },
                    "filter": []
                })
            },
            "panelsJSON": json.dumps([])
        },
        "references": []
    }

    try:
        res = requests.post(create_url, json=dashboard_payload, headers=HEADERS, timeout=10)
        if res.status_code in [200, 201]:
            logger.info("🎉 Dashboard principale creata per la prima volta con successo!")
        else:
            logger.warning(f"Risposta creazione dashboard: {res.status_code} - {res.text}")
    except Exception as e:
        logger.error(f"Errore creazione dashboard: {e}")

# ============================================================================
# 5. FUNZIONE PRINCIPALE DI AVVIO
# ============================================================================
def main():
    logger.info("==================================================")
    logger.info("  Kibana Automatic Setup & Dashboard Configurator ")
    logger.info("==================================================")
    if wait_for_kibana():
        create_data_views()
        if not import_backup_if_present():
            create_main_dashboard()
        logger.info("==================================================")
        logger.info("SETUP COMPLETATO!")
        logger.info(f"Dashboard disponibile su: {KIBANA_URL}/app/dashboards#/view/weather-pollution-overview")
        logger.info(f"Discover disponibile su:   {KIBANA_URL}/app/discover")
        logger.info("==================================================")

if __name__ == "__main__":
    main()
