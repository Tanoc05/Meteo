#!/usr/bin/env python3
# ============================================================================
# UTILITÀ DI BACKUP: SALVATAGGIO DASHBOARD E GRAFICI KIBANA SU FILE
# SCOPO: Esporta tutte le Dashboard, Mappe, Visualizzazioni e Data Views
#        create su Kibana dentro il file 'kibana/saved_dashboard.ndjson'.
#        Così anche se cancelli i container o i volumi Docker, al prossimo avvio
#        setup_kibana.py ricaricherà automaticamente tutti i tuoi grafici!
# ============================================================================

import os
import requests

# Indirizzo locale di Kibana
KIBANA_URL = os.getenv("KIBANA_URL", "http://localhost:5601")

# Percorso del file di destinazione (kibana/saved_dashboard.ndjson)
BACKUP_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "saved_dashboard.ndjson")

# Header richiesto dalle API di Kibana
HEADERS = {
    "kbn-xsrf": "true",
    "Content-Type": "application/json"
}

def backup_kibana_objects():
    print(f"Esportazione di Dashboard, Mappe e Grafici da {KIBANA_URL}...")

    # Endpoint ufficiale di Kibana per esportare tutti gli oggetti salvati
    url = f"{KIBANA_URL}/api/saved_objects/_export"

    # Chiediamo a Kibana di esportare dashboard, mappe, grafici Lens e Data Views
    # includeReferencesDeep=True include automaticamente tutti i pannelli collegati alla dashboard
    payload = {
        "type": ["dashboard", "map", "lens", "visualization", "search", "index-pattern"],
        "includeReferencesDeep": True
    }

    try:
        res = requests.post(url, json=payload, headers=HEADERS, timeout=15)
        if res.status_code == 200 and len(res.content) > 0:
            # Salva il contenuto esportato dentro il file saved_dashboard.ndjson
            with open(BACKUP_FILE, "wb") as f:
                f.write(res.content)
            print(f"✅ Backup salvato con successo in: {BACKUP_FILE}")
            print("Ora puoi riavviare o ricreare i container quante volte vuoi: i tuoi grafici verranno ricaricati in automatico!")
        else:
            print(f"⚠️ Errore durante l'esportazione: {res.status_code} - {res.text}")
    except Exception as e:
        print(f"❌ Impossibile connettersi a Kibana ({KIBANA_URL}): {e}")

if __name__ == "__main__":
    backup_kibana_objects()
