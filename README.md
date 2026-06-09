# TP 2B - Pipeline complet API -> PostgreSQL

Pipeline orchestre avec Open-Meteo, transformation, chargement PostgreSQL, tracabilite et parametrage.

---

## Lancer

```bash
cd tp2b_pipeline_complet
docker compose up -d
```

http://localhost:8080 (admin / admin)

---

## Le DAG : open_meteo_pipeline_v2

```
fetch_weather --> transform_weather --> load_weather --> log_ingestion
```

| Tache             | Responsabilite                                              |
|-------------------|-------------------------------------------------------------|
| fetch_weather     | Appelle Open-Meteo pour chaque ville (liste configurable)   |
| transform_weather | Selectionne et structure les champs pour la table cible     |
| load_weather      | Insere dans la table meteo                                  |
| log_ingestion     | Trace l'execution dans ingestion_log (suivi)                |

---

## Parametrage (pas de hardcode)

Tout est configurable via les Variables Airflow (UI > Admin > Variables) :

| Variable              | Contenu                                                       |
|-----------------------|---------------------------------------------------------------|
| cities                | Liste JSON des villes avec nom, lat, lon                      |
| open_meteo_base_url   | URL de base de l'API                                          |
| open_meteo_fields     | Champs current a demander a l'API                             |

La connexion PostgreSQL est geree via les Connections Airflow (`postgres_meteo`).

Pour ajouter une ville, il suffit de modifier la Variable `cities` dans l'UI. Pas besoin de toucher au code.

---

## Tables SQL

### meteo (donnees metier)

```sql
CREATE TABLE meteo (
    id SERIAL PRIMARY KEY,
    ville VARCHAR(100) NOT NULL,
    latitude NUMERIC(7,4) NOT NULL,
    longitude NUMERIC(7,4) NOT NULL,
    temperature_c NUMERIC(5,2) NOT NULL,
    humidite_pct INTEGER NOT NULL,
    vent_kmh NUMERIC(5,2) NOT NULL,
    code_meteo INTEGER NOT NULL,
    date_mesure TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### ingestion_log (tracabilite)

```sql
CREATE TABLE ingestion_log (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(200) NOT NULL,
    run_id VARCHAR(200) NOT NULL,
    source VARCHAR(200) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    rows_received INTEGER DEFAULT 0,
    rows_inserted INTEGER DEFAULT 0,
    status VARCHAR(50) NOT NULL,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
```

---

## Verifier

```bash
# Donnees meteo
docker exec -it tp2b_pipeline_complet-postgres-1 psql -U airflow -d meteo -c "SELECT ville, temperature_c, humidite_pct, vent_kmh, date_mesure FROM meteo;"

# Suivi d'ingestion
docker exec -it tp2b_pipeline_complet-postgres-1 psql -U airflow -d meteo -c "SELECT dag_id, run_id, rows_received, rows_inserted, status, finished_at FROM ingestion_log;"
```



---

## Arreter

```bash
docker compose down
```
