# TP 2B - Pipeline complet API -> MinIO -> PostgreSQL

Pipeline orchestre avec Open-Meteo, archivage MinIO (stockage objet), transformation, chargement PostgreSQL, tracabilite et parametrage.

---

## Architecture

```
                          +-----------+
                          |  MinIO    |
                          | (archive) |
                          +-----+-----+
                                ^
                                |
fetch_weather --> save_raw_to_minio --> transform_weather --> load_weather --> log_ingestion
      |                                       |                    |                |
      v                                       v                    v                v
  Open-Meteo API                      Structuration         PostgreSQL       ingestion_log
```

---

## Services

| Service    | Port(s)         | Role                                    |
|------------|----------------|-----------------------------------------|
| airflow    | 8080           | Orchestrateur (UI + scheduler)          |
| postgres   | 5432           | Base de donnees relationnelle            |
| minio      | 9000 / 9001    | Stockage objet S3-compatible            |
| minio-init | -              | Cree le bucket au demarrage             |

---

## Lancer

```bash
cd tp2b_pipeline_complet
docker compose up -d
```

- Airflow : http://localhost:8080 (admin / admin)
- MinIO Console : http://localhost:9001 (minioadmin / minioadmin)

---

## Le DAG : open_meteo_pipeline_v2

```
fetch_weather --> save_raw_to_minio --> transform_weather --> load_weather --> log_ingestion
```

| Tache              | Responsabilite                                              |
|--------------------|-------------------------------------------------------------|
| fetch_weather      | Appelle Open-Meteo pour chaque ville (liste configurable)   |
| save_raw_to_minio  | Archive la reponse brute (JSON) dans le bucket MinIO        |
| transform_weather  | Selectionne et structure les champs pour la table cible     |
| load_weather       | Insere dans la table meteo                                  |
| log_ingestion      | Trace l'execution dans ingestion_log (suivi)                |

---

## Parametrage (pas de hardcode)

Tout est configurable via les Variables Airflow (UI > Admin > Variables) :

| Variable              | Contenu                                                       |
|-----------------------|---------------------------------------------------------------|
| cities                | Liste JSON des villes avec nom, lat, lon                      |
| open_meteo_base_url   | URL de base de l'API                                          |
| open_meteo_fields     | Champs current a demander a l'API                             |
| minio_bucket          | Nom du bucket MinIO pour les donnees brutes                   |

Les connexions sont gerees via les Connections Airflow :
- `postgres_meteo` : acces PostgreSQL
- `minio_s3` : acces MinIO (type AWS, endpoint http://minio:9000)

Pour ajouter une ville, il suffit de modifier la Variable `cities` dans l'UI. Pas besoin de toucher au code.

---

## MinIO - Stockage objet

Les reponses brutes de l'API sont archivees dans MinIO au format :

```
s3://meteo-raw/meteo/{YYYY-MM-DD}/raw_responses.json
```

Cela permet :
- Rejouer un traitement sans re-appeler l'API
- Auditer les donnees recues
- Comparer les transformations avec la source

Acces console : http://localhost:9001

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

# Fichiers dans MinIO (via console http://localhost:9001 ou mc)
# Naviguer dans : meteo-raw > meteo > {date} > raw_responses.json
```



---

## Arreter

```bash
docker compose down
```
