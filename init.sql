-- Table meteo : donnees meteo nettoyees et pretes a l'usage
CREATE TABLE IF NOT EXISTS meteo (
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

-- Table de suivi d'ingestion : trace chaque execution du pipeline
CREATE TABLE IF NOT EXISTS ingestion_log (
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
