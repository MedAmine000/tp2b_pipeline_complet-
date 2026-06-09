"""TP 2B - Pipeline complet Open-Meteo -> PostgreSQL avec tracabilite.

4 taches :
- fetch_weather     : appel API Open-Meteo (villes configurables via Variable)
- transform_weather : selection et structuration des champs utiles
- load_weather      : insertion dans la table meteo
- log_ingestion     : ecriture dans la table de suivi ingestion_log

Parametrage :
- Variable "cities"              : liste des villes (JSON)
- Variable "open_meteo_base_url" : URL de base de l'API
- Variable "open_meteo_fields"   : champs current a demander
- Connection "postgres_meteo"    : acces PostgreSQL
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import json

POSTGRES_CONN_ID = "postgres_meteo"


def get_config():
    """Charge la configuration depuis les Variables Airflow."""
    cities = json.loads(Variable.get("cities"))
    base_url = Variable.get("open_meteo_base_url")
    fields = Variable.get("open_meteo_fields")
    return cities, base_url, fields


def fetch_weather(**kwargs):
    """Appelle l'API Open-Meteo pour chaque ville configuree."""
    cities, base_url, fields = get_config()
    results = []

    for city in cities:
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "current": fields,
        }
        resp = requests.get(base_url, params=params, timeout=10)
        resp.raise_for_status()

        raw = resp.json()
        raw["_city_name"] = city["name"]
        results.append(raw)
        print(f"[fetch] {city['name']} OK")

    kwargs["ti"].xcom_push(key="fetch_count", value=len(results))
    return results


def transform_weather(**kwargs):
    """Extrait les champs utiles et structure pour la table cible."""
    ti = kwargs["ti"]
    raw_list = ti.xcom_pull(task_ids="fetch_weather")

    if not raw_list:
        raise ValueError("Aucune donnee recue de fetch_weather")

    records = []
    for raw in raw_list:
        current = raw.get("current")
        if current is None:
            print(f"[transform] SKIP {raw.get('_city_name')} : pas de bloc current")
            continue

        records.append({
            "ville": raw["_city_name"],
            "latitude": raw["latitude"],
            "longitude": raw["longitude"],
            "temperature_c": current["temperature_2m"],
            "humidite_pct": current["relative_humidity_2m"],
            "vent_kmh": current["wind_speed_10m"],
            "code_meteo": current["weather_code"],
            "date_mesure": current["time"],
        })

    print(f"[transform] {len(records)} enregistrements prets")
    return records


def load_weather(**kwargs):
    """Insere les enregistrements dans PostgreSQL."""
    ti = kwargs["ti"]
    records = ti.xcom_pull(task_ids="transform_weather")

    if not records:
        raise ValueError("Rien a charger")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    inserted = 0
    for r in records:
        hook.run(
            """INSERT INTO meteo (ville, latitude, longitude, temperature_c,
                                  humidite_pct, vent_kmh, code_meteo, date_mesure)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            parameters=(
                r["ville"], r["latitude"], r["longitude"],
                r["temperature_c"], r["humidite_pct"], r["vent_kmh"],
                r["code_meteo"], r["date_mesure"],
            ),
        )
        inserted += 1

    print(f"[load] {inserted} lignes inserees")
    ti.xcom_push(key="inserted_count", value=inserted)


def log_ingestion(**kwargs):
    """Ecrit une ligne de tracabilite dans ingestion_log."""
    ti = kwargs["ti"]
    dag_run = kwargs["dag_run"]

    fetch_count = ti.xcom_pull(task_ids="fetch_weather", key="fetch_count") or 0
    inserted_count = ti.xcom_pull(task_ids="load_weather", key="inserted_count") or 0

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        """INSERT INTO ingestion_log
           (dag_id, run_id, source, started_at, finished_at,
            rows_received, rows_inserted, status)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        parameters=(
            dag_run.dag_id,
            dag_run.run_id,
            Variable.get("open_meteo_base_url"),
            dag_run.start_date.isoformat() if dag_run.start_date else None,
            datetime.utcnow().isoformat(),
            fetch_count,
            inserted_count,
            "success",
        ),
    )
    print(f"[log] Ingestion tracee : {fetch_count} recues, {inserted_count} inserees")


default_args = {
    "owner": "korniti",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="open_meteo_pipeline_v2",
    description="Pipeline complet : API Open-Meteo -> transform -> PostgreSQL + tracabilite",
    default_args=default_args,
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["tp2b", "meteo", "open-meteo", "tracabilite"],
) as dag:

    t_fetch = PythonOperator(task_id="fetch_weather", python_callable=fetch_weather)
    t_transform = PythonOperator(task_id="transform_weather", python_callable=transform_weather)
    t_load = PythonOperator(task_id="load_weather", python_callable=load_weather)
    t_log = PythonOperator(task_id="log_ingestion", python_callable=log_ingestion)

    t_fetch >> t_transform >> t_load >> t_log
