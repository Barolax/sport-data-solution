"""
DAG principal — Sport Data Solution
Pipeline ETL : Ingestion → Génération → Validation → Transformation → Quality → Notification
"""

import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Ajouter le dossier parent au PYTHONPATH pour les imports
sys.path.insert(0, "/opt/airflow")

# === Default args ===
default_args = {
    "owner": "sport-data-solution",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# === Fonctions appelées par les tasks ===
def task_ingest_rh():
    from src.ingestion.ingest import upload_to_gcs, load_rh_to_bigquery
    rh_path = "/opt/airflow/data/raw/Donnees_RH.xlsx"
    upload_to_gcs(rh_path, "donnees_rh.xlsx")
    load_rh_to_bigquery(rh_path)


def task_ingest_sport():
    from src.ingestion.ingest import upload_to_gcs, load_sport_to_bigquery
    sport_path = "/opt/airflow/data/raw/Donnees_Sportive.xlsx"
    upload_to_gcs(sport_path, "donnees_sportive.xlsx")
    load_sport_to_bigquery(sport_path)


def task_generate_strava():
    from src.generation.generate_strava import (
        get_sportifs_from_bigquery,
        generate_all_activities,
        create_spark_session,
        activities_to_spark_df,
        load_to_bigquery,
    )
    sportifs = get_sportifs_from_bigquery()
    all_activities = generate_all_activities(sportifs)
    spark = create_spark_session()
    try:
        spark_df = activities_to_spark_df(spark, all_activities)
        spark_df.show(5)
    finally:
        spark.stop()
    load_to_bigquery(all_activities)


def task_validate_distances():
    from src.validation.validate_distances import validate_commute_declarations, save_to_bigquery
    results_df = validate_commute_declarations()
    save_to_bigquery(results_df)


def task_great_expectations():
    from ge_tests.validate_bronze import run
    run()


def task_notify_slack():
    from src.notifications.send_recent_activities import get_recent_activities
    from src.notifications.slack_notifier import build_message, send_slack_message
    import time

    activities = get_recent_activities(limit=5)
    for act in activities:
        msg = build_message(
            prenom=act["prenom"],
            nom=act["nom"],
            sport_type=act["sport_type"],
            distance_m=act["distance_m"],
            duration_s=act["duration_s"],
            comment=act["commentaire"],
        )
        send_slack_message(msg)
        time.sleep(1)


# === DAG Definition ===
with DAG(
    dag_id="sport_pipeline",
    default_args=default_args,
    description="Pipeline ETL avantages sportifs — Raw → Bronze → Silver → Gold",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sport", "etl", "poc"],
) as dag:

    # --- Start ---
    start = EmptyOperator(task_id="start")

    # --- 1. Ingestion : Excel → GCS → BigQuery (Bronze) ---
    ingest_rh = PythonOperator(
        task_id="ingest_rh_data",
        python_callable=task_ingest_rh,
    )

    ingest_sport = PythonOperator(
        task_id="ingest_sport_data",
        python_callable=task_ingest_sport,
    )

    # --- 2. Génération données Strava (PySpark) ---
    generate_strava = PythonOperator(
        task_id="generate_strava_simulation",
        python_callable=task_generate_strava,
    )

    # --- 3. Validation distances (Google Maps API) ---
    validate_distances = PythonOperator(
        task_id="validate_distances_google_maps",
        python_callable=task_validate_distances,
    )

    # --- 4. Transformations dbt (Silver + Gold) ---
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_sport && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_sport && dbt test --profiles-dir .",
    )

    # --- 5. Data Quality (Great Expectations) ---
    run_great_expectations = PythonOperator(
        task_id="run_great_expectations",
        python_callable=task_great_expectations,
    )

    # --- 6. Notifications Slack ---
    notify_slack = PythonOperator(
        task_id="notify_slack_activities",
        python_callable=task_notify_slack,
    )

    # --- End ---
    end = EmptyOperator(task_id="end")

    # === Task dependencies ===
    start >> [ingest_rh, ingest_sport]
    [ingest_rh, ingest_sport] >> generate_strava
    generate_strava >> validate_distances
    validate_distances >> dbt_run >> dbt_test
    dbt_test >> run_great_expectations
    run_great_expectations >> notify_slack >> end