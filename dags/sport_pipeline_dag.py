"""
DAG principal — Sport Data Solution
Pipeline ETL : Ingestion → Génération → Validation → Transformation → Quality → Notification
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# === Default args ===
default_args = {
    "owner": "sport-data-solution",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

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
        python_callable=lambda: print("TODO: Upload RH Excel to GCS + load to BigQuery bronze"),
    )

    ingest_sport = PythonOperator(
        task_id="ingest_sport_data",
        python_callable=lambda: print("TODO: Upload Sport Excel to GCS + load to BigQuery bronze"),
    )

    # --- 2. Génération données Strava (PySpark) ---
    generate_strava = PythonOperator(
        task_id="generate_strava_simulation",
        python_callable=lambda: print("TODO: PySpark — generate 12 months of sport activities"),
    )

    # --- 3. Validation distances (Google Maps API) ---
    validate_distances = PythonOperator(
        task_id="validate_distances_google_maps",
        python_callable=lambda: print("TODO: Google Maps API — validate commute distances"),
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
        python_callable=lambda: print("TODO: Run Great Expectations checkpoints"),
    )

    # --- 6. Notifications Slack ---
    notify_slack = PythonOperator(
        task_id="notify_slack_activities",
        python_callable=lambda: print("TODO: Send Slack messages for new activities"),
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
