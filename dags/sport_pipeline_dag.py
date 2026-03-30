"""
DAG principal — Sport Data Solution
Pipeline ETL : Ingestion → Génération → Validation → Transformation → Quality → Notification

Features :
- XCom : les tâches se passent des métriques (nb lignes, statuts) entre elles
- Alertes Slack : notification automatique en cas d'échec d'une tâche
- Airflow Variables : les secrets sont stockés chiffrés dans l'UI Airflow
"""

import sys
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

# Ajouter le dossier parent au PYTHONPATH pour les imports
sys.path.insert(0, "/opt/airflow")


# =============================================================
# ALERTE SLACK EN CAS D'ÉCHEC
# =============================================================
def on_failure_callback(context):
    """
    Callback appelé automatiquement quand une tâche échoue.
    Envoie une alerte sur le canal #sport-alertes via un webhook dédié.
    """
    import json
    import urllib.request

    # Récupérer le webhook alertes depuis les Variables Airflow
    alertes_webhook = Variable.get("slack_webhook_alertes_secret")

    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    execution_date = context["execution_date"]
    exception = context.get("exception", "Erreur inconnue")

    message = (
        f"🚨 *ALERTE PIPELINE* 🚨\n"
        f"Le DAG `{dag_id}` a échoué !\n"
        f"• Tâche : `{task_id}`\n"
        f"• Date d'exécution : {execution_date}\n"
        f"• Erreur : {str(exception)[:200]}\n"
        f"Vérifiez les logs Airflow pour plus de détails."
    )

    payload = json.dumps({"text": message}).encode("utf-8")
    req = urllib.request.Request(
        alertes_webhook,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req)
    except Exception as e:
        print(f"❌ Erreur envoi alerte Slack: {e}")


# === Default args (avec callback d'échec) ===
default_args = {
    "owner": "sport-data-solution",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": on_failure_callback,
}


# =============================================================
# FONCTIONS AVEC XCOM
# =============================================================
def task_ingest_rh(**context):
    """Ingestion RH → GCS + BigQuery. Push le nombre de lignes via XCom."""
    from src.ingestion.ingest import upload_to_gcs, load_rh_to_bigquery
    from google.cloud import bigquery

    project_id = Variable.get("gcp_project_id")

    rh_path = "/opt/airflow/data/raw/Donnees_RH.xlsx"
    upload_to_gcs(rh_path, "donnees_rh.xlsx")
    load_rh_to_bigquery(rh_path)

    client = bigquery.Client(project=project_id)
    table = client.get_table(f"{project_id}.sport_bronze.employees")
    row_count = table.num_rows

    metrics = {"table": "employees", "rows_loaded": row_count, "status": "success"}
    context["ti"].xcom_push(key="ingest_rh_metrics", value=metrics)
    return metrics


def task_ingest_sport(**context):
    """Ingestion Sport → GCS + BigQuery. Push le nombre de lignes via XCom."""
    from src.ingestion.ingest import upload_to_gcs, load_sport_to_bigquery
    from google.cloud import bigquery

    # ⚠️ TEST ALERTE — Test du callback
    #raise Exception("🧪 Test alerte — erreur volontaire pour tester le callback")

    project_id = Variable.get("gcp_project_id")

    sport_path = "/opt/airflow/data/raw/Donnees_Sportive.xlsx"
    upload_to_gcs(sport_path, "donnees_sportive.xlsx")
    load_sport_to_bigquery(sport_path)

    client = bigquery.Client(project=project_id)
    table = client.get_table(f"{project_id}.sport_bronze.sports_declared")
    row_count = table.num_rows

    metrics = {"table": "sports_declared", "rows_loaded": row_count, "status": "success"}
    context["ti"].xcom_push(key="ingest_sport_metrics", value=metrics)
    return metrics


def task_generate_strava(**context):
    """Génération Strava. Récupère les métriques d'ingestion via XCom."""
    from src.generation.generate_strava import (
        get_sportifs_from_bigquery,
        generate_all_activities,
        load_to_bigquery,
    )

    ti = context["ti"]
    rh_metrics = ti.xcom_pull(task_ids="ingest_rh_data", key="ingest_rh_metrics")
    sport_metrics = ti.xcom_pull(task_ids="ingest_sport_data", key="ingest_sport_metrics")
    print(f"📊 XCom reçu — RH: {rh_metrics}, Sport: {sport_metrics}")

    sportifs = get_sportifs_from_bigquery()
    all_activities = generate_all_activities(sportifs)
    load_to_bigquery(all_activities)

    metrics = {
        "activities_generated": len(all_activities),
        "sportifs_count": len(sportifs),
        "status": "success",
    }
    context["ti"].xcom_push(key="strava_metrics", value=metrics)
    return metrics


def task_validate_distances(**context):
    """Validation Google Maps. Push les anomalies via XCom."""
    from src.validation.validate_distances import validate_commute_declarations, save_to_bigquery

    results_df = validate_commute_declarations()
    save_to_bigquery(results_df)

    total = len(results_df)
    valid = int(results_df["is_valid"].sum())
    anomalies = total - valid

    metrics = {
        "total_checked": total,
        "valid": valid,
        "anomalies": anomalies,
        "status": "success",
    }
    context["ti"].xcom_push(key="validation_metrics", value=metrics)
    return metrics


def task_great_expectations(**context):
    """Great Expectations. Push les résultats via XCom."""
    from ge_tests.validate_bronze import run
    run()

    metrics = {"status": "success"}
    context["ti"].xcom_push(key="ge_metrics", value=metrics)
    return metrics


def task_notify_slack(**context):
    """
    Notification Slack des dernières activités.
    Récupère les métriques de toutes les tâches précédentes via XCom
    et envoie un résumé du pipeline.
    """
    from src.notifications.send_recent_activities import get_recent_activities
    from src.notifications.slack_notifier import build_message, send_slack_message
    import time

    ti = context["ti"]

    rh_metrics = ti.xcom_pull(task_ids="ingest_rh_data", key="ingest_rh_metrics") or {}
    sport_metrics = ti.xcom_pull(task_ids="ingest_sport_data", key="ingest_sport_metrics") or {}
    strava_metrics = ti.xcom_pull(task_ids="generate_strava_simulation", key="strava_metrics") or {}
    validation_metrics = ti.xcom_pull(task_ids="validate_distances_google_maps", key="validation_metrics") or {}

    summary = (
        f"✅ *Pipeline Sport Data Solution — Exécution terminée*\n"
        f"• Employés chargés : {rh_metrics.get('rows_loaded', 'N/A')}\n"
        f"• Sports déclarés : {sport_metrics.get('rows_loaded', 'N/A')}\n"
        f"• Activités générées : {strava_metrics.get('activities_generated', 'N/A')}\n"
        f"• Trajets validés : {validation_metrics.get('valid', 'N/A')}/{validation_metrics.get('total_checked', 'N/A')}\n"
        f"• Anomalies : {validation_metrics.get('anomalies', 'N/A')}"
    )
    send_slack_message(summary)

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

    start = EmptyOperator(task_id="start")

    ingest_rh = PythonOperator(
        task_id="ingest_rh_data",
        python_callable=task_ingest_rh,
        provide_context=True,
    )

    ingest_sport = PythonOperator(
        task_id="ingest_sport_data",
        python_callable=task_ingest_sport,
        provide_context=True,
    )

    generate_strava = PythonOperator(
        task_id="generate_strava_simulation",
        python_callable=task_generate_strava,
        provide_context=True,
    )

    validate_distances = PythonOperator(
        task_id="validate_distances_google_maps",
        python_callable=task_validate_distances,
        provide_context=True,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_sport && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_sport && dbt test --profiles-dir .",
    )

    run_great_expectations = PythonOperator(
        task_id="run_great_expectations",
        python_callable=task_great_expectations,
        provide_context=True,
    )

    notify_slack = PythonOperator(
        task_id="notify_slack_activities",
        python_callable=task_notify_slack,
        provide_context=True,
        retries=2,  #2 retries pour notif Slack en cas d'échec 
        trigger_rule="all_done",  # S'exécute même si une task précédente échoue
    )

    end = EmptyOperator(task_id="end")

    # === Task dependencies ===
    start >> [ingest_rh, ingest_sport]
    [ingest_rh, ingest_sport] >> generate_strava
    generate_strava >> validate_distances
    validate_distances >> dbt_run >> dbt_test
    dbt_test >> run_great_expectations
    run_great_expectations >> notify_slack >> end