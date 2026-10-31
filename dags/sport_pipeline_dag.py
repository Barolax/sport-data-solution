"""
DAG principal — Sport Data Solution
Pipeline ETL : Ingestion → Génération → Validation → Transformation → Quality → Notification

Features :
- XCom : les tâches se passent des métriques (nb lignes, statuts) entre elles
- Alertes Slack : notification automatique en cas d'échec d'une tâche
- Airflow Variables : les secrets sont stockés chiffrés dans l'UI Airflow
- Chemins relatifs : portable entre environnements (local, Docker, prod)
"""

import json
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# =============================================================
# CONSTANTES
# =============================================================

# Chemins relatifs depuis la racine du projet
PROJECT_ROOT = Path(__file__).parent.parent
DATA_RAW_PATH = PROJECT_ROOT / "data" / "raw"
DBT_PROJECT_PATH = PROJECT_ROOT / "dbt_sport"

# Clés XCom (constantes pour éviter les typos)
XCOM_KEY_INGEST_RH = "ingest_rh_metrics"
XCOM_KEY_INGEST_SPORT = "ingest_sport_metrics"
XCOM_KEY_STRAVA = "strava_metrics"
XCOM_KEY_VALIDATION = "validation_metrics"
XCOM_KEY_GE = "ge_metrics"

# Variables Airflow (avec valeurs par défaut pour le mode démo)
def get_airflow_var(key: str, default: str = None) -> str:
    """Récupère une Variable Airflow avec fallback."""
    try:
        return Variable.get(key)
    except Exception:
        if default:
            return default
        raise


# =============================================================
# ALERTE SLACK EN CAS D'ÉCHEC
# =============================================================

def on_failure_callback(context):
    """
    Callback appelé automatiquement quand une tâche échoue.
    Envoie une alerte sur le canal #sport-alertes via un webhook dédié.
    """
    try:
        alertes_webhook = Variable.get("slack_webhook_alertes_secret")
    except Exception:
        print("⚠️ Variable slack_webhook_alertes_secret non configurée")
        return

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
    from google.cloud import bigquery
    from src.ingestion.ingest import load_rh_to_bigquery, upload_to_gcs

    project_id = get_airflow_var("gcp_project_id")
    dataset_bronze = get_airflow_var("bq_dataset_bronze", "sport_bronze")

    rh_path = DATA_RAW_PATH / "Donnees_RH.xlsx"
    upload_to_gcs(str(rh_path), "donnees_rh.xlsx")
    load_rh_to_bigquery(str(rh_path))

    client = bigquery.Client(project=project_id)
    table = client.get_table(f"{project_id}.{dataset_bronze}.employees")
    row_count = table.num_rows

    metrics = {"table": "employees", "rows_loaded": row_count, "status": "success"}
    context["ti"].xcom_push(key=XCOM_KEY_INGEST_RH, value=metrics)
    return metrics


def task_ingest_sport(**context):
    """Ingestion Sport → GCS + BigQuery. Push le nombre de lignes via XCom."""
    from google.cloud import bigquery
    from src.ingestion.ingest import load_sport_to_bigquery, upload_to_gcs

    project_id = get_airflow_var("gcp_project_id")
    dataset_bronze = get_airflow_var("bq_dataset_bronze", "sport_bronze")

    sport_path = DATA_RAW_PATH / "Donnees_Sportive.xlsx"
    upload_to_gcs(str(sport_path), "donnees_sportive.xlsx")
    load_sport_to_bigquery(str(sport_path))

    client = bigquery.Client(project=project_id)
    table = client.get_table(f"{project_id}.{dataset_bronze}.sports_declared")
    row_count = table.num_rows

    metrics = {"table": "sports_declared", "rows_loaded": row_count, "status": "success"}
    context["ti"].xcom_push(key=XCOM_KEY_INGEST_SPORT, value=metrics)
    return metrics


def task_generate_strava(**context):
    """Génération Strava. Récupère les métriques d'ingestion via XCom."""
    from src.generation.generate_strava import (
        generate_all_activities,
        get_sportifs_from_bigquery,
        load_to_bigquery,
    )

    ti = context["ti"]
    rh_metrics = ti.xcom_pull(task_ids="ingest_rh_data", key=XCOM_KEY_INGEST_RH)
    sport_metrics = ti.xcom_pull(task_ids="ingest_sport_data", key=XCOM_KEY_INGEST_SPORT)
    print(f"📊 XCom reçu — RH: {rh_metrics}, Sport: {sport_metrics}")

    sportifs = get_sportifs_from_bigquery()
    all_activities = generate_all_activities(sportifs)
    load_to_bigquery(all_activities)

    metrics = {
        "activities_generated": len(all_activities),
        "sportifs_count": len(sportifs),
        "status": "success",
    }
    context["ti"].xcom_push(key=XCOM_KEY_STRAVA, value=metrics)
    return metrics


def task_validate_distances(**context):
    """Validation Google Maps. Push les anomalies via XCom."""
    from src.validation.validate_distances import (
        save_to_bigquery,
        validate_commute_declarations,
    )

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
    context["ti"].xcom_push(key=XCOM_KEY_VALIDATION, value=metrics)
    return metrics


def task_great_expectations(**context):
    """Great Expectations. Push les résultats via XCom."""
    from ge_tests.validate_bronze import run

    run()

    metrics = {"status": "success"}
    context["ti"].xcom_push(key=XCOM_KEY_GE, value=metrics)
    return metrics


def task_notify_slack(**context):
    """
    Notification Slack des dernières activités.
    Récupère les métriques de toutes les tâches précédentes via XCom
    et envoie un résumé du pipeline.
    """
    import time

    from src.notifications.send_recent_activities import get_recent_activities
    from src.notifications.slack_notifier import build_message, send_slack_message

    ti = context["ti"]

    rh_metrics = ti.xcom_pull(task_ids="ingest_rh_data", key=XCOM_KEY_INGEST_RH) or {}
    sport_metrics = ti.xcom_pull(task_ids="ingest_sport_data", key=XCOM_KEY_INGEST_SPORT) or {}
    strava_metrics = ti.xcom_pull(task_ids="generate_strava_simulation", key=XCOM_KEY_STRAVA) or {}
    validation_metrics = ti.xcom_pull(task_ids="validate_distances_google_maps", key=XCOM_KEY_VALIDATION) or {}

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
        bash_command=f"cd {DBT_PROJECT_PATH} && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_PATH} && dbt test --profiles-dir .",
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
        retries=2,
        trigger_rule="all_done",
    )

    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    # === Task dependencies ===
    start >> [ingest_rh, ingest_sport]
    [ingest_rh, ingest_sport] >> generate_strava
    generate_strava >> validate_distances
    validate_distances >> dbt_run >> dbt_test
    # Branches parallèles après dbt_test
    dbt_test >> run_great_expectations >> end
    dbt_test >> notify_slack >> end