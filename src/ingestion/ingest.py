"""
Ingestion des fichiers RH et Sportif
Excel → GCS (raw) → BigQuery (bronze)
"""

import os
from google.cloud import storage, bigquery
from dotenv import load_dotenv

load_dotenv()

# --- Config ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BQ_DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE", "sport_bronze")


def upload_to_gcs(local_path: str, destination_blob: str) -> str:
    """Upload un fichier local vers GCS (zone raw)."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"raw/{destination_blob}")
    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded {local_path} → gs://{BUCKET_NAME}/raw/{destination_blob}")
    return f"gs://{BUCKET_NAME}/raw/{destination_blob}"


def load_excel_to_bigquery(gcs_uri: str, table_id: str) -> None:
    """Charge un fichier Excel depuis GCS vers BigQuery (bronze)."""
    # TODO: Implémenter le chargement
    # Option 1 : Lire depuis GCS avec pandas, puis load to BigQuery
    # Option 2 : Convertir en CSV/Parquet d'abord
    print(f"TODO: Load {gcs_uri} → {BQ_DATASET_BRONZE}.{table_id}")


def ingest_rh_data():
    """Pipeline d'ingestion des données RH."""
    local_path = "/opt/airflow/data/raw/Donnees_RH.xlsx"
    gcs_uri = upload_to_gcs(local_path, "donnees_rh.xlsx")
    load_excel_to_bigquery(gcs_uri, "employees")


def ingest_sport_data():
    """Pipeline d'ingestion des données sportives."""
    local_path = "/opt/airflow/data/raw/Donnees_Sportive.xlsx"
    gcs_uri = upload_to_gcs(local_path, "donnees_sportive.xlsx")
    load_excel_to_bigquery(gcs_uri, "sports_declared")
