"""
Ingestion des fichiers RH et Sportif
Excel → GCS (raw) → BigQuery (bronze)

Usage en local :
    python -m src.ingestion.ingest
"""

import os
import pandas as pd
from google.cloud import storage, bigquery
from dotenv import load_dotenv

load_dotenv()

# --- Config ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BQ_DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE", "sport_bronze")


def get_bq_client() -> bigquery.Client:
    """Initialise le client BigQuery."""
    return bigquery.Client(project=PROJECT_ID)


def get_gcs_client() -> storage.Client:
    """Initialise le client GCS."""
    return storage.Client(project=PROJECT_ID)


def upload_to_gcs(local_path: str, destination_blob: str) -> str:
    """Upload un fichier local vers GCS (zone raw)."""
    client = get_gcs_client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"raw/{destination_blob}")
    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded {local_path} → gs://{BUCKET_NAME}/raw/{destination_blob}")
    return f"gs://{BUCKET_NAME}/raw/{destination_blob}"


def load_rh_to_bigquery(local_path: str) -> None:
    """
    Charge le fichier RH Excel dans BigQuery (bronze).
    Table : sport_bronze.employees
    """
    client = get_bq_client()
    table_id = f"{PROJECT_ID}.{BQ_DATASET_BRONZE}.employees"

    # Lecture Excel
    df = pd.read_excel(local_path, engine="openpyxl")

    # Renommage des colonnes pour BigQuery (pas d'espaces, pas d'accents)
    df.columns = [
        "id_salarie",
        "nom",
        "prenom",
        "date_naissance",
        "bu",
        "date_embauche",
        "salaire_brut",
        "type_contrat",
        "nb_jours_cp",
        "adresse_domicile",
        "moyen_deplacement",
    ]

    # Typage
    df["id_salarie"] = df["id_salarie"].astype(int)
    df["salaire_brut"] = df["salaire_brut"].astype(float)
    df["nb_jours_cp"] = df["nb_jours_cp"].astype(int)
    df["date_naissance"] = pd.to_datetime(df["date_naissance"])
    df["date_embauche"] = pd.to_datetime(df["date_embauche"])

    # Schéma BigQuery
    schema = [
        bigquery.SchemaField("id_salarie", "INTEGER"),
        bigquery.SchemaField("nom", "STRING"),
        bigquery.SchemaField("prenom", "STRING"),
        bigquery.SchemaField("date_naissance", "DATE"),
        bigquery.SchemaField("bu", "STRING"),
        bigquery.SchemaField("date_embauche", "DATE"),
        bigquery.SchemaField("salaire_brut", "FLOAT"),
        bigquery.SchemaField("type_contrat", "STRING"),
        bigquery.SchemaField("nb_jours_cp", "INTEGER"),
        bigquery.SchemaField("adresse_domicile", "STRING"),
        bigquery.SchemaField("moyen_deplacement", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"✅ Loaded {table.num_rows} rows → {table_id}")


def load_sport_to_bigquery(local_path: str) -> None:
    """
    Charge le fichier Sportif Excel dans BigQuery (bronze).
    Table : sport_bronze.sports_declared
    """
    client = get_bq_client()
    table_id = f"{PROJECT_ID}.{BQ_DATASET_BRONZE}.sports_declared"

    # Lecture Excel
    df = pd.read_excel(local_path, engine="openpyxl")

    # Renommage des colonnes
    df.columns = [
        "id_salarie",
        "sport_pratique",
    ]

    # Typage
    df["id_salarie"] = df["id_salarie"].astype(int)
    df["sport_pratique"] = df["sport_pratique"].where(df["sport_pratique"].notna(), None)

    # Schéma BigQuery
    schema = [
        bigquery.SchemaField("id_salarie", "INTEGER"),
        bigquery.SchemaField("sport_pratique", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"✅ Loaded {table.num_rows} rows → {table_id}")


def run_ingestion():
    """Lance l'ingestion complète : GCS + BigQuery."""
    rh_path = "data/raw/Donnees_RH.xlsx"
    sport_path = "data/raw/Donnees_Sportive.xlsx"

    print("=" * 50)
    print("📤 Upload vers GCS (raw)...")
    print("=" * 50)
    upload_to_gcs(rh_path, "donnees_rh.xlsx")
    upload_to_gcs(sport_path, "donnees_sportive.xlsx")

    print()
    print("=" * 50)
    print("📥 Chargement dans BigQuery (bronze)...")
    print("=" * 50)
    load_rh_to_bigquery(rh_path)
    load_sport_to_bigquery(sport_path)

    print()
    print("🎉 Ingestion terminée !")


if __name__ == "__main__":
    run_ingestion()