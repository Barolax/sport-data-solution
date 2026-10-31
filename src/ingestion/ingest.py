"""
Ingestion des fichiers RH et Sportif
Excel → GCS (raw) → BigQuery (bronze)

Usage en local :
    python -m src.ingestion.ingest

Mode démo (sans cloud) :
    DEMO_MODE=true python -m src.ingestion.ingest
"""

import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Ajoute la racine du projet au PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

load_dotenv()

# Mode démo : pas de connexion cloud
DEMO_MODE = os.getenv("DEMO_MODE", "false").lower() == "true"

if not DEMO_MODE:
    from google.cloud import bigquery, storage

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BQ_DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE")

# Validation
if not DEMO_MODE and (not PROJECT_ID or not BUCKET_NAME or not BQ_DATASET_BRONZE):
    raise ValueError("""
    ❌ Configuration manquante.
    
    Variables requises :
    - GCP_PROJECT_ID
    - GCS_BUCKET_NAME  
    - BQ_DATASET_BRONZE
    
    Ou activer le mode démo : DEMO_MODE=true
    """)

# Chemins
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_RAW_PATH = PROJECT_ROOT / "data" / "raw"


def get_bq_client():
    """Initialise le client BigQuery."""
    if DEMO_MODE:
        raise RuntimeError("Mode démo : pas de connexion BigQuery")
    return bigquery.Client(project=PROJECT_ID)


def get_gcs_client():
    """Initialise le client GCS."""
    if DEMO_MODE:
        raise RuntimeError("Mode démo : pas de connexion GCS")
    return storage.Client(project=PROJECT_ID)


def upload_to_gcs(local_path: str, destination_blob: str) -> str:
    """Upload un fichier local vers GCS (zone raw)."""
    if DEMO_MODE:
        print(f"🎭 Mode démo : upload simulé {local_path} → gs://demo-bucket/raw/{destination_blob}")
        return f"gs://demo-bucket/raw/{destination_blob}"
    
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
    if DEMO_MODE:
        print(f"🎭 Mode démo : chargement simulé {local_path} → BigQuery employees")
        # Lecture réelle pour valider le fichier
        df = pd.read_excel(local_path, engine="openpyxl")
        print(f"   {len(df)} lignes détectées")
        return
    
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
    if DEMO_MODE:
        print(f"🎭 Mode démo : chargement simulé {local_path} → BigQuery sports_declared")
        df = pd.read_excel(local_path, engine="openpyxl")
        print(f"   {len(df)} lignes détectées")
        return
    
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
    rh_path = DATA_RAW_PATH / "Donnees_RH.xlsx"
    sport_path = DATA_RAW_PATH / "Donnees_Sportive.xlsx"

    print("=" * 50)
    print("📤 Upload vers GCS (raw)...")
    print("=" * 50)
    upload_to_gcs(str(rh_path), "donnees_rh.xlsx")
    upload_to_gcs(str(sport_path), "donnees_sportive.xlsx")

    print()
    print("=" * 50)
    print("📥 Chargement dans BigQuery (bronze)...")
    print("=" * 50)
    load_rh_to_bigquery(str(rh_path))
    load_sport_to_bigquery(str(sport_path))

    print()
    print("🎉 Ingestion terminée !")


if __name__ == "__main__":
    run_ingestion()
