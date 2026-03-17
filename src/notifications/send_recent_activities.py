"""
Démo Slack : envoie les N dernières activités sportives depuis BigQuery.

Usage :
    python -m src.notifications.send_recent_activities
    python -m src.notifications.send_recent_activities --limit 3
"""

import os
import argparse
import time
from google.cloud import bigquery
from dotenv import load_dotenv
from src.notifications.slack_notifier import build_message, send_slack_message

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE", "sport_bronze")


def get_recent_activities(limit: int = 5) -> list[dict]:
    """Récupère les N dernières activités avec les noms des salariés."""
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT
            a.id,
            a.id_salarie,
            e.prenom,
            e.nom,
            a.sport_type,
            a.distance_m,
            TIMESTAMP_DIFF(a.date_fin, a.date_debut, SECOND) AS duration_s,
            a.commentaire,
            a.date_debut
        FROM `{PROJECT_ID}.{BQ_DATASET_BRONZE}.strava_activities` a
        JOIN `{PROJECT_ID}.{BQ_DATASET_BRONZE}.employees` e
            ON a.id_salarie = e.id_salarie
        ORDER BY a.date_debut DESC
        LIMIT {limit}
    """
    results = client.query(query).result()
    return [dict(row) for row in results]


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=5, help="Nombre d'activités à envoyer")
    args = parser.parse_args()

    print(f"📨 Envoi des {args.limit} dernières activités sur Slack...\n")

    activities = get_recent_activities(args.limit)

    for act in activities:
        msg = build_message(
            prenom=act["prenom"],
            nom=act["nom"],
            sport_type=act["sport_type"],
            distance_m=act["distance_m"],
            duration_s=act["duration_s"],
            comment=act["commentaire"],
        )
        print(f"  → {msg[:80]}...")
        send_slack_message(msg)
        time.sleep(1)  # Pause entre chaque message pour pas flood Slack

    print(f"\n✅ {len(activities)} messages envoyés sur Slack !")


if __name__ == "__main__":
    run()