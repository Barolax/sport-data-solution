"""
Génération de données simulées Strava avec PySpark
Produit ~5000+ lignes d'activités sportives sur 12 mois
basées sur les profils sportifs des salariés.

Usage :
    python -m src.generation.generate_strava
"""

import os
import random
from datetime import datetime, timedelta
from faker import Faker
from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    FloatType, TimestampType
)
from dotenv import load_dotenv

load_dotenv()

# --- Config ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE", "sport_bronze")
SIMULATION_MONTHS = int(os.getenv("STRAVA_SIMULATION_MONTHS", 12))

fake = Faker("fr_FR")
random.seed(42)

# =============================================================
# Profils sportifs : fréquence, distance, allure par sport
# =============================================================
# Chaque profil définit :
#   - freq_min/max : nombre de séances par mois
#   - dist_min/max : distance en mètres (None si non applicable)
#   - pace_min/max : allure en secondes par km (pour calculer la durée)
#   - duration_min/max : durée en secondes (si pas de distance)
#   - comments : exemples de commentaires réalistes

SPORT_PROFILES = {
    "Runing": {
        "display_name": "Course à pied",
        "freq_min": 4, "freq_max": 10,
        "dist_min": 3000, "dist_max": 21000,
        "pace_min": 270, "pace_max": 420,  # 4:30 à 7:00 /km
        "comments": [
            "Bonne séance ce matin !",
            "Reprise après une semaine de repos",
            "Nouveau record personnel 💪",
            "Sortie tranquille, récup active",
            "Fractionné au parc, ça pique !",
            "Belle sortie sous le soleil ☀️",
            "Première sortie de la semaine",
            "Entraînement pour le semi-marathon",
            "Run matinal, la ville est calme",
            "Séance difficile mais satisfaisante",
        ],
    },
    "Randonnée": {
        "display_name": "Randonnée",
        "freq_min": 2, "freq_max": 5,
        "dist_min": 5000, "dist_max": 25000,
        "pace_min": 500, "pace_max": 900,  # 8:20 à 15:00 /km
        "comments": [
            "Magnifique sentier avec vue sur la mer",
            "Rando en famille, les enfants ont adoré",
            "Nouveau spot à découvrir, je recommande !",
            "Chemin un peu boueux mais ça valait le coup",
            "Pause pique-nique au sommet 🌄",
            "Superbe boucle en forêt",
            "Rando du dimanche, parfait pour décompresser",
        ],
    },
    "Tennis": {
        "display_name": "Tennis",
        "freq_min": 4, "freq_max": 6,
        "dist_min": None, "dist_max": None,
        "duration_min": 3600, "duration_max": 7200,
        "comments": [
            "Match serré, victoire en 3 sets !",
            "Entraînement avec le coach",
            "Double avec les collègues 🎾",
            "Travail du service aujourd'hui",
            "Belle session malgré le vent",
        ],
    },
    "Natation": {
        "display_name": "Natation",
        "freq_min": 3, "freq_max": 7,
        "dist_min": 500, "dist_max": 3000,
        "pace_min": 100, "pace_max": 180,  # secondes par 100m → converti par km
        "comments": [
            "50 longueurs ce matin, bon rythme",
            "Séance technique : crawl et dos",
            "Piscine bondée mais bonne séance",
            "Travail des intervalles en bassin",
            "Nage en eau libre, sensation de liberté 🏊",
        ],
    },
    "Football": {
        "display_name": "Football",
        "freq_min": 1, "freq_max": 3,
        "dist_min": None, "dist_max": None,
        "duration_min": 3600, "duration_max": 5400,
        "comments": [
            "Match du mercredi soir, on a gagné 3-1 !",
            "Entraînement collectif au stade",
            "Petit foot entre collègues ⚽",
            "Tournoi inter-entreprises ce weekend",
        ],
    },
    "Rugby": {
        "display_name": "Rugby",
        "freq_min": 1, "freq_max": 3,
        "dist_min": None, "dist_max": None,
        "duration_min": 3600, "duration_max": 5400,
        "comments": [
            "Entraînement physique intense",
            "Match du weekend, belle victoire 🏉",
            "Travail des touches et mêlées",
            "Session cardio + plaquages",
        ],
    },
    "Badminton": {
        "display_name": "Badminton",
        "freq_min": 1, "freq_max": 4,
        "dist_min": None, "dist_max": None,
        "duration_min": 2700, "duration_max": 5400,
        "comments": [
            "Session de double, très fun !",
            "Entraînement au club 🏸",
            "Tournoi interne, demi-finale atteinte",
            "Smash en progrès !",
        ],
    },
    "Voile": {
        "display_name": "Voile",
        "freq_min": 1, "freq_max": 3,
        "dist_min": 5000, "dist_max": 30000,
        "pace_min": 300, "pace_max": 800,
        "comments": [
            "Sortie en mer magnifique ⛵",
            "Vent parfait aujourd'hui, belle nav",
            "Régate du club, on finit 3ème !",
            "Navigation au coucher de soleil 🌅",
        ],
    },
    "Judo": {
        "display_name": "Judo",
        "freq_min": 2, "freq_max": 4,
        "dist_min": None, "dist_max": None,
        "duration_min": 3600, "duration_max": 5400,
        "comments": [
            "Cours technique : projections",
            "Randori intense ce soir 🥋",
            "Préparation pour la compétition",
            "Bonne séance de ne-waza",
        ],
    },
    "Boxe": {
        "display_name": "Boxe",
        "freq_min": 2, "freq_max": 5,
        "dist_min": None, "dist_max": None,
        "duration_min": 2700, "duration_max": 5400,
        "comments": [
            "Shadow boxing + sac de frappe 🥊",
            "Sparring avec le coach",
            "Séance cardio-boxe, en sueur !",
            "Travail des enchaînements",
        ],
    },
    "Escalade": {
        "display_name": "Escalade",
        "freq_min": 1, "freq_max": 4,
        "dist_min": None, "dist_max": None,
        "duration_min": 3600, "duration_max": 7200,
        "comments": [
            "Nouvelle voie en 6b, validée ! 🧗",
            "Session bloc entre amis",
            "Falaise ce weekend, top conditions",
            "Travail des dévers aujourd'hui",
        ],
    },
    "Triathlon": {
        "display_name": "Triathlon",
        "freq_min": 3, "freq_max": 7,
        "dist_min": 5000, "dist_max": 40000,
        "pace_min": 200, "pace_max": 400,
        "comments": [
            "Enchaînement natation-vélo ce matin",
            "Brick run après le vélo, les jambes en coton",
            "Préparation pour le triathlon de Montpellier",
            "Séance complète : swim-bike-run 🏊🚴🏃",
        ],
    },
    "Équitation": {
        "display_name": "Équitation",
        "freq_min": 1, "freq_max": 3,
        "dist_min": None, "dist_max": None,
        "duration_min": 3600, "duration_max": 7200,
        "comments": [
            "Balade en forêt avec Luna 🐴",
            "Cours de dressage au centre équestre",
            "Séance de saut d'obstacles",
            "Randonnée équestre, magnifique paysage",
        ],
    },
    "Tennis de table": {
        "display_name": "Tennis de table",
        "freq_min": 1, "freq_max": 4,
        "dist_min": None, "dist_max": None,
        "duration_min": 1800, "duration_max": 3600,
        "comments": [
            "Match au club, victoire serrée ! 🏓",
            "Entraînement sur le revers",
            "Tournoi entre collègues",
        ],
    },
    "Basketball": {
        "display_name": "Basketball",
        "freq_min": 1, "freq_max": 3,
        "dist_min": None, "dist_max": None,
        "duration_min": 3600, "duration_max": 5400,
        "comments": [
            "Match du vendredi soir 🏀",
            "Shootaround au playground",
            "3v3 entre collègues, ambiance top",
        ],
    },
}


def get_sportifs_from_bigquery() -> list[dict]:
    """
    Récupère les salariés sportifs depuis BigQuery (bronze).
    Jointure employees + sports_declared.
    """
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT
            e.id_salarie,
            e.prenom,
            e.nom,
            s.sport_pratique
        FROM `{PROJECT_ID}.{BQ_DATASET_BRONZE}.employees` e
        INNER JOIN `{PROJECT_ID}.{BQ_DATASET_BRONZE}.sports_declared` s
            ON e.id_salarie = s.id_salarie
        WHERE s.sport_pratique IS NOT NULL
    """
    results = client.query(query).result()
    return [dict(row) for row in results]


def generate_activity_for_month(
    employee: dict,
    profile: dict,
    year: int,
    month: int,
    activity_id_start: int,
) -> list[dict]:
    """
    Génère les activités d'un salarié pour un mois donné.
    """
    import calendar

    activities = []
    nb_activities = random.randint(profile["freq_min"], profile["freq_max"])
    days_in_month = calendar.monthrange(year, month)[1]

    for i in range(nb_activities):
        # Date de début aléatoire dans le mois
        day = random.randint(1, days_in_month)
        hour = random.choice([6, 7, 8, 9, 12, 17, 18, 19, 20])
        minute = random.randint(0, 59)
        started_at = datetime(year, month, day, hour, minute)

        # Distance et durée selon le profil
        distance_m = None
        duration_s = None

        if profile.get("dist_min") is not None:
            # Sport avec distance
            distance_m = round(random.uniform(profile["dist_min"], profile["dist_max"]))
            pace = random.uniform(profile["pace_min"], profile["pace_max"])
            duration_s = int((distance_m / 1000) * pace)
        else:
            # Sport sans distance (durée directe)
            duration_s = random.randint(profile["duration_min"], profile["duration_max"])

        ended_at = started_at + timedelta(seconds=duration_s)

        # Commentaire (30% de chance d'en avoir un)
        comment = None
        if random.random() < 0.30 and profile.get("comments"):
            comment = random.choice(profile["comments"])

        activities.append({
            "id": activity_id_start + i,
            "id_salarie": employee["id_salarie"],
            "date_debut": started_at,
            "sport_type": profile.get("display_name", employee["sport_pratique"]),
            "distance_m": float(distance_m) if distance_m else None,
            "date_fin": ended_at,
            "commentaire": comment,
            "source": "simulation",
        })

    return activities


def generate_all_activities(sportifs: list[dict]) -> list[dict]:
    """
    Génère toutes les activités pour tous les sportifs sur 12 mois.
    """
    all_activities = []
    activity_id = 1

    # Calculer les 12 derniers mois
    now = datetime.now()
    months = []
    for i in range(SIMULATION_MONTHS, 0, -1):
        dt = now - timedelta(days=30 * i)
        months.append((dt.year, dt.month))

    print(f"📅 Génération sur {len(months)} mois : {months[0]} → {months[-1]}")
    print(f"👥 {len(sportifs)} sportifs à simuler")

    for employee in sportifs:
        sport = employee["sport_pratique"]
        profile = SPORT_PROFILES.get(sport)

        if not profile:
            print(f"⚠️  Sport inconnu : {sport} pour {employee['prenom']} {employee['nom']}, skip")
            continue

        for year, month in months:
            activities = generate_activity_for_month(
                employee, profile, year, month, activity_id
            )
            all_activities.extend(activities)
            activity_id += len(activities)

    print(f"✅ {len(all_activities)} activités générées")
    return all_activities


def create_spark_session() -> SparkSession:
    """Crée une session Spark locale."""
    return (
        SparkSession.builder
        .appName("SportDataSolution-StravaSimulation")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )


def activities_to_spark_df(spark: SparkSession, activities: list[dict]):
    """Convertit la liste d'activités en DataFrame Spark."""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("id_salarie", IntegerType(), False),
        StructField("date_debut", TimestampType(), False),
        StructField("sport_type", StringType(), False),
        StructField("distance_m", FloatType(), True),
        StructField("date_fin", TimestampType(), False),
        StructField("commentaire", StringType(), True),
        StructField("source", StringType(), True),
    ])

    return spark.createDataFrame(activities, schema=schema)


def load_to_bigquery(activities: list[dict]) -> None:
    """Charge les activités générées dans BigQuery (bronze)."""
    import pandas as pd

    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{BQ_DATASET_BRONZE}.strava_activities"

    df = pd.DataFrame(activities)

    schema = [
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("id_salarie", "INTEGER"),
        bigquery.SchemaField("date_debut", "TIMESTAMP"),
        bigquery.SchemaField("sport_type", "STRING"),
        bigquery.SchemaField("distance_m", "FLOAT"),
        bigquery.SchemaField("date_fin", "TIMESTAMP"),
        bigquery.SchemaField("commentaire", "STRING"),
        bigquery.SchemaField("source", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"✅ Loaded {table.num_rows} rows → {table_id}")


def run():
    """Point d'entrée principal."""
    print("=" * 60)
    print("🏃 Génération des données Strava simulées (PySpark)")
    print("=" * 60)

    # 1. Récupérer les sportifs depuis BigQuery
    print("\n📥 Lecture des sportifs depuis BigQuery (bronze)...")
    sportifs = get_sportifs_from_bigquery()

    # 2. Générer les activités
    print(f"\n⚡ Génération des activités...")
    all_activities = generate_all_activities(sportifs)

    # 3. Créer un DataFrame Spark (pour montrer l'usage de PySpark)
    print(f"\n🔧 Création du DataFrame Spark...")
    spark = create_spark_session()
    try:
        spark_df = activities_to_spark_df(spark, all_activities)
        print(f"   Schema Spark :")
        spark_df.printSchema()
        print(f"   Aperçu (5 premières lignes) :")
        spark_df.show(5, truncate=False)
        print(f"   Stats par sport :")
        spark_df.groupBy("sport_type").count().orderBy("count", ascending=False).show()
    finally:
        spark.stop()

    # 4. Charger dans BigQuery
    print(f"\n📤 Chargement dans BigQuery (bronze)...")
    load_to_bigquery(all_activities)

    print(f"\n🎉 Génération terminée ! {len(all_activities)} activités sur {SIMULATION_MONTHS} mois.")


if __name__ == "__main__":
    run()