"""
Génération de données simulées Strava avec PySpark
Produit ~5000+ lignes d'activités sportives sur 12 mois
basées sur les profils sportifs des salariés.
"""

import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()


def create_spark_session() -> SparkSession:
    """Crée une session Spark configurée pour BigQuery."""
    return (
        SparkSession.builder
        .appName("SportDataSolution-StravaSimulation")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )


def generate_strava_activities(spark: SparkSession) -> None:
    """
    Génère les activités sportives simulées.

    Métadonnées par enregistrement :
    - ID : identifiant unique de l'activité
    - ID salarié : référence vers la table employees
    - Date de début : datetime de l'activité
    - Type : type de sport (Course à pied, Randonnée, Vélo, etc.)
    - Distance (m) : distance en mètres (vide si non pertinent, ex: escalade)
    - Date de fin : datetime de fin
    - Commentaire : texte optionnel

    Règles de génération :
    - Basé sur les 95 salariés ayant déclaré un sport
    - 12 mois d'historique
    - Minimum 5000 lignes au total
    - Fréquence réaliste selon le type de sport
    - Distances et durées cohérentes
    """
    # TODO: Implémenter la génération
    # 1. Lire les salariés sportifs depuis BigQuery (bronze)
    # 2. Pour chaque sportif, générer des activités réalistes
    # 3. Écrire le résultat dans BigQuery (bronze)
    print("TODO: Generate Strava simulation data")


def run():
    """Point d'entrée principal."""
    spark = create_spark_session()
    try:
        generate_strava_activities(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
