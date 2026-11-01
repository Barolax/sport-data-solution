"""
Validation des distances domicile → bureau via Google Maps API
Vérifie la cohérence des déclarations de mode de transport sportif.

Règles :
- Marche/Running → max 15 km
- Vélo/Trottinette/Autres → max 25 km

Usage :
    python -m src.validation.validate_distances

Mode démo (sans APIs externes) :
    DEMO_MODE=true python -m src.validation.validate_distances
"""

import os
import sys
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Ajoute la racine du projet au PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

load_dotenv()

# Mode démo : pas de connexion aux APIs externes
DEMO_MODE = os.getenv("DEMO_MODE", "false").lower() == "true"

if not DEMO_MODE:
    import googlemaps
    from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
COMPANY_ADDRESS = os.getenv("COMPANY_ADDRESS", "1362 Av. des Platanes, 34970 Lattes")

# Validation
if not DEMO_MODE and (not PROJECT_ID or not BQ_DATASET_BRONZE or not GOOGLE_MAPS_API_KEY):
    raise ValueError("""
    ❌ Configuration manquante.
    
    Variables requises :
    - GCP_PROJECT_ID
    - BQ_DATASET_BRONZE
    - GOOGLE_MAPS_API_KEY
    
    Ou activer le mode démo : DEMO_MODE=true
    """)

# Seuils de distance par mode de transport (en km)
DISTANCE_THRESHOLDS = {
    "Marche/running": 15,
    "Vélo/Trottinette/Autres": 25,
}

# Seuil pour les suggestions de mobilité sportive
SUGGESTION_THRESHOLD_KM = 10

# Modes Google Maps correspondants
TRANSPORT_MODES = {
    "Marche/running": "walking",
    "Vélo/Trottinette/Autres": "bicycling",
}


# =============================================================
# DONNÉES DE DÉMO
# =============================================================

DEMO_COMMUTERS = pd.DataFrame({
    "id_salarie": [1, 2, 3],
    "prenom": ["Alice", "Bob", "Charlie"],
    "nom": ["Dupont", "Martin", "Durand"],
    "adresse_domicile": [
        "10 rue de la Paix, Montpellier",
        "25 avenue de la Mer, Palavas",
        "5 place de la Comédie, Montpellier"
    ],
    "moyen_deplacement": ["Marche/running", "Vélo/Trottinette/Autres", "Marche/running"]
})

DEMO_NON_COMMUTERS = pd.DataFrame({
    "id_salarie": [4, 5],
    "prenom": ["David", "Emma"],
    "nom": ["Petit", "Roux"],
    "adresse_domicile": [
        "50 avenue de Toulouse, Montpellier",
        "8 rue de l'Université, Montpellier"
    ],
    "moyen_deplacement": ["Voiture", "Transports en commun"]
})


def get_gmaps_client():
    """Initialise le client Google Maps."""
    if DEMO_MODE:
        raise RuntimeError("Mode démo : pas de connexion Google Maps")
    return googlemaps.Client(key=GOOGLE_MAPS_API_KEY)


def get_bq_client():
    """Initialise le client BigQuery."""
    if DEMO_MODE:
        raise RuntimeError("Mode démo : pas de connexion BigQuery")
    return bigquery.Client(project=PROJECT_ID)


def get_sport_commuters() -> pd.DataFrame:
    """
    Récupère les salariés ayant déclaré un mode de transport sportif
    depuis BigQuery (bronze).
    """
    if DEMO_MODE:
        print("🎭 Mode démo : utilisation des commuters de démo")
        return DEMO_COMMUTERS.copy()
    
    client = get_bq_client()
    query = f"""
        SELECT
            id_salarie,
            prenom,
            nom,
            adresse_domicile,
            moyen_deplacement
        FROM `{PROJECT_ID}.{BQ_DATASET_BRONZE}.employees`
        WHERE moyen_deplacement IN ('Marche/running', 'Vélo/Trottinette/Autres')
        ORDER BY id_salarie
    """
    return client.query(query).to_dataframe()


def get_non_sport_commuters() -> pd.DataFrame:
    """
    Récupère les salariés qui NE viennent PAS en mode sportif
    (voiture ou transports en commun).
    """
    if DEMO_MODE:
        print("🎭 Mode démo : utilisation des non-commuters de démo")
        return DEMO_NON_COMMUTERS.copy()
    
    client = get_bq_client()
    query = f"""
        SELECT
            id_salarie,
            prenom,
            nom,
            adresse_domicile,
            moyen_deplacement
        FROM `{PROJECT_ID}.{BQ_DATASET_BRONZE}.employees`
        WHERE moyen_deplacement NOT IN ('Marche/running', 'Vélo/Trottinette/Autres')
        ORDER BY id_salarie
    """
    return client.query(query).to_dataframe()


def calculate_distance(origin: str, mode: str) -> dict:
    """
    Calcule la distance entre une adresse et le bureau.

    Returns:
        Dict avec distance_km, duration_text, status
    """
    if DEMO_MODE:
        # Simulation de distances réalistes
        import random
        random.seed(hash(origin) % 1000)  # Seed basé sur l'adresse pour cohérence
        distance_km = round(random.uniform(2, 30), 2)
        duration_min = int(distance_km * 3)  # ~20 km/h en moyenne
        return {
            "distance_km": distance_km,
            "duration_text": f"{duration_min} min",
            "status": "OK",
        }
    
    try:
        client = get_gmaps_client()
        result = client.distance_matrix(
            origins=[origin],
            destinations=[COMPANY_ADDRESS],
            mode=mode,
        )
        element = result["rows"][0]["elements"][0]

        if element["status"] != "OK":
            return {
                "distance_km": None,
                "duration_text": None,
                "status": element["status"],
            }

        distance_km = round(element["distance"]["value"] / 1000, 2)
        duration_text = element["duration"]["text"]

        return {
            "distance_km": distance_km,
            "duration_text": duration_text,
            "status": "OK",
        }
    except Exception as e:
        return {
            "distance_km": None,
            "duration_text": None,
            "status": f"ERROR: {str(e)}",
        }


def validate_commute_declarations() -> pd.DataFrame:
    """
    Valide les déclarations de trajet sportif de tous les salariés éligibles.

    Returns:
        DataFrame avec les résultats de validation
    """
    print("📥 Récupération des salariés avec trajet sportif...")
    commuters = get_sport_commuters()
    print(f"   {len(commuters)} salariés à valider\n")

    results = []

    for _, row in commuters.iterrows():
        mode = TRANSPORT_MODES.get(row["moyen_deplacement"], "walking")
        threshold = DISTANCE_THRESHOLDS.get(row["moyen_deplacement"], 15)

        # Calcul de distance (réel ou simulé)
        dist_info = calculate_distance(row["adresse_domicile"], mode)

        # Vérification du seuil
        is_valid = None
        anomaly_reason = None

        if dist_info["distance_km"] is not None:
            if dist_info["distance_km"] <= threshold:
                is_valid = True
            else:
                is_valid = False
                anomaly_reason = (
                    f"Distance {dist_info['distance_km']} km > seuil {threshold} km "
                    f"pour {row['moyen_deplacement']}"
                )
        else:
            is_valid = False
            anomaly_reason = f"Impossible de calculer la distance ({dist_info['status']})"

        # Affichage en temps réel
        status_icon = "✅" if is_valid else "❌"
        dist_str = f"{dist_info['distance_km']} km" if dist_info["distance_km"] else "N/A"
        print(
            f"   {status_icon} {row['prenom']} {row['nom']} — "
            f"{row['moyen_deplacement']} — {dist_str} "
            f"(seuil: {threshold} km)"
        )

        results.append({
            "id_salarie": row["id_salarie"],
            "prenom": row["prenom"],
            "nom": row["nom"],
            "adresse_domicile": row["adresse_domicile"],
            "moyen_deplacement": row["moyen_deplacement"],
            "google_maps_mode": mode,
            "distance_km": dist_info["distance_km"],
            "duration_text": dist_info["duration_text"],
            "threshold_km": threshold,
            "is_valid": is_valid,
            "anomaly_reason": anomaly_reason,
        })

        # Pause pour respecter les rate limits de l'API (seulement en mode réel)
        if not DEMO_MODE:
            time.sleep(0.1)

    return pd.DataFrame(results)


def save_to_bigquery(df: pd.DataFrame, table_name: str = "commute_distances") -> None:
    """Sauvegarde les résultats de validation dans BigQuery (bronze)."""
    if DEMO_MODE:
        print(f"\n🎭 Mode démo : sauvegarde simulée de {len(df)} lignes → {table_name}")
        print(f"   Aperçu :")
        print(df.head().to_string())
        return
    
    client = get_bq_client()
    table_id = f"{PROJECT_ID}.{BQ_DATASET_BRONZE}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"\n✅ Loaded {table.num_rows} rows → {table_id}")


def check_sport_suggestions() -> pd.DataFrame:
    """
    Vérifie les salariés non-sportifs qui habitent à moins de 10km.
    Suggère un mode de transport sportif pour ces salariés.
    """
    print("\n📥 Récupération des salariés non-sportifs...")
    non_sport = get_non_sport_commuters()
    print(f"   {len(non_sport)} salariés à analyser\n")

    suggestions = []

    for _, row in non_sport.iterrows():
        # On calcule la distance en vélo (mode le plus large)
        dist_info = calculate_distance(row["adresse_domicile"], "bicycling")

        distance_km = dist_info["distance_km"]
        could_bike = distance_km is not None and distance_km <= SUGGESTION_THRESHOLD_KM
        could_walk = distance_km is not None and distance_km <= 5  # Marche raisonnable ≤ 5km

        if could_bike:
            if could_walk:
                suggestion = "Marche ou vélo possible"
                icon = "🚶"
            else:
                suggestion = "Vélo ou trottinette possible"
                icon = "🚴"
            print(f"   {icon} {row['prenom']} {row['nom']} — {distance_km} km — {suggestion}")
        else:
            suggestion = None
            dist_str = f"{distance_km} km" if distance_km else "N/A"
            print(f"   ⏭️  {row['prenom']} {row['nom']} — {dist_str} — Trop loin")

        suggestions.append({
            "id_salarie": row["id_salarie"],
            "prenom": row["prenom"],
            "nom": row["nom"],
            "adresse_domicile": row["adresse_domicile"],
            "moyen_deplacement_actuel": row["moyen_deplacement"],
            "distance_km": distance_km,
            "could_bike": could_bike,
            "could_walk": could_walk,
            "suggestion": suggestion,
        })

        if not DEMO_MODE:
            time.sleep(0.1)

    return pd.DataFrame(suggestions)


def run():
    """Point d'entrée principal."""
    print("=" * 60)
    print("📍 Validation des distances domicile → bureau")
    if DEMO_MODE:
        print("🎭 MODE DÉMO — Données fictives, pas d'appels API")
    print(f"   Entreprise : {COMPANY_ADDRESS}")
    print("=" * 60)

    # === PARTIE 1 : Validation des trajets sportifs ===
    print("\n" + "=" * 60)
    print("📋 PARTIE 1 — Validation des déclarations sportives")
    print("=" * 60)

    results_df = validate_commute_declarations()

    total = len(results_df)
    valid = results_df["is_valid"].sum()
    anomalies = total - valid

    print(f"\n   Total salariés vérifiés : {total}")
    print(f"   ✅ Déclarations valides  : {valid}")
    print(f"   ❌ Anomalies détectées   : {anomalies}")

    if anomalies > 0:
        print(f"\n⚠️  Détail des anomalies :")
        anomaly_df = results_df[results_df["is_valid"] == False]
        for _, row in anomaly_df.iterrows():
            print(
                f"   • {row['prenom']} {row['nom']} — "
                f"{row['moyen_deplacement']} — {row['anomaly_reason']}"
            )

    print(f"\n📤 Sauvegarde des résultats...")
    save_to_bigquery(results_df, "commute_distances")

    # === PARTIE 2 : Suggestions mobilité sportive ===
    print("\n" + "=" * 60)
    print(f"💡 PARTIE 2 — Suggestions mobilité sportive (< {SUGGESTION_THRESHOLD_KM} km)")
    print("=" * 60)

    suggestions_df = check_sport_suggestions()

    eligible = suggestions_df["could_bike"].sum()
    walkable = suggestions_df["could_walk"].sum()

    print(f"\n   Total salariés non-sportifs analysés : {len(suggestions_df)}")
    print(f"   🚴 Pourraient venir à vélo           : {eligible}")
    print(f"   🚶 Pourraient venir à pied            : {walkable}")

    print(f"\n📤 Sauvegarde des suggestions...")
    save_to_bigquery(suggestions_df, "commute_suggestions")

    print(f"\n🎉 Validation et suggestions terminées !")


if __name__ == "__main__":
    run()
