"""
Validation des distances domicile → bureau via Google Maps API
Vérifie la cohérence des déclarations de mode de transport sportif.

Règles :
- Marche/Running → max 15 km
- Vélo/Trottinette/Autres → max 25 km
"""

import os
import googlemaps
from dotenv import load_dotenv

load_dotenv()

# --- Config ---
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
COMPANY_ADDRESS = os.getenv("COMPANY_ADDRESS", "1362 Av. des Platanes, 34970 Lattes")

# Seuils de distance par mode de transport (en km)
DISTANCE_THRESHOLDS = {
    "Marche/running": 15,
    "Vélo/Trottinette/Autres": 25,
}

# Modes Google Maps correspondants
TRANSPORT_MODES = {
    "Marche/running": "walking",
    "Vélo/Trottinette/Autres": "bicycling",
}


def get_gmaps_client() -> googlemaps.Client:
    """Initialise le client Google Maps."""
    return googlemaps.Client(key=GOOGLE_MAPS_API_KEY)


def calculate_distance(client: googlemaps.Client, origin: str, mode: str) -> float | None:
    """
    Calcule la distance entre une adresse et le bureau.

    Args:
        client: Client Google Maps
        origin: Adresse du domicile du salarié
        mode: Mode de transport Google Maps (walking, bicycling)

    Returns:
        Distance en km, ou None si erreur
    """
    try:
        result = client.distance_matrix(
            origins=[origin],
            destinations=[COMPANY_ADDRESS],
            mode=mode,
        )
        distance_m = result["rows"][0]["elements"][0]["distance"]["value"]
        return distance_m / 1000  # Conversion en km
    except (KeyError, IndexError) as e:
        print(f"⚠️ Erreur calcul distance pour {origin}: {e}")
        return None


def validate_commute_declarations() -> list[dict]:
    """
    Valide les déclarations de trajet sportif de tous les salariés éligibles.

    Returns:
        Liste des anomalies détectées
    """
    # TODO: Implémenter
    # 1. Lire les salariés avec transport sportif depuis BigQuery
    # 2. Pour chaque salarié, calculer la distance via Google Maps
    # 3. Comparer avec le seuil du mode de transport
    # 4. Retourner les anomalies
    print("TODO: Validate commute declarations")
    return []
