"""
Notifications Slack — Messages automatiques pour chaque activité sportive.

Exemples de messages :
- "Bravo Juliette Mendes ! Tu viens de courir 10,8 km en 46 min ! 🔥🏅"
- "Magnifique Laurence Morvan ! Une randonnée de 10 km terminée ! 🌄"
"""

import os
import json
import urllib.request
from dotenv import load_dotenv

load_dotenv()

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# Emojis par type de sport
SPORT_EMOJIS = {
    "Course à pied": "🏃🔥",
    "Runing": "🏃🔥",
    "Running": "🏃🔥",
    "Randonnée": "🌄⛰️",
    "Vélo": "🚴💨",
    "Natation": "🏊💧",
    "Tennis": "🎾🏆",
    "Football": "⚽🏟️",
    "Rugby": "🏉💪",
    "Badminton": "🏸✨",
    "Escalade": "🧗🪨",
    "Triathlon": "🏊🚴🏃",
    "Boxe": "🥊💥",
    "Judo": "🥋🤼",
    "Basketball": "🏀🔥",
    "Voile": "⛵🌊",
    "Équitation": "🐴🏇",
    "Tennis de table": "🏓⚡",
}


def format_duration(seconds: int) -> str:
    """Convertit des secondes en format lisible (ex: '46 min', '1h12')."""
    if seconds < 3600:
        return f"{seconds // 60} min"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours}h{minutes:02d}" if minutes else f"{hours}h"


def build_message(
    prenom: str,
    nom: str,
    sport_type: str,
    distance_m: float | None,
    duration_s: int,
    comment: str | None = None,
) -> str:
    """Construit le message Slack pour une activité."""
    emojis = SPORT_EMOJIS.get(sport_type, "🏅💪")
    duration_str = format_duration(duration_s)

    if distance_m and distance_m > 0:
        distance_km = round(distance_m / 1000, 1)
        msg = f"Bravo {prenom} {nom} ! Tu viens de faire {distance_km} km de {sport_type.lower()} en {duration_str} ! {emojis}"
    else:
        msg = f"Bravo {prenom} {nom} ! Belle séance de {sport_type.lower()} ({duration_str}) ! {emojis}"

    if comment:
        msg += f'\n> "{comment}"'

    return msg


def send_slack_message(message: str) -> bool:
    """Envoie un message via le webhook Slack."""
    if not SLACK_WEBHOOK_URL:
        print(f"⚠️ SLACK_WEBHOOK_URL non configuré. Message: {message}")
        return False

    payload = json.dumps({"text": message}).encode("utf-8")
    req = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as response:
            return response.status == 200
    except Exception as e:
        print(f"❌ Erreur envoi Slack: {e}")
        return False


def notify_new_activities() -> None:
    """Envoie les notifications Slack pour les nouvelles activités."""
    # TODO: Implémenter
    # 1. Lire les activités récentes depuis BigQuery
    # 2. Joindre avec les noms des salariés
    # 3. Construire et envoyer un message par activité
    print("TODO: Notify new activities on Slack")
