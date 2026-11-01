"""
Notifications Slack — Messages automatiques pour chaque activité sportive.

Exemples de messages :
- "Bravo Juliette Mendes ! Tu viens de courir 10,8 km en 46 min ! 🔥🏅"
- "Magnifique Laurence Morvan ! Une randonnée de 10 km terminée ! 🌄"

Usage :
    from src.notifications.slack_notifier import build_message, send_slack_message

Mode démo (sans webhook Slack) :
    DEMO_MODE=true python -m src.notifications.slack_notifier
"""

import json
import os
import sys
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

# Ajoute la racine du projet au PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

load_dotenv()

# Mode démo : pas de connexion Slack
DEMO_MODE = os.getenv("DEMO_MODE", "false").lower() == "true"

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# Validation
if not DEMO_MODE and not SLACK_WEBHOOK_URL:
    print("⚠️  SLACK_WEBHOOK_URL non configuré — les messages seront affichés mais non envoyés")

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
    """Envoie un message via le webhook Slack (ou simule en mode démo)."""
    if DEMO_MODE:
        print(f"🎭 Mode démo — Message Slack simulé :")
        print(f"   {message}")
        return True
    
    if not SLACK_WEBHOOK_URL:
        print(f"⚠️  SLACK_WEBHOOK_URL non configuré. Message: {message}")
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
    # TODO: Implémenter la lecture depuis BigQuery
    print("🎭 Mode démo — Notification d'activités simulée")
    
    # Exemple de message de démo
    demo_message = build_message(
        prenom="Alice",
        nom="Dupont",
        sport_type="Course à pied",
        distance_m=5000,
        duration_s=1800,
        comment="Bonne séance ce matin !"
    )
    send_slack_message(demo_message)


def run_demo():
    """Fonction de démonstration."""
    print("=" * 60)
    print("📨 Notifications Slack — Démonstration")
    if DEMO_MODE:
        print("🎭 MODE DÉMO — Messages simulés, pas d'envoi réel")
    print("=" * 60)
    
    # Test des messages
    test_cases = [
        ("Alice", "Dupont", "Course à pied", 5000, 1800, "Bonne séance !"),
        ("Bob", "Martin", "Vélo", 15000, 3600, None),
        ("Charlie", "Durand", "Natation", None, 1200, "50 longueurs"),
        ("David", "Petit", "Escalade", None, 5400, "Nouvelle voie en 6b !"),
    ]
    
    print("\n📝 Messages de test :")
    for prenom, nom, sport, dist, dur, comment in test_cases:
        msg = build_message(prenom, nom, sport, dist, dur, comment)
        send_slack_message(msg)
        print()
    
    print("🎉 Démonstration terminée !")


if __name__ == "__main__":
    run_demo()
