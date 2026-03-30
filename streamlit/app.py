"""
Streamlit — Interface de déclaration d'activités sportives
Permet aux employés de déclarer une activité qui sera ajoutée dans BigQuery
et publiée automatiquement sur Slack.

Usage :
    streamlit run streamlit/app.py
"""

import os
import uuid
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE", "sport_bronze")


# =============================================================
# BIGQUERY
# =============================================================

def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)


@st.cache_data(ttl=300)
def get_employees():
    """Récupère la liste des employés depuis BigQuery."""
    client = get_bq_client()
    query = f"""
        SELECT id_salarie, prenom, nom
        FROM `{PROJECT_ID}.{BQ_DATASET_BRONZE}.employees`
        ORDER BY nom, prenom
    """
    return client.query(query).to_dataframe()


def get_employee_activities(employee_id: int):
    """Récupère les activités simulées + manuelles d'un employé."""
    client = get_bq_client()

    query = f"""
        SELECT
            date_debut,
            sport_type,
            distance_m,
            TIMESTAMP_DIFF(date_fin, date_debut, SECOND) AS duration_s,
            commentaire,
            'simulation' AS origine
        FROM `{PROJECT_ID}.{BQ_DATASET_BRONZE}.strava_activities`
        WHERE id_salarie = @employee_id

        UNION ALL

        SELECT
            date_debut,
            sport_type,
            distance_m,
            TIMESTAMP_DIFF(date_fin, date_debut, SECOND) AS duration_s,
            commentaire,
            'streamlit' AS origine
        FROM `{PROJECT_ID}.{BQ_DATASET_BRONZE}.manual_activities`
        WHERE id_salarie = @employee_id

        ORDER BY date_debut DESC
        LIMIT 20
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("employee_id", "INT64", employee_id)
        ]
    )

    return client.query(query, job_config=job_config).to_dataframe()


def insert_activity(activity: dict):
    """Insère une nouvelle activité dans BigQuery."""
    client = get_bq_client()
    table_id = f"{PROJECT_ID}.{BQ_DATASET_BRONZE}.manual_activities"

    errors = client.insert_rows_json(table_id, [activity])

    if errors:
        st.error(f"Erreur lors de l'insertion : {errors}")
        return False

    return True


# =============================================================
# SLACK
# =============================================================

def send_slack_notification(prenom, nom, sport_type, distance_m, duration_s, comment):
    """Envoie un message Slack pour la nouvelle activité."""
    from src.notifications.slack_notifier import build_message, send_slack_message

    msg = build_message(
        prenom=prenom,
        nom=nom,
        sport_type=sport_type,
        distance_m=distance_m,
        duration_s=duration_s,
        comment=comment,
    )
    return send_slack_message(msg)


# =============================================================
# INTERFACE STREAMLIT
# =============================================================

st.set_page_config(
    page_title="Sport Data Solution",
    page_icon="🏃",
    layout="wide",
)

st.title("🏃 Sport Data Solution")
st.subheader("Déclaration d'activités sportives")

if not PROJECT_ID:
    st.error("La variable d'environnement GCP_PROJECT_ID est absente.")
    st.stop()

# --- Sidebar : sélection employé ---
st.sidebar.header("👤 Employé")

employees = get_employees()

if employees.empty:
    st.error("Aucun employé trouvé dans BigQuery.")
    st.stop()

employee_options = {
    f"{row['prenom']} {row['nom']}": row["id_salarie"]
    for _, row in employees.iterrows()
}

selected_name = st.sidebar.selectbox(
    "Sélectionner un employé",
    list(employee_options.keys())
)
selected_id = employee_options[selected_name]

# --- Onglets ---
tab1, tab2 = st.tabs(["📝 Nouvelle activité", "📊 Mes activités"])

# =============================================================
# ONGLET 1 : Déclarer une nouvelle activité
# =============================================================
with tab1:
    st.markdown("### Déclarer une nouvelle activité")

    col1, col2 = st.columns(2)

    with col1:
        sport_types = [
            "Course à pied",
            "Randonnée",
            "Vélo",
            "Natation",
            "Tennis",
            "Football",
            "Rugby",
            "Badminton",
            "Boxe",
            "Escalade",
            "Triathlon",
            "Judo",
            "Voile",
            "Équitation",
            "Tennis de table",
            "Basketball",
            "Autre",
        ]

        sport_type = st.selectbox("Type de sport", sport_types)
        date_activite = st.date_input("Date de l'activité", value=datetime.now().date())
        heure_debut = st.time_input(
            "Heure de début",
            value=datetime.now().replace(second=0, microsecond=0).time()
        )

    with col2:
        sports_sans_distance = [
            "Tennis",
            "Football",
            "Rugby",
            "Badminton",
            "Boxe",
            "Escalade",
            "Judo",
            "Équitation",
            "Tennis de table",
            "Basketball",
        ]

        has_distance = sport_type not in sports_sans_distance

        if has_distance:
            distance_km = st.number_input(
                "Distance (km)",
                min_value=0.0,
                max_value=200.0,
                value=5.0,
                step=0.5,
            )
            distance_m = float(distance_km * 1000)
        else:
            distance_m = None
            st.info(f"Pas de distance requise pour {sport_type}")

        duree_minutes = st.number_input(
            "Durée (minutes)",
            min_value=1,
            max_value=600,
            value=60,
            step=5,
        )
        duration_s = int(duree_minutes * 60)

    comment = st.text_input(
        "Commentaire (optionnel)",
        placeholder="Super séance aujourd'hui !"
    )

    if st.button("✅ Déclarer l'activité", type="primary", use_container_width=True):
        date_debut = datetime.combine(date_activite, heure_debut)
        date_fin = date_debut + timedelta(seconds=duration_s)

        activity = {
            "activity_id": str(uuid.uuid4()),
            "id_salarie": int(selected_id),
            "date_debut": date_debut.isoformat(),
            "sport_type": sport_type,
            "distance_m": distance_m,
            "date_fin": date_fin.isoformat(),
            "commentaire": comment if comment else None,
            "source": "streamlit",
            "created_at": datetime.utcnow().isoformat(),
        }

        with st.spinner("Enregistrement en cours..."):
            success = insert_activity(activity)

        if success:
            st.success(f"✅ Activité enregistrée ! {sport_type} — {duree_minutes} min")

            prenom, nom = selected_name.split(" ", 1)

            with st.spinner("Envoi de la notification Slack..."):
                slack_ok = send_slack_notification(
                    prenom=prenom,
                    nom=nom,
                    sport_type=sport_type,
                    distance_m=distance_m,
                    duration_s=duration_s,
                    comment=comment if comment else None,
                )

            if slack_ok:
                st.success("📨 Message publié sur Slack !")
            else:
                st.warning("⚠️ Activité enregistrée, mais échec de l'envoi Slack.")

            st.balloons()
            st.cache_data.clear()

# =============================================================
# ONGLET 2 : Voir mes activités
# =============================================================
with tab2:
    st.markdown(f"### Activités de {selected_name}")

    activities = get_employee_activities(int(selected_id))

    if activities.empty:
        st.info("Aucune activité enregistrée pour cet employé.")
    else:
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total activités", len(activities))

        with col2:
            total_km = activities["distance_m"].dropna().sum() / 1000
            st.metric("Distance totale", f"{total_km:.1f} km")

        with col3:
            total_hours = activities["duration_s"].sum() / 3600
            st.metric("Temps total", f"{total_hours:.1f} h")

        display_df = activities.copy()
        display_df["date_debut"] = pd.to_datetime(display_df["date_debut"]).dt.strftime("%d/%m/%Y %H:%M")
        display_df["distance_km"] = (display_df["distance_m"] / 1000).round(1)
        display_df["duree"] = display_df["duration_s"].apply(
            lambda s: f"{int(s//3600)}h{int((s % 3600)//60):02d}" if s >= 3600 else f"{int(s//60)} min"
        )

        st.dataframe(
            display_df[
                ["date_debut", "sport_type", "distance_km", "duree", "commentaire", "origine"]
            ].rename(
                columns={
                    "date_debut": "Date",
                    "sport_type": "Sport",
                    "distance_km": "Distance (km)",
                    "duree": "Durée",
                    "commentaire": "Commentaire",
                    "origine": "Origine",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )