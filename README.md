# 🏃 Sport Data Solution — POC Avantages Sportifs

## 📋 Contexte

Sport Data Solution est une start-up spécialisée dans le monitoring et l'analyse de performance sportive. Ce POC vise à mettre en place un système d'avantages pour les salariés pratiquant une activité physique régulière :

- **Prime sportive** : +5% du salaire brut annuel pour les salariés se rendant au bureau via un mode de transport sportif (vélo, marche, running, trottinette...)
- **5 journées bien-être** : pour les salariés ayant au minimum 15 activités physiques déclarées dans l'année

## 🏗️ Architecture

Pipeline ETL de bout en bout suivant une **Medallion Architecture** (Raw → Bronze → Silver → Gold) :

![Architecture](docs/architecture.png)

| Couche | Outil | Rôle |
|--------|-------|------|
| Orchestration | **Airflow** | Scheduling et monitoring du pipeline |
| Stockage raw | **GCS (Cloud Storage)** | Fichiers bruts versionnés |
| Entrepôt | **BigQuery** | Tables bronze / silver / gold |
| Transformations | **dbt** | Modèles SQL, tests, lineage |
| Génération données | **PySpark** | Simulation activités Strava (12 mois) |
| Validation distances | **Google Maps API** | Distance domicile → bureau par mode de transport |
| Data Quality | **Great Expectations** | Tests de cohérence sur les données |
| Notifications | **Slack Webhook** | Message automatique par activité sportive |
| Visualisation | **Looker Studio** | Dashboards KPI connectés à BigQuery |
| Infra | **Docker Compose** | Conteneurisation de tous les services |

## 📁 Structure du projet

```
sport-data-solution/
├── dags/                       # DAGs Airflow
│   └── sport_pipeline_dag.py
├── src/
│   ├── ingestion/              # Chargement raw → GCS → BigQuery
│   ├── generation/             # PySpark — simulation données Strava
│   ├── validation/             # Google Maps API — vérification distances
│   └── notifications/          # Slack Webhook
├── dbt_sport/                  # Projet dbt (BigQuery)
│   ├── models/
│   │   ├── staging/            # Bronze → Silver
│   │   └── marts/              # Silver → Gold (KPIs, primes, éligibilités)
│   ├── tests/
│   └── dbt_project.yml
├── great_expectations/         # Tests de qualité des données
├── data/raw/                   # Fichiers Excel source
├── docs/                       # Documentation et schémas
├── looker/                     # Config Looker Studio
├── docker-compose.yml
├── .env.example
└── .gitignore
```

## 🚀 Installation et lancement

### Prérequis

- Docker & Docker Compose
- Compte GCP avec BigQuery et Cloud Storage activés
- Clé API Google Maps
- Webhook Slack configuré

### Configuration

```bash
# Cloner le repo
git clone https://github.com/<username>/sport-data-solution.git
cd sport-data-solution

# Copier et remplir les variables d'environnement
cp .env.example .env

# Lancer les services
docker-compose up -d
```

### Variables d'environnement

Voir `.env.example` pour la liste complète des variables à configurer.

## 📊 KPIs suivis

- Coût total des primes sportives par BU
- Nombre de jours bien-être accordés
- Répartition des activités par type de sport
- Taux d'éligibilité par BU
- Évolution mensuelle des activités sur 12 mois
- Anomalies de déclaration (distances incohérentes)

## 🔮 Évolutions envisagées

- Connexion directe à l'API Strava (remplacement de la simulation)
- Challenges sportifs inter-équipes (par BU)
- Application mobile pour les déclarations
- Intégration d'un système de badges et gamification

## 👤 Auteur

Projet réalisé dans le cadre de la certification **Expert en ingénierie et science des données** (RNCP niveau 7) — OpenClassrooms.
