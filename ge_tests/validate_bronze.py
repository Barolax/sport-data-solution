"""
Great Expectations — Tests de qualité sur les données bronze (BigQuery).

Vérifie la cohérence des données entrantes :
- Données RH (employees)
- Données sportives (sports_declared)
- Activités simulées (strava_activities)
- Distances domicile-bureau (commute_distances)

Usage :
    python -m great_expectations.validate_bronze
"""

import os
import great_expectations as gx
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectTableRowCountToBeBetween,
    ExpectColumnValuesToBeOfType,
)
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE", "sport_bronze")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")


def run():
    print("=" * 60)
    print("🧪 Great Expectations — Validation des données bronze")
    print("=" * 60)

    # --- Initialisation du context ---
    context = gx.get_context()

    # --- Connexion BigQuery ---
    connection_string = f"bigquery://{PROJECT_ID}/{BQ_DATASET_BRONZE}"

    datasource = context.data_sources.add_or_update_sql(
        name="bigquery_bronze",
        connection_string=connection_string,
        create_temp_table=False,
    )

    # =============================================================
    # 1. EMPLOYEES
    # =============================================================
    print("\n📋 Validation : employees")

    employees_asset = datasource.add_table_asset(
        name="employees",
        table_name="employees",
    )
    employees_batch = employees_asset.add_batch_definition_whole_table(
        "employees_batch"
    ).get_batch()

    employees_suite = context.suites.add_or_update(
        gx.ExpectationSuite(name="employees_suite")
    )

    # Tests employees
    employees_suite.add_expectation(
        ExpectTableRowCountToBeBetween(min_value=100, max_value=500)
    )
    employees_suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="id_salarie")
    )
    employees_suite.add_expectation(
        ExpectColumnValuesToBeUnique(column="id_salarie")
    )
    employees_suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="nom")
    )
    employees_suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="prenom")
    )
    employees_suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="salaire_brut")
    )
    employees_suite.add_expectation(
        ExpectColumnValuesToBeBetween(column="salaire_brut", min_value=15000, max_value=200000)
    )
    employees_suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="date_naissance")
    )
    employees_suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="date_embauche")
    )
    employees_suite.add_expectation(
        ExpectColumnValuesToBeInSet(
            column="type_contrat", value_set=["CDI", "CDD"]
        )
    )
    employees_suite.add_expectation(
        ExpectColumnValuesToBeInSet(
            column="moyen_deplacement",
            value_set=[
                "Marche/running",
                "Vélo/Trottinette/Autres",
                "Transports en commun",
                "véhicule thermique/électrique",
            ],
        )
    )
    employees_suite.add_expectation(
        ExpectColumnValuesToBeBetween(column="nb_jours_cp", min_value=0, max_value=60)
    )

    employees_validation = employees_batch.validate(employees_suite)
    _print_results("employees", employees_validation)

    # =============================================================
    # 2. SPORTS DECLARED
    # =============================================================
    print("\n🏃 Validation : sports_declared")

    sports_asset = datasource.add_table_asset(
        name="sports_declared",
        table_name="sports_declared",
    )
    sports_batch = sports_asset.add_batch_definition_whole_table(
        "sports_batch"
    ).get_batch()

    sports_suite = context.suites.add_or_update(
        gx.ExpectationSuite(name="sports_suite")
    )

    sports_suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="id_salarie")
    )
    sports_suite.add_expectation(
        ExpectTableRowCountToBeBetween(min_value=100, max_value=500)
    )

    sports_validation = sports_batch.validate(sports_suite)
    _print_results("sports_declared", sports_validation)

    # =============================================================
    # 3. STRAVA ACTIVITIES
    # =============================================================
    print("\n⚡ Validation : strava_activities")

    activities_asset = datasource.add_table_asset(
        name="strava_activities",
        table_name="strava_activities",
    )
    activities_batch = activities_asset.add_batch_definition_whole_table(
        "activities_batch"
    ).get_batch()

    activities_suite = context.suites.add_or_update(
        gx.ExpectationSuite(name="activities_suite")
    )

    activities_suite.add_expectation(
        ExpectTableRowCountToBeBetween(min_value=3000, max_value=10000)
    )
    activities_suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="id")
    )
    activities_suite.add_expectation(
        ExpectColumnValuesToBeUnique(column="id")
    )
    activities_suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="id_salarie")
    )
    activities_suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="date_debut")
    )
    activities_suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="date_fin")
    )
    activities_suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="sport_type")
    )
    # Distance peut être null (escalade, sports sans distance)
    # Mais si elle existe, elle doit être positive
    activities_suite.add_expectation(
        ExpectColumnValuesToBeBetween(
            column="distance_m", min_value=0, max_value=100000,
            mostly=0.99,  # Tolère 1% de nulls
        )
    )

    activities_validation = activities_batch.validate(activities_suite)
    _print_results("strava_activities", activities_validation)

    # =============================================================
    # 4. COMMUTE DISTANCES
    # =============================================================
    print("\n📍 Validation : commute_distances")

    commute_asset = datasource.add_table_asset(
        name="commute_distances",
        table_name="commute_distances",
    )
    commute_batch = commute_asset.add_batch_definition_whole_table(
        "commute_batch"
    ).get_batch()

    commute_suite = context.suites.add_or_update(
        gx.ExpectationSuite(name="commute_suite")
    )

    commute_suite.add_expectation(
        ExpectTableRowCountToBeBetween(min_value=50, max_value=200)
    )
    commute_suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="id_salarie")
    )
    commute_suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="distance_km")
    )
    commute_suite.add_expectation(
        ExpectColumnValuesToBeBetween(column="distance_km", min_value=0, max_value=100)
    )
    commute_suite.add_expectation(
        ExpectColumnValuesToBeInSet(
            column="moyen_deplacement",
            value_set=["Marche/running", "Vélo/Trottinette/Autres"],
        )
    )

    commute_validation = commute_batch.validate(commute_suite)
    _print_results("commute_distances", commute_validation)

    # =============================================================
    # RÉSUMÉ
    # =============================================================
    all_results = [
        ("employees", employees_validation),
        ("sports_declared", sports_validation),
        ("strava_activities", activities_validation),
        ("commute_distances", commute_validation),
    ]

    print(f"\n{'=' * 60}")
    print("📊 Résumé Great Expectations")
    print(f"{'=' * 60}")

    total_tests = 0
    total_passed = 0

    for table_name, validation in all_results:
        passed = sum(1 for r in validation.results if r.success)
        total = len(validation.results)
        total_tests += total
        total_passed += passed
        status = "✅" if validation.success else "❌"
        print(f"   {status} {table_name}: {passed}/{total} tests passés")

    print(f"\n   Total : {total_passed}/{total_tests} tests passés")

    if total_passed == total_tests:
        print("\n🎉 Toutes les validations sont passées !")
    else:
        print(f"\n⚠️  {total_tests - total_passed} test(s) en échec")


def _print_results(table_name: str, validation_result) -> None:
    """Affiche les résultats de validation pour une table."""
    for result in validation_result.results:
        status = "✅" if result.success else "❌"
        expectation_type = result.expectation_config.type
        # Simplifier le nom
        short_name = expectation_type.replace("expect_", "").replace("_", " ")
        print(f"   {status} {short_name}")


if __name__ == "__main__":
    run()