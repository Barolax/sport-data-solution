-- mart_wellness_days.sql
-- Gold : Calcul de l'éligibilité aux 5 journées bien-être (≥ 15 activités/an)

WITH employees AS (
    SELECT * FROM {{ ref('stg_employees') }}
),

activities AS (
    SELECT * FROM {{ ref('stg_activities') }}
),

activity_counts AS (
    SELECT
        employee_id,
        COUNT(*) AS total_activities,
        COUNT(DISTINCT sport_type) AS distinct_sports,
        MIN(started_at) AS first_activity,
        MAX(started_at) AS last_activity,
        SUM(distance_meters) AS total_distance_meters,
        SUM(duration_seconds) AS total_duration_seconds
    FROM activities
    GROUP BY employee_id
),

eligibility AS (
    SELECT
        e.employee_id,
        e.full_name,
        e.business_unit,
        e.contract_type,
        COALESCE(a.total_activities, 0) AS total_activities,
        COALESCE(a.distinct_sports, 0) AS distinct_sports,
        a.first_activity,
        a.last_activity,
        COALESCE(a.total_distance_meters, 0) AS total_distance_meters,
        COALESCE(a.total_duration_seconds, 0) AS total_duration_seconds,
        -- Éligibilité : minimum 15 activités dans l'année
        CASE
            WHEN COALESCE(a.total_activities, 0) >= 15
                THEN TRUE
            ELSE FALSE
        END AS is_eligible,
        -- Jours accordés
        CASE
            WHEN COALESCE(a.total_activities, 0) >= 15
                THEN 5
            ELSE 0
        END AS wellness_days_granted
    FROM employees e
    LEFT JOIN activity_counts a ON e.employee_id = a.employee_id
)

SELECT
    *,
    SUM(wellness_days_granted) OVER () AS total_wellness_days,
    SUM(wellness_days_granted) OVER (PARTITION BY business_unit) AS bu_wellness_days
FROM eligibility
