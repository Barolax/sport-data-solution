-- stg_activities.sql
-- Bronze → Silver : Nettoyage des activités sportives simulées (Strava)

WITH source AS (
    SELECT * FROM {{ source('bronze', 'strava_activities') }}
),

cleaned AS (
    SELECT
        CAST(id AS INT64) AS activity_id,
        CAST(id_salarie AS INT64) AS employee_id,
        date_debut AS started_at,
        date_fin AS ended_at,
        -- Correction des typos sur les types de sport
        CASE
            WHEN LOWER(sport_type) IN ('runing', 'running', 'course à pied')
                THEN 'Course à pied'
            WHEN LOWER(sport_type) LIKE '%randonnée%'
                THEN 'Randonnée'
            WHEN LOWER(sport_type) LIKE '%natation%'
                THEN 'Natation'
            ELSE INITCAP(TRIM(sport_type))
        END AS sport_type,
        CAST(distance_m AS FLOAT64) AS distance_meters,
        ROUND(CAST(distance_m AS FLOAT64) / 1000, 2) AS distance_km,
        TIMESTAMP_DIFF(date_fin, date_debut, SECOND) AS duration_seconds,
        TRIM(commentaire) AS comment
    FROM source
    WHERE date_debut IS NOT NULL
)

SELECT * FROM cleaned
