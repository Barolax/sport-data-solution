-- stg_sports_declared.sql
-- Bronze → Silver : Nettoyage des déclarations sportives

WITH source AS (
    SELECT * FROM {{ source('bronze', 'sports_declared') }}
),

cleaned AS (
    SELECT
        CAST(id_salarie AS INT64) AS employee_id,
        -- Correction des typos sur les noms de sport
        CASE
            WHEN LOWER(TRIM(sport_pratique)) IN ('runing', 'running')
                THEN 'Course à pied'
            WHEN LOWER(TRIM(sport_pratique)) = 'natation'
                THEN 'Natation'
            ELSE TRIM(sport_pratique)
        END AS declared_sport,
        CASE
            WHEN sport_pratique IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_sportif
    FROM source
)

SELECT * FROM cleaned