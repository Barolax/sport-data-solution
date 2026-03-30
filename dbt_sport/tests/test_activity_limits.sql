-- test_activity_limits.sql
-- Vérifie que les activités respectent des seuils réalistes par sport
-- Si cette requête retourne des lignes, le test émet un WARNING

{{ config(severity='warn') }}

SELECT
    activity_id,
    employee_id,
    sport_type,
    distance_km,
    duration_seconds,
    ROUND(duration_seconds / 3600.0, 1) AS duration_hours,
    CASE
        WHEN sport_type = 'Course à pied' AND distance_km > 50
            THEN 'Distance course > 50 km'
        WHEN sport_type = 'Course à pied' AND duration_seconds > 28800
            THEN 'Durée course > 8h'
        WHEN sport_type = 'Randonnée' AND distance_km > 50
            THEN 'Distance rando > 50 km'
        WHEN sport_type = 'Randonnée' AND duration_seconds > 43200
            THEN 'Durée rando > 12h'
        WHEN sport_type = 'Natation' AND distance_km > 10
            THEN 'Distance natation > 10 km'
        WHEN sport_type = 'Natation' AND duration_seconds > 14400
            THEN 'Durée natation > 4h'
        WHEN sport_type IN ('Vélo', 'Triathlon') AND distance_km > 200
            THEN 'Distance vélo/triathlon > 200 km'
        WHEN sport_type = 'Voile' AND distance_km > 100
            THEN 'Distance voile > 100 km'
        WHEN duration_seconds > 43200
            THEN 'Durée > 12h (tout sport)'
        WHEN duration_seconds < 60
            THEN 'Durée < 1 min'
        WHEN distance_km IS NOT NULL AND distance_km < 0
            THEN 'Distance négative'
    END AS anomaly_reason
FROM {{ ref('stg_activities') }}
WHERE
    -- Course à pied
    (sport_type = 'Course à pied' AND (distance_km > 50 OR duration_seconds > 28800))
    -- Randonnée
    OR (sport_type = 'Randonnée' AND (distance_km > 50 OR duration_seconds > 43200))
    -- Natation
    OR (sport_type = 'Natation' AND (distance_km > 10 OR duration_seconds > 14400))
    -- Vélo / Triathlon
    OR (sport_type IN ('Vélo', 'Triathlon') AND distance_km > 200)
    -- Voile
    OR (sport_type = 'Voile' AND distance_km > 100)
    -- Règles générales
    OR duration_seconds > 43200  -- Plus de 12h
    OR duration_seconds < 60     -- Moins d'1 min
    OR (distance_km IS NOT NULL AND distance_km < 0)  -- Pas de distance négative