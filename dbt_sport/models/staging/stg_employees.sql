-- stg_employees.sql
-- Bronze → Silver : Nettoyage et standardisation des données RH

WITH source AS (
    SELECT * FROM {{ source('bronze', 'employees') }}
),

cleaned AS (
    SELECT
        CAST(id_salarie AS INT64) AS employee_id,
        TRIM(nom) AS last_name,
        -- Correction encodage UTF-8 (ex: TimothÃ©e → Timothée)
        TRIM(REPLACE(REPLACE(REPLACE(prenom, 'Ã©', 'é'), 'Ã¨', 'è'), 'Ã ', 'à')) AS first_name,
        CONCAT(
            TRIM(REPLACE(REPLACE(REPLACE(prenom, 'Ã©', 'é'), 'Ã¨', 'è'), 'Ã ', 'à')),
            ' ',
            TRIM(nom)
        ) AS full_name,
        date_naissance AS birth_date,
        TRIM(bu) AS business_unit,
        date_embauche AS hire_date,
        CAST(salaire_brut AS FLOAT64) AS gross_salary,
        TRIM(type_contrat) AS contract_type,
        CAST(nb_jours_cp AS INT64) AS leave_days,
        TRIM(adresse_domicile) AS home_address,
        -- Standardisation du mode de déplacement
        CASE
            WHEN LOWER(moyen_deplacement) LIKE '%marche%'
                OR LOWER(moyen_deplacement) LIKE '%running%'
                THEN 'Marche/running'
            WHEN LOWER(moyen_deplacement) LIKE '%vélo%'
                OR LOWER(moyen_deplacement) LIKE '%trottinette%'
                THEN 'Vélo/Trottinette/Autres'
            WHEN LOWER(moyen_deplacement) LIKE '%transport%'
                THEN 'Transports en commun'
            WHEN LOWER(moyen_deplacement) LIKE '%véhicule%'
                OR LOWER(moyen_deplacement) LIKE '%thermique%'
                THEN 'Véhicule thermique/électrique'
            ELSE moyen_deplacement
        END AS commute_mode,
        -- Flag : trajet sportif ou non
        CASE
            WHEN LOWER(moyen_deplacement) LIKE '%marche%'
                OR LOWER(moyen_deplacement) LIKE '%running%'
                OR LOWER(moyen_deplacement) LIKE '%vélo%'
                OR LOWER(moyen_deplacement) LIKE '%trottinette%'
                THEN TRUE
            ELSE FALSE
        END AS is_sport_commute
    FROM source
)

SELECT * FROM cleaned