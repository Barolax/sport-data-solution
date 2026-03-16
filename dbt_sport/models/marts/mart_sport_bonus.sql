-- mart_sport_bonus.sql
-- Gold : Calcul de l'éligibilité et du montant de la prime sportive (5%)

WITH employees AS (
    SELECT * FROM {{ ref('stg_employees') }}
),

distances AS (
    SELECT * FROM {{ source('bronze', 'commute_distances') }}
),

eligibility AS (
    SELECT
        e.employee_id,
        e.full_name,
        e.business_unit,
        e.gross_salary,
        e.commute_mode,
        e.is_sport_commute,
        d.distance_km AS commute_distance_km,
        d.is_valid AS distance_valid,
        -- Éligibilité : trajet sportif ET distance valide
        CASE
            WHEN e.is_sport_commute
                AND d.is_valid = TRUE
                THEN TRUE
            ELSE FALSE
        END AS is_eligible,
        -- Montant de la prime (5% du salaire brut)
        CASE
            WHEN e.is_sport_commute
                AND d.is_valid = TRUE
                THEN ROUND(e.gross_salary * 0.05, 2)
            ELSE 0
        END AS bonus_amount
    FROM employees e
    LEFT JOIN distances d ON e.employee_id = d.id_salarie
)

SELECT
    *,
    -- Agrégation pour le dashboard
    SUM(bonus_amount) OVER () AS total_bonus_cost,
    SUM(bonus_amount) OVER (PARTITION BY business_unit) AS bu_bonus_cost
FROM eligibility
