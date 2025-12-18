
-- models/marts/dimensions/dim_branches.sql
{{ config(materialized='table') }}

WITH branch_metrics AS (
    SELECT
        branch_id,
        COUNT(DISTINCT customer_id) as total_customers,
        COUNT(*) as total_accounts,
        SUM(balance) as total_deposits
    FROM {{ ref('stg_accounts') }}
    GROUP BY branch_id
)

SELECT
    b.branch_pk as branch_key,
    b.branch_code,
    b.branch_name,
    b.city,
    b.state,
    b.branch_type,
    b.latitude,
    b.longitude,
    b.geolocation,
    COALESCE(bm.total_customers, 0) as total_customers,
    COALESCE(bm.total_accounts, 0) as total_accounts,
    COALESCE(bm.total_deposits, 0) as total_deposits,
    CASE 
        WHEN bm.total_customers >= 200 THEN 'High Traffic'
        WHEN bm.total_customers >= 100 THEN 'Medium Traffic'
        ELSE 'Low Traffic'
    END as branch_traffic_tier,
    b.loaded_at as dw_created_at,
    CURRENT_TIMESTAMP as dw_updated_at
FROM {{ ref('stg_branches') }} b
LEFT JOIN branch_metrics bm ON b.branch_pk = bm.branch_id
