-- models/marts/dimensions/dim_cards.sql
{{ config(
    materialized='table',
    unique_key='card_key'
) }}

SELECT
    card_pk as card_key,
    card_number,
    account_id,
    customer_id,
    card_type,
    card_network,
    card_status,
    issue_date,
    expiry_date,
    is_expired,
    expires_soon,
    credit_limit,
    available_credit,
    credit_utilization_percent,
    CASE 
        WHEN credit_utilization_percent >= 80 THEN 'High Utilization'
        WHEN credit_utilization_percent >= 50 THEN 'Medium Utilization'
        WHEN credit_utilization_percent >= 20 THEN 'Low Utilization'
        ELSE 'Minimal Utilization'
    END as utilization_tier,
    CASE 
        WHEN is_expired THEN 'Expired'
        WHEN expires_soon THEN 'Expiring Soon'
        WHEN card_status = 'Active' THEN 'Active'
        ELSE card_status
    END as card_health_status,
    loaded_at as dw_created_at,
    CURRENT_TIMESTAMP as dw_updated_at
FROM {{ ref('stg_cards') }}
